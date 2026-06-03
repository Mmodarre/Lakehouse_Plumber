"""Core-level byte-for-byte parity proof for ``build_generation_plan``.

``lhp.core.codegen.plan_builder.build_generation_plan`` is the
parity-guaranteed primitive behind the public generation *plan* surface: it
drives the UNMODIFIED real generate flow against a throwaway temp dir, reads
the formatted tree back, and returns an in-memory
:class:`~lhp.core.codegen.plan_builder.GenerationPlanResult`. This test pins the
load-bearing invariant: **the in-memory plan equals a real generate's on-disk
output, byte for byte, across every artifact kind.**

Strategy: build one project that exercises ALL five artifact kinds, run a REAL
generate to a temp ``generated/<env>`` via the public facade, then run
``build_generation_plan`` over the same orchestrator and inputs. The two
``{relative_path: content}`` maps must be IDENTICAL.

The fixture produces every ``PlannedArtifact`` kind:

* ``flowgroup`` — three ordinary load+write flowgroups.
* ``test_hook`` — ``test_reporting`` config + a flowgroup carrying a ``test``
  action with a ``test_id`` (with ``include_tests=True``) → the per-pipeline
  ``_test_reporting_hook.py`` + copied ``test_reporting_providers/`` modules.
* ``monitoring`` — ``monitoring`` config → the synthetic
  ``<monitoring_pipeline>/monitoring.py`` flowgroup file (carrying the
  ``FLOWGROUP_ID = "monitoring"`` marker).
* ``helper`` — ``enable_job_monitoring: true`` makes the monitoring flowgroup
  import a local helper, copied under ``custom_python_functions/``.
* ``aux`` — the same monitoring path also emits the inline
  ``jobs_stats_loader.py`` auxiliary module at the pipeline-dir top level.

The monitoring pipeline drives the real ``ProcessPoolExecutor`` (synthetic
flowgroup + the others), so this is an integration test.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import LakehousePlumberApplicationFacade, collect_response
from lhp.core.codegen.plan_builder import build_generation_plan


def _write_all_kinds_project(project_root: Path) -> None:
    """Write a project that generates every artifact kind on ``env=dev``."""
    for sub in ("presets", "templates", "substitutions", "config", "py_functions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)

    # lhp.yaml: event_log (required by monitoring) + monitoring (with
    # enable_job_monitoring → helper + aux) + test_reporting (→ test_hook).
    (project_root / "lhp.yaml").write_text(
        "name: plan_parity_proj\n"
        "version: '1.0'\n"
        "event_log:\n"
        '  catalog: "${catalog}"\n'
        '  schema: "_meta"\n'
        '  name_suffix: "_event_log"\n'
        "monitoring:\n"
        "  enabled: true\n"
        "  enable_job_monitoring: true\n"
        '  checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"\n'
        '  job_config_path: "config/monitoring_job_config.yaml"\n'
        "test_reporting:\n"
        "  module_path: py_functions/publisher.py\n"
        "  function_name: publish_results\n"
    )
    (project_root / "config" / "monitoring_job_config.yaml").write_text(
        "max_concurrent_runs: 1\nperformance_target: STANDARD\n"
    )
    (project_root / "py_functions" / "publisher.py").write_text(
        "def publish_results(results, config, context, spark):\n"
        "    return {'published': len(results), 'failed': 0, 'errors': []}\n"
    )
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(
            {
                "dev": {
                    "catalog": "dev_cat",
                    "bronze_schema": "bronze",
                    "landing_path": "/mnt/landing",
                }
            },
            f,
        )

    def _load_write(fg: str) -> dict:
        return {
            "pipeline": None,  # filled by caller
            "flowgroup": fg,
            "actions": [
                {
                    "name": f"load_{fg}",
                    "type": "load",
                    "target": f"v_{fg}",
                    "source": {
                        "type": "cloudfiles",
                        "path": "${landing_path}/x",
                        "format": "json",
                    },
                },
                {
                    "name": f"write_{fg}",
                    "type": "write",
                    "source": f"v_{fg}",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "${catalog}",
                        "schema": "${bronze_schema}",
                        "table": f"t_{fg}",
                        "create_table": True,
                    },
                },
            ],
        }

    for pipeline, flowgroups in {"p_a": ["fg1", "fg2"], "p_b": ["fg3"]}.items():
        pdir = project_root / "pipelines" / pipeline
        pdir.mkdir(parents=True, exist_ok=True)
        for fg in flowgroups:
            cfg = _load_write(fg)
            cfg["pipeline"] = pipeline
            with open(pdir / f"{fg}.yaml", "w") as f:
                yaml.dump(cfg, f)

    # A valid (load+write) flowgroup that ALSO carries a test action with a
    # test_id → drives the test-reporting hook for pipeline p_a.
    tst = {
        "pipeline": "p_a",
        "flowgroup": "tst_dq",
        "actions": [
            {
                "name": "load_for_test",
                "type": "load",
                "target": "v_test_src",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/t",
                    "format": "json",
                },
            },
            {
                "name": "write_test_src",
                "type": "write",
                "source": "v_test_src",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": "t_test_src",
                    "create_table": True,
                },
            },
            {
                "name": "tst_uniq",
                "type": "test",
                "test_type": "uniqueness",
                "source": "v_test_src",
                "target": "tst_uniq",
                "columns": ["id"],
                "on_violation": "warn",
                "test_id": "SIT-01",
            },
        ],
    }
    with open(project_root / "pipelines" / "p_a" / "tst_dq.yaml", "w") as f:
        yaml.dump(tst, f)


@pytest.fixture
def all_kinds_project(tmp_path: Path):
    """Facade + discovered pipeline fields over the all-kinds project."""
    project_root = tmp_path / "proj"
    project_root.mkdir()
    _write_all_kinds_project(project_root)
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
        # Discover the pipeline list exactly as the CLI/facade would — this
        # includes the synthetic monitoring pipeline injected by discovery.
        pipeline_fields = sorted(
            {
                fg.pipeline
                for fg in facade._orchestrator.bootstrap.discover_all_flowgroups()
            }
        )
        yield facade, project_root, pipeline_fields
    finally:
        os.chdir(original_cwd)


def _disk_tree(output_dir: Path) -> dict[str, str]:
    """Read a generated tree into ``{relative_path: utf-8 content}``."""
    return {
        str(p.relative_to(output_dir)): p.read_text(encoding="utf-8")
        for p in sorted(output_dir.rglob("*"))
        if p.is_file()
    }


@pytest.mark.integration
class TestBuildGenerationPlanParity:
    """The in-memory plan == a real generate's disk output, byte for byte."""

    def test_plan_matches_real_generate_byte_for_byte(self, all_kinds_project):
        facade, project_root, pipeline_fields = all_kinds_project
        output_dir = project_root / "generated" / "dev"

        collect_response(
            facade.generation.generate_pipelines(
                pipeline_fields=pipeline_fields,
                env="dev",
                output_dir=output_dir,
                include_tests=True,
            )
        )
        real = _disk_tree(output_dir)

        plan = build_generation_plan(
            facade._orchestrator,
            env="dev",
            pipeline_fields=pipeline_fields,
            include_tests=True,
        )
        plan_map = {str(a.path): a.content for a in plan.artifacts}

        assert set(plan_map) == set(real), (
            f"file set diverged — only in plan: {sorted(set(plan_map) - set(real))}; "
            f"only on disk: {sorted(set(real) - set(plan_map))}"
        )
        mismatches = [p for p in real if plan_map[p] != real[p]]
        assert not mismatches, f"content diverged for: {mismatches}"
        assert plan_map == real

    def test_plan_covers_all_artifact_kinds(self, all_kinds_project):
        """The fixture exercises every ``PlannedArtifact`` kind, and each lands
        on the expected file (classification is correct, not just present).
        """
        facade, _project_root, pipeline_fields = all_kinds_project

        plan = build_generation_plan(
            facade._orchestrator,
            env="dev",
            pipeline_fields=pipeline_fields,
            include_tests=True,
        )
        kind_by_path = {str(a.path): a.kind for a in plan.artifacts}

        assert {a.kind for a in plan.artifacts} == {
            "flowgroup",
            "aux",
            "helper",
            "test_hook",
            "monitoring",
        }
        monitoring_pl = next(
            p for p in pipeline_fields if p.endswith("_event_log_monitoring")
        )
        assert kind_by_path["p_a/fg1.py"] == "flowgroup"
        assert kind_by_path["p_a/_test_reporting_hook.py"] == "test_hook"
        assert kind_by_path["p_a/test_reporting_providers/publisher.py"] == "test_hook"
        assert kind_by_path[f"{monitoring_pl}/monitoring.py"] == "monitoring"
        assert (
            kind_by_path[
                f"{monitoring_pl}/custom_python_functions/jobs_stats_loader.py"
            ]
            == "helper"
        )
        assert kind_by_path[f"{monitoring_pl}/jobs_stats_loader.py"] == "aux"

    def test_plan_streams_one_delta_per_pipeline_in_order(self, all_kinds_project):
        """``on_pipeline_complete`` fires once per pipeline, in input order, as the underlying delta-generator yields."""
        facade, _project_root, pipeline_fields = all_kinds_project

        seen: list[tuple[str, bool]] = []
        plan = build_generation_plan(
            facade._orchestrator,
            env="dev",
            pipeline_fields=pipeline_fields,
            include_tests=True,
            on_pipeline_complete=lambda d: seen.append((d.pipeline_name, d.success)),
        )
        assert [name for name, _ in seen] == list(pipeline_fields)
        assert all(success for _, success in seen)
        assert plan.pipeline_count == len(pipeline_fields)

    def test_plan_paths_are_relative_to_output_root(self, all_kinds_project):
        """Every artifact path is RELATIVE (matches the ``generated/<env>``
        layout) — never absolute, never temp-dir-rooted.
        """
        facade, _project_root, pipeline_fields = all_kinds_project

        plan = build_generation_plan(
            facade._orchestrator,
            env="dev",
            pipeline_fields=pipeline_fields,
            include_tests=True,
        )
        assert plan.artifacts, "expected a non-empty plan"
        for artifact in plan.artifacts:
            assert not artifact.path.is_absolute(), artifact.path
            # First path component is the pipeline output sub-dir.
            assert artifact.path.parts[0] == artifact.pipeline
