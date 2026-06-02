"""Facade-level byte-for-byte parity proof for ``GenerationFacade.plan_generation``.

``plan_generation`` (A2) is the *plan-only* analogue of ``generate_pipelines``:
it drives the UNMODIFIED real generate flow against a throwaway temp dir (via the
parity-guaranteed ``build_generation_plan`` primitive), reads the formatted tree
back into a :class:`~lhp.api.GenerationPlan`, and writes NOTHING to the real
``generated/<env>``. This module pins the load-bearing invariant at the public
surface: **what ``plan_generation`` REPORTS it would write equals what a real
``generate_pipelines`` ACTUALLY writes to disk, byte for byte, across every
artifact kind.** The core-level analogue lives in
``tests/core/codegen/test_plan_builder_parity.py`` (A1); this is the
``lhp.api``-only mirror through the streaming facade.

``plan_generation``'s only narrowing knob is ``pipeline_filter`` (a single
pipeline field); with ``pipeline_filter=None`` it plans NOTHING (see A2's
``test_unfiltered_call_yields_empty_plan_with_full_frame``). So the faithful
multi-pipeline parity strategy is: run ONE batch ``generate_pipelines`` over all
discovered pipeline fields (the real, on-disk, post-format output), then run
``plan_generation`` once PER pipeline field and UNION the resulting
``PlannedFileView``s. Each pipeline's output tree is independent (the worklist
builds a per-pipeline output sub-dir), so the per-pipeline plan union must equal
the batch generate's disk read-back exactly.

The fixture exercises every ``PlannedFileView`` kind (mirroring A1's parity
fixture):

* ``flowgroup`` — ordinary load+write flowgroups (``p_a/fg1``, ``p_a/fg2``,
  ``p_b/fg3``).
* ``test_hook`` — a ``test_reporting`` config + a flowgroup carrying a ``test``
  action with a ``test_id`` (with ``include_tests=True``) → the per-pipeline
  ``_test_reporting_hook.py`` + copied ``test_reporting_providers/`` modules.
* ``monitoring`` — a ``monitoring`` config → the synthetic
  ``<monitoring_pipeline>/monitoring.py`` flowgroup file.
* ``helper`` — ``enable_job_monitoring: true`` makes the monitoring flowgroup
  import a local helper, copied under ``custom_python_functions/``.
* ``aux`` — the same monitoring path emits the inline ``jobs_stats_loader.py``
  auxiliary module at the pipeline-dir top level (plus the ``__init__.py``
  auxiliaries the copies bring along).

The synthetic monitoring pipeline drives the real ``ProcessPoolExecutor``, so
this is an integration test. Tests import strictly from :mod:`lhp.api` — no
internal modules.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest
import yaml

from lhp.api import (
    GenerationPlan,
    LakehousePlumberApplicationFacade,
    collect_response,
)


def _write_all_kinds_project(project_root: Path) -> None:
    """Write a project that generates every artifact kind on ``env=dev``.

    Mirrors the A1 parity fixture (``tests/core/codegen/test_plan_builder_parity
    .py``): ``event_log`` + ``monitoring`` (with ``enable_job_monitoring`` →
    helper + aux) + ``test_reporting`` (→ test_hook), three ordinary flowgroups,
    and one flowgroup carrying a ``test`` action.
    """
    for sub in ("presets", "templates", "substitutions", "config", "py_functions"):
        (project_root / sub).mkdir(parents=True, exist_ok=True)

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
    """Facade + discovered pipeline fields over the all-kinds project.

    Discovers the pipeline list exactly as the CLI/facade would — INCLUDING the
    synthetic monitoring pipeline injected by discovery — so the parity run
    covers every pipeline a real ``generate`` would.
    """
    project_root = tmp_path / "proj"
    project_root.mkdir()
    _write_all_kinds_project(project_root)
    original_cwd = os.getcwd()
    os.chdir(project_root)
    try:
        facade = LakehousePlumberApplicationFacade.for_project(
            project_root, enforce_version=False
        )
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


def _plan_union(facade, env: str, pipeline_fields, *, include_tests: bool):
    """Union the per-pipeline ``plan_generation`` outputs into ``{path: content}``.

    ``plan_generation`` plans ONE pipeline at a time (its only knob is
    ``pipeline_filter``); the union over every discovered field is the
    plan-side analogue of a single batch ``generate_pipelines``. Returns the
    ``{relative_path: content}`` map and the set of every ``kind`` seen.
    """
    plan_map: dict[str, str] = {}
    kinds: set[str] = set()
    for field in pipeline_fields:
        plan = collect_response(
            facade.generation.plan_generation(
                env, pipeline_filter=field, include_tests=include_tests
            )
        )
        assert isinstance(plan, GenerationPlan)
        for view in plan.files:
            # A PlannedFileView path is RELATIVE to the output root (== the
            # generated/<env> layout), so it lines up 1:1 with the disk
            # read-back's ``relative_to(output_dir)`` keys.
            assert not view.path.is_absolute(), view.path
            assert view.pipeline == field
            plan_map[str(view.path)] = view.content
            kinds.add(view.kind)
    return plan_map, kinds


# Every kind the all-kinds fixture is expected to produce across all pipelines.
_ALL_KINDS = {"flowgroup", "aux", "helper", "test_hook", "monitoring"}


@pytest.mark.integration
class TestPlanGenerationParity:
    """``plan_generation``'s plan == a real ``generate_pipelines`` disk output."""

    def test_plan_matches_real_generate_byte_for_byte(self, all_kinds_project):
        """Run a REAL batch ``generate_pipelines`` to ``generated/dev``, read it
        back off disk (post-format), then union a per-pipeline ``plan_generation``
        over the SAME project; the two ``{path: content}`` maps must be IDENTICAL
        byte for byte — across flowgroup, aux, helper, test_hook, and monitoring.
        """
        facade, project_root, pipeline_fields = all_kinds_project
        output_dir = project_root / "generated" / "dev"

        # The real generate, through the public facade (writes + formats on disk).
        collect_response(
            facade.generation.generate_pipelines(
                pipeline_fields=pipeline_fields,
                env="dev",
                output_dir=output_dir,
                include_tests=True,
            )
        )
        real = _disk_tree(output_dir)
        assert real, "expected a non-empty real generate"

        # The plan-side map: union of per-pipeline plan_generation runs.
        plan_map, plan_kinds = _plan_union(
            facade, "dev", pipeline_fields, include_tests=True
        )

        # Same set of files, and identical content for every one of them.
        assert set(plan_map) == set(real), (
            "file set diverged — "
            f"only in plan: {sorted(set(plan_map) - set(real))}; "
            f"only on disk: {sorted(set(real) - set(plan_map))}"
        )
        mismatches = [p for p in real if plan_map[p] != real[p]]
        assert not mismatches, f"content diverged for: {mismatches}"
        # The whole maps are equal (ordering-independent byte equality).
        assert plan_map == real

        # All five kinds were genuinely exercised by the parity comparison.
        assert plan_kinds == _ALL_KINDS

    def test_plan_covers_all_kinds_on_expected_paths(self, all_kinds_project):
        """Beyond byte-equality, each kind lands on the file a real generate puts
        it on — classification is correct, not merely present. The expected paths
        are the same ones the real generate produces (asserted to exist on disk).
        """
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

        # Build the plan-side kind-by-path map across all pipelines.
        kind_by_path: dict[str, str] = {}
        for field in pipeline_fields:
            plan = collect_response(
                facade.generation.plan_generation(
                    "dev", pipeline_filter=field, include_tests=True
                )
            )
            for view in plan.files:
                kind_by_path[str(view.path)] = view.kind

        monitoring_pl = next(
            p for p in pipeline_fields if p.endswith("_event_log_monitoring")
        )
        expectations = {
            "p_a/fg1.py": "flowgroup",
            "p_a/_test_reporting_hook.py": "test_hook",
            "p_a/test_reporting_providers/publisher.py": "test_hook",
            f"{monitoring_pl}/monitoring.py": "monitoring",
            f"{monitoring_pl}/custom_python_functions/jobs_stats_loader.py": "helper",
            f"{monitoring_pl}/jobs_stats_loader.py": "aux",
        }
        for path, expected_kind in expectations.items():
            # The same path a real generate produced (sanity-anchors the fixture).
            assert path in real, f"fixture no longer produces {path}"
            assert kind_by_path[path] == expected_kind, (
                f"{path}: plan classified as {kind_by_path.get(path)!r}, "
                f"expected {expected_kind!r}"
            )
        # Together the spot-checks cover all five kinds.
        assert set(expectations.values()) == _ALL_KINDS

    def test_plan_generation_leaves_real_output_untouched(self, all_kinds_project):
        """A plan run writes NOTHING to the real ``generated/<env>``.

        On a FRESH project (no prior real generate), draining ``plan_generation``
        for every pipeline must leave the real target non-existent — yet the plan
        is non-empty and still REPORTS that directory as ``output_location``.
        """
        facade, project_root, pipeline_fields = all_kinds_project
        generated = project_root / "generated" / "dev"

        total_files = 0
        for field in pipeline_fields:
            plan = collect_response(
                facade.generation.plan_generation(
                    "dev", pipeline_filter=field, include_tests=True
                )
            )
            assert isinstance(plan, GenerationPlan)
            assert plan.output_location == generated
            total_files += len(plan.files)

        assert total_files >= 1, "expected a non-empty plan across all pipelines"
        # Nothing was written to the real target: it does not exist (or,
        # defensively, holds no files).
        if generated.exists():
            assert [p for p in generated.rglob("*") if p.is_file()] == []
        else:
            assert not generated.exists()

    def test_filtered_plan_matches_filtered_generate(self, all_kinds_project):
        """Per-pipeline parity: a single-pipeline ``plan_generation`` equals a
        single-pipeline ``generate_pipelines`` disk read-back, for the pipeline
        that exercises the most kinds (the monitoring pipeline → monitoring +
        helper + aux).
        """
        facade, project_root, pipeline_fields = all_kinds_project
        monitoring_pl = next(
            p for p in pipeline_fields if p.endswith("_event_log_monitoring")
        )
        output_dir = project_root / "generated" / "dev"

        # Real generate, narrowed to just the monitoring pipeline.
        collect_response(
            facade.generation.generate_pipelines(
                pipeline_filter=monitoring_pl,
                env="dev",
                output_dir=output_dir,
                include_tests=True,
            )
        )
        real = _disk_tree(output_dir)

        plan = collect_response(
            facade.generation.plan_generation(
                "dev", pipeline_filter=monitoring_pl, include_tests=True
            )
        )
        plan_map = {str(view.path): view.content for view in plan.files}

        assert set(plan_map) == set(real), (
            "filtered file set diverged — "
            f"only in plan: {sorted(set(plan_map) - set(real))}; "
            f"only on disk: {sorted(set(real) - set(plan_map))}"
        )
        assert plan_map == real
        # The narrowed pipeline carries monitoring + helper + aux (its full set).
        assert {view.kind for view in plan.files} == {"monitoring", "helper", "aux"}
