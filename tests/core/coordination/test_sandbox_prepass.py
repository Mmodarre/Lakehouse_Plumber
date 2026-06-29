"""Sandbox pre-pass: substitution parity, warning folding, tolerant resolution.

Exercises :func:`lhp.core.coordination.sandbox_prepass.build_sandbox_rewrite_plan`
and its :meth:`ActionOrchestrator.build_sandbox_rewrite_plan` delegation seam
against a real on-disk project (the construction idiom is copied from
``test_bootstrap_discovery_memo._build_multipipeline_project``) whose write
targets are TOKENIZED (``${catalog}`` / ``${bronze_schema}`` / a ``${prefix}``
token inside the table LEAF) so the core acceptance — pre-pass rename keys
carry the WORKER-resolved spelling — is proven against actual substitution,
not echoes of raw YAML.

:stability: provisional
"""

import re
from pathlib import Path

import pytest
import yaml

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.core.coordination.sandbox_prepass import build_sandbox_rewrite_plan
from lhp.core.dependencies import build_producer_indexes
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import OrchestrationDependencies
from lhp.core.sandbox import TableRenameStrategy
from lhp.models.processing import SandboxRunConfig


def _flowgroup(pipeline: str, flowgroup: str, tables: list) -> dict:
    """One flowgroup dict with a tokenized load->write chain per table."""
    actions = []
    for table in tables:
        safe = re.sub(r"[^A-Za-z0-9_]", "", table)
        actions.append(
            {
                "name": f"load_{safe}",
                "type": "load",
                "target": f"v_{safe}_raw",
                "source": {
                    "type": "cloudfiles",
                    "path": "${landing_path}/" + safe,
                    "format": "json",
                },
            }
        )
        actions.append(
            {
                "name": f"write_{safe}",
                "type": "write",
                "source": f"v_{safe}_raw",
                "write_target": {
                    "type": "streaming_table",
                    "catalog": "${catalog}",
                    "schema": "${bronze_schema}",
                    "table": table,
                    "create_table": True,
                },
            }
        )
    return {"pipeline": pipeline, "flowgroup": flowgroup, "actions": actions}


def _build_project(tmp_path: Path, flowgroup_specs: list) -> Path:
    """Build an on-disk project (idiom from test_bootstrap_discovery_memo)."""
    project_root = tmp_path / "project"
    (project_root / "presets").mkdir(parents=True)
    (project_root / "templates").mkdir()
    (project_root / "substitutions").mkdir()
    substitutions = {
        "dev": {
            "catalog": "dev_catalog",
            "bronze_schema": "bronze",
            "landing_path": "/mnt/dev/landing",
            "prefix": "dev",
        }
    }
    with open(project_root / "substitutions" / "dev.yaml", "w") as f:
        yaml.dump(substitutions, f)
    for spec in flowgroup_specs:
        pipeline_dir = project_root / "pipelines" / spec["pipeline"]
        pipeline_dir.mkdir(parents=True, exist_ok=True)
        with open(pipeline_dir / f"{spec['flowgroup']}.yaml", "w") as f:
            yaml.dump(spec, f)
    return project_root


def _run_config(pipelines: tuple) -> SandboxRunConfig:
    return SandboxRunConfig(
        namespace="alice",
        table_pattern="{namespace}__{table}",
        strategy="table",
        pipelines=pipelines,
    )


def _prepass_plan(orchestrator, project_root: Path, run, discovered):
    """Call the module function with explicitly handed collaborators."""
    return build_sandbox_rewrite_plan(
        processing=orchestrator.processing,
        bootstrap=orchestrator.bootstrap,
        orchestration_dependencies=OrchestrationDependencies(),
        project_root=project_root,
        env="dev",
        run=run,
        flowgroups=discovered,
    )


@pytest.mark.integration
def test_rename_keys_match_worker_resolved_spelling(tmp_path):
    """(a) Rename keys equal the WORKER-resolved spelling on tokenized YAML.

    The worker baseline is computed through the EXACT pool path: the same
    ``bootstrap.make_context`` envelope and the same
    ``processing.process_flowgroup`` call
    (``_flowgroup_pool._process_one_flowgroup_impl``), with a substitution
    manager built the way the worklist builder builds it.
    """
    project_root = _build_project(
        tmp_path,
        [
            _flowgroup("in_pipe", "in_fg", ["${prefix}_orders"]),
            _flowgroup("out_pipe", "out_fg", ["${prefix}_customers"]),
        ],
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())
    run = _run_config(("in_pipe",))

    plan = _prepass_plan(orchestrator, project_root, run, discovered)

    # Worker-resolved baseline: identical resolution path to the pool.
    sub_mgr = EnhancedSubstitutionManager(
        project_root / "substitutions" / "dev.yaml", "dev"
    )
    in_fg = next(fg for fg in discovered if fg.pipeline == "in_pipe")
    ctx_out = orchestrator.processing.process_flowgroup(
        orchestrator.bootstrap.make_context(in_fg), sub_mgr, include_tests=False
    )
    worker_tp, _ = build_producer_indexes([ctx_out.flowgroup])

    assert set(plan.renames.table_producers) == set(worker_tp)
    # The tokenized leaf actually resolved (not the raw "${prefix}_orders").
    assert set(plan.renames.table_producers) == {"dev_catalog.bronze.dev_orders"}
    # D7: out-of-scope production is NOT in the rename set.
    assert "dev_catalog.bronze.dev_customers" not in plan.renames.table_producers
    assert plan.renames.strategy == TableRenameStrategy(
        namespace="alice", table_pattern="{namespace}__{table}"
    )


@pytest.mark.integration
def test_mixed_producer_sinks_fold_into_one_val_065_record(tmp_path):
    """(b) D8: ALL mixed-producer findings fold into exactly ONE record.

    Two distinct sink tables are each produced by both partitions; the plan
    must carry a single ``LHP-VAL-065`` record (``file=None``,
    ``flowgroup=None`` — the dedup layers key on ``(code, file)``) naming
    every conflicted table and its out-of-scope producers.
    """
    project_root = _build_project(
        tmp_path,
        [
            _flowgroup(
                "in_pipe", "in_fg", ["${prefix}_orders", "shared_a", "shared_b"]
            ),
            _flowgroup("out_pipe", "out_fg", ["shared_a", "shared_b", "solo"]),
        ],
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())

    plan = _prepass_plan(
        orchestrator, project_root, _run_config(("in_pipe",)), discovered
    )

    assert len(plan.warnings) == 1
    record = plan.warnings[0]
    assert record.code == "LHP-VAL-065"
    assert record.file is None
    assert record.flowgroup is None
    # Both conflicted tables and their out-of-scope producer ids are named.
    assert "dev_catalog.bronze.shared_a" in record.message
    assert "dev_catalog.bronze.shared_b" in record.message
    assert "out_fg.write_shared_a" in record.message
    assert "out_fg.write_shared_b" in record.message
    # Non-conflicted sinks are not flagged.
    assert "dev_orders" not in record.message
    assert "solo" not in record.message
    # D8 is rewrite + WARNING, never removal: the sinks stay in the renames.
    assert "dev_catalog.bronze.shared_a" in plan.renames.table_producers


@pytest.mark.integration
def test_no_mixed_producers_yields_no_warnings(tmp_path):
    """(c) Disjoint in-/out-of-scope sink sets produce zero warnings."""
    project_root = _build_project(
        tmp_path,
        [
            _flowgroup("in_pipe", "in_fg", ["${prefix}_orders"]),
            _flowgroup("out_pipe", "out_fg", ["${prefix}_customers"]),
        ],
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())

    plan = _prepass_plan(
        orchestrator, project_root, _run_config(("in_pipe",)), discovered
    )

    assert plan.warnings == ()


@pytest.mark.integration
def test_out_of_scope_resolution_failure_is_tolerated(tmp_path):
    """(d) An out-of-scope flowgroup that fails resolution does not raise.

    ``${nonexistent_token}`` in the out-of-scope write target makes
    ``process_flowgroup`` raise LHP-CFG-010 (unresolved-token validation on
    the real-env manager). The pre-pass must swallow it (the gated pool
    surfaces the real error later) and keep the in-scope renames intact.
    """
    project_root = _build_project(
        tmp_path,
        [
            _flowgroup("in_pipe", "in_fg", ["${prefix}_orders"]),
            _flowgroup("out_pipe", "out_fg", ["${nonexistent_token}_x"]),
        ],
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())

    plan = _prepass_plan(
        orchestrator, project_root, _run_config(("in_pipe",)), discovered
    )

    assert set(plan.renames.table_producers) == {"dev_catalog.bronze.dev_orders"}
    # The raw out-of-scope keys carry the unresolved token spelling, which
    # cannot collide with the resolved in-scope keys: no phantom D8 warning.
    assert plan.warnings == ()


@pytest.mark.integration
def test_orchestrator_seam_returns_same_plan(tmp_path):
    """(e) The ActionOrchestrator seam delegates to the module function.

    The seam hands the orchestrator's OWN collaborators (processing,
    bootstrap, substitution factory, project root); its plan must equal the
    module function's plan built from the same inputs.
    """
    project_root = _build_project(
        tmp_path,
        [
            _flowgroup("in_pipe", "in_fg", ["${prefix}_orders", "shared_a"]),
            _flowgroup("out_pipe", "out_fg", ["shared_a"]),
        ],
    )
    orchestrator = build_facade_orchestrator(project_root, enforce_version=False)
    discovered = list(orchestrator.bootstrap.discover_all_flowgroups())
    run = _run_config(("in_pipe",))

    plan_seam = orchestrator.build_sandbox_rewrite_plan(
        env="dev", run=run, flowgroups=discovered
    )
    plan_module = _prepass_plan(orchestrator, project_root, run, discovered)

    assert plan_seam == plan_module
    assert set(plan_seam.renames.table_producers) == {
        "dev_catalog.bronze.dev_orders",
        "dev_catalog.bronze.shared_a",
    }
    assert len(plan_seam.warnings) == 1
    assert plan_seam.warnings[0].code == "LHP-VAL-065"
