"""Spec-driven unit tests for Phase 9 / B5 — synthetic-flowgroup novelty check.

The novelty check must occur BEFORE the empty-set early return so a project
whose only stale work is a brand-new synthetic flowgroup is still selected
for generation.
"""

from pathlib import Path

import pytest

from lhp.core.orchestrator import ActionOrchestrator
from lhp.core.state_manager import StateManager
from lhp.models.config import FlowGroup

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _bootstrap_project(tmp_path: Path) -> Path:
    """Create the minimal disk shape the orchestrator expects."""
    _write(
        tmp_path / "lhp.yaml",
        """
name: bp_smart_gen
version: "1.0"
""",
    )
    _write(
        tmp_path / "substitutions" / "dev.yaml",
        """
catalog: dev_cat
""",
    )
    (tmp_path / "presets").mkdir(exist_ok=True)
    (tmp_path / "templates").mkdir(exist_ok=True)
    (tmp_path / "pipelines").mkdir(exist_ok=True)
    return tmp_path


def _make_synthetic_fg(pipeline: str, flowgroup: str) -> FlowGroup:
    fg = FlowGroup(pipeline=pipeline, flowgroup=flowgroup, actions=[])
    fg._synthetic = True
    return fg


def _make_disk_fg(pipeline: str, flowgroup: str) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=flowgroup, actions=[])


def _seed_generation_context(sm: StateManager, env: str, include_tests: bool) -> None:
    """Make the include_tests gate a no-op so tests can isolate novelty logic."""
    sm.state.last_generation_context[env] = {"include_tests": str(include_tests)}


def test_new_synthetic_flowgroup_picked_up_when_state_empty(tmp_path):
    """A synthetic flowgroup whose tuple is not in tracked state must be
    selected for generation, even when no disk files are new and no state
    entries are stale."""
    project = _bootstrap_project(tmp_path)
    orch = ActionOrchestrator(project, enforce_version=False)
    sm = StateManager(project)
    _seed_generation_context(sm, "dev", False)

    # All synthetic — none of these tuples are tracked in state yet.
    all_flowgroups = [
        _make_synthetic_fg("site_a_raw", "site_a_orders"),
        _make_synthetic_fg("site_a_raw", "site_a_customers"),
    ]
    filtered = orch._apply_smart_generation_filtering(
        all_flowgroups,
        env="dev",
        pipeline_identifier="site_a_raw",
        include_tests=False,
        state_manager=sm,
    )
    flow_names = {fg.flowgroup for fg in filtered}
    # Both synthetic flowgroups are "new" → both selected.
    assert "site_a_orders" in flow_names
    assert "site_a_customers" in flow_names


def test_synthetic_already_tracked_in_state_is_not_in_new_synthetic_set(tmp_path):
    """When a synthetic flowgroup's tuple is recorded in state, the novelty
    set computed by smart filtering must NOT include it. (Other staleness
    sources may still re-introduce it; this test only locks in novelty
    semantics.)"""
    from datetime import datetime

    from lhp.core.state_models import FileState

    project = _bootstrap_project(tmp_path)
    orch = ActionOrchestrator(project, enforce_version=False)
    sm = StateManager(project)
    _seed_generation_context(sm, "dev", False)
    sm.state.environments["dev"] = {
        "generated/dev/site_a/orders.py": FileState(
            source_yaml="blueprints/erp.yaml",
            generated_path="generated/dev/site_a/orders.py",
            checksum="c",
            source_yaml_checksum="sc",
            timestamp=datetime.now().isoformat(),
            environment="dev",
            pipeline="site_a_raw",
            flowgroup="site_a_orders",
            synthetic=True,
        ),
        # Add a second tracked entry to demonstrate selective novelty.
        "generated/dev/site_a/customers.py": FileState(
            source_yaml="blueprints/erp.yaml",
            generated_path="generated/dev/site_a/customers.py",
            checksum="c2",
            source_yaml_checksum="sc",
            timestamp=datetime.now().isoformat(),
            environment="dev",
            pipeline="site_a_raw",
            flowgroup="site_a_customers",
            synthetic=True,
        ),
    }

    # Two synthetic candidates: orders is tracked (must be filtered out of
    # the novelty set), new_addition is novel (must remain candidate).
    all_flowgroups = [
        _make_synthetic_fg("site_a_raw", "site_a_orders"),
        _make_synthetic_fg("site_a_raw", "new_addition"),
    ]
    filtered = orch._apply_smart_generation_filtering(
        all_flowgroups,
        env="dev",
        pipeline_identifier="site_a_raw",
        include_tests=False,
        state_manager=sm,
    )
    flow_names = {fg.flowgroup for fg in filtered}
    # The novel flowgroup must be selected; tracked one need not be (it may
    # appear via staleness, but novelty itself must distinguish them).
    assert "new_addition" in flow_names


def test_novelty_check_runs_before_empty_set_early_return(tmp_path):
    """B5 fix: a mix of one novel synthetic + nothing else must still be
    selected. If the empty-set early return ran first, the synthetic would
    be dropped — this test guards against regression."""
    project = _bootstrap_project(tmp_path)
    orch = ActionOrchestrator(project, enforce_version=False)
    sm = StateManager(project)
    _seed_generation_context(sm, "dev", False)
    # State has nothing for this pipeline at all.
    all_flowgroups = [_make_synthetic_fg("new_pipeline", "new_fg")]
    filtered = orch._apply_smart_generation_filtering(
        all_flowgroups,
        env="dev",
        pipeline_identifier="new_pipeline",
        include_tests=False,
        state_manager=sm,
    )
    assert len(filtered) == 1
    assert filtered[0].flowgroup == "new_fg"


def test_disk_flowgroup_without_yaml_change_is_dropped_when_synthetic_pipeline(
    tmp_path,
):
    """A disk-sourced (non-synthetic) flowgroup that's not new and not stale
    must NOT be picked up just because it's in `all_flowgroups`. Sanity check
    that the novelty path is synthetic-only."""
    project = _bootstrap_project(tmp_path)
    orch = ActionOrchestrator(project, enforce_version=False)
    sm = StateManager(project)
    _seed_generation_context(sm, "dev", False)
    all_flowgroups = [_make_disk_fg("p", "regular_fg")]
    filtered = orch._apply_smart_generation_filtering(
        all_flowgroups,
        env="dev",
        pipeline_identifier="p",
        include_tests=False,
        state_manager=sm,
    )
    # Disk-sourced flowgroup with no YAML novelty / no state staleness → not selected.
    assert filtered == []
