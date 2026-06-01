"""Spec-driven unit tests for orchestrator integration (Phase 6, Phase 11).

Covers ``discover_all_flowgroups`` returning blueprint-expanded + disk-sourced
flowgroups, the no-op behavior when no blueprints are present, synthetic
source-path index wiring, and ``_lookup_pipeline_slice`` cache behavior.
"""

from pathlib import Path

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.models import FlowGroup

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _bootstrap_project(tmp_path: Path) -> Path:
    _write(tmp_path / "lhp.yaml", 'name: orch_test\nversion: "1.0"\n')
    _write(tmp_path / "substitutions" / "dev.yaml", "catalog: c\n")
    for d in ("presets", "templates", "pipelines"):
        (tmp_path / d).mkdir(exist_ok=True)
    return tmp_path


def test_no_blueprints_expand_returns_empty(tmp_path):
    """When there are no blueprint or instance files, _expand_blueprints
    must return ([], {}) without error."""
    _bootstrap_project(tmp_path)
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    contexts, provenance = orch.bootstrap._expand_blueprints()
    assert contexts == []
    assert provenance == {}


def test_discover_all_flowgroups_includes_blueprint_expansions(tmp_path):
    """Disk-sourced + blueprint-expanded flowgroups should both surface."""
    _bootstrap_project(tmp_path)
    # Disk-sourced flowgroup.
    _write(
        tmp_path / "pipelines" / "regular.yaml",
        """
pipeline: regular_pipeline
flowgroup: regular_fg
actions:
  - name: load_x
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_x
""",
    )
    # Blueprint + 2 instances.
    _write(
        tmp_path / "blueprints" / "erp.yaml",
        """
name: erp
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        "blueprint: erp\nsite_name: apac_sg\n",
    )
    _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "uk.yaml",
        "blueprint: erp\nsite_name: emea_uk\n",
    )
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    all_fgs = orch.discover_all_flowgroups()
    flow_names = {fg.flowgroup for fg in all_fgs}
    assert "regular_fg" in flow_names
    assert "apac_sg_orders" in flow_names
    assert "emea_uk_orders" in flow_names

    # Synthetic blueprint flowgroups are wired into the discoverer's
    # source-path index — find_source_yaml_for_flowgroup resolves them to
    # the originating blueprint path.
    apac_fg = next(fg for fg in all_fgs if fg.flowgroup == "apac_sg_orders")
    emea_fg = next(fg for fg in all_fgs if fg.flowgroup == "emea_uk_orders")
    assert orch.discovery.find_source_yaml_for_flowgroup(apac_fg) is not None
    assert orch.discovery.find_source_yaml_for_flowgroup(emea_fg) is not None


def test_expand_blueprints_returns_synthetic_flowgroups(tmp_path):
    """Returned synthetic flowgroup contexts must carry synthetic=True."""
    _bootstrap_project(tmp_path)
    _write(
        tmp_path / "blueprints" / "erp.yaml",
        """
name: erp
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        "blueprint: erp\nsite_name: apac_sg\n",
    )
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    contexts, provenance = orch.bootstrap._expand_blueprints()
    assert len(contexts) == 1
    assert contexts[0].synthetic is True
    assert ("apac_sg_raw", "apac_sg_orders") in provenance


def _fg(pipeline: str, name: str) -> FlowGroup:
    return FlowGroup(pipeline=pipeline, flowgroup=name, actions=[])


def test_lookup_pipeline_slice_returns_correct_subset(tmp_path):
    _bootstrap_project(tmp_path)
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    flowgroups = [
        _fg("p1", "fg_a"),
        _fg("p1", "fg_b"),
        _fg("p2", "fg_c"),
    ]
    slice_p1 = orch._lookup_pipeline_slice(flowgroups, "p1")
    slice_p2 = orch._lookup_pipeline_slice(flowgroups, "p2")
    assert {fg.flowgroup for fg in slice_p1} == {"fg_a", "fg_b"}
    assert {fg.flowgroup for fg in slice_p2} == {"fg_c"}


def test_lookup_pipeline_slice_caches_by_id(tmp_path):
    """Phase 11: same list passed twice → cache is reused. New list → cache
    invalidates and rebuilds."""
    _bootstrap_project(tmp_path)
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    list_a = [_fg("p", "x"), _fg("p", "y"), _fg("q", "z")]
    list_b = [_fg("p", "only_x")]

    out_a1 = orch._lookup_pipeline_slice(list_a, "p")
    cached_id_after_a = orch._pipeline_slice_cache_id
    assert cached_id_after_a == id(list_a)

    # Same list again — cache must be reused (id unchanged).
    out_a2 = orch._lookup_pipeline_slice(list_a, "p")
    assert orch._pipeline_slice_cache_id == cached_id_after_a
    assert {fg.flowgroup for fg in out_a1} == {fg.flowgroup for fg in out_a2}

    # Different list — cache must rebuild.
    out_b = orch._lookup_pipeline_slice(list_b, "p")
    assert orch._pipeline_slice_cache_id == id(list_b)
    assert {fg.flowgroup for fg in out_b} == {"only_x"}
