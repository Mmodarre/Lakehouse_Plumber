"""Spec-driven unit tests for orchestrator integration (Phase 6, Phase 11).

Covers ``discover_all_flowgroups`` returning blueprint-expanded + disk-sourced
flowgroups, the no-op behavior when no blueprints are present, and synthetic
source-path index wiring.
"""

from pathlib import Path

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator

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
    all_fgs = orch.bootstrap.discover_all_flowgroups()
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
