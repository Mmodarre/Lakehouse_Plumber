"""Behavior-locking test for ``InspectionFacade.list_flowgroups`` filtering.

The *filtered* branch of ``list_flowgroups(pipeline_filter=X)`` routes through
``bootstrap.discover_all_flowgroups()`` + an in-memory ``fg.pipeline == X``
filter. The filtered result is therefore the SAME expanded set as the
unfiltered branch, narrowed to pipeline X::

    set(list_flowgroups(pipeline_filter=X))
        == {fg for fg in list_flowgroups() if fg.pipeline == X}

The fixture builds a project whose pipeline ``apac_sg_raw`` exists *only* via
blueprint expansion. Raw disk discovery never emits a flowgroup for that
pipeline. If the filtered branch reverted to raw disk discovery,
``list_flowgroups(pipeline_filter="apac_sg_raw")`` would be empty — the
``inclusion`` and ``equivalence`` assertions would both fail. The
``test_disk_discovery_omits_blueprint_flowgroups`` guard documents exactly
what would change on revert.
"""

from __future__ import annotations

from pathlib import Path

import pytest

from lhp.api import InspectionFacade
from lhp.core.coordination.layers import build_facade_orchestrator

pytestmark = pytest.mark.unit

# The pipeline that exists ONLY through blueprint expansion.
_BLUEPRINT_PIPELINE = "apac_sg_raw"
# The flowgroup synthesised into that pipeline by the blueprint.
_BLUEPRINT_FLOWGROUP = "apac_sg_orders"


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def _build_blueprint_project(tmp_path: Path) -> Path:
    """A project with a disk flowgroup plus a blueprint that expands into a
    *distinct* pipeline (``apac_sg_raw``) that no disk file declares."""
    _write(tmp_path / "lhp.yaml", 'name: list_fg_test\nversion: "1.0"\n')
    _write(tmp_path / "substitutions" / "dev.yaml", "catalog: c\n")
    for d in ("presets", "templates", "pipelines"):
        (tmp_path / d).mkdir(exist_ok=True)

    # A plain disk-sourced flowgroup in its own pipeline.
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
    # Blueprint whose pipeline name is parameter-derived; the instance below
    # binds ``site_name=apac_sg`` so the expanded pipeline is ``apac_sg_raw``
    # and the expanded flowgroup is ``apac_sg_orders``. No disk YAML declares
    # ``pipeline: apac_sg_raw`` directly.
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
        "use_blueprint: erp\nparameters:\n  site_name: apac_sg\n",
    )
    return tmp_path


def _make_facade(tmp_path: Path) -> InspectionFacade:
    orch = build_facade_orchestrator(tmp_path, enforce_version=False)
    return InspectionFacade(orch)


def _key(view) -> tuple[str, str]:
    return (view.pipeline, view.name)


def test_filtered_list_flowgroups_equals_filtered_unfiltered(tmp_path):
    """Equivalence: filtering by pipeline X yields exactly the unfiltered
    set narrowed to X (Decision B)."""
    facade = _make_facade(tmp_path := _build_blueprint_project(tmp_path))

    filtered = facade.list_flowgroups(pipeline_filter=_BLUEPRINT_PIPELINE)
    unfiltered = facade.list_flowgroups()

    expected = {_key(v) for v in unfiltered if v.pipeline == _BLUEPRINT_PIPELINE}
    assert {_key(v) for v in filtered} == expected
    # The expected set is non-empty (otherwise the equivalence is vacuous).
    assert expected, "fixture must produce flowgroups in the filtered pipeline"


def test_filtered_list_flowgroups_includes_blueprint_expansion(tmp_path):
    """Inclusion: the filtered result for a blueprint-only pipeline contains
    the blueprint-expanded flowgroup that raw disk discovery would omit.

    This is the behavior-locking assertion: on a revert to raw disk
    discovery the filtered result would be empty and this fails.
    """
    facade = _make_facade(tmp_path := _build_blueprint_project(tmp_path))

    filtered = facade.list_flowgroups(pipeline_filter=_BLUEPRINT_PIPELINE)
    keys = {_key(v) for v in filtered}

    assert (_BLUEPRINT_PIPELINE, _BLUEPRINT_FLOWGROUP) in keys

    # The disk flowgroup belongs to a different pipeline and must NOT leak
    # into the filtered result.
    assert ("regular_pipeline", "regular_fg") not in keys


def test_disk_discovery_omits_blueprint_flowgroups(tmp_path):
    """Guard documenting the revert-failure mechanism directly.

    The old filtered branch resolved through the discovery service's
    disk-only ``discover_flowgroups_by_pipeline_field`` (no blueprint
    expansion). This asserts that raw path returns NOTHING for the
    blueprint-only pipeline — i.e. exactly the flowgroup the Decision-B
    filtered branch now includes is the one raw discovery omits. If the
    facade reverted to this path, the inclusion test above would fail.
    """
    orch = build_facade_orchestrator(
        _build_blueprint_project(tmp_path), enforce_version=False
    )

    disk_only = orch.discovery.discover_flowgroups_by_pipeline_field(
        _BLUEPRINT_PIPELINE
    )
    assert disk_only == [] or all(
        fg.flowgroup != _BLUEPRINT_FLOWGROUP for fg in disk_only
    )

    # ...whereas the bootstrap (expanded) path that the facade now uses
    # *does* surface the blueprint flowgroup for that pipeline.
    expanded = orch.bootstrap.discover_all_flowgroups()
    assert any(
        fg.pipeline == _BLUEPRINT_PIPELINE and fg.flowgroup == _BLUEPRINT_FLOWGROUP
        for fg in expanded
    )
