"""Spec-driven unit tests for the B1 fix in YAMLParser.parse_flowgroups_from_file.

A blueprint accidentally placed under pipelines/ must raise code 040 (CONFIG
category) rather than crashing on missing `actions:` (Phase 3, B1).
"""

from pathlib import Path

import pytest

from lhp.parsers.blueprint_parser import BlueprintParser
from lhp.parsers.yaml_parser import YAMLParser
from lhp.errors import ErrorCategory, LHPError

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


def test_blueprint_in_pipelines_dir_raises_040(tmp_path):
    """A blueprint shape file in pipelines/ must trigger the discriminator."""
    bp_in_pipelines = _write(
        tmp_path / "pipelines" / "stray_blueprint.yaml",
        """
name: stray_blueprint
parameters:
  - name: site_name
    required: true
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    parser = YAMLParser()
    with pytest.raises(LHPError) as exc:
        parser.parse_flowgroups_from_file(bp_in_pipelines)
    assert exc.value.code == "LHP-CFG-040"
    assert exc.value.category == ErrorCategory.CONFIG


def test_regular_flowgroup_file_still_parses_ok(tmp_path):
    """A real flowgroup file (with actions) must not trip the discriminator."""
    fg_file = _write(
        tmp_path / "pipelines" / "real_fg.yaml",
        """
pipeline: my_pipeline
flowgroup: my_fg
actions:
  - name: load_raw
    type: load
    source:
      type: sql
      sql: "SELECT 1"
    target: v_raw
""",
    )
    parser = YAMLParser()
    flowgroups = parser.parse_flowgroups_from_file(fg_file)
    assert len(flowgroups) == 1
    assert flowgroups[0].pipeline == "my_pipeline"


def test_looks_like_blueprint_is_public_static(tmp_path):
    """The B1 discriminator helper is callable as a static method."""
    blueprint_doc = {"parameters": [], "flowgroups": []}
    assert BlueprintParser.looks_like_blueprint(blueprint_doc) is True
    flowgroup_doc = {"pipeline": "p", "flowgroup": "fg", "actions": []}
    assert BlueprintParser.looks_like_blueprint(flowgroup_doc) is False


def test_looks_like_instance_recognises_new_and_legacy_shapes():
    """`looks_like_instance` is the routing discriminator that lets instance
    files coexist with flowgroups under pipelines/."""
    new_form = {"use_blueprint": "halo_bronze", "parameters": {"variant": "acme"}}
    legacy_form = {"blueprint": "halo_bronze", "variant": "acme"}
    assert BlueprintParser.looks_like_instance(new_form) is True
    assert BlueprintParser.looks_like_instance(legacy_form) is True

    flowgroup_doc = {"pipeline": "p", "flowgroup": "fg", "actions": []}
    blueprint_doc = {"parameters": [], "flowgroups": []}
    assert BlueprintParser.looks_like_instance(flowgroup_doc) is False
    assert BlueprintParser.looks_like_instance(blueprint_doc) is False
    assert BlueprintParser.looks_like_instance(None) is False
    assert BlueprintParser.looks_like_instance("not a dict") is False


def test_instance_file_in_pipelines_dir_routes_silently(tmp_path):
    """Instance-shape files under pipelines/ must NOT raise during flowgroup
    parsing — they're routed to the BlueprintDiscoverer via early return."""
    inst_in_pipelines = _write(
        tmp_path / "pipelines" / "halo" / "bronze" / "halo_bronze_acme_BP001.yaml",
        """
use_blueprint: halo_bronze
parameters:
  variant: acme
  domain_id: ACME001
""",
    )
    parser = YAMLParser()
    flowgroups = parser.parse_flowgroups_from_file(inst_in_pipelines)
    # Empty list = "skip me, the BlueprintDiscoverer handles this file"
    assert flowgroups == []


def test_legacy_instance_file_in_pipelines_dir_also_routes(tmp_path):
    inst = _write(
        tmp_path / "pipelines" / "halo" / "bronze" / "legacy.yaml",
        """
blueprint: halo_bronze
variant: acme
""",
    )
    parser = YAMLParser()
    assert parser.parse_flowgroups_from_file(inst) == []
