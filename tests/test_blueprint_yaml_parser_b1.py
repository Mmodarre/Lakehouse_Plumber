"""Spec-driven unit tests for the B1 fix in YAMLParser.parse_flowgroups_from_file.

A blueprint accidentally placed under pipelines/ must raise code 040 (CONFIG
category) rather than crashing on missing `actions:` (Phase 3, B1).
"""

from pathlib import Path

import pytest

from lhp.parsers.blueprint_parser import BlueprintParser
from lhp.parsers.yaml_parser import YAMLParser
from lhp.utils.error_formatter import ErrorCategory, LHPError

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
