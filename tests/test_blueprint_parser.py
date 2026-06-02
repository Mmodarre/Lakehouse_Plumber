"""Spec-driven unit tests for BlueprintParser (Phase 3, Phase 5 step 2).

Covers error codes 041, 042, 043, 047-050 (blueprint), 051-054 (instance),
plus the public ``looks_like_blueprint`` helper.
"""

from pathlib import Path

import pytest

from lhp.errors import ErrorCategory, LHPError
from lhp.parsers.blueprint_parser import BlueprintParser

pytestmark = pytest.mark.unit


def _write(path: Path, content: str) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    return path


# ---------------------------------------------------------------------------
# parse_blueprint_file
# ---------------------------------------------------------------------------


def test_parse_blueprint_file_valid(tmp_path):
    bp_file = _write(
        tmp_path / "blueprints" / "erp.yaml",
        """
name: erp_ingestion
version: "1.0"
description: "ERP ingestion"
parameters:
  - name: site_name
    required: true
  - name: partition_key
    required: false
    default: order_date
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    parser = BlueprintParser()
    bp = parser.parse_blueprint_file(bp_file)
    assert bp.name == "erp_ingestion"
    assert len(bp.parameters) == 2
    assert bp.parameters[0].name == "site_name"
    assert bp.parameters[0].required is True
    assert bp.parameters[1].default == "order_date"
    assert len(bp.flowgroups) == 1


def test_blueprint_empty_file_raises_047(tmp_path):
    bp_file = _write(tmp_path / "blueprints" / "empty.yaml", "")
    with pytest.raises(LHPError) as exc:
        BlueprintParser().parse_blueprint_file(bp_file)
    assert exc.value.code == "LHP-CFG-047"
    assert exc.value.category == ErrorCategory.CONFIG


def test_blueprint_multi_doc_file_raises_048(tmp_path):
    bp_file = _write(
        tmp_path / "blueprints" / "multi.yaml",
        """
name: a
parameters: []
flowgroups: []
---
name: b
parameters: []
flowgroups: []
""",
    )
    with pytest.raises(LHPError) as exc:
        BlueprintParser().parse_blueprint_file(bp_file)
    assert exc.value.code == "LHP-CFG-048"


def test_blueprint_wrong_shape_raises_049(tmp_path):
    bp_file = _write(
        tmp_path / "blueprints" / "wrong.yaml",
        """
pipeline: my_pipeline
flowgroup: my_fg
actions:
  - name: load
    type: load
""",
    )
    with pytest.raises(LHPError) as exc:
        BlueprintParser().parse_blueprint_file(bp_file)
    assert exc.value.code == "LHP-CFG-049"


def test_blueprint_invalid_definition_raises_050(tmp_path):
    # Missing required `name`, missing required `flowgroups`. Pydantic will reject.
    bp_file = _write(
        tmp_path / "blueprints" / "bad.yaml",
        """
parameters: []
flowgroups: not_a_list
""",
    )
    with pytest.raises(LHPError) as exc:
        BlueprintParser().parse_blueprint_file(bp_file)
    assert exc.value.code == "LHP-CFG-050"


# ---------------------------------------------------------------------------
# parse_instance_file
# ---------------------------------------------------------------------------


def _make_blueprint(parser: BlueprintParser, tmp_path: Path):
    bp_file = _write(
        tmp_path / "blueprints" / "erp.yaml",
        """
name: erp_ingestion
parameters:
  - name: site_name
    required: true
  - name: site_id
    required: true
  - name: partition_key
    required: false
    default: order_date
flowgroups:
  - pipeline: "%{site_name}_raw"
    flowgroup: "%{site_name}_orders"
    actions: []
""",
    )
    bp = parser.parse_blueprint_file(bp_file)
    return {"erp_ingestion": bp}


def test_parse_instance_file_valid(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "sg.yaml",
        """
blueprint: erp_ingestion
site_name: apac_sg
site_id: SG001
""",
    )
    inst = parser.parse_instance_file(inst_file, blueprints)
    assert inst.blueprint == "erp_ingestion"
    values = inst.parameter_values()
    assert values["site_name"] == "apac_sg"
    assert values["site_id"] == "SG001"


def test_parse_instance_file_new_use_blueprint_syntax(tmp_path):
    """The preferred ``use_blueprint:`` + nested ``parameters:`` form parses
    cleanly through the parser and round-trips to the canonical normalized
    state."""
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "sg.yaml",
        """
use_blueprint: erp_ingestion
parameters:
  site_name: apac_sg
  site_id: SG001
""",
    )
    inst = parser.parse_instance_file(inst_file, blueprints)
    assert inst.blueprint_name == "erp_ingestion"
    assert inst.use_blueprint == "erp_ingestion"
    assert inst.parameter_values() == {"site_name": "apac_sg", "site_id": "SG001"}


def test_parse_instance_file_legacy_form_emits_deprecation(tmp_path, caplog):
    """Legacy form parses successfully but emits a deprecation warning per
    file (deduplicated)."""
    import logging

    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "legacy_sg.yaml",
        """
blueprint: erp_ingestion
site_name: apac_sg
site_id: SG001
""",
    )
    # Reset dedupe state so this test is independent of others.
    from lhp.models import BlueprintInstance

    BlueprintInstance._legacy_warned_paths.clear()
    caplog.set_level(logging.WARNING, logger="lhp.models.config")
    inst = parser.parse_instance_file(inst_file, blueprints)
    assert inst.blueprint_name == "erp_ingestion"
    deprecation = [
        r for r in caplog.records if "Deprecated blueprint instance syntax" in r.message
    ]
    assert len(deprecation) == 1
    assert str(inst_file) in deprecation[0].args[0]


def test_parse_instance_file_mutual_exclusion_raises_061(tmp_path):
    """Mixing ``use_blueprint:`` and legacy ``blueprint:`` keys triggers
    LHP-VAL-061."""
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "pipelines" / "erp" / "bronze" / "bad.yaml",
        """
use_blueprint: erp_ingestion
blueprint: erp_ingestion
parameters:
  site_name: apac_sg
  site_id: SG001
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-VAL-061"


def test_instance_unknown_blueprint_raises_041(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "bad.yaml",
        """
blueprint: missing_blueprint
site_name: x
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-VAL-041"
    assert exc.value.category == ErrorCategory.VALIDATION


def test_instance_missing_required_parameter_raises_042(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "bad.yaml",
        """
blueprint: erp_ingestion
site_name: only_one
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-VAL-042"


def test_instance_unknown_param_raises_043_with_near_miss(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "typo.yaml",
        """
blueprint: erp_ingestion
site_name: apac_sg
site_id: SG001
partion_key: foo
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-VAL-043"
    # near-miss text should mention the correct parameter name
    msg = str(exc.value)
    assert "partition_key" in msg


def test_instance_empty_file_raises_051(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(tmp_path / "instances" / "empty.yaml", "")
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-CFG-051"


def test_instance_multi_doc_raises_052(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "multi.yaml",
        """
blueprint: erp_ingestion
site_name: a
site_id: A
---
blueprint: erp_ingestion
site_name: b
site_id: B
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code == "LHP-CFG-052"


def test_instance_missing_blueprint_key_raises_053(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "noblue.yaml",
        """
site_name: apac_sg
site_id: SG001
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    # The 053 code may use either CONFIG or VALIDATION category; spec table
    # lists 053 as a parser-side error — accept the code regardless of prefix.
    assert exc.value.code.endswith("-053")


def test_instance_invalid_definition_raises_054(tmp_path):
    parser = BlueprintParser()
    blueprints = _make_blueprint(parser, tmp_path)
    inst_file = _write(
        tmp_path / "instances" / "bad.yaml",
        """
blueprint:
  - not
  - a
  - string
""",
    )
    with pytest.raises(LHPError) as exc:
        parser.parse_instance_file(inst_file, blueprints)
    assert exc.value.code.endswith("-054")


# ---------------------------------------------------------------------------
# looks_like_blueprint static helper
# ---------------------------------------------------------------------------


def test_looks_like_blueprint_true_for_blueprint_shape():
    doc = {"parameters": [], "flowgroups": []}
    assert BlueprintParser.looks_like_blueprint(doc) is True


def test_looks_like_blueprint_false_when_actions_present():
    doc = {"parameters": [], "flowgroups": [], "actions": []}
    assert BlueprintParser.looks_like_blueprint(doc) is False


def test_looks_like_blueprint_false_for_regular_flowgroup():
    doc = {"pipeline": "p", "flowgroup": "fg", "actions": []}
    assert BlueprintParser.looks_like_blueprint(doc) is False


def test_looks_like_blueprint_false_when_only_one_marker_present():
    assert BlueprintParser.looks_like_blueprint({"parameters": []}) is False
    assert BlueprintParser.looks_like_blueprint({"flowgroups": []}) is False


def test_looks_like_blueprint_false_for_non_dict():
    assert BlueprintParser.looks_like_blueprint(None) is False
    assert BlueprintParser.looks_like_blueprint([]) is False
    assert BlueprintParser.looks_like_blueprint("hello") is False
