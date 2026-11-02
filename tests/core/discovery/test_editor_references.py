"""``action_file_references`` reports a materialized view's expectations file."""

import pytest

from lhp.core.discovery import action_file_references

pytestmark = pytest.mark.unit


def _mv_action(expectations_file: str) -> dict:
    return {
        "name": "write_gate",
        "type": "write",
        "write_target": {
            "type": "materialized_view",
            "table": "gate",
            "sql_path": "sql/gate.sql",
            "expectations_file": expectations_file,
        },
    }


def test_write_target_expectations_file_is_an_expectations_reference(tmp_path):
    (tmp_path / "expectations").mkdir()
    (tmp_path / "expectations" / "gate.yaml").write_text("[]")

    refs = action_file_references(_mv_action("expectations/gate.yaml"), tmp_path)

    [ref] = [r for r in refs if r.kind == "expectations"]
    assert ref.path == "expectations/gate.yaml"
    assert ref.field_path == ("write_target", "expectations_file")
    assert ref.action_name == "write_gate"
    assert ref.exists is True
    assert [r.field_path for r in refs] == [
        ("write_target", "sql_path"),
        ("write_target", "expectations_file"),
    ]


def test_missing_file_is_reported_as_not_existing(tmp_path):
    refs = action_file_references(_mv_action("expectations/absent.yaml"), tmp_path)

    [ref] = [r for r in refs if r.kind == "expectations"]
    assert ref.exists is False


def test_bare_file_name_is_still_a_reference(tmp_path):
    (tmp_path / "rules.json").write_text("{}")

    refs = action_file_references(_mv_action("rules.json"), tmp_path)

    [ref] = [r for r in refs if r.kind == "expectations"]
    assert (ref.path, ref.exists) == ("rules.json", True)


def test_unresolved_token_is_never_reported_as_existing(tmp_path):
    refs = action_file_references(_mv_action("${env}/gate.yaml"), tmp_path)

    [ref] = [r for r in refs if r.kind == "expectations"]
    assert (ref.path, ref.exists) == ("${env}/gate.yaml", False)
