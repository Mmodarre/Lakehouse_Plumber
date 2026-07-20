"""Unit tests for the shared unified schema/tags format.

Directly exercises the strict whitelist enforced by ``validate`` (raising
LHP-CFG-067) and the ``schema_has_tags`` predicate that detects the CFG-069
silent-drop footgun.
"""

from pathlib import Path

import pytest

from lhp.errors import LHPError
from lhp.parsers import unified_schema_format as usf

PATH = Path("schemas/orders.yaml")


@pytest.mark.unit
def test_validate_accepts_full_unified_shape():
    usf.validate(
        {
            "table": "orders",
            "tags": {"team": "platform"},
            "columns": [
                {"name": "id", "type": "BIGINT", "nullable": False},
                {
                    "name": "email",
                    "type": "STRING",
                    "comment": "PII",
                    "tags": {"pii": "high"},
                },
            ],
            "version": "1.0",
            "description": "legacy",
            "primary_key": ["id"],
        },
        PATH,
    )


@pytest.mark.unit
def test_validate_accepts_name_alias_and_empty():
    usf.validate({"name": "orders"}, PATH)
    usf.validate({}, PATH)


@pytest.mark.unit
def test_validate_non_mapping_rejected():
    with pytest.raises(LHPError) as exc:
        usf.validate([1, 2], PATH)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_validate_unknown_top_level_key_rejected():
    with pytest.raises(LHPError) as exc:
        usf.validate({"name": "orders", "bogus": 1}, PATH)
    assert exc.value.code == "LHP-CFG-067"
    assert "bogus" in exc.value.details


@pytest.mark.unit
def test_validate_columns_must_be_list():
    with pytest.raises(LHPError) as exc:
        usf.validate({"columns": {"id": {}}}, PATH)
    assert exc.value.code == "LHP-CFG-067"
    assert "must be a list" in exc.value.details


@pytest.mark.unit
def test_validate_column_entry_must_be_mapping():
    with pytest.raises(LHPError) as exc:
        usf.validate({"columns": ["id"]}, PATH)
    assert exc.value.code == "LHP-CFG-067"


@pytest.mark.unit
def test_validate_unknown_column_key_rejected():
    with pytest.raises(LHPError) as exc:
        usf.validate({"columns": [{"name": "id", "typ": "BIGINT"}]}, PATH)
    assert exc.value.code == "LHP-CFG-067"
    assert "typ" in exc.value.details


@pytest.mark.unit
def test_schema_has_tags_true_for_table_tags():
    assert usf.schema_has_tags({"tags": {"team": "x"}}) is True


@pytest.mark.unit
def test_schema_has_tags_true_for_column_tags():
    data = {"columns": [{"name": "id"}, {"name": "email", "tags": {"pii": "high"}}]}
    assert usf.schema_has_tags(data) is True


@pytest.mark.unit
def test_schema_has_tags_false_without_tags():
    data = {"name": "orders", "columns": [{"name": "id", "type": "BIGINT"}]}
    assert usf.schema_has_tags(data) is False
    assert usf.schema_has_tags({}) is False
    assert usf.schema_has_tags("not a dict") is False
