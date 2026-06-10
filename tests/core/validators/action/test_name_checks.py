"""Unit tests for the shared 3-part-name check.

Mirrors :mod:`lhp.core.validators.action._name_checks` (constitution §8.2).
"""

import pytest

from lhp.core.validators.action._name_checks import require_three_part_name


@pytest.mark.unit
def test_reference_label_non_three_part():
    result = require_three_part_name("foo.bar", "'reference'", "Action 'a'")
    assert result == (
        "Action 'a': 'reference' must be a 3-part name "
        "(catalog.schema.table), got 'foo.bar'"
    )


@pytest.mark.unit
def test_delta_sink_label_non_three_part():
    result = require_three_part_name("justname", "Delta sink 'tableName'", "Action 'a'")
    assert result == (
        "Action 'a': Delta sink 'tableName' must be a 3-part name "
        "(catalog.schema.table), got 'justname'"
    )


@pytest.mark.unit
def test_quarantine_label_non_three_part():
    result = require_three_part_name("a.b.c.d", "quarantine.dlq_table", "Action 'a'")
    assert result == (
        "Action 'a': quarantine.dlq_table must be a 3-part name "
        "(catalog.schema.table), got 'a.b.c.d'"
    )


@pytest.mark.unit
def test_valid_three_part_name_returns_none():
    assert require_three_part_name("cat.sch.tbl", "'source'", "Action 'a'") is None


@pytest.mark.unit
@pytest.mark.parametrize("value", [5, {"k": "v"}, None])
def test_non_str_returns_none(value):
    assert require_three_part_name(value, "'source'", "Action 'a'") is None
