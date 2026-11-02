"""The ``mode`` → ``readMode`` hint in unknown-field errors (LHP-CFG-001).

``mode`` is pointed at ``readMode`` only in a section that accepts
``readMode``. A materialized_view or sink ``write_target`` has no read mode, so
a ``mode`` there was never a misspelt one.
"""

import pytest

from lhp.core.validators.field._field_catalog import (
    ACTION_FIELDS,
    LOAD_SOURCE_FIELDS,
    WRITE_TARGET_FIELDS,
)
from lhp.core.validators.field._field_suggestions import raise_unknown_fields_error
from lhp.errors import LHPConfigError

pytestmark = pytest.mark.unit


def _example(unknown: str, expected_fields: set, section: str) -> str:
    with pytest.raises(LHPConfigError) as exc_info:
        raise_unknown_fields_error("act", section, {unknown}, expected_fields, section)
    return exc_info.value.example


@pytest.mark.parametrize("unknown", ["mode", "read_mode"])
def test_read_mode_hint_where_read_mode_is_valid(unknown):
    example = _example(unknown, LOAD_SOURCE_FIELDS["cloudfiles"], "source")

    assert f"'{unknown}' → 'readMode'" in example


def test_action_level_read_mode_still_hints():
    assert "'read_mode' → 'readMode'" in _example("read_mode", ACTION_FIELDS, "action")


@pytest.mark.parametrize("target_type", ["materialized_view", "sink"])
def test_no_read_mode_hint_where_read_mode_is_not_valid(target_type):
    example = _example("mode", WRITE_TARGET_FIELDS[target_type], "write_target")

    assert "readMode" not in example
