"""Unit tests for parsing ``LHP-<CAT>-<NNN>`` codes back into their parts."""

import pytest

from lhp.errors import ErrorCategory, parse_error_code

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("LHP-VAL-009", (ErrorCategory.VALIDATION, "009")),
        ("LHP-CFG-072", (ErrorCategory.CONFIG, "072")),
        ("LHP-IO-001", (ErrorCategory.IO, "001")),
        ("LHP-DEPR-001", (ErrorCategory.DEPRECATION, "001")),
    ],
)
def test_valid_code_parses(code, expected):
    assert parse_error_code(code) == expected


@pytest.mark.parametrize(
    "code", [None, "", "LHP-CFG", "LHP-CFG-001-extra", "VAL-009", "LHP-XYZ-001"]
)
def test_unparseable_code_returns_none(code):
    assert parse_error_code(code) is None
