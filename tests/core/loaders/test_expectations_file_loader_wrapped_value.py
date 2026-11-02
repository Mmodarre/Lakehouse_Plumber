"""``load_expectations_file`` checks the value of a wrapped ``expectations`` key.

Before, ``{"expectations": X}`` returned X unchecked: a null value generated a
view with no rules, and a string crashed the parser. X must now be a list or a
mapping, like a bare document. Real files on disk, no mocks.
"""

import pytest

from lhp.core.loaders import load_expectations_file
from lhp.errors import LHPValidationError

pytestmark = pytest.mark.unit


@pytest.mark.parametrize(
    ("text", "actual_type"),
    [
        ("expectations: null\n", "NoneType"),
        ("expectations:\n", "NoneType"),
        ("expectations: id > 0\n", "str"),
        ("version: '1.0'\nexpectations: 3\n", "int"),
    ],
    ids=["null", "empty", "string", "int"],
)
def test_wrapped_value_that_is_not_a_list_or_mapping(tmp_path, text, actual_type):
    (tmp_path / "rules.yaml").write_text(text)

    with pytest.raises(LHPValidationError) as exc_info:
        load_expectations_file("rules.yaml", tmp_path)

    error = exc_info.value
    assert error.code == "LHP-VAL-008"
    assert error.details == (
        "Expectations file 'rules.yaml' must hold a mapping or a list of "
        f"expectations, but its 'expectations' key holds a {actual_type}."
    )
    assert error.context["Actual Type"] == actual_type


@pytest.mark.parametrize("value", [[], {}], ids=["empty-list", "empty-mapping"])
def test_empty_wrapped_list_or_mapping_is_returned(tmp_path, value):
    (tmp_path / "rules.yaml").write_text(f"expectations: {value!r}\n")

    assert load_expectations_file("rules.yaml", tmp_path) == value
