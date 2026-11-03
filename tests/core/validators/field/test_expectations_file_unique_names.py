"""``validate_expectations_file(..., unique_names=True)`` rejects shared names.

The parsed rules are keyed by name, so two rules with one name would lose one
of them. The check is opt-in: the materialized-view path turns it on, and the
data_quality transform keeps its existing behaviour. Real files, no mocks.
"""

import pytest
import yaml

from lhp.core.validators.field import validate_expectations_file
from lhp.errors import LHPError

pytestmark = pytest.mark.unit

_PREFIX = "Action[0] 'a'"
_DUPLICATES = [
    {"name": "same", "constraint": "x > 0", "failureAction": "warn"},
    {"name": "same", "constraint": "y > 0", "failureAction": "fail"},
]


def _file(tmp_path, rules) -> str:
    (tmp_path / "rules.yaml").write_text(yaml.safe_dump(rules))
    return "rules.yaml"


def test_shared_name_is_an_error_when_names_must_be_unique(tmp_path):
    [error] = validate_expectations_file(
        _file(tmp_path, _DUPLICATES), tmp_path, _PREFIX, unique_names=True
    )

    assert isinstance(error, LHPError)
    assert error.code == "LHP-VAL-006"
    assert error.title == "Duplicate expectation name"
    assert error.suggestions == [
        "Give each expectation its own name",
        "In a list, a rule is named by 'name' when it has one, then by "
        "'message', then 'Constraint failed: <constraint>'",
    ]


def test_names_are_not_checked_by_default(tmp_path):
    assert (
        validate_expectations_file(_file(tmp_path, _DUPLICATES), tmp_path, _PREFIX)
        == []
    )


def test_distinct_names_pass(tmp_path):
    rules = [
        {"name": "a", "constraint": "x > 0"},
        {"name": "b", "constraint": "x > 0"},
    ]

    assert (
        validate_expectations_file(
            _file(tmp_path, rules), tmp_path, _PREFIX, unique_names=True
        )
        == []
    )


def test_non_text_names_are_rejected_before_the_name_check(tmp_path):
    rules = {
        "x > 0": {"action": "warn", "name": ["not", "text"]},
        "y > 0": {"action": "warn", "name": ["not", "text"]},
    }

    errors = validate_expectations_file(
        _file(tmp_path, rules), tmp_path, _PREFIX, unique_names=True
    )

    assert [
        (e.code, e.context["Expectation"], e.context["Field"])
        for e in errors
        if isinstance(e, LHPError)
    ] == [("LHP-VAL-008", "x > 0", "name"), ("LHP-VAL-008", "y > 0", "name")]
