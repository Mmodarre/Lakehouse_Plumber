"""``expectation_names`` lists the name the parser gives each rule.

The duplicate-name check relies on it, so it must agree with
``DQEParser.parse_expectations`` for both file formats.
"""

import pytest

from lhp.core.processing import DQEParser, expectation_names

pytestmark = pytest.mark.unit


def test_list_rule_is_named_by_message_then_name_then_constraint():
    rules = [
        {"name": "n1", "message": "m1", "constraint": "a > 0"},
        {"name": "n2", "constraint": "b > 0"},
        {"constraint": "c > 0"},
        {"expression": "d > 0"},
    ]

    assert expectation_names(rules) == [
        "m1",
        "n2",
        "Constraint failed: c > 0",
        "Constraint failed: d > 0",
    ]


def test_mapping_rule_is_named_by_name_then_constraint():
    rules = {"a > 0": {"action": "fail", "name": "n1"}, "b > 0": {"action": "drop"}}

    assert expectation_names(rules) == ["n1", "b > 0"]


def test_mapping_rule_with_a_null_or_empty_name_is_named_by_its_constraint():
    rules = {
        "a > 0": {"action": "fail", "name": None},
        "b > 0": {"action": "drop", "name": ""},
    }

    assert expectation_names(rules) == ["a > 0", "b > 0"]
    assert DQEParser().parse_expectations(rules) == (
        {},
        {"b > 0": "b > 0"},
        {"a > 0": "a > 0"},
    )


def test_quarantine_names_a_null_named_mapping_rule_by_its_constraint():
    rules = {"a > 0": {"action": "fail", "name": None}}

    assert DQEParser().get_all_expectations_as_drop(rules) == {"a > 0": "a > 0"}


def test_dqe_mode_keys_a_list_rule_by_message_as_it_always_has():
    """The key is the expectation's name in the event log: it must not change."""
    rule = {
        "name": "has_id",
        "message": "Order id must be present",
        "constraint": "id IS NOT NULL",
        "failureAction": "fail",
    }

    assert DQEParser().parse_expectations([rule]) == (
        {},
        {},
        {"Order id must be present": "id IS NOT NULL"},
    )
    assert expectation_names([rule]) == ["Order id must be present"]


def test_quarantine_keeps_naming_a_list_rule_by_name_first():
    rule = {"name": "n1", "message": "m1", "constraint": "a > 0"}

    assert list(DQEParser().get_all_expectations_as_drop([rule])) == ["n1"]


def test_duplicates_are_listed_once_per_rule():
    rules = [
        {"name": "same", "constraint": "a > 0", "failureAction": "warn"},
        {"name": "same", "constraint": "b > 0", "failureAction": "warn"},
    ]

    assert expectation_names(rules) == ["same", "same"]
    warn, _, _ = DQEParser().parse_expectations(rules)
    assert warn == {"same": "b > 0"}


@pytest.mark.parametrize(
    "rules",
    [
        [
            {"name": "w", "constraint": "a > 0", "failureAction": "warn"},
            {"name": "d", "constraint": "b > 0", "type": "expect_or_drop"},
            {"name": "f", "constraint": "c > 0", "failureAction": "FAIL"},
        ],
        {
            "a > 0": {"action": "warn", "name": "w"},
            "b > 0": {"action": "drop", "name": "d"},
            "c > 0": {"action": "fail", "name": "f"},
        },
    ],
    ids=["list", "mapping"],
)
def test_names_match_the_parsed_result(rules):
    parsed = DQEParser().parse_expectations(rules)

    assert expectation_names(rules) == [name for c in parsed for name in c]
    assert parsed == (
        {"w": "a > 0"},
        {"d": "b > 0"},
        {"f": "c > 0"},
    )


def test_unvalidated_rule_is_an_internal_error():
    with pytest.raises(AssertionError, match="failureAction='fial'"):
        expectation_names([{"constraint": "a > 0", "failureAction": "fial"}])
