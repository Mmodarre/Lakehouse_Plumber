"""``DQEParser.parse_expectations`` over both expectation-file formats.

The list format is covered by ``tests/test_dqe.py`` and
``tests/test_dqe_edge_cases.py``. These tests cover the mapping format (each
constraint mapped to its ``action`` and ``name``) and that both formats give the
same categories.
"""

import pytest

from lhp.core.processing import DQEParser

pytestmark = pytest.mark.unit


class TestConstraintMapping:
    def test_actions_split_into_warn_drop_and_fail(self):
        expect_all, expect_all_or_drop, expect_all_or_fail = (
            DQEParser().parse_expectations(
                {
                    "id IS NOT NULL": {"action": "fail", "name": "id_check"},
                    "age > 0": {"action": "drop", "name": "age_check"},
                    "email IS NOT NULL": {"action": "warn", "name": "email_check"},
                }
            )
        )

        assert expect_all == {"email_check": "email IS NOT NULL"}
        assert expect_all_or_drop == {"age_check": "age > 0"}
        assert expect_all_or_fail == {"id_check": "id IS NOT NULL"}

    def test_action_is_case_insensitive(self):
        _, expect_all_or_drop, expect_all_or_fail = DQEParser().parse_expectations(
            {
                "a > 0": {"action": "FAIL", "name": "a"},
                "b > 0": {"action": "Drop", "name": "b"},
            }
        )

        assert expect_all_or_fail == {"a": "a > 0"}
        assert expect_all_or_drop == {"b": "b > 0"}

    def test_missing_action_warns(self):
        expect_all, expect_all_or_drop, expect_all_or_fail = (
            DQEParser().parse_expectations({"a > 0": {"name": "a"}})
        )

        assert expect_all == {"a": "a > 0"}
        assert expect_all_or_drop == {}
        assert expect_all_or_fail == {}

    def test_name_defaults_to_the_constraint(self):
        _, expect_all_or_drop, _ = DQEParser().parse_expectations(
            {"amount > 0": {"action": "drop"}}
        )

        assert expect_all_or_drop == {"amount > 0": "amount > 0"}

    def test_file_order_is_kept(self):
        expect_all, _, _ = DQEParser().parse_expectations(
            {
                "c > 0": {"action": "warn", "name": "third"},
                "a > 0": {"action": "warn", "name": "first"},
                "b > 0": {"action": "warn", "name": "second"},
            }
        )

        assert list(expect_all) == ["third", "first", "second"]

    def test_empty_mapping_has_no_rules(self):
        assert DQEParser().parse_expectations({}) == ({}, {}, {})


def test_both_formats_give_the_same_categories():
    rule_list = [
        {"name": "id_check", "constraint": "id IS NOT NULL", "failureAction": "fail"},
        {"name": "age_check", "expression": "age > 0", "failureAction": "drop"},
        {"name": "email_check", "constraint": "email LIKE '%@%'"},
    ]
    constraint_mapping = {
        "id IS NOT NULL": {"action": "fail", "name": "id_check"},
        "age > 0": {"action": "drop", "name": "age_check"},
        "email LIKE '%@%'": {"name": "email_check"},
    }

    parser = DQEParser()

    assert parser.parse_expectations(rule_list) == parser.parse_expectations(
        constraint_mapping
    )
