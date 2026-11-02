"""How a rule's DLT category is decided, and the parser's fail-closed invariant.

``list_rule_category`` / ``mapping_rule_category`` are shared by the
expectations-file validator and ``DQEParser``. A rule the validator would
reject is an internal error in the parser, never a silent warn expectation.
"""

import pytest

from lhp.core.processing import (
    DQEParser,
    list_rule_category,
    mapping_rule_category,
)

pytestmark = pytest.mark.unit


class TestListRuleCategory:
    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            ({"failureAction": "Fail"}, ("failureAction", "Fail", "expect_or_fail")),
            ({"failureAction": "drop"}, ("failureAction", "drop", "expect_or_drop")),
            ({"failureAction": "WARN"}, ("failureAction", "WARN", "expect")),
            ({"failureAction": "fial"}, ("failureAction", "fial", None)),
            ({"type": "expect_or_drop"}, ("type", "expect_or_drop", "expect_or_drop")),
            ({"type": "Expect"}, ("type", "Expect", None)),
            ({}, ("type", "expect", "expect")),
            (
                {"failureAction": None, "type": "expect_or_fail"},
                ("type", "expect_or_fail", "expect_or_fail"),
            ),
            ({"failureAction": "", "type": "bogus"}, ("type", "bogus", None)),
            (
                {"failureAction": "drop", "type": "bogus"},
                ("failureAction", "drop", "expect_or_drop"),
            ),
            ({"action": "drop"}, ("action", "drop", None)),
            ({"action": "drop", "failureAction": "drop"}, ("action", "drop", None)),
        ],
    )
    def test_category(self, rule, expected):
        assert list_rule_category(rule) == expected


class TestMappingRuleCategory:
    @pytest.mark.parametrize(
        ("config", "expected"),
        [
            ({"action": "FAIL"}, ("FAIL", "expect_or_fail")),
            ({"action": "drop"}, ("drop", "expect_or_drop")),
            ({"action": "warn"}, ("warn", "expect")),
            ({}, (None, "expect")),
            ({"action": None}, (None, "expect")),
            ({"action": "fial"}, ("fial", None)),
        ],
    )
    def test_category(self, config, expected):
        assert mapping_rule_category(config) == expected


class TestParserFailsClosed:
    @pytest.mark.parametrize(
        ("rule", "match"),
        [
            ({"constraint": "a > 0", "failureAction": "fial"}, "failureAction='fial'"),
            ({"constraint": "a > 0", "action": "drop"}, "action='drop'"),
        ],
    )
    def test_unvalidated_list_rule(self, rule, match):
        with pytest.raises(AssertionError, match=match):
            DQEParser().parse_expectations([rule])

    def test_unvalidated_mapping_rule(self):
        with pytest.raises(AssertionError, match="action='fial'"):
            DQEParser().parse_expectations({"a > 0": {"action": "fial"}})

    def test_null_actions_mean_the_default(self):
        assert DQEParser().parse_expectations(
            [{"name": "a", "constraint": "a > 0", "failureAction": None}]
        ) == ({"a": "a > 0"}, {}, {})
        assert DQEParser().parse_expectations({"b > 0": {"action": None}}) == (
            {"b > 0": "b > 0"},
            {},
            {},
        )
