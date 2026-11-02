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
    missing_constraint_error,
    rule_constraint,
)
from lhp.errors import LHPError

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
            ({"type": None}, ("type", "expect", "expect")),
            ({"type": ""}, ("type", "expect", "expect")),
            ({"failureAction": None, "type": None}, ("type", "expect", "expect")),
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


class TestRuleConstraint:
    @pytest.mark.parametrize(
        ("rule", "expected"),
        [
            ({"constraint": "a > 0", "expression": "b > 0"}, "a > 0"),
            ({"expression": "b > 0"}, "b > 0"),
            ({"constraint": "", "expression": "b > 0"}, "b > 0"),
            ({"name": "x"}, None),
        ],
    )
    def test_constraint_then_expression(self, rule, expected):
        assert rule_constraint(rule) == expected


class TestMissingConstraint:
    def test_mapping_rule_with_an_empty_key_raises_val_001(self):
        with pytest.raises(LHPError) as exc_info:
            DQEParser().parse_expectations({"": {"action": "drop", "name": "blank"}})

        assert exc_info.value.code == "LHP-VAL-001"
        assert exc_info.value.context == {"Expectation": "blank", "Field": "constraint"}

    def test_parser_and_validator_share_one_error(self):
        parser_error = missing_constraint_error("checks_nothing")
        validator_error = missing_constraint_error(
            "checks_nothing",
            prefix="Action[0] 'dq'",
            where="in expectations file 'rules.yaml'",
        )

        assert parser_error.details == (
            "Expectation 'checks_nothing' has no constraint, so it checks nothing."
        )
        assert validator_error.details == (
            "Action[0] 'dq': expectation 'checks_nothing' in expectations file "
            "'rules.yaml' has no constraint, so it checks nothing."
        )
        for attribute in ("code", "title", "suggestions", "context"):
            assert getattr(parser_error, attribute) == getattr(
                validator_error, attribute
            )
