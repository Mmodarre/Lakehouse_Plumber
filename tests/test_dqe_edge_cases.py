"""Tests for DQE edge cases and error handling."""

import logging

from lhp.core.processing.dqe import DQEParser


class TestDQEParseExpectationsEdgeCases:
    def test_parse_expectations_missing_constraint(self, caplog):
        """Test handling of expectations without constraint/expression."""
        parser = DQEParser()

        expectations = [
            {"type": "expect", "message": "Test expectation without constraint"},
            {"type": "expect", "constraint": "col > 0", "message": "Valid expectation"},
        ]

        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(
                expectations
            )

        assert len(expect_all) == 1
        assert "Valid expectation" in expect_all
        assert expect_all["Valid expectation"] == "col > 0"

        assert "Expectation missing constraint/expression" in caplog.text
        assert "Test expectation without constraint" in caplog.text

    def test_parse_expectations_unknown_type(self, caplog):
        """Test handling of unknown expectation types."""
        parser = DQEParser()

        expectations = [
            {
                "type": "unknown_type",
                "constraint": "col IS NOT NULL",
                "message": "Unknown type expectation",
            },
            {"type": "expect", "constraint": "col > 0", "message": "Valid expectation"},
        ]

        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(
                expectations
            )

        assert len(expect_all) == 1
        assert "Valid expectation" in expect_all

        assert "Unknown expectation type: unknown_type" in caplog.text

    def test_parse_expectations_failure_action_mapping(self):
        parser = DQEParser()

        expectations = [
            {
                "failureAction": "fail",
                "constraint": "id IS NOT NULL",
                "message": "ID required",
            },
            {"failureAction": "drop", "constraint": "age > 0", "message": "Valid age"},
            {
                "failureAction": "warn",
                "constraint": "email IS NOT NULL",
                "message": "Email preferred",
            },
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert "ID required" in expect_fail
        assert expect_fail["ID required"] == "id IS NOT NULL"

        assert "Valid age" in expect_drop
        assert expect_drop["Valid age"] == "age > 0"

        assert "Email preferred" in expect_all
        assert expect_all["Email preferred"] == "email IS NOT NULL"

    def test_parse_expectations_expression_field(self):
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "expression": "col IS NOT NULL",
                "message": "Expression field test",
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert "Expression field test" in expect_all
        assert expect_all["Expression field test"] == "col IS NOT NULL"

    def test_parse_expectations_no_message_fallback(self):
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "constraint": "col > 0",
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert "Constraint failed: col > 0" in expect_all
        assert expect_all["Constraint failed: col > 0"] == "col > 0"

    def test_parse_expectations_name_field_as_message(self):
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "constraint": "col IS NOT NULL",
                "name": "not_null_check",
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert "not_null_check" in expect_all
        assert expect_all["not_null_check"] == "col IS NOT NULL"

    def test_parse_expectations_empty_list(self):
        """Test handling of empty expectations list."""
        parser = DQEParser()

        expectations = []

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        assert expect_all == {}
        assert expect_drop == {}
        assert expect_fail == {}

    def test_parse_expectations_multiple_missing_constraints(self, caplog):
        """Test handling multiple expectations with missing constraints."""
        parser = DQEParser()

        expectations = [
            {"type": "expect", "message": "No constraint 1"},
            {"type": "expect", "message": "No constraint 2"},
            {"type": "expect", "constraint": "col > 0", "message": "Valid one"},
        ]

        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(
                expectations
            )

        assert len(expect_all) == 1
        assert "Valid one" in expect_all

        warning_messages = [
            record.message for record in caplog.records if record.levelname == "WARNING"
        ]
        assert len(warning_messages) == 2
        assert all("missing constraint/expression" in msg for msg in warning_messages)
