"""Tests for DQE edge cases and error handling."""

import logging

from lhp.core.processing.dqe import DQEParser


class TestDQEParseExpectationsEdgeCases:
    """Test DQE parse_expectations method edge cases - targeting coverage lines 39-43, 47-48, 52, 61."""

    def test_parse_expectations_missing_constraint(self, caplog):
        """Test handling of expectations without constraint/expression (line 43)."""
        parser = DQEParser()

        expectations = [
            {"type": "expect", "message": "Test expectation without constraint"},
            {"type": "expect", "constraint": "col > 0", "message": "Valid expectation"},
        ]

        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(
                expectations
            )

        # Should skip the expectation without constraint (line 43)
        assert len(expect_all) == 1
        assert "Valid expectation" in expect_all
        assert expect_all["Valid expectation"] == "col > 0"

        # Should log warning for missing constraint
        assert "Expectation missing constraint/expression" in caplog.text
        assert "Test expectation without constraint" in caplog.text

    def test_parse_expectations_unknown_type(self, caplog):
        """Test handling of unknown expectation types (line 61)."""
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

        # Should skip the unknown type expectation (line 61)
        assert len(expect_all) == 1
        assert "Valid expectation" in expect_all

        # Should log warning for unknown type
        assert "Unknown expectation type: unknown_type" in caplog.text

    def test_parse_expectations_failure_action_mapping(self):
        """Test failureAction to expectation type mapping (lines 30-38)."""
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

        # Verify failureAction mapping
        assert "ID required" in expect_fail
        assert expect_fail["ID required"] == "id IS NOT NULL"

        assert "Valid age" in expect_drop
        assert expect_drop["Valid age"] == "age > 0"

        assert "Email preferred" in expect_all
        assert expect_all["Email preferred"] == "email IS NOT NULL"

    def test_parse_expectations_expression_field(self):
        """Test support for 'expression' field instead of 'constraint' (line 39)."""
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "expression": "col IS NOT NULL",  # Using 'expression' instead of 'constraint'
                "message": "Expression field test",
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        # Should use expression field as constraint
        assert "Expression field test" in expect_all
        assert expect_all["Expression field test"] == "col IS NOT NULL"

    def test_parse_expectations_no_message_fallback(self):
        """Test fallback message generation when no message provided (lines 47-48)."""
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "constraint": "col > 0",
                # No message provided
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        # Should generate fallback message (line 48)
        assert "Constraint failed: col > 0" in expect_all
        assert expect_all["Constraint failed: col > 0"] == "col > 0"

    def test_parse_expectations_name_field_as_message(self):
        """Test using 'name' field as message fallback (line 40)."""
        parser = DQEParser()

        expectations = [
            {
                "type": "expect",
                "constraint": "col IS NOT NULL",
                "name": "not_null_check",
                # No message field, should use name
            }
        ]

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        # Should use name field as message
        assert "not_null_check" in expect_all
        assert expect_all["not_null_check"] == "col IS NOT NULL"

    def test_parse_expectations_empty_list(self):
        """Test handling of empty expectations list."""
        parser = DQEParser()

        expectations = []

        expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)

        # Should return empty dictionaries
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

        # Should only have the valid expectation
        assert len(expect_all) == 1
        assert "Valid one" in expect_all

        # Should log warnings for both missing constraints
        warning_messages = [
            record.message for record in caplog.records if record.levelname == "WARNING"
        ]
        assert len(warning_messages) == 2
        assert all("missing constraint/expression" in msg for msg in warning_messages)
