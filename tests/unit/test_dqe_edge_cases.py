"""Tests for DQE edge cases and error handling."""

import pytest
import tempfile
import yaml
from pathlib import Path
from unittest.mock import patch
import logging

from lhp.utils.dqe import DQEParser


class TestDQEParseExpectationsEdgeCases:
    """Test DQE parse_expectations method edge cases - targeting coverage lines 39-43, 47-48, 52, 61."""
    
    def test_parse_expectations_missing_constraint(self, caplog):
        """Test handling of expectations without constraint/expression (line 43)."""
        parser = DQEParser()
        
        expectations = [
            {
                "type": "expect",
                "message": "Test expectation without constraint"
            },
            {
                "type": "expect",
                "constraint": "col > 0",
                "message": "Valid expectation"
            }
        ]
        
        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)
        
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
                "message": "Unknown type expectation"
            },
            {
                "type": "expect",
                "constraint": "col > 0",
                "message": "Valid expectation"
            }
        ]
        
        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)
        
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
                "message": "ID required"
            },
            {
                "failureAction": "drop",
                "constraint": "age > 0",
                "message": "Valid age"
            },
            {
                "failureAction": "warn",
                "constraint": "email IS NOT NULL",
                "message": "Email preferred"
            }
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
                "message": "Expression field test"
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
                "constraint": "col > 0"
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
                "name": "not_null_check"
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
            {"type": "expect", "constraint": "col > 0", "message": "Valid one"}
        ]
        
        with caplog.at_level(logging.WARNING):
            expect_all, expect_drop, expect_fail = parser.parse_expectations(expectations)
        
        # Should only have the valid expectation
        assert len(expect_all) == 1
        assert "Valid one" in expect_all
        
        # Should log warnings for both missing constraints
        warning_messages = [record.message for record in caplog.records if record.levelname == 'WARNING']
        assert len(warning_messages) == 2
        assert all("missing constraint/expression" in msg for msg in warning_messages)


class TestDQELoadExpectationsFromFile:
    """Test DQE load_expectations_from_file method."""
    
    def test_load_expectations_from_file_success(self, caplog):
        """Test successful loading of expectations from file."""
        parser = DQEParser()
        
        expectations_data = {
            "expectations": [
                {
                    "name": "not_null_check",
                    "constraint": "col IS NOT NULL",
                    "type": "expect"
                },
                {
                    "name": "positive_check",
                    "constraint": "col > 0",
                    "type": "expect_or_fail"
                }
            ]
        }
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(expectations_data, f)
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with caplog.at_level(logging.INFO, logger="lhp.utils.dqe"):
                expectations = parser.load_expectations_from_file(yaml_file)
            
            # Should return list of expectations
            assert len(expectations) == 2
            assert expectations[0]["name"] == "not_null_check"
            assert expectations[1]["name"] == "positive_check"
            
            # Should log info message
            assert f"Loaded 2 expectations from {yaml_file}" in caplog.text
        finally:
            yaml_file.unlink()
    
    def test_load_expectations_from_file_not_found(self):
        """Test error handling when expectations file doesn't exist."""
        parser = DQEParser()
        
        non_existent_file = Path("/non/existent/expectations.yaml")
        
        with pytest.raises(FileNotFoundError) as exc_info:
            parser.load_expectations_from_file(non_existent_file)
        
        assert "Expectations file not found" in str(exc_info.value)
        assert str(non_existent_file) in str(exc_info.value)
    
    def test_load_expectations_from_file_invalid_yaml(self):
        """Test error handling for invalid YAML in expectations file."""
        parser = DQEParser()
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            f.write("invalid: yaml: content: [")  # Invalid YAML
            f.flush()
            yaml_file = Path(f.name)
        
        try:
            with pytest.raises(ValueError) as exc_info:
                parser.load_expectations_from_file(yaml_file)
            
            assert "Invalid YAML in expectations file" in str(exc_info.value)
        finally:
            yaml_file.unlink()


class TestDQEValidateExpectations:
    """Test DQE validate_expectations method."""
    
    def test_validate_expectations_success(self):
        """Test successful validation of expectations."""
        parser = DQEParser()
        
        expectations = [
            {
                "constraint": "col IS NOT NULL",
                "type": "expect",
                "message": "Column should not be null"
            },
            {
                "expression": "col > 0",
                "failureAction": "fail",
                "message": "Column should be positive"
            }
        ]
        
        errors = parser.validate_expectations(expectations)
        
        # Should have no validation errors
        assert errors == []
    
    def test_validate_expectations_missing_constraint(self):
        """Test validation error for missing constraint/expression."""
        parser = DQEParser()
        
        expectations = [
            {
                "type": "expect",
                "message": "Missing constraint"
            }
        ]
        
        errors = parser.validate_expectations(expectations)
        
        # Should have validation error
        assert len(errors) == 1
        assert "Missing 'constraint' or 'expression' field" in errors[0]
        assert "Expectation 0" in errors[0]
    
    def test_validate_expectations_invalid_failure_action(self):
        """Test validation error for invalid failureAction."""
        parser = DQEParser()
        
        expectations = [
            {
                "constraint": "col > 0",
                "failureAction": "invalid_action",
                "message": "Invalid action"
            }
        ]
        
        errors = parser.validate_expectations(expectations)
        
        # Should have validation error
        assert len(errors) == 1
        assert "Invalid failureAction 'invalid_action'" in errors[0]
        assert "Must be one of: fail, drop, warn" in errors[0]
    
    def test_validate_expectations_invalid_type(self):
        """Test validation error for invalid expectation type."""
        parser = DQEParser()
        
        expectations = [
            {
                "constraint": "col > 0",
                "type": "invalid_type",
                "message": "Invalid type"
            }
        ]
        
        errors = parser.validate_expectations(expectations)
        
        # Should have validation error
        assert len(errors) == 1
        assert "Invalid type 'invalid_type'" in errors[0]
        assert "Must be one of: ['expect', 'expect_or_drop', 'expect_or_fail']" in errors[0] 