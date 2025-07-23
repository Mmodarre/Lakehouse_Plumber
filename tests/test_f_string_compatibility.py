"""
Test f-string compatibility across Python versions.

This test specifically targets the f-string syntax issues discovered 
in error_formatter.py that caused problems on Windows with older Python versions.
"""

import pytest
from lhp.utils.error_formatter import ErrorCategory, LHPError


class TestFStringCompatibility:
    """Test f-string compatibility across Python versions."""

    def test_error_formatter_import(self):
        """Test that error_formatter can be imported without syntax errors."""
        # This import should work on all supported Python versions (3.8+)
        from lhp.utils.error_formatter import ErrorFormatter
        assert ErrorFormatter is not None

    def test_unknown_type_with_suggestion_method_exists(self):
        """Test that the problematic method exists and can be called."""
        from lhp.utils.error_formatter import ErrorFormatter
        
        # This method contains the f-string that was causing issues
        method = getattr(ErrorFormatter, 'unknown_type_with_suggestion', None)
        assert method is not None, "unknown_type_with_suggestion method should exist"

    def test_unknown_type_with_suggestion_works(self):
        """Test that the f-string formatting works correctly."""
        from lhp.utils.error_formatter import ErrorFormatter
        
        # Test the method that had the problematic f-string
        error = ErrorFormatter.unknown_type_with_suggestion(
            value_type="action",
            provided_value="invalid_action", 
            valid_values=["load", "transform", "write"],
            example_usage="action: load"
        )
        
        assert isinstance(error, LHPError)
        assert "ACT" in error.code  # ErrorCategory.ACTION -> "ACT"
        assert "invalid_action" in error.title
        assert "load" in str(error.suggestions)

    def test_suggestion_formatting_with_quotes(self):
        """Test the specific f-string pattern that was causing issues."""
        from lhp.utils.error_formatter import ErrorFormatter
        
        # Test with values that require quotes in the suggestion
        valid_values = ["test'quote", "normal", "another_one"]
        
        error = ErrorFormatter.unknown_type_with_suggestion(
            value_type="test_type",
            provided_value="bad_value",
            valid_values=valid_values,
            example_usage="test_type: normal"
        )
        
        # The error should be created without syntax errors
        assert isinstance(error, LHPError)
        assert "bad_value" in error.title
        
        # Check that suggestions are properly formatted
        suggestions_text = " ".join(error.suggestions)
        assert "test'quote" in suggestions_text or "'test'quote'" in suggestions_text

    def test_empty_suggestions_list(self):
        """Test edge case with empty suggestions (no close matches)."""
        from lhp.utils.error_formatter import ErrorFormatter
        
        # Use a value that won't match anything
        error = ErrorFormatter.unknown_type_with_suggestion(
            value_type="action",
            provided_value="xyz123_no_match",
            valid_values=["load", "transform", "write"],
            example_usage="action: load"
        )
        
        assert isinstance(error, LHPError)
        # Should not crash even with no suggestions
        assert "xyz123_no_match" in error.title

    def test_various_quote_combinations(self):
        """Test different quote combinations that could cause f-string issues."""
        from lhp.utils.error_formatter import ErrorFormatter
        
        # Test values with different quote types
        problematic_values = [
            "value'with'single",
            'value"with"double',
            "value with spaces",
            "value_normal",
            "123numeric",
        ]
        
        for value in problematic_values:
            error = ErrorFormatter.unknown_type_with_suggestion(
                value_type="test",
                provided_value="bad",
                valid_values=[value],
                example_usage=f"test: {value}"
            )
            
            assert isinstance(error, LHPError)
            # Should complete without syntax errors
            assert error.title is not None

    def test_validator_f_string_patterns(self):
        """Test that validator.py f-string patterns work correctly."""
        from lhp.core.validator import ConfigValidator
        
        # Test that ConfigValidator can be instantiated without f-string errors
        validator = ConfigValidator()
        assert validator is not None
        
        # Test the specific pattern that was causing issues
        # Simulating the nested f-string pattern that was problematic
        test_users = [
            {'flowgroup': 'test_flow', 'action': 'test_action1'},
            {'flowgroup': 'another_flow', 'action': 'test_action2'}
        ]
        
        # This pattern should work without syntax errors
        user_list = [f"{u['flowgroup']}.{u['action']}" for u in test_users]
        result = f"Used by: {', '.join(user_list)}"
        
        expected = "Used by: test_flow.test_action1, another_flow.test_action2"
        assert result == expected 