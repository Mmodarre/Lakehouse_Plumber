"""Tests for f-string syntax compatibility across Python versions (issues found in error_formatter.py)."""

import pytest

from lhp.errors import ErrorCategory, LHPError


class TestFStringCompatibility:
    def test_error_factory_import(self):
        from lhp.errors import ErrorFactory

        assert ErrorFactory is not None

    def test_unknown_type_with_suggestion_method_exists(self):
        from lhp.errors import ErrorFactory

        method = getattr(ErrorFactory, "unknown_type_with_suggestion", None)
        assert method is not None, "unknown_type_with_suggestion method should exist"

    def test_unknown_type_with_suggestion_works(self):
        from lhp.errors import ErrorFactory

        error = ErrorFactory.unknown_type_with_suggestion(
            value_type="action",
            provided_value="invalid_action",
            valid_values=["load", "transform", "write"],
            example_usage="action: load",
        )

        assert isinstance(error, LHPError)
        assert "ACT" in error.code
        assert "invalid_action" in error.title
        assert "load" in str(error.suggestions)

    def test_suggestion_formatting_with_quotes(self):
        from lhp.errors import ErrorFactory

        valid_values = ["test'quote", "normal", "another_one"]

        error = ErrorFactory.unknown_type_with_suggestion(
            value_type="test_type",
            provided_value="bad_value",
            valid_values=valid_values,
            example_usage="test_type: normal",
        )

        assert isinstance(error, LHPError)
        assert "bad_value" in error.title

        suggestions_text = " ".join(error.suggestions)
        assert "test'quote" in suggestions_text or "'test'quote'" in suggestions_text

    def test_empty_suggestions_list(self):
        from lhp.errors import ErrorFactory

        error = ErrorFactory.unknown_type_with_suggestion(
            value_type="action",
            provided_value="xyz123_no_match",
            valid_values=["load", "transform", "write"],
            example_usage="action: load",
        )

        assert isinstance(error, LHPError)
        assert "xyz123_no_match" in error.title

    def test_various_quote_combinations(self):
        from lhp.errors import ErrorFactory

        problematic_values = [
            "value'with'single",
            'value"with"double',
            "value with spaces",
            "value_normal",
            "123numeric",
        ]

        for value in problematic_values:
            error = ErrorFactory.unknown_type_with_suggestion(
                value_type="test",
                provided_value="bad",
                valid_values=[value],
                example_usage=f"test: {value}",
            )

            assert isinstance(error, LHPError)
            assert error.title is not None

    def test_validator_f_string_patterns(self):
        from lhp.core.validators import ConfigValidator

        validator = ConfigValidator()
        assert validator is not None

        test_users = [
            {"flowgroup": "test_flow", "action": "test_action1"},
            {"flowgroup": "another_flow", "action": "test_action2"},
        ]

        # This pattern should work without syntax errors
        user_list = [f"{u['flowgroup']}.{u['action']}" for u in test_users]
        result = f"Used by: {', '.join(user_list)}"

        expected = "Used by: test_flow.test_action1, another_flow.test_action2"
        assert result == expected
