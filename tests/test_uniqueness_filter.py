"""Tests for uniqueness test with filter support."""

import pytest

from lhp.generators.test import TestActionGenerator
from lhp.models import Action, ActionType


class TestUniquenessFilter:
    """Test filter support for uniqueness tests."""

    def test_action_model_accepts_filter(self):
        """Test that Action model accepts optional filter field."""
        # Test with filter
        action = Action(
            name="test_active_unique",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="customer_dim",
            columns=["customer_id"],
            filter="__END_AT IS NULL",  # Optional filter field
        )

        assert action.filter == "__END_AT IS NULL"
        assert action.name == "test_active_unique"
        assert action.columns == ["customer_id"]

    def test_action_model_filter_is_optional(self):
        """Test that filter field is optional and defaults to None."""
        # Test without filter - should work (backward compatibility)
        action = Action(
            name="test_unique",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="customer_table",
            columns=["id"],
        )

        assert action.filter is None  # Should default to None
        assert action.name == "test_unique"
        assert action.columns == ["id"]

    def test_filter_with_complex_conditions(self):
        """Test that filter can handle complex SQL conditions."""
        action = Action(
            name="test_complex_filter",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="products",
            columns=["product_code", "region"],
            filter="status = 'ACTIVE' AND effective_date <= current_date() AND region IN ('US', 'EU')",
        )

        assert (
            action.filter
            == "status = 'ACTIVE' AND effective_date <= current_date() AND region IN ('US', 'EU')"
        )

    def test_end_to_end_with_filter(self):
        """Test end-to-end code generation with filter."""
        action = Action(
            name="test_customer_active",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="silver.customer_dim",
            columns=["customer_id"],
            filter="is_current = true",
            on_violation="fail",
        )

        generator = TestActionGenerator()
        code = generator.generate(action=action)

        # Verify generated code
        assert "from pyspark import pipelines as dp" in code
        assert "@dp.expect_all_or_fail" in code
        assert "WHERE is_current = true" in code
        assert "GROUP BY customer_id" in code
        assert "no_duplicates" in code
        assert "duplicate_count == 0" in code
