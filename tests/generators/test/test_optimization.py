"""Tests for Test action SQL optimizations."""

import pytest
from lhp.models import Action, ActionType
from lhp.core.registry import ActionRegistry

# Each test type now resolves to its own stateless leaf generator. These tests
# obtain the correct leaf via the registry (keyed by the action's test_type) and
# drive it with the stateless ``generate(action, context)`` contract.
registry = ActionRegistry()


def _generate(action):
    """Resolve the leaf for this action's test_type and generate its code."""
    generator = registry.get_generator(ActionType.TEST, action.test_type)
    return generator.generate(action=action, context={})


class TestTestActionOptimization:
    """Test SQL optimizations for test actions."""

    def test_completeness_selects_only_required_columns(self):
        """Test that completeness test only selects required columns."""
        action = Action(
            name='test_completeness',
            type=ActionType.TEST,
            test_type='completeness',
            source='test_table',
            required_columns=['col1', 'col2', 'col3']
        )

        code = _generate(action)

        # Should select only the required columns
        assert 'SELECT col1, col2, col3' in code
        # Should NOT select all columns
        assert 'SELECT *' not in code
        # Should have the correct table
        assert 'FROM test_table' in code

    def test_range_selects_only_tested_column(self):
        """Test that range test only selects the column being tested."""
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='date_column',
            min_value='2020-01-01',
            max_value='2024-12-31'
        )

        code = _generate(action)

        # Should select only the column being tested
        assert 'SELECT date_column' in code
        # Should NOT select all columns
        assert 'SELECT *' not in code
        # Should have the correct table
        assert 'FROM test_table' in code

    def test_completeness_empty_columns_fallback(self):
        """Test that completeness falls back to * if no columns specified."""
        action = Action(
            name='test_completeness',
            type=ActionType.TEST,
            test_type='completeness',
            source='test_table',
            required_columns=[]  # Empty list
        )

        code = _generate(action)

        # Should fall back to SELECT * when no columns specified
        assert 'SELECT *' in code

    def test_range_no_column_fallback(self):
        """Test that range falls back to * if no column specified."""
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            min_value=0,
            max_value=100
            # No column specified
        )

        code = _generate(action)

        # Should fall back to SELECT * when no column specified
        assert 'SELECT *' in code

    def test_other_tests_unchanged(self):
        """Test that other test types are not affected by optimization."""
        # Row count should still use its specific SQL pattern
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source_table', 'target_table']
        )
        code = _generate(action)
        assert 'COUNT(*)' in code

        # Uniqueness should still check for duplicates
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='test_table',
            columns=['id']
        )
        code = _generate(action)
        assert 'GROUP BY' in code
        assert 'HAVING COUNT(*) > 1' in code
