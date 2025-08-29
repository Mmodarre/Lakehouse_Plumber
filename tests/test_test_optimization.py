"""Tests for Test action SQL optimizations."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.generators.test import TestActionGenerator


class TestTestActionOptimization:
    """Test SQL optimizations for test actions."""
    
    def test_completeness_selects_only_required_columns(self):
        """Test that completeness test only selects required columns."""
        generator = TestActionGenerator()
        action = Action(
            name='test_completeness',
            type=ActionType.TEST,
            test_type='completeness',
            source='test_table',
            required_columns=['col1', 'col2', 'col3']
        )
        
        code = generator.generate(action=action)
        
        # Should select only the required columns
        assert 'SELECT col1, col2, col3' in code
        # Should NOT select all columns
        assert 'SELECT *' not in code
        # Should have the correct table
        assert 'FROM test_table' in code
    
    def test_range_selects_only_tested_column(self):
        """Test that range test only selects the column being tested."""
        generator = TestActionGenerator()
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='date_column',
            min_value='2020-01-01',
            max_value='2024-12-31'
        )
        
        code = generator.generate(action=action)
        
        # Should select only the column being tested
        assert 'SELECT date_column' in code
        # Should NOT select all columns
        assert 'SELECT *' not in code
        # Should have the correct table
        assert 'FROM test_table' in code
    
    def test_completeness_empty_columns_fallback(self):
        """Test that completeness falls back to * if no columns specified."""
        generator = TestActionGenerator()
        action = Action(
            name='test_completeness',
            type=ActionType.TEST,
            test_type='completeness',
            source='test_table',
            required_columns=[]  # Empty list
        )
        
        code = generator.generate(action=action)
        
        # Should fall back to SELECT * when no columns specified
        assert 'SELECT *' in code
    
    def test_range_no_column_fallback(self):
        """Test that range falls back to * if no column specified."""
        generator = TestActionGenerator()
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            min_value=0,
            max_value=100
            # No column specified
        )
        
        code = generator.generate(action=action)
        
        # Should fall back to SELECT * when no column specified
        assert 'SELECT *' in code
    
    def test_other_tests_unchanged(self):
        """Test that other test types are not affected by optimization."""
        generator = TestActionGenerator()
        
        # Row count should still use its specific SQL pattern
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source_table', 'target_table']
        )
        code = generator.generate(action=action)
        assert 'COUNT(*)' in code
        
        # Uniqueness should still check for duplicates
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='test_table',
            columns=['id']
        )
        code = generator.generate(action=action)
        assert 'GROUP BY' in code
        assert 'HAVING COUNT(*) > 1' in code
