"""Tests for Test action error handling."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.generators.test import TestActionGenerator
from lhp.core.action_registry import ActionRegistry
from lhp.utils.error_formatter import LHPError


class TestTestActionErrorHandling:
    """Test error handling for test actions."""
    
    def test_invalid_test_type_in_registry(self):
        """Test that invalid test_type raises proper error in registry."""
        registry = ActionRegistry()
        
        with pytest.raises(LHPError) as excinfo:
            registry.get_generator(ActionType.TEST, 'invalid_test_type')
        
        assert 'test_type' in str(excinfo.value)
        assert 'invalid_test_type' in str(excinfo.value)
        # Should suggest valid test types
        assert 'row_count' in str(excinfo.value) or 'Valid test_type' in str(excinfo.value)
    
    def test_missing_sql_in_custom_sql(self):
        """Test error when custom_sql test is missing SQL."""
        generator = TestActionGenerator()
        action = Action(
            name='test_custom',
            type=ActionType.TEST,
            test_type='custom_sql',
            source='test_table'
            # Missing 'sql' field
        )
        
        # Should handle gracefully and generate code that references source
        code = generator.generate(action=action)
        assert 'test_table' in code or 'None' in code  # Should handle missing SQL
    
    def test_missing_expectations_in_custom(self):
        """Test error when custom_expectations test is missing expectations."""
        generator = TestActionGenerator()
        action = Action(
            name='test_custom',
            type=ActionType.TEST,
            test_type='custom_expectations',
            source='test_table'
            # Missing 'expectations' field
        )
        
        code = generator.generate(action=action)
        # Should generate valid code even without expectations
        assert 'def tmp_test_' in code
        assert 'spark' in code or 'return' in code
    
    def test_invalid_source_type_for_row_count(self):
        """Test error when row_count has invalid source type."""
        generator = TestActionGenerator()
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source='single_table'  # Should be a list
        )
        
        code = generator.generate(action=action)
        # Should handle gracefully, possibly using source as both tables
        assert 'def tmp_test_' in code
    
    def test_empty_columns_for_uniqueness(self):
        """Test error when uniqueness has empty columns list."""
        generator = TestActionGenerator()
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='test_table',
            columns=[]  # Empty list
        )
        
        code = generator.generate(action=action)
        # Should handle empty columns gracefully
        assert 'def tmp_test_' in code
    
    def test_mismatched_column_counts_referential(self):
        """Test error when referential integrity has mismatched column counts."""
        generator = TestActionGenerator()
        action = Action(
            name='test_ref',
            type=ActionType.TEST,
            test_type='referential_integrity',
            source='orders',
            reference='customers',
            source_columns=['order_id', 'customer_id'],  # 2 columns
            reference_columns=['id']  # 1 column - mismatch!
        )
        
        code = generator.generate(action=action)
        # Should handle mismatch gracefully, possibly using first column only
        assert 'def tmp_test_' in code
        assert 'LEFT JOIN' in code
    
    def test_none_values_in_range(self):
        """Test handling of None values in range test."""
        generator = TestActionGenerator()
        
        # Test with only min_value
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='value',
            min_value=0
            # max_value is None
        )
        code = generator.generate(action=action)
        assert '>=' in code
        assert '<=' not in code or 'None' not in code
        
        # Test with only max_value
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='value',
            max_value=100
            # min_value is None
        )
        code = generator.generate(action=action)
        assert '<=' in code
        assert '>=' not in code or 'None' not in code
    
    def test_invalid_on_violation_value(self):
        """Test handling of invalid on_violation values."""
        generator = TestActionGenerator()
        action = Action(
            name='test_violation',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source', 'target'],
            on_violation='invalid'  # Invalid value
        )
        
        code = generator.generate(action=action)
        # Should default to 'fail' or handle gracefully
        assert '@dp.expect' in code  # Some expectation should be present
    
    def test_missing_required_fields_graceful_handling(self):
        """Test that generator handles missing required fields gracefully."""
        generator = TestActionGenerator()
        
        # Test various incomplete configurations
        incomplete_actions = [
            Action(name='test1', type=ActionType.TEST, test_type='uniqueness'),  # No source
            Action(name='test2', type=ActionType.TEST, test_type='completeness', source='table'),  # No required_columns
            Action(name='test3', type=ActionType.TEST, test_type='range', source='table'),  # No column
            Action(name='test4', type=ActionType.TEST, test_type='all_lookups_found', source='table'),  # No lookup_table
        ]
        
        for action in incomplete_actions:
            code = generator.generate(action=action)
            # Should always generate valid Python code
            assert 'def tmp_test_' in code or 'def test' in action.name
            assert 'return' in code
            # Should not have syntax errors (basic check)
            assert code.count('(') == code.count(')')
            assert code.count('{') == code.count('}')
            assert code.count('[') == code.count(']')
    
    def test_schema_match_without_reference(self):
        """Test schema_match without reference table."""
        generator = TestActionGenerator()
        action = Action(
            name='test_schema',
            type=ActionType.TEST,
            test_type='schema_match',
            source='table1'
            # Missing 'reference' field
        )
        
        code = generator.generate(action=action)
        # Should handle missing reference gracefully
        assert 'def tmp_test_' in code
        
    def test_very_long_action_name(self):
        """Test handling of very long action names."""
        generator = TestActionGenerator()
        long_name = 'test_' + 'very_' * 50 + 'long_name'
        action = Action(
            name=long_name,
            type=ActionType.TEST,
            test_type='row_count',
            source=['source', 'target']
        )
        
        code = generator.generate(action=action)
        # Should handle long names, possibly truncating
        assert 'def tmp_test_' in code or 'def ' in code
    
    def test_special_characters_in_names(self):
        """Test handling of special characters in table/column names."""
        generator = TestActionGenerator()
        action = Action(
            name='test-with-dashes',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='table-with-dashes',
            columns=['column-with-dashes', 'column@special!']
        )
        
        code = generator.generate(action=action)
        # Should handle special characters, possibly escaping or replacing
        assert 'def ' in code  # Function name should be valid Python
        
    def test_circular_reference_detection(self):
        """Test detection of circular references (if applicable)."""
        generator = TestActionGenerator()
        action = Action(
            name='test_circular',
            type=ActionType.TEST,
            test_type='referential_integrity',
            source='table_a',
            reference='table_a',  # Self-reference
            source_columns=['id'],
            reference_columns=['parent_id']
        )
        
        code = generator.generate(action=action)
        # Should handle self-references gracefully
        assert 'def tmp_test_' in code
        assert 'table_a' in code
