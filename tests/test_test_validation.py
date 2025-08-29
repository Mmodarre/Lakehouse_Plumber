"""Tests for Test action validation."""

import pytest
from lhp.models.config import Action, ActionType, TestActionType, ViolationAction
from lhp.core.validator import ConfigValidator
from lhp.utils.error_formatter import LHPError


class TestTestActionValidation:
    """Test validation for test actions."""
    
    def test_valid_test_type(self):
        """Test that valid test_type values are accepted."""
        validator = ConfigValidator()
        
        # Test all valid test types with minimal required fields
        test_configs = {
            'row_count': {'source': ['source', 'target']},
            'uniqueness': {'source': 'test_table', 'columns': ['id']},
            'referential_integrity': {
                'source': 'orders',
                'reference': 'customers',
                'source_columns': ['customer_id'],
                'reference_columns': ['id']
            },
            'completeness': {'source': 'test_table', 'required_columns': ['id', 'name']},
            'range': {'source': 'test_table', 'column': 'value', 'min_value': 0, 'max_value': 100},
            'schema_match': {'source': 'table1', 'reference': 'table2'},
            'all_lookups_found': {
                'source': 'fact_table',
                'lookup_table': 'dim_table',
                'lookup_columns': ['id'],
                'lookup_result_columns': ['sk']
            },
            'custom_sql': {'source': 'test_table', 'sql': 'SELECT * FROM test'},
            'custom_expectations': {
                'source': 'test_table',
                'expectations': [{'name': 'test', 'expression': 'true'}]
            }
        }
        
        for test_type in TestActionType:
            config = test_configs.get(test_type.value, {'source': 'test_table'})
            action = Action(
                name='test_action',
                type=ActionType.TEST,
                test_type=test_type.value,
                **config
            )
            errors = validator.validate_action(action, 0)
            assert len(errors) == 0, f"Valid test_type '{test_type.value}' should not produce errors: {errors}"
    
    def test_invalid_test_type(self):
        """Test that invalid test_type values are rejected."""
        validator = ConfigValidator()
        
        action = Action(
            name='test_invalid',
            type=ActionType.TEST,
            test_type='invalid_test_type',
            source='test_table'
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Invalid test_type should produce validation errors"
        assert any('invalid_test_type' in error for error in errors)
    
    def test_missing_test_type(self):
        """Test that missing test_type is handled with default."""
        validator = ConfigValidator()
        
        action = Action(
            name='test_default',
            type=ActionType.TEST,
            source='test_table'
        )
        
        # Missing test_type should either default or produce error
        errors = validator.validate_action(action, 0)
        # Based on current implementation, it should require test_type
        assert len(errors) > 0, "Missing test_type should produce validation error"
        assert any('test_type' in error.lower() for error in errors)
    
    def test_valid_on_violation(self):
        """Test that valid on_violation values are accepted."""
        validator = ConfigValidator()
        
        for violation in ['fail', 'warn']:
            action = Action(
                name='test_violation',
                type=ActionType.TEST,
                test_type='row_count',
                source=['source', 'target'],
                on_violation=violation
            )
            errors = validator.validate_action(action, 0)
            assert len(errors) == 0, f"Valid on_violation '{violation}' should not produce errors"
    
    def test_invalid_on_violation(self):
        """Test that invalid on_violation values are rejected."""
        validator = ConfigValidator()
        
        action = Action(
            name='test_invalid_violation',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source', 'target'],
            on_violation='invalid_action'
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Invalid on_violation should produce validation errors"
        assert any('on_violation' in error or 'invalid_action' in error for error in errors)
    
    def test_row_count_required_fields(self):
        """Test that row_count requires proper source configuration."""
        validator = ConfigValidator()
        
        # Row count with single source (should fail)
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source='single_table'  # Should be list of 2 tables
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Row count with single source should produce error"
        
        # Row count with two sources (should pass)
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source_table', 'target_table']
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0, "Row count with two sources should pass validation"
    
    def test_uniqueness_required_fields(self):
        """Test that uniqueness requires columns field."""
        validator = ConfigValidator()
        
        # Uniqueness without columns (should fail)
        action = Action(
            name='test_uniqueness',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='test_table'
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Uniqueness without columns should produce error"
        
        # Uniqueness with columns (should pass)
        action = Action(
            name='test_uniqueness',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='test_table',
            columns=['id']
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0, "Uniqueness with columns should pass validation"
    
    def test_referential_integrity_required_fields(self):
        """Test that referential_integrity requires reference and column fields."""
        validator = ConfigValidator()
        
        # Missing reference
        action = Action(
            name='test_ref',
            type=ActionType.TEST,
            test_type='referential_integrity',
            source='orders'
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Referential integrity without reference should fail"
        
        # Complete configuration
        action = Action(
            name='test_ref',
            type=ActionType.TEST,
            test_type='referential_integrity',
            source='orders',
            reference='customers',
            source_columns=['customer_id'],
            reference_columns=['id']
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0, "Complete referential integrity should pass"
    
    def test_range_required_fields(self):
        """Test that range requires column and at least one bound."""
        validator = ConfigValidator()
        
        # Range without column
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            min_value=0,
            max_value=100
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Range without column should fail"
        
        # Range without bounds
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='value'
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) > 0, "Range without min/max values should fail"
        
        # Valid range
        action = Action(
            name='test_range',
            type=ActionType.TEST,
            test_type='range',
            source='test_table',
            column='value',
            min_value=0,
            max_value=100
        )
        
        errors = validator.validate_action(action, 0)
        assert len(errors) == 0, "Valid range configuration should pass"
