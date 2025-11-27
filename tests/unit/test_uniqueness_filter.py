"""Tests for uniqueness test with filter support."""

import pytest
from lhp.models.config import Action, ActionType
from lhp.generators.test import TestActionGenerator


class TestUniquenessFilter:
    """Test filter support for uniqueness tests."""
    
    def test_action_model_accepts_filter(self):
        """Test that Action model accepts optional filter field."""
        # Test with filter
        action = Action(
            name='test_active_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='customer_dim',
            columns=['customer_id'],
            filter='__END_AT IS NULL'  # Optional filter field
        )
        
        assert action.filter == '__END_AT IS NULL'
        assert action.name == 'test_active_unique'
        assert action.columns == ['customer_id']
        
    def test_action_model_filter_is_optional(self):
        """Test that filter field is optional and defaults to None."""
        # Test without filter - should work (backward compatibility)
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='customer_table',
            columns=['id']
        )
        
        assert action.filter is None  # Should default to None
        assert action.name == 'test_unique'
        assert action.columns == ['id']
    
    def test_filter_with_complex_conditions(self):
        """Test that filter can handle complex SQL conditions."""
        action = Action(
            name='test_complex_filter',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='products',
            columns=['product_code', 'region'],
            filter="status = 'ACTIVE' AND effective_date <= current_date() AND region IN ('US', 'EU')"
        )
        
        assert action.filter == "status = 'ACTIVE' AND effective_date <= current_date() AND region IN ('US', 'EU')"
    
    def test_uniqueness_sql_generation_with_filter(self):
        """Test SQL generation includes WHERE clause when filter is provided."""
        generator = TestActionGenerator()
        action = Action(
            name='test_active_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='customer_dim',
            columns=['customer_id'],
            filter='__END_AT IS NULL'
        )
        
        sql = generator._generate_test_sql('uniqueness')
        generator.config = action.model_dump()  # Set config for SQL generation
        sql = generator._generate_test_sql('uniqueness')
        
        # Should include WHERE clause
        assert 'WHERE __END_AT IS NULL' in sql
        assert 'GROUP BY customer_id' in sql
        assert 'HAVING COUNT(*) > 1' in sql
    
    def test_uniqueness_sql_generation_without_filter(self):
        """Test SQL generation without filter (backward compatibility)."""
        generator = TestActionGenerator()
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='customer_table',
            columns=['id']
        )
        
        generator.config = action.model_dump()
        sql = generator._generate_test_sql('uniqueness')
        
        # Should NOT include WHERE clause
        assert 'WHERE' not in sql
        assert 'GROUP BY id' in sql
        assert 'HAVING COUNT(*) > 1' in sql
    
    def test_end_to_end_with_filter(self):
        """Test end-to-end code generation with filter."""
        action = Action(
            name='test_customer_active',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='silver.customer_dim',
            columns=['customer_id'],
            filter='is_current = true',
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action)
        
        # Verify generated code
        assert 'from pyspark import pipelines as dp' in code
        assert '@dp.expect_all_or_fail' in code
        assert 'WHERE is_current = true' in code
        assert 'GROUP BY customer_id' in code
        assert 'no_duplicates' in code
        assert 'duplicate_count == 0' in code
    
    def test_filter_with_empty_string(self):
        """Test that empty filter string is treated as no filter."""
        action = Action(
            name='test_unique',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='table',
            columns=['id'],
            filter=''  # Empty string
        )
        
        generator = TestActionGenerator()
        generator.config = action.model_dump()
        sql = generator._generate_test_sql('uniqueness')
        
        # Empty filter should not add WHERE clause
        assert 'WHERE' not in sql or 'WHERE  GROUP BY' not in sql
