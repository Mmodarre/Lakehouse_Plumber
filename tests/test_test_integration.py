"""Integration tests for Test action feature."""

import pytest
import tempfile
import os
from pathlib import Path
from lhp.core.orchestrator import ActionOrchestrator
from lhp.models.config import FlowGroup, Action, ActionType
from lhp.generators.test.test_generator import TestActionGenerator


class TestTestActionIntegration:
    """Integration tests for test actions."""
    
    def test_row_count_end_to_end(self):
        """Test ROW_COUNT test type generates correct code end-to-end."""
        # Create test action
        action = Action(
            name='test_no_data_loss',
            type=ActionType.TEST,
            test_type='row_count',
            source=['raw.customers', 'bronze.customers'],
            target='tmp_test_no_data_loss',
            on_violation='fail',
            tolerance=0,
            description='Ensure no records lost during transformation'
        )
        
        # Generate code
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={'pipeline': 'test_pipeline'})
        
        # Verify code contains expected elements
        assert 'from pyspark import pipelines as dp' in code
        assert '@dp.temporary_view(' in code or '@dp.table(' in code
        assert 'tmp_test_no_data_loss' in code
        assert 'SELECT * FROM' in code
        assert 'COUNT(*)' in code
        assert 'source_count' in code
        assert 'target_count' in code
        assert 'raw.customers' in code
        assert 'bronze.customers' in code
        
        # Verify expectations
        assert '@dp.expect_all_or_fail' in code or '@dp.expect_or_fail' in code
        assert 'row_count_match' in code
        assert 'abs(source_count - target_count) <= 0' in code
    
    def test_uniqueness_end_to_end(self):
        """Test UNIQUENESS test type generates correct code end-to-end."""
        action = Action(
            name='test_customer_pk',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='bronze.customers',
            columns=['customer_id'],
            on_violation='fail',
            description='Validate customer_id uniqueness'
        )
        
        # Generate code
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={'pipeline': 'test_pipeline'})
        
        # Verify SQL
        assert 'SELECT customer_id, COUNT(*)' in code
        assert 'duplicate_count' in code
        assert 'FROM bronze.customers' in code
        assert 'GROUP BY customer_id' in code
        assert 'HAVING COUNT(*) > 1' in code
        
        # Verify expectations
        assert 'no_duplicates' in code
        assert 'duplicate_count == 0' in code
    
    def test_referential_integrity_end_to_end(self):
        """Test REFERENTIAL_INTEGRITY test type generates correct code."""
        action = Action(
            name='test_orders_fk',
            type=ActionType.TEST,
            test_type='referential_integrity',
            source='orders',
            reference='customers',
            source_columns=['customer_id'],
            reference_columns=['customer_id'],
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify SQL
        assert 'LEFT JOIN' in code
        assert 'orders' in code
        assert 'customers' in code
        assert 's.customer_id = r.customer_id' in code
        
        # Verify expectations
        assert 'referential_integrity' in code
        assert 'ref_customer_id IS NOT NULL' in code
    
    def test_completeness_end_to_end(self):
        """Test COMPLETENESS test type generates correct code."""
        action = Action(
            name='test_required_fields',
            type=ActionType.TEST,
            test_type='completeness',
            source='customers',
            required_columns=['email', 'phone', 'address'],
            on_violation='warn'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify SQL - should select only required columns
        assert 'SELECT email, phone, address' in code
        assert 'FROM customers' in code
        
        # Verify expectations
        assert 'required_fields_complete' in code
        assert 'email IS NOT NULL' in code
        assert 'phone IS NOT NULL' in code
        assert 'address IS NOT NULL' in code
        assert ' AND ' in code  # Columns should be joined with AND
        
        # Should use warn, not fail
        assert '@dp.expect_all(' in code or '@dp.expect(' in code
    
    def test_range_end_to_end(self):
        """Test RANGE test type generates correct code."""
        action = Action(
            name='test_order_date_range',
            type=ActionType.TEST,
            test_type='range',
            source='orders',
            column='order_date',
            min_value='2020-01-01',
            max_value='2024-12-31',
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify SQL - should select only the tested column
        assert 'SELECT order_date' in code
        assert 'FROM orders' in code
        
        # Verify expectations
        assert 'value_in_range' in code
        assert "order_date >= '2020-01-01'" in code
        assert "order_date <= '2024-12-31'" in code
    
    def test_all_lookups_found_end_to_end(self):
        """Test ALL_LOOKUPS_FOUND test type generates correct code."""
        action = Action(
            name='test_customer_dimension',
            type=ActionType.TEST,
            test_type='all_lookups_found',
            source='orders',
            lookup_table='customer_dim',
            lookup_columns=['customer_id'],
            lookup_result_columns=['customer_sk'],
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify SQL
        assert 'LEFT JOIN' in code
        assert 'orders' in code
        assert 'customer_dim' in code
        assert 's.customer_id = l.customer_id' in code
        assert 'lookup_customer_sk' in code
        
        # Verify expectations
        assert 'all_lookups_found' in code
        assert 'lookup_customer_sk IS NOT NULL' in code
    
    def test_schema_match_end_to_end(self):
        """Test SCHEMA_MATCH test type generates correct code."""
        action = Action(
            name='test_schema_consistency',
            type=ActionType.TEST,
            test_type='schema_match',
            source='current_data',
            reference='historical_data',
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify SQL
        assert 'information_schema.columns' in code
        assert 'source_schema' in code
        assert 'reference_schema' in code
        assert 'FULL OUTER JOIN' in code
        assert 'column_name' in code
        assert 'data_type' in code
        
        # Verify expectations
        assert 'schemas_match' in code
        assert 'false' in code  # Fails if any schema difference exists
    
    def test_custom_sql_end_to_end(self):
        """Test CUSTOM_SQL test type generates correct code."""
        action = Action(
            name='test_business_rule',
            type=ActionType.TEST,
            test_type='custom_sql',
            source='orders',
            sql='''
                SELECT 
                    customer_id,
                    SUM(order_total) as total_spent,
                    COUNT(*) as order_count
                FROM orders
                GROUP BY customer_id
                HAVING SUM(order_total) > 1000000
            ''',
            expectations=[
                {
                    'name': 'high_value_customer_orders',
                    'expression': 'order_count >= 10',
                    'on_violation': 'warn'
                },
                {
                    'name': 'spending_threshold',
                    'expression': 'total_spent <= 5000000',
                    'on_violation': 'fail'
                }
            ]
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify custom SQL is used
        assert 'SUM(order_total)' in code
        assert 'GROUP BY customer_id' in code
        assert 'HAVING SUM(order_total) > 1000000' in code
        
        # Verify both expectations
        assert 'high_value_customer_orders' in code
        assert 'order_count >= 10' in code
        assert 'spending_threshold' in code
        assert 'total_spent <= 5000000' in code
    
    def test_custom_expectations_end_to_end(self):
        """Test CUSTOM_EXPECTATIONS test type generates correct code."""
        action = Action(
            name='test_data_quality',
            type=ActionType.TEST,
            test_type='custom_expectations',
            source='customers',
            expectations=[
                {
                    'name': 'valid_email',
                    'expression': "email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Z|a-z]{2,}$'",
                    'on_violation': 'fail'
                },
                {
                    'name': 'valid_phone',
                    'expression': "LENGTH(REGEXP_REPLACE(phone, '[^0-9]', '')) >= 10",
                    'on_violation': 'warn'
                }
            ]
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Should use data quality generator (no SQL, just expectations)
        assert 'valid_email' in code
        assert 'email RLIKE' in code
        assert 'valid_phone' in code
        assert 'REGEXP_REPLACE' in code
    
    def test_flowgroup_with_test_actions(self):
        """Test that flowgroups can contain test actions."""
        flowgroup = FlowGroup(
            pipeline='quality_tests',
            flowgroup='data_validation',
            actions=[
                Action(
                    name='test_row_count',
                    type=ActionType.TEST,
                    test_type='row_count',
                    source=['raw.data', 'bronze.data'],
                    on_violation='fail'
                ),
                Action(
                    name='test_uniqueness',
                    type=ActionType.TEST,
                    test_type='uniqueness',
                    source='bronze.data',
                    columns=['id'],
                    on_violation='fail'
                )
            ]
        )
        
        # Verify flowgroup can be created with test actions
        assert len(flowgroup.actions) == 2
        assert all(action.type == ActionType.TEST for action in flowgroup.actions)
    
    def test_tolerance_in_row_count(self):
        """Test that tolerance parameter works in row_count tests."""
        action = Action(
            name='test_with_tolerance',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source_table', 'target_table'],
            tolerance=10,
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Verify tolerance is used in expectation
        assert 'abs(source_count - target_count) <= 10' in code
    
    def test_default_target_naming(self):
        """Test that default target naming follows v_test_<name> pattern."""
        action = Action(
            name='my_test',
            type=ActionType.TEST,
            test_type='row_count',
            source=['a', 'b']
        )
        
        generator = TestActionGenerator()
        
        # Generate code first (which sets self.config)
        code = generator.generate(action=action, context={})
        
        # Verify default target naming (updated to tmp_test_)
        assert 'tmp_test_my_test' in code
        assert '@dp.table(name="tmp_test_my_test"' in code
        assert 'temporary=True' in code
    
    def test_on_violation_defaults_to_fail(self):
        """Test that on_violation defaults to 'fail' when not specified."""
        action = Action(
            name='test_default_violation',
            type=ActionType.TEST,
            test_type='uniqueness',
            source='table',
            columns=['id']
            # on_violation not specified
        )
        
        generator = TestActionGenerator()
        expectations = generator._build_expectations('uniqueness')
        
        # Should default to 'fail'
        assert expectations[0]['on_violation'] == 'fail'
    
    def test_generates_temporary_table(self):
        """Test that test actions generate temporary tables instead of views."""
        action = Action(
            name='test_temp_table',
            type=ActionType.TEST,
            test_type='row_count',
            source=['source_table', 'target_table'],
            on_violation='fail'
        )
        
        generator = TestActionGenerator()
        code = generator.generate(action=action, context={})
        
        # Should generate temporary table, not view
        assert '@dp.table(' in code
        assert 'temporary=True' in code
        assert '@dp.temporary_view(' not in code
        
        # Should still have expectations and function
        assert '@dp.expect_all_or_fail' in code
        assert 'def tmp_test_test_temp_table():' in code
