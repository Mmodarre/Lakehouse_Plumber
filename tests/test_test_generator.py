"""Tests for TestActionGenerator class."""

import pytest
from unittest.mock import Mock, patch
from lhp.generators.test.test_generator import TestActionGenerator
from lhp.core.base_generator import BaseActionGenerator
from lhp.models.config import Action, ActionType, TestActionType


class TestTestActionGenerator:
    """Test the TestActionGenerator class."""
    
    def test_test_generator_exists(self):
        """Test that TestActionGenerator class exists and can be instantiated."""
        # Test that TestActionGenerator exists
        assert TestActionGenerator is not None
        
        # Test that we can create an instance
        config = {
            'name': 'test_sample',
            'type': 'test', 
            'test_type': 'row_count',
            'source': ['v_source', 'v_target']
        }
        context = {'pipeline': 'test_pipeline'}
        
        generator = TestActionGenerator(config=config, context=context)
        assert generator is not None
        assert generator.config == config
        assert generator.context == context
    
    def test_test_generator_inherits_base_generator(self):
        """Test that TestActionGenerator inherits from BaseActionGenerator."""
        # Test inheritance
        assert issubclass(TestActionGenerator, BaseActionGenerator)
        
        # Create instance and verify it's also a BaseActionGenerator instance
        config = {'name': 'test', 'test_type': 'row_count'}
        context = {}
        generator = TestActionGenerator(config=config, context=context)
        assert isinstance(generator, BaseActionGenerator)
    
    def test_generate_method_exists(self):
        """Test that generate() method exists and returns a string."""
        generator = TestActionGenerator()
        
        # Create a test action
        action = Action(
            name='test_row_count',
            type=ActionType.TEST,
            test_type='row_count',
            source=['v_source', 'v_target']
        )
        
        # Call generate method
        result = generator.generate(action=action, context={})
        
        # Should return a string
        assert isinstance(result, str)
        assert len(result) > 0
    
    def test_build_transform_config_method(self):
        """Test that _build_transform_config() creates proper config."""
        generator = TestActionGenerator(
            config={
                'name': 'test_row_count',
                'test_type': 'row_count',
                'source': ['v_source', 'v_target'],
                'on_violation': 'fail'
            },
            context={}
        )
        
        # Call the method (we'll need to implement this)
        config = generator._build_transform_config('row_count')
        
        # Check the config structure
        assert config['type'] == 'transform'
        assert config['transform_type'] == 'sql'
        assert 'sql' in config
        assert 'expectations' in config
        assert config['target'] == 'tmp_test_test_row_count'
    
    def test_generate_test_sql_method(self):
        """Test that _generate_test_sql() creates SQL for test types."""
        generator = TestActionGenerator(
            config={
                'name': 'test_count',
                'test_type': 'row_count',
                'source': ['v_source', 'v_target']
            },
            context={}
        )
        
        # Generate SQL for row_count test
        sql = generator._generate_test_sql('row_count')
        
        # Check SQL contains expected elements
        assert 'SELECT' in sql
        assert 'COUNT(*)' in sql
        assert 'v_source' in sql
        assert 'v_target' in sql
    
    def test_build_expectations_method(self):
        """Test that _build_expectations() creates proper expectations."""
        generator = TestActionGenerator(
            config={
                'name': 'test_count',
                'test_type': 'row_count',
                'source': ['v_source', 'v_target'],
                'on_violation': 'fail',
                'tolerance': 5
            },
            context={}
        )
        
        # Build expectations for row_count
        expectations = generator._build_expectations('row_count')
        
        # Check expectations structure
        assert len(expectations) > 0
        assert expectations[0]['name'] == 'row_count_match'
        assert 'expression' in expectations[0]
        assert expectations[0]['on_violation'] == 'fail'
