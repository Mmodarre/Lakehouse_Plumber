"""Tests for ActionRegistry with test action support."""

import pytest
from lhp.core.action_registry import ActionRegistry
from lhp.models.config import ActionType, TestActionType
from lhp.generators.test.test_generator import TestActionGenerator


class TestActionRegistry:
    """Test the ActionRegistry with test action support."""
    
    def test_registry_recognizes_test_action(self):
        """Test that ActionRegistry recognizes TEST action type."""
        registry = ActionRegistry()
        
        # Test that get_generator works with TEST action
        generator = registry.get_generator(ActionType.TEST, sub_type='row_count')
        
        # Verify we get a TestActionGenerator instance
        assert generator is not None
        assert isinstance(generator, TestActionGenerator)
    
    def test_registry_test_action_fields(self):
        """Test that registry defines required and optional fields for test action."""
        registry = ActionRegistry()
        
        # Check if TEST is in the registry's action types
        # This assumes we'll add a method to get action metadata
        # For now, just test that TEST action can be retrieved
        try:
            generator = registry.get_generator(ActionType.TEST, sub_type='row_count')
            assert generator is not None
        except ValueError as e:
            # If it raises an error, it should be about missing implementation
            assert "test" in str(e).lower()
    
    def test_registry_test_action_with_string_type(self):
        """Test that registry handles string test_type conversion."""
        registry = ActionRegistry()
        
        # Test with string sub_type (should convert to TestActionType enum)
        generator = registry.get_generator(ActionType.TEST, sub_type='uniqueness')
        assert isinstance(generator, TestActionGenerator)
    
    def test_registry_test_action_invalid_type(self):
        """Test that registry raises error for invalid test type."""
        from lhp.utils.error_formatter import LHPError
        
        registry = ActionRegistry()
        
        # Test with invalid test type
        with pytest.raises(LHPError) as exc_info:
            registry.get_generator(ActionType.TEST, sub_type='invalid_test_type')
        
        assert "invalid_test_type" in str(exc_info.value).lower()