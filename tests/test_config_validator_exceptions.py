"""
Exception handling tests for ConfigValidator.
"""

import pytest
from unittest.mock import patch
from lhp.core.validator import ConfigValidator
from lhp.models.config import FlowGroup, Action, ActionType, TransformType


class TestConfigValidatorExceptions:
    """Exception handling tests for ConfigValidator."""
    
    def test_field_validation_exceptions(self):
        """Test field validation exception handling.
        
        Target lines: 99-100, 186-189, 271-272
        Tests exception handling in validate_action_fields, validate_load_source, and validate_write_target.
        """
        validator = ConfigValidator()
        
        # Test 1: validate_action_fields exception (lines 99-100)
        with patch.object(validator.field_validator, 'validate_action_fields') as mock_action_fields:
            mock_action_fields.side_effect = Exception("Action field validation failed")
            
            action = Action(
                name="test_action",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "delta", "table": "test"}
            )
            
            errors = validator.validate_action(action, 0)
            
            # Should catch exception and add to errors (lines 99-100)
            assert "Action field validation failed" in errors
            assert len(errors) == 1  # Should return early after exception
            mock_action_fields.assert_called_once()
        
        # Test 2: validate_load_source exception (lines 186-189)
        with patch.object(validator.field_validator, 'validate_load_source') as mock_load_source:
            mock_load_source.side_effect = Exception("Load source validation failed")
            
            action = Action(
                name="test_load",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "delta", "table": "test"}
            )
            
            errors = validator.validate_action(action, 0)
            
            # Should catch exception and add to errors (lines 186-189)
            assert "Load source validation failed" in errors
            mock_load_source.assert_called_once()
        
        # Test 3: validate_write_target exception (lines 271-272)
        with patch.object(validator.field_validator, 'validate_write_target') as mock_write_target:
            mock_write_target.side_effect = Exception("Write target validation failed")
            
            action = Action(
                name="test_write",
                type=ActionType.WRITE,
                source="v_test",
                write_target={"type": "streaming_table", "database": "test", "table": "test"}
            )
            
            errors = validator.validate_action(action, 0)
            
            # Should catch exception and add to errors (lines 271-272)
            assert "Write target validation failed" in errors
            mock_write_target.assert_called_once()

    def test_dependency_resolver_exceptions(self):
        """Test dependency resolver exception handling.
        
        Target line: 76
        Tests exception handling in dependency_resolver.validate_relationships.
        """
        validator = ConfigValidator()
        
        # Mock dependency_resolver to raise exception
        with patch.object(validator.dependency_resolver, 'validate_relationships') as mock_validate:
            mock_validate.side_effect = Exception("Dependency validation failed")
            
            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="test_action",
                        type=ActionType.LOAD,
                        target="v_test",
                        source={"type": "delta", "table": "test"}
                    )
                ]
            )
            
            errors = validator.validate_flowgroup(flowgroup)
            
            # Should catch exception and add to errors (line 76)
            assert "Dependency validation failed" in errors
            mock_validate.assert_called_once_with(flowgroup.actions)

    @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings:UserWarning")
    def test_unknown_action_type_validation(self):
        """Test unknown action type handling (defensive programming).
        
        Target line: 121
        Tests the else clause when action.type is not LOAD, TRANSFORM, or WRITE.
        This is defensive programming - catches unexpected states gracefully.
        """
        validator = ConfigValidator()
        
        # Create valid action, then manually set invalid type to test safety net
        action = Action(
            name="test_action",
            type=ActionType.LOAD,  # Valid initially
            target="v_test",
            source={"type": "delta", "table": "test"}
        )
        
        # Manually set invalid type to bypass Pydantic (simulates edge cases)
        action.type = "INVALID_TYPE"
        
        errors = validator.validate_action(action, 0)
        
        # Should hit line 121 and catch the invalid type
        assert any("Unknown action type 'INVALID_TYPE'" in error for error in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 