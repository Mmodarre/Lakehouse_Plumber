"""
Action validation tests for ConfigValidator.
"""

import pytest
from unittest.mock import patch
from lhp.core.validator import ConfigValidator
from lhp.models.config import Action, ActionType, TransformType


class TestConfigValidatorActions:
    """Action validation tests for ConfigValidator."""
    
    def test_load_action_early_returns(self):
        """Test all early return paths in load action validation.
        
        Target lines: 146-147, 157-159, 173, 179
        Tests early returns in _validate_load_action method.
        """
        validator = ConfigValidator()
        
        # Test 1: Missing source configuration (lines 146-147)
        action = Action(
            name="test_load_no_source",
            type=ActionType.LOAD,
            target="v_test"
            # Missing source entirely
        )
        errors = validator.validate_action(action, 0)
        assert any("must have a 'source' configuration" in error for error in errors)
        
        # Test 2: Source not a dict (lines 157-159)
        action = Action(
            name="test_load_string_source",
            type=ActionType.LOAD,
            target="v_test",
            source="string_source"  # Should be dict
        )
        errors = validator.validate_action(action, 0)
        assert any("source must be a configuration object" in error for error in errors)
        
        # Test 3: Missing source type (line 173)
        action = Action(
            name="test_load_no_type",
            type=ActionType.LOAD,
            target="v_test",
            source={"path": "/data"}  # Missing 'type' field
        )
        errors = validator.validate_action(action, 0)
        assert any("source must have a 'type' field" in error for error in errors)
        
        # Test 4: Unknown source type (line 179)
        # Mock action_registry to return False for unknown type
        with patch.object(validator.action_registry, 'is_generator_available') as mock_available:
            mock_available.return_value = False
            
            action = Action(
                name="test_load_unknown_type",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "unknown_type"}
            )
            errors = validator.validate_action(action, 0)
            assert any("Unknown load source type 'unknown_type'" in error for error in errors)
            mock_available.assert_called_with(ActionType.LOAD, "unknown_type")

    def test_transform_action_early_returns(self):
        """Test early return paths in transform action validation.
        
        Target lines: 199, 208-209
        Tests early returns in _validate_transform_action method.
        """
        validator = ConfigValidator()
        
        # Test 1: Missing transform_type (line 199)
        action = Action(
            name="test_transform_no_type",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output"
            # Missing transform_type
        )
        errors = validator.validate_action(action, 0)
        assert any("must have 'transform_type'" in error for error in errors)
        
        # Test 2: Unknown transform type (lines 208-209)
        # Mock action_registry to return False for unknown type
        with patch.object(validator.action_registry, 'is_generator_available') as mock_available:
            mock_available.return_value = False
            
            action = Action(
                name="test_transform_unknown_type",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,  # Valid enum value
                source="v_input",
                target="v_output"
            )
            
            errors = validator.validate_action(action, 0)
            assert any("Unknown transform type" in error for error in errors)
            mock_available.assert_called_with(ActionType.TRANSFORM, TransformType.SQL)

    @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings:UserWarning")
    def test_write_action_early_returns_and_warnings(self):
        """Test write action early returns and target warnings.
        
        Target lines: 221, 226, 231, 233, 238-241, 255-256, 260-261, 266-267, 277-279
        Tests early returns in _validate_write_action method and target field warning.
        """
        validator = ConfigValidator()
        
        # Test 1: Missing write_target (line 221)
        action = Action(
            name="test_write_no_target",
            type=ActionType.WRITE,
            source="v_test"
            # Missing write_target
        )
        errors = validator.validate_action(action, 0)
        assert any("must have 'write_target' configuration" in error for error in errors)
        
        # Test 2: write_target not a dict (lines 226)
        action = Action(
            name="test_write_string_target",
            type=ActionType.WRITE,
            source="v_test",
            write_target={"type": "streaming_table", "database": "test", "table": "test"}  # Valid initially
        )
        # Manually set invalid write_target to bypass Pydantic validation
        action.write_target = "string_target"  # Should be dict
        
        errors = validator.validate_action(action, 0)
        assert any("write_target must be a configuration object" in error for error in errors)
        
        # Test 3: Missing target type (line 231)
        action = Action(
            name="test_write_no_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={"database": "test", "table": "test"}  # Missing 'type' field
        )
        errors = validator.validate_action(action, 0)
        assert any("write_target must have a 'type' field" in error for error in errors)
        
        # Test 4: Unknown write target type (lines 238-241)
        with patch.object(validator.action_registry, 'is_generator_available') as mock_available:
            mock_available.return_value = False
            
            action = Action(
                name="test_write_unknown_type",
                type=ActionType.WRITE,
                source="v_test",
                write_target={"type": "unknown_type", "database": "test", "table": "test"}
            )
            errors = validator.validate_action(action, 0)
            assert any("Unknown write target type 'unknown_type'" in error for error in errors)
            mock_available.assert_called_with(ActionType.WRITE, "unknown_type")
        
        # Test 5: Write action with target field should log warning (line 277-279)
        with patch.object(validator.logger, 'warning') as mock_warning:
            action = Action(
                name="test_write_with_target",
                type=ActionType.WRITE,
                source="v_test",
                target="v_should_not_have_target",  # Write actions shouldn't have targets
                write_target={"type": "streaming_table", "database": "test", "table": "test"}
            )
            
            errors = validator.validate_action(action, 0)
            
            # Should log warning (line 277-279)
            mock_warning.assert_called_once()
            warning_call = mock_warning.call_args[0][0]
            assert "Write actions typically don't have 'target' field" in warning_call
            assert len(errors) == 0  # Should not be an error, just a warning

    def test_write_target_validation_edge_cases(self):
        """Test write target validation edge cases.
        
        Target lines: 382, 392, 397
        Tests unknown write target types and missing database/table combinations.
        """
        validator = ConfigValidator()
        
        # Test 1: Unknown write target type (lines 382, 392, 397)
        with patch.object(validator.action_registry, 'is_generator_available') as mock_available:
            mock_available.return_value = False
            
            action = Action(
                name="test_unknown_write_target",
                type=ActionType.WRITE,
                source="v_test",
                write_target={
                    "type": "unknown_writer_type",
                    "database": "test",
                    "table": "test"
                }
            )
            
            errors = validator.validate_action(action, 0)
            assert any("Unknown write target type 'unknown_writer_type'" in error for error in errors)
        
        # Test 2: Missing database in write target (edge case)
        action = Action(
            name="test_missing_database",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "table": "test"
                # Missing database
            }
        )
        
        errors = validator.validate_action(action, 0)
        # This should be caught by field validation
        assert len(errors) > 0
        
        # Test 3: Missing table in write target (edge case)
        action = Action(
            name="test_missing_table",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "database": "test"
                # Missing table
            }
        )
        
        errors = validator.validate_action(action, 0)
        # This should be caught by field validation
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"]) 