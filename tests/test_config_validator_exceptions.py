"""Exception handling tests for ConfigValidator."""

from unittest.mock import patch

import pytest

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType, FlowGroup, TransformType


class TestConfigValidatorExceptions:
    """Exception handling tests for ConfigValidator."""

    def test_field_validation_exceptions(self):
        validator = ConfigValidator()

        with patch.object(
            validator.field_validator, "validate_action_fields"
        ) as mock_action_fields:
            mock_action_fields.side_effect = Exception("Action field validation failed")

            action = Action(
                name="test_action",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "delta", "table": "test"},
            )

            errors = validator.validate_action(action, 0)

            assert "Action field validation failed" in errors
            assert len(errors) == 1  # should return early after exception
            mock_action_fields.assert_called_once()

        with patch.object(
            validator.field_validator, "validate_load_source"
        ) as mock_load_source:
            mock_load_source.side_effect = Exception("Load source validation failed")

            action = Action(
                name="test_load",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "delta", "table": "test"},
            )

            errors = validator.validate_action(action, 0)

            assert "Load source validation failed" in errors
            mock_load_source.assert_called_once()

        with patch.object(
            validator.field_validator, "validate_write_target"
        ) as mock_write_target:
            mock_write_target.side_effect = Exception("Write target validation failed")

            action = Action(
                name="test_write",
                type=ActionType.WRITE,
                source="v_test",
                write_target={
                    "type": "streaming_table",
                    "database": "test",
                    "table": "test",
                },
            )

            errors = validator.validate_action(action, 0)

            assert "Write target validation failed" in errors
            mock_write_target.assert_called_once()

    def test_dependency_resolver_exceptions(self):
        validator = ConfigValidator()

        with patch.object(
            validator.dependency_resolver, "validate_relationships"
        ) as mock_validate:
            mock_validate.side_effect = Exception("Dependency validation failed")

            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="test_action",
                        type=ActionType.LOAD,
                        target="v_test",
                        source={"type": "delta", "table": "test"},
                    )
                ],
            )

            errors = validator.validate_flowgroup(flowgroup)

            assert "Dependency validation failed" in errors
            mock_validate.assert_called_once_with(flowgroup.actions)

    @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings:UserWarning")
    def test_unknown_action_type_validation(self):
        """Else clause when action.type is not LOAD/TRANSFORM/WRITE — defensive catch for unexpected states."""
        validator = ConfigValidator()

        action = Action(
            name="test_action",
            type=ActionType.LOAD,  # Valid initially
            target="v_test",
            source={"type": "delta", "table": "test"},
        )

        action.type = "INVALID_TYPE"  # bypass Pydantic to simulate edge case

        errors = validator.validate_action(action, 0)

        assert any("Unknown action type 'INVALID_TYPE'" in error for error in errors)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
