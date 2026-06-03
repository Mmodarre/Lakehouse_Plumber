"""Action validation tests for ConfigValidator."""

from unittest.mock import patch

import pytest

from lhp.core.validators import ConfigValidator
from lhp.models import Action, ActionType, TransformType


class TestConfigValidatorActions:
    def test_load_action_early_returns(self):
        validator = ConfigValidator()

        action = Action(
            name="test_load_no_source",
            type=ActionType.LOAD,
            target="v_test",
            # Missing source entirely
        )
        errors = validator.validate_action(action, 0)
        assert any("must have a 'source' configuration" in error for error in errors)

        action = Action(
            name="test_load_string_source",
            type=ActionType.LOAD,
            target="v_test",
            source="string_source",  # Should be dict
        )
        errors = validator.validate_action(action, 0)
        assert any("source must be a configuration object" in error for error in errors)

        action = Action(
            name="test_load_no_type",
            type=ActionType.LOAD,
            target="v_test",
            source={"path": "/data"},  # Missing 'type' field
        )
        errors = validator.validate_action(action, 0)
        assert any("source must have a 'type' field" in error for error in errors)

        # Mock action_registry to return False for unknown type
        with patch.object(
            validator.action_registry, "is_generator_available"
        ) as mock_available:
            mock_available.return_value = False

            action = Action(
                name="test_load_unknown_type",
                type=ActionType.LOAD,
                target="v_test",
                source={"type": "unknown_type"},
            )
            errors = validator.validate_action(action, 0)
            assert any(
                "Unknown load source type 'unknown_type'" in error for error in errors
            )
            mock_available.assert_called_with(ActionType.LOAD, "unknown_type")

    def test_transform_action_early_returns(self):
        validator = ConfigValidator()

        action = Action(
            name="test_transform_no_type",
            type=ActionType.TRANSFORM,
            source="v_input",
            target="v_output",
            # Missing transform_type
        )
        errors = validator.validate_action(action, 0)
        assert any("must have 'transform_type'" in error for error in errors)

        # Mock action_registry to return False for unknown type
        with patch.object(
            validator.action_registry, "is_generator_available"
        ) as mock_available:
            mock_available.return_value = False

            action = Action(
                name="test_transform_unknown_type",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.SQL,  # Valid enum value
                source="v_input",
                target="v_output",
            )

            errors = validator.validate_action(action, 0)
            assert any("Unknown transform type" in error for error in errors)
            mock_available.assert_called_with(ActionType.TRANSFORM, TransformType.SQL)

    @pytest.mark.filterwarnings("ignore:Pydantic serializer warnings:UserWarning")
    def test_write_action_early_returns_and_warnings(self):
        validator = ConfigValidator()

        action = Action(
            name="test_write_no_target",
            type=ActionType.WRITE,
            source="v_test",
            # Missing write_target
        )
        errors = validator.validate_action(action, 0)
        assert any(
            "must have 'write_target' configuration" in error for error in errors
        )

        action = Action(
            name="test_write_string_target",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
            },  # Valid initially
        )
        action.write_target = "string_target"  # bypass Pydantic validation

        errors = validator.validate_action(action, 0)
        assert any(
            "write_target must be a configuration object" in error for error in errors
        )

        action = Action(
            name="test_write_no_type",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "catalog": "test_cat",
                "schema": "test_sch",
                "table": "test",
            },  # Missing 'type' field
        )
        errors = validator.validate_action(action, 0)
        assert any("write_target must have a 'type' field" in error for error in errors)

        with patch.object(
            validator.action_registry, "is_generator_available"
        ) as mock_available:
            mock_available.return_value = False

            action = Action(
                name="test_write_unknown_type",
                type=ActionType.WRITE,
                source="v_test",
                write_target={
                    "type": "unknown_type",
                    "catalog": "test_cat",
                    "schema": "test_sch",
                    "table": "test",
                },
            )
            errors = validator.validate_action(action, 0)
            assert any(
                "Unknown write target type 'unknown_type'" in error for error in errors
            )
            mock_available.assert_called_with(ActionType.WRITE, "unknown_type")

        # WriteActionValidator emits via its own module logger (not the injected
        # ConfigValidator logger), so patch the write module's logger directly.
        with patch("lhp.core.validators.action.write.logger") as mock_logger:
            action = Action(
                name="test_write_with_target",
                type=ActionType.WRITE,
                source="v_test",
                target="v_should_not_have_target",  # Write actions shouldn't have targets
                write_target={
                    "type": "streaming_table",
                    "catalog": "test_cat",
                    "schema": "test_sch",
                    "table": "test",
                },
            )

            errors = validator.validate_action(action, 0)

            mock_logger.warning.assert_called_once()
            warning_call = mock_logger.warning.call_args[0][0]
            assert "Write actions typically don't have 'target' field" in warning_call
            assert len(errors) == 0  # warning only, not an error

    def test_write_target_validation_edge_cases(self):
        validator = ConfigValidator()

        with patch.object(
            validator.action_registry, "is_generator_available"
        ) as mock_available:
            mock_available.return_value = False

            action = Action(
                name="test_unknown_write_target",
                type=ActionType.WRITE,
                source="v_test",
                write_target={
                    "type": "unknown_writer_type",
                    "catalog": "test_cat",
                    "schema": "test_sch",
                    "table": "test",
                },
            )

            errors = validator.validate_action(action, 0)
            assert any(
                "Unknown write target type 'unknown_writer_type'" in error
                for error in errors
            )

        action = Action(
            name="test_missing_catalog_schema",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "table": "test",
                # Missing catalog and schema
            },
        )

        errors = validator.validate_action(action, 0)
        assert len(errors) > 0

        action = Action(
            name="test_missing_table",
            type=ActionType.WRITE,
            source="v_test",
            write_target={
                "type": "streaming_table",
                "catalog": "test_cat",
                "schema": "test_sch",
                # Missing table
            },
        )

        errors = validator.validate_action(action, 0)
        assert len(errors) > 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
