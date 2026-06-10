"""Reference and rule validation tests for ConfigValidator."""

from unittest.mock import patch

import pytest

from lhp.core.validators import ConfigValidator, TableCreationValidator
from lhp.models import Action, ActionType, FlowGroup


class TestConfigValidatorReferences:
    """Reference and rule validation tests for ConfigValidator."""

    def test_table_creation_rules_validation(self):
        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="creator1",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test",
                            "table": "duplicate_table",
                            "create_table": True,
                        },
                    ),
                    Action(
                        name="creator2",
                        type=ActionType.WRITE,
                        source="v_source2",
                        write_target={
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test",
                            "table": "duplicate_table",
                            "create_table": True,
                        },
                    ),
                ],
            )
        ]

        with pytest.raises(Exception) as exc_info:
            TableCreationValidator().validate(flowgroups)

        assert "Multiple table creators detected" in str(exc_info.value)

        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="user_only",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test",
                            "table": "no_creator_table",
                            "create_table": False,
                        },
                    )
                ],
            )
        ]

        errors = TableCreationValidator().validate(flowgroups)
        assert any("has no creator" in error for error in errors)

        flowgroups = [
            FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                actions=[
                    Action(
                        name="creator",
                        type=ActionType.WRITE,
                        source="v_source1",
                        write_target={
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test",
                            "table": "valid_table",
                            "create_table": True,
                        },
                    ),
                    Action(
                        name="user",
                        type=ActionType.WRITE,
                        source="v_source2",
                        write_target={
                            "type": "streaming_table",
                            "catalog": "test_cat",
                            "schema": "test",
                            "table": "valid_table",
                            "create_table": False,
                        },
                    ),
                ],
            )
        ]

        errors = TableCreationValidator().validate(flowgroups)
        assert len(errors) == 0

    def test_template_usage_warning(self):
        validator = ConfigValidator()

        with patch.object(validator.logger, "warning") as mock_warning:
            flowgroup = FlowGroup(
                pipeline="test_pipeline",
                flowgroup="test_flowgroup",
                use_template="test_template",  # Has use_template
                # Missing template_parameters
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

            mock_warning.assert_called_once()
            warning_call = mock_warning.call_args[0][0]
            assert (
                "FlowGroup uses template 'test_template' but no parameters provided"
                in warning_call
            )


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
