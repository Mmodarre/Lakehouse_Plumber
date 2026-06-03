"""Tests for operational metadata column validation in schema transforms."""

from pathlib import Path

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigFieldValidator, TransformActionValidator
from lhp.models import (
    Action,
    ActionType,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    TransformType,
)


class TestMetadataColumnValidation:
    """Test validation of operational metadata columns in schema transforms."""

    def setup_method(self):
        self.action_registry = ActionRegistry()
        self.field_validator = ConfigFieldValidator()

        self.project_config = ProjectConfig(
            name="test_project",
            operational_metadata=ProjectOperationalMetadataConfig(
                columns={
                    "_processing_timestamp": MetadataColumnConfig(
                        expression="F.current_timestamp()",
                        description="Processing timestamp",
                    ),
                    "_source_file_path": MetadataColumnConfig(
                        expression="F.col('_metadata.file_path')",
                        description="Source file path",
                    ),
                }
            ),
        )

        self.validator = TransformActionValidator(
            self.action_registry,
            self.field_validator,
            project_root=Path.cwd(),
            project_config=self.project_config,
        )

    def test_rename_metadata_column_raises_error(self):
        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
_processing_timestamp -> proc_time: TIMESTAMP
order_id: BIGINT
            """,
        )

        errors = self.validator.validate(action, "test")

        assert len(errors) >= 1
        assert any("_processing_timestamp" in error for error in errors)
        assert any("Cannot rename" in error for error in errors)

    def test_cast_metadata_column_raises_error(self):
        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
_source_file_path: STRING
order_id: BIGINT
            """,
        )

        errors = self.validator.validate(action, "test")

        assert len(errors) >= 1
        assert any("_source_file_path" in error for error in errors)
        assert any("Cannot type-cast" in error for error in errors)

    def test_rename_and_cast_metadata_column_raises_multiple_errors(self):
        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
_processing_timestamp -> proc_time: TIMESTAMP
order_id: BIGINT
            """,
        )

        errors = self.validator.validate(action, "test")

        # Should get error for rename AND error for type cast of renamed column
        assert len(errors) >= 2
        metadata_errors = [
            e for e in errors if "_processing_timestamp" in e or "proc_time" in e
        ]
        assert len(metadata_errors) >= 2

    def test_valid_schema_transform_without_metadata_passes(self):
        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
order_id: BIGINT
customer_name: STRING
amount: DECIMAL(18,2)
            """,
        )

        errors = self.validator.validate(action, "test")

        metadata_errors = [e for e in errors if "metadata" in e.lower()]
        assert len(metadata_errors) == 0

    def test_no_project_config_skips_validation(self):
        validator_no_config = TransformActionValidator(
            self.action_registry,
            self.field_validator,
            project_root=Path.cwd(),
            project_config=None,
        )

        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
_processing_timestamp -> proc_time: TIMESTAMP
            """,
        )

        errors = validator_no_config.validate(action, "test")

        metadata_errors = [e for e in errors if "metadata" in e.lower()]
        assert len(metadata_errors) == 0

    def test_legacy_format_with_metadata_column_raises_error(self):
        action = Action(
            name="test_transform",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_source",
            target="v_target",
            schema_inline="""
column_mapping:
  _processing_timestamp: proc_time
type_casting:
  proc_time: TIMESTAMP
            """,
        )

        errors = self.validator.validate(action, "test")

        assert len(errors) >= 1
        assert any("_processing_timestamp" in error for error in errors)
