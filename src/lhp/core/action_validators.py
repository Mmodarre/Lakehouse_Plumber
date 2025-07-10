"""
Action-specific validators to reduce complexity in main ConfigValidator.
"""

from abc import ABC, abstractmethod
from typing import List
from ..models.config import (
    Action,
    ActionType,
    LoadSourceType,
    TransformType,
    WriteTargetType,
)
from .dlt_cdc_validators import (
    DltTableOptionsValidator,
    CdcConfigValidator,
    SnapshotCdcConfigValidator,
    CdcSchemaValidator,
)


class BaseActionValidator(ABC):
    """Base class for action validators."""

    def __init__(self, action_registry, field_validator):
        self.action_registry = action_registry
        self.field_validator = field_validator

    @abstractmethod
    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate an action and return list of error messages."""
        pass


class LoadActionValidator(BaseActionValidator):
    """Validator for load actions."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate load action configuration."""
        errors = []

        # Load actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Load actions must have a 'target' view name")

        # Load actions must have source configuration
        if not action.source:
            errors.append(f"{prefix}: Load actions must have a 'source' configuration")
            return errors

        # Source must be a dict for load actions
        if not isinstance(action.source, dict):
            errors.append(
                f"{prefix}: Load action source must be a configuration object"
            )
            return errors

        # Must have source type
        source_type = action.source.get("type")
        if not source_type:
            errors.append(f"{prefix}: Load action source must have a 'type' field")
            return errors

        # Validate source type is supported
        if not self.action_registry.is_generator_available(
            ActionType.LOAD, source_type
        ):
            errors.append(f"{prefix}: Unknown load source type '{source_type}'")
            return errors

        # Strict field validation for source configuration
        try:
            self.field_validator.validate_load_source(action.source, action.name)
        except Exception as e:
            errors.append(str(e))
            return errors

        # Type-specific validation
        errors.extend(self._validate_source_type(action, prefix, source_type))

        return errors

    def _validate_source_type(
        self, action: Action, prefix: str, source_type: str
    ) -> List[str]:
        """Validate specific source type requirements."""
        errors = []

        try:
            load_type = LoadSourceType(source_type)

            if load_type == LoadSourceType.CLOUDFILES:
                errors.extend(self._validate_cloudfiles_source(action, prefix))
            elif load_type == LoadSourceType.DELTA:
                errors.extend(self._validate_delta_source(action, prefix))
            elif load_type == LoadSourceType.JDBC:
                errors.extend(self._validate_jdbc_source(action, prefix))
            elif load_type == LoadSourceType.PYTHON:
                errors.extend(self._validate_python_source(action, prefix))

        except ValueError:
            pass  # Already handled above

        return errors

    def _validate_cloudfiles_source(self, action: Action, prefix: str) -> List[str]:
        """Validate CloudFiles source configuration."""
        errors = []
        if not action.source.get("path"):
            errors.append(f"{prefix}: CloudFiles source must have 'path'")
        if not action.source.get("format"):
            errors.append(f"{prefix}: CloudFiles source must have 'format'")
        return errors

    def _validate_delta_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Delta source configuration."""
        errors = []
        if not action.source.get("table"):
            errors.append(f"{prefix}: Delta source must have 'table'")
        return errors

    def _validate_jdbc_source(self, action: Action, prefix: str) -> List[str]:
        """Validate JDBC source configuration."""
        errors = []
        required_fields = ["url", "user", "password", "driver"]
        for field in required_fields:
            if not action.source.get(field):
                errors.append(f"{prefix}: JDBC source must have '{field}'")

        # Must have either query or table
        if not action.source.get("query") and not action.source.get("table"):
            errors.append(f"{prefix}: JDBC source must have either 'query' or 'table'")

        return errors

    def _validate_python_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Python source configuration."""
        errors = []
        if not action.source.get("module_path"):
            errors.append(f"{prefix}: Python source must have 'module_path'")
        return errors


class TransformActionValidator(BaseActionValidator):
    """Validator for transform actions."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate transform action configuration."""
        errors = []

        # Transform actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Transform actions must have a 'target' view name")

        # Must have transform_type
        if not action.transform_type:
            errors.append(f"{prefix}: Transform actions must have 'transform_type'")
            return errors

        # Validate transform type is supported
        if not self.action_registry.is_generator_available(
            ActionType.TRANSFORM, action.transform_type
        ):
            errors.append(f"{prefix}: Unknown transform type '{action.transform_type}'")
            return errors

        # Type-specific validation
        errors.extend(self._validate_transform_type(action, prefix))

        return errors

    def _validate_transform_type(self, action: Action, prefix: str) -> List[str]:
        """Validate specific transform type requirements."""
        errors = []

        try:
            transform_type = TransformType(action.transform_type)

            if transform_type == TransformType.SQL:
                errors.extend(self._validate_sql_transform(action, prefix))
            elif transform_type == TransformType.DATA_QUALITY:
                errors.extend(self._validate_data_quality_transform(action, prefix))
            elif transform_type == TransformType.PYTHON:
                errors.extend(self._validate_python_transform(action, prefix))
            elif transform_type == TransformType.TEMP_TABLE:
                errors.extend(self._validate_temp_table_transform(action, prefix))

        except ValueError:
            pass  # Already handled above

        return errors

    def _validate_sql_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate SQL transform configuration."""
        errors = []
        # Must have SQL query
        if not action.sql and not action.sql_path:
            errors.append(f"{prefix}: SQL transform must have 'sql' or 'sql_path'")
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: SQL transform must have 'source' view(s)")
        return errors

    def _validate_data_quality_transform(
        self, action: Action, prefix: str
    ) -> List[str]:
        """Validate data quality transform configuration."""
        errors = []
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: Data quality transform must have 'source'")
        return errors

    def _validate_python_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate Python transform configuration."""
        errors = []
        # Must have source with module_path
        if not isinstance(action.source, dict):
            errors.append(
                f"{prefix}: Python transform source must be a configuration object"
            )
        elif not action.source.get("module_path"):
            errors.append(
                f"{prefix}: Python transform must have 'module_path' in source"
            )
        return errors

    def _validate_temp_table_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate temp table transform configuration."""
        errors = []
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: Temp table transform must have 'source'")
        return errors


class WriteActionValidator(BaseActionValidator):
    """Validator for write actions."""

    def __init__(self, action_registry, field_validator, logger):
        super().__init__(action_registry, field_validator)
        self.logger = logger
        self.dlt_validator = DltTableOptionsValidator()
        self.cdc_validator = CdcConfigValidator()
        self.snapshot_cdc_validator = SnapshotCdcConfigValidator()
        self.cdc_schema_validator = CdcSchemaValidator()

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate write action configuration."""
        errors = []

        # Write actions should not have a target (they are the final output)
        if action.target:
            self.logger.warning(
                f"{prefix}: Write actions typically don't have 'target' field"
            )

        # Write actions must have write_target configuration
        if not action.write_target:
            errors.append(
                f"{prefix}: Write actions must have 'write_target' configuration"
            )
            return errors

        # write_target must be a dict
        if not isinstance(action.write_target, dict):
            errors.append(
                f"{prefix}: Write action write_target must be a configuration object"
            )
            return errors

        # Must have target type
        target_type = action.write_target.get("type")
        if not target_type:
            errors.append(
                f"{prefix}: Write action write_target must have a 'type' field"
            )
            return errors

        # Validate target type is supported
        if not self.action_registry.is_generator_available(
            ActionType.WRITE, target_type
        ):
            errors.append(f"{prefix}: Unknown write target type '{target_type}'")
            return errors

        # Strict field validation for write target configuration
        try:
            self.field_validator.validate_write_target(action.write_target, action.name)
        except Exception as e:
            errors.append(str(e))
            return errors

        # Type-specific validation
        errors.extend(self._validate_write_target_type(action, prefix, target_type))

        # Validate DLT table options (applies to all write target types)
        errors.extend(self.dlt_validator.validate(action, prefix))

        # Validate mode-specific configurations for streaming tables
        if target_type == "streaming_table":
            errors.extend(self._validate_streaming_table_modes(action, prefix))

        return errors

    def _validate_write_target_type(
        self, action: Action, prefix: str, target_type: str
    ) -> List[str]:
        """Validate specific write target type requirements."""
        errors = []

        try:
            write_type = WriteTargetType(target_type)

            if write_type in [
                WriteTargetType.STREAMING_TABLE,
                WriteTargetType.MATERIALIZED_VIEW,
            ]:
                errors.extend(
                    self._validate_table_requirements(action, prefix, target_type)
                )

                if write_type == WriteTargetType.STREAMING_TABLE:
                    errors.extend(self._validate_streaming_table(action, prefix))
                elif write_type == WriteTargetType.MATERIALIZED_VIEW:
                    errors.extend(self._validate_materialized_view(action, prefix))

        except ValueError:
            pass  # Already handled above

        return errors

    def _validate_table_requirements(
        self, action: Action, prefix: str, target_type: str
    ) -> List[str]:
        """Validate common table requirements (database, table/name)."""
        errors = []
        # Must have database and table/name
        if not action.write_target.get("database"):
            errors.append(f"{prefix}: {target_type} must have 'database'")
        if not action.write_target.get("table") and not action.write_target.get("name"):
            errors.append(f"{prefix}: {target_type} must have 'table' or 'name'")
        return errors

    def _validate_streaming_table(self, action: Action, prefix: str) -> List[str]:
        """Validate streaming table specific requirements."""
        errors = []

        # Check if this is snapshot_cdc mode, which defines source differently
        mode = action.write_target.get("mode", "standard")
        if mode != "snapshot_cdc":
            if not action.source:
                errors.append(
                    f"{prefix}: Streaming table must have 'source' to read from"
                )
            # Validate source is string or list
            elif not isinstance(action.source, (str, list)):
                errors.append(
                    f"{prefix}: Streaming table source must be a string or list of view names"
                )

        return errors

    def _validate_materialized_view(self, action: Action, prefix: str) -> List[str]:
        """Validate materialized view specific requirements."""
        errors = []

        # Materialized view can have either source view or SQL
        if not action.source and not action.write_target.get("sql"):
            errors.append(
                f"{prefix}: Materialized view must have either 'source' or 'sql' in write_target"
            )
        # If source is provided, it should be string or list
        elif action.source and not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Materialized view source must be a string or list of view names"
            )

        return errors

    def _validate_streaming_table_modes(self, action: Action, prefix: str) -> List[str]:
        """Validate streaming table mode-specific configurations."""
        errors = []

        mode = action.write_target.get("mode", "standard")

        if mode == "snapshot_cdc":
            errors.extend(self.snapshot_cdc_validator.validate(action, prefix))
        elif mode == "cdc":
            errors.extend(self.cdc_validator.validate(action, prefix))
            # Validate CDC schema if provided
            if action.write_target.get("table_schema") or action.write_target.get(
                "schema"
            ):
                errors.extend(self.cdc_schema_validator.validate(action, prefix))

        return errors
