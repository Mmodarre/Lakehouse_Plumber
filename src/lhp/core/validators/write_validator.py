"""Write action validator."""

from typing import List
from ...models.config import Action, ActionType, WriteTargetType
from .base_validator import BaseActionValidator
from ..dlt_cdc_validators import (
    DltTableOptionsValidator,
    CdcConfigValidator,
    SnapshotCdcConfigValidator,
    CdcSchemaValidator,
)


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
            
            elif write_type == WriteTargetType.SINK:
                errors.extend(self._validate_sink(action, prefix))

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
    
    def _validate_sink(self, action: Action, prefix: str) -> List[str]:
        """Validate sink write target."""
        errors = []
        sink_config = action.write_target
        
        # Must have sink_type
        if not sink_config.get("sink_type"):
            errors.append(f"{prefix}: Sink must have 'sink_type'")
            return errors
        
        # Must have sink_name
        if not sink_config.get("sink_name"):
            errors.append(f"{prefix}: Sink must have 'sink_name'")
        
        # Must have source to read from
        if not action.source:
            errors.append(f"{prefix}: Sink must have 'source' to read from")
        elif not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Sink source must be a string or list of view names"
            )
        
        # Type-specific validation
        sink_type = sink_config["sink_type"]
        
        if sink_type == "delta":
            errors.extend(self._validate_delta_sink(action, prefix))
        elif sink_type == "kafka":
            errors.extend(self._validate_kafka_sink(action, prefix))
        elif sink_type == "custom":
            errors.extend(self._validate_custom_sink(action, prefix))
        else:
            errors.append(f"{prefix}: Unknown sink_type '{sink_type}'")
        
        return errors
    
    def _validate_delta_sink(self, action: Action, prefix: str) -> List[str]:
        """Validate Delta sink configuration.
        
        Delta sinks require either 'tableName' OR 'path' (not both).
        Other options are passed through for future DLT support.
        """
        errors = []
        sink_config = action.write_target
        
        # Delta sinks must have options
        if not sink_config.get("options"):
            errors.append(
                f"{prefix}: Delta sink requires 'options' with either 'tableName' or 'path'"
            )
            return errors
        
        options = sink_config["options"]
        has_table_name = "tableName" in options
        has_path = "path" in options
        
        # Must have exactly one: tableName or path
        if not has_table_name and not has_path:
            errors.append(
                f"{prefix}: Delta sink options must include either 'tableName' or 'path'"
            )
        elif has_table_name and has_path:
            errors.append(
                f"{prefix}: Delta sink options cannot have both 'tableName' and 'path'. Use one or the other."
            )
        
        # Note: Other options are allowed and passed through silently
        # for future DLT support (e.g., checkpointLocation, mergeSchema, etc.)
        
        return errors
    
    def _validate_kafka_sink(self, action: Action, prefix: str) -> List[str]:
        """Validate Kafka/Event Hubs sink configuration."""
        errors = []
        sink_config = action.write_target
        
        # Required fields
        if not sink_config.get("bootstrap_servers"):
            errors.append(f"{prefix}: Kafka sink must have 'bootstrap_servers'")
        
        if not sink_config.get("topic"):
            errors.append(f"{prefix}: Kafka sink must have 'topic'")
        
        # Validate options using shared validator
        if sink_config.get("options"):
            try:
                from ...utils.kafka_validator import KafkaOptionsValidator
                validator = KafkaOptionsValidator()
                validator.process_options(
                    sink_config["options"], 
                    action.name,
                    is_source=False
                )
            except Exception as e:
                errors.append(f"{prefix}: {str(e)}")
        
        return errors
    
    def _validate_custom_sink(self, action: Action, prefix: str) -> List[str]:
        """Validate custom Python sink configuration."""
        errors = []
        sink_config = action.write_target
        
        # Required fields
        if not sink_config.get("module_path"):
            errors.append(f"{prefix}: Custom sink must have 'module_path'")
        
        if not sink_config.get("custom_sink_class"):
            errors.append(f"{prefix}: Custom sink must have 'custom_sink_class'")
        
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

