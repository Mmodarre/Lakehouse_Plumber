"""Write action validator."""

import logging
from typing import List

from lhp.models import Action, ActionType, WriteTargetType

from ....errors import LHPError
from .._base import BaseActionValidator, ValidationError
from ..compatibility import (
    CdcConfigValidator,
    CdcSchemaValidator,
    DltTableOptionsValidator,
    SnapshotCdcConfigValidator,
)

logger = logging.getLogger(__name__)


class WriteActionValidator(BaseActionValidator):
    def __init__(self, action_registry, field_validator, logger):
        super().__init__(action_registry, field_validator)
        self.logger = logger
        self.dlt_validator = DltTableOptionsValidator()
        self.cdc_validator = CdcConfigValidator()
        self.snapshot_cdc_validator = SnapshotCdcConfigValidator()
        self.cdc_schema_validator = CdcSchemaValidator()

    def validate(self, action: Action, prefix: str) -> List[ValidationError]:
        logger.debug(f"Validating write action '{action.name}'")
        errors = []

        # Write actions are the final output, so they should have no target.
        if action.target:
            self.logger.warning(
                f"{prefix}: Write actions typically don't have 'target' field"
            )

        if not action.write_target:
            errors.append(
                f"{prefix}: Write actions must have 'write_target' configuration"
            )
            return errors

        if not isinstance(action.write_target, dict):
            errors.append(
                f"{prefix}: Write action write_target must be a configuration object"
            )
            return errors

        target_type = action.write_target.get("type")
        if not target_type:
            errors.append(
                f"{prefix}: Write action write_target must have a 'type' field"
            )
            return errors

        if not self.action_registry.is_generator_available(
            ActionType.WRITE, target_type
        ):
            errors.append(f"{prefix}: Unknown write target type '{target_type}'")
            return errors

        try:
            self.field_validator.validate_write_target(action.write_target, action.name)
        except LHPError as e:
            errors.append(e)
            return errors
        except Exception as e:
            errors.append(str(e))
            return errors

        errors.extend(self._validate_write_target_type(action, prefix, target_type))

        # DLT table options apply to all write target types.
        errors.extend(self.dlt_validator.validate(action, prefix))

        if target_type == "streaming_table":
            errors.extend(self._validate_streaming_table_modes(action, prefix))

        return errors

    def _validate_write_target_type(
        self, action: Action, prefix: str, target_type: str
    ) -> List[str]:
        logger.debug(
            f"Validating write target type '{target_type}' for action '{action.name}'"
        )
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

        except ValueError as e:
            logger.debug(f"Unrecognized write target type for '{action.name}': {e}")
            pass  # Already handled above

        return errors

    def _validate_table_requirements(
        self, action: Action, prefix: str, target_type: str
    ) -> List[str]:
        errors = []
        wt = action.write_target
        if not wt.get("catalog"):
            errors.append(f"{prefix}: {target_type} must have 'catalog'")
        if not wt.get("schema"):
            errors.append(f"{prefix}: {target_type} must have 'schema'")
        if not wt.get("table"):
            errors.append(f"{prefix}: {target_type} must have 'table'")
        return errors

    def _validate_streaming_table(self, action: Action, prefix: str) -> List[str]:
        errors = []

        # snapshot_cdc mode defines its source differently, so skip the check.
        mode = action.write_target.get("mode", "standard")
        if mode != "snapshot_cdc":
            if not action.source:
                errors.append(
                    f"{prefix}: Streaming table must have 'source' to read from"
                )
            elif not isinstance(action.source, (str, list)):
                errors.append(
                    f"{prefix}: Streaming table source must be a string or list of view names"
                )
            elif (
                mode == "cdc"
                and isinstance(action.source, list)
                and len(action.source) > 1
            ):
                errors.append(
                    f"{prefix}: CDC mode does not support multiple source views "
                    f"in a single action. Define one write action per source, "
                    f"each with compatible cdc_config, targeting the same "
                    f"catalog.schema.table."
                )

        return errors

    def _validate_materialized_view(self, action: Action, prefix: str) -> List[str]:
        errors = []

        if (
            not action.source
            and not action.write_target.get("sql")
            and not action.write_target.get("sql_path")
        ):
            errors.append(
                f"{prefix}: Materialized view must have either 'source', 'sql', or 'sql_path' in write_target"
            )
        elif action.source and not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Materialized view source must be a string or list of view names"
            )

        return errors

    def _validate_sink(self, action: Action, prefix: str) -> List[str]:
        errors = []
        sink_config = action.write_target

        if not sink_config.get("sink_type"):
            errors.append(f"{prefix}: Sink must have 'sink_type'")
            return errors

        if not sink_config.get("sink_name"):
            errors.append(f"{prefix}: Sink must have 'sink_name'")

        if not action.source:
            errors.append(f"{prefix}: Sink must have 'source' to read from")
        elif not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Sink source must be a string or list of view names"
            )

        sink_type = sink_config["sink_type"]

        if sink_type == "delta":
            errors.extend(self._validate_delta_sink(action, prefix))
        elif sink_type == "kafka":
            errors.extend(self._validate_kafka_sink(action, prefix))
        elif sink_type == "custom":
            errors.extend(self._validate_custom_sink(action, prefix))
        elif sink_type == "foreachbatch":
            errors.extend(self._validate_foreachbatch_sink(action, prefix))
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

        if not sink_config.get("options"):
            errors.append(
                f"{prefix}: Delta sink requires 'options' with either 'tableName' or 'path'"
            )
            return errors

        options = sink_config["options"]
        has_table_name = "tableName" in options
        has_path = "path" in options

        if not has_table_name and not has_path:
            errors.append(
                f"{prefix}: Delta sink options must include either 'tableName' or 'path'"
            )
        elif has_table_name and has_path:
            errors.append(
                f"{prefix}: Delta sink options cannot have both 'tableName' and 'path'. Use one or the other."
            )

        if has_table_name:
            table_name_val = options["tableName"]
            if isinstance(table_name_val, str) and table_name_val.count(".") != 2:
                errors.append(
                    f"{prefix}: Delta sink 'tableName' must be a 3-part name "
                    f"(catalog.schema.table), got '{table_name_val}'"
                )

        # Note: Other options are allowed and passed through silently
        # for future DLT support (e.g., checkpointLocation, mergeSchema, etc.)

        return errors

    def _validate_kafka_sink(self, action: Action, prefix: str) -> List[str]:
        errors = []
        sink_config = action.write_target

        if not sink_config.get("bootstrap_servers"):
            errors.append(f"{prefix}: Kafka sink must have 'bootstrap_servers'")

        if not sink_config.get("topic"):
            errors.append(f"{prefix}: Kafka sink must have 'topic'")

        if sink_config.get("options"):
            try:
                from ..field.kafka_options import KafkaOptionsValidator

                validator = KafkaOptionsValidator()
                validator.process_options(
                    sink_config["options"], action.name, is_source=False
                )
            except Exception as e:
                errors.append(f"{prefix}: {str(e)}")

        return errors

    def _validate_custom_sink(self, action: Action, prefix: str) -> List[str]:
        errors = []
        sink_config = action.write_target

        if not sink_config.get("module_path"):
            errors.append(f"{prefix}: Custom sink must have 'module_path'")

        if not sink_config.get("custom_sink_class"):
            errors.append(f"{prefix}: Custom sink must have 'custom_sink_class'")

        return errors

    def _validate_foreachbatch_sink(self, action: Action, prefix: str) -> List[str]:
        errors = []
        sink_config = action.write_target

        if action.source and not isinstance(action.source, str):
            errors.append(
                f"{prefix}: ForEachBatch sink only supports single source view (string), not list or dict"
            )

        has_module_path = bool(sink_config.get("module_path"))
        has_batch_handler = bool(sink_config.get("batch_handler"))

        if has_module_path and has_batch_handler:
            errors.append(
                f"{prefix}: ForEachBatch sink must have either 'module_path' or 'batch_handler', not both"
            )
        elif not has_module_path and not has_batch_handler:
            errors.append(
                f"{prefix}: ForEachBatch sink must have either 'module_path' or 'batch_handler'"
            )

        if has_batch_handler:
            batch_handler = sink_config.get("batch_handler", "").strip()
            if not batch_handler:
                errors.append(
                    f"{prefix}: ForEachBatch sink 'batch_handler' cannot be empty"
                )

        return errors

    def _validate_streaming_table_modes(self, action: Action, prefix: str) -> List[str]:
        errors = []

        mode = action.write_target.get("mode", "standard")

        if mode == "snapshot_cdc":
            errors.extend(self.snapshot_cdc_validator.validate(action, prefix))
        elif mode == "cdc":
            errors.extend(self.cdc_validator.validate(action, prefix))
            if action.write_target.get("table_schema") or action.write_target.get(
                "schema"
            ):
                errors.extend(self.cdc_schema_validator.validate(action, prefix))

        return errors
