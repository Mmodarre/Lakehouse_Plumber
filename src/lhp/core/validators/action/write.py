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
from ._write_sinks import validate_sink

logger = logging.getLogger(__name__)


class WriteActionValidator(BaseActionValidator):
    def __init__(self, action_registry, field_validator):
        super().__init__(action_registry, field_validator)
        self.dlt_validator = DltTableOptionsValidator()
        self.cdc_validator = CdcConfigValidator()
        self.snapshot_cdc_validator = SnapshotCdcConfigValidator()
        self.cdc_schema_validator = CdcSchemaValidator()

    def validate(self, action: Action, prefix: str) -> List[ValidationError]:
        logger.debug(f"Validating write action '{action.name}'")
        errors = []

        # Write actions are the final output, so they should have no target.
        if action.target:
            logger.warning(
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
                errors.extend(validate_sink(action, prefix))

        except ValueError as e:
            logger.debug(f"Unrecognized write target type for '{action.name}': {e}")
            # Already handled above

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
