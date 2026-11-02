import difflib
import logging
from typing import List

from lhp.models import Action, ActionType, WriteTargetType

from ....errors import ErrorFactory, LHPError, codes
from .._base import BaseActionValidator, ValidationError
from ..compatibility import (
    CdcConfigValidator,
    CdcSchemaValidator,
    DltTableOptionsValidator,
    ReplaceFlowConfigValidator,
    SnapshotCdcConfigValidator,
)
from ._write_sinks import validate_sink

logger = logging.getLogger(__name__)

#: Every streaming_table ``write_target.mode``. An absent mode means "standard".
STREAMING_TABLE_MODES = frozenset({"standard", "cdc", "snapshot_cdc", "replace"})


class WriteActionValidator(BaseActionValidator):
    def __init__(self, action_registry, field_validator, project_root=None):
        super().__init__(action_registry, field_validator)
        self.dlt_validator = DltTableOptionsValidator()
        self.cdc_validator = CdcConfigValidator()
        self.snapshot_cdc_validator = SnapshotCdcConfigValidator()
        self.replace_validator = ReplaceFlowConfigValidator()
        self.cdc_schema_validator = CdcSchemaValidator(project_root)

    def validate(self, action: Action, prefix: str) -> List[ValidationError]:
        logger.debug(f"Validating write action '{action.name}'")
        errors: List[ValidationError] = []

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
            elif (
                mode == "replace"
                and isinstance(action.source, list)
                and len(action.source) > 1
            ):
                errors.append(
                    f"{prefix}: replace mode does not support multiple source views "
                    f"in a single action. A REPLACE USING target must be served by a "
                    f"single flow reading one streaming source."
                )

        # REPLACE USING requires a streaming source (spark.readStream); a batch
        # read is rejected by the Databricks runtime.
        if mode == "replace" and action.readMode == "batch":
            errors.append(
                f"{prefix}: replace mode requires a streaming source; readMode "
                f"'batch' is not supported (REPLACE USING reads via "
                f"spark.readStream)."
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

    def _validate_streaming_table_modes(
        self, action: Action, prefix: str
    ) -> List[ValidationError]:
        errors: List[ValidationError] = []

        mode = action.write_target.get("mode", "standard")

        if mode == "snapshot_cdc":
            errors.extend(self.snapshot_cdc_validator.validate(action, prefix))
        elif mode == "cdc":
            errors.extend(self.cdc_validator.validate(action, prefix))
            if action.write_target.get("table_schema") or action.write_target.get(
                "schema"
            ):
                errors.extend(self.cdc_schema_validator.validate(action, prefix))
        elif mode == "replace":
            errors.extend(self.replace_validator.validate(action, prefix))
        elif not isinstance(mode, str) or mode not in STREAMING_TABLE_MODES:
            errors.append(_unknown_streaming_table_mode(action.name, mode))

        return errors


def _unknown_streaming_table_mode(action_name: str, mode: object) -> LHPError:
    valid = sorted(STREAMING_TABLE_MODES)
    suggestions = [f"Use one of: {', '.join(valid)} (omit 'mode' for 'standard')"]
    if isinstance(mode, str):
        close = difflib.get_close_matches(mode.lower(), valid, n=1)
        if close:
            suggestions.insert(0, f"Did you mean '{close[0]}'?")
    return ErrorFactory.validation_error(
        codes.VAL_006,
        title="Invalid value for 'write_target.mode'",
        details=(
            f"Action '{action_name}' has invalid value '{mode}' for field "
            f"'write_target.mode'. Valid values: {', '.join(valid)}"
        ),
        suggestions=suggestions,
        example="write_target:\n  type: streaming_table\n  mode: cdc",
        context={
            "Action": action_name,
            "Field": "write_target.mode",
            "Provided": str(mode),
        },
    )
