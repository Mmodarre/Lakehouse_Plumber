"""Validator for snapshot CDC configuration."""

import logging
from typing import Any, Dict, List

from lhp.models import Action

logger = logging.getLogger(__name__)


class SnapshotCdcConfigValidator:
    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating snapshot CDC configuration for {prefix}")
        errors = []

        if not action.write_target:
            return errors

        snapshot_cdc_config = action.write_target.get("snapshot_cdc_config")
        if not snapshot_cdc_config:
            errors.append(f"{prefix}: snapshot_cdc mode requires 'snapshot_cdc_config'")
            return errors

        if not isinstance(snapshot_cdc_config, dict):
            errors.append(f"{prefix}: 'snapshot_cdc_config' must be a dictionary")
            return errors

        errors.extend(self._validate_source_configuration(snapshot_cdc_config, prefix))
        errors.extend(self._validate_keys_configuration(snapshot_cdc_config, prefix))
        errors.extend(self._validate_scd_configuration(snapshot_cdc_config, prefix))
        errors.extend(
            self._validate_track_history_configuration(snapshot_cdc_config, prefix)
        )

        return errors

    def _validate_source_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        has_source = config.get("source") is not None
        has_source_function = config.get("source_function") is not None

        if not has_source and not has_source_function:
            errors.append(
                f"{prefix}: snapshot_cdc_config must have either 'source' or 'source_function'"
            )
        elif has_source and has_source_function:
            errors.append(
                f"{prefix}: snapshot_cdc_config cannot have both 'source' and 'source_function'"
            )

        if has_source_function:
            source_function = config["source_function"]
            if not isinstance(source_function, dict):
                errors.append(f"{prefix}: 'source_function' must be a dictionary")
            else:
                if not source_function.get("file"):
                    errors.append(f"{prefix}: source_function must have 'file'")
                if not source_function.get("function"):
                    errors.append(f"{prefix}: source_function must have 'function'")

                params = source_function.get("parameters")
                if params is not None:
                    if not isinstance(params, dict):
                        errors.append(
                            f"{prefix}: source_function 'parameters' must be a dictionary"
                        )

        return errors

    def _validate_keys_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        keys = config.get("keys")
        if not keys:
            errors.append(f"{prefix}: snapshot_cdc_config must have 'keys'")
        elif not isinstance(keys, list):
            errors.append(f"{prefix}: 'keys' must be a list")
        elif not keys:  # Empty list
            errors.append(f"{prefix}: 'keys' cannot be empty")
        else:
            for i, key in enumerate(keys):
                if not isinstance(key, str):
                    errors.append(f"{prefix}: keys[{i}] must be a string")

        return errors

    def _validate_scd_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        scd_type = config.get("stored_as_scd_type")
        if scd_type is not None:
            if not isinstance(scd_type, int) or scd_type not in [1, 2]:
                errors.append(f"{prefix}: 'stored_as_scd_type' must be 1 or 2")

        return errors

    def _validate_track_history_configuration(
        self, config: Dict[str, Any], prefix: str
    ) -> List[str]:
        errors = []

        has_track_list = config.get("track_history_column_list") is not None
        has_track_except = config.get("track_history_except_column_list") is not None

        if has_track_list and has_track_except:
            errors.append(
                f"{prefix}: cannot have both 'track_history_column_list' and 'track_history_except_column_list'"
            )

        if has_track_list:
            track_list = config["track_history_column_list"]
            if not isinstance(track_list, list):
                errors.append(f"{prefix}: 'track_history_column_list' must be a list")
            else:
                for i, col in enumerate(track_list):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: track_history_column_list[{i}] must be a string"
                        )

        if has_track_except:
            except_list = config["track_history_except_column_list"]
            if not isinstance(except_list, list):
                errors.append(
                    f"{prefix}: 'track_history_except_column_list' must be a list"
                )
            else:
                for i, col in enumerate(except_list):
                    if not isinstance(col, str):
                        errors.append(
                            f"{prefix}: track_history_except_column_list[{i}] must be a string"
                        )

        return errors
