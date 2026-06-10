"""Shared helpers for CDC configuration validation.

Split out of ``CdcConfigValidator`` so ``cdc_config.py`` stays within the
§1 validator file-size budget.
"""

from typing import Any, Dict, List


def validate_scd_options(cdc_config: Dict[str, Any], prefix: str) -> List[str]:
    errors = []

    scd_type = cdc_config.get("scd_type")
    if scd_type is not None:
        if not isinstance(scd_type, int) or scd_type not in [1, 2]:
            errors.append(f"{prefix}: 'scd_type' must be 1 or 2")

    ignore_null_updates = cdc_config.get("ignore_null_updates")
    if ignore_null_updates is not None:
        if not isinstance(ignore_null_updates, bool):
            errors.append(f"{prefix}: 'ignore_null_updates' must be a boolean")

    apply_as_deletes = cdc_config.get("apply_as_deletes")
    if apply_as_deletes is not None:
        if not isinstance(apply_as_deletes, str):
            errors.append(f"{prefix}: 'apply_as_deletes' must be a string expression")

    apply_as_truncates = cdc_config.get("apply_as_truncates")
    if apply_as_truncates is not None:
        if not isinstance(apply_as_truncates, str):
            errors.append(f"{prefix}: 'apply_as_truncates' must be a string expression")
        # apply_as_truncates is only valid with SCD Type 1.
        if cdc_config.get("scd_type") == 2:
            errors.append(
                f"{prefix}: 'apply_as_truncates' is not supported with SCD Type 2"
            )

    track_history_column_list = cdc_config.get("track_history_column_list")
    track_history_except_list = cdc_config.get("track_history_except_column_list")

    if track_history_column_list is not None and track_history_except_list is not None:
        errors.append(
            f"{prefix}: cannot have both 'track_history_column_list' and 'track_history_except_column_list'"
        )

    if track_history_column_list is not None:
        if not isinstance(track_history_column_list, list):
            errors.append(f"{prefix}: 'track_history_column_list' must be a list")
        else:
            for i, col in enumerate(track_history_column_list):
                if not isinstance(col, str):
                    errors.append(
                        f"{prefix}: track_history_column_list[{i}] must be a string"
                    )

    if track_history_except_list is not None:
        if not isinstance(track_history_except_list, list):
            errors.append(
                f"{prefix}: 'track_history_except_column_list' must be a list"
            )
        else:
            for i, col in enumerate(track_history_except_list):
                if not isinstance(col, str):
                    errors.append(
                        f"{prefix}: track_history_except_column_list[{i}] must be a string"
                    )

    return errors


def validate_column_lists(cdc_config: Dict[str, Any], prefix: str) -> List[str]:
    errors = []

    has_column_list = cdc_config.get("column_list") is not None
    has_except_column_list = cdc_config.get("except_column_list") is not None

    if has_column_list and has_except_column_list:
        errors.append(
            f"{prefix}: cannot have both 'column_list' and 'except_column_list'"
        )

    if has_column_list:
        column_list = cdc_config["column_list"]
        if not isinstance(column_list, list):
            errors.append(f"{prefix}: 'column_list' must be a list")
        else:
            for i, col in enumerate(column_list):
                if not isinstance(col, str):
                    errors.append(f"{prefix}: column_list[{i}] must be a string")

    if has_except_column_list:
        except_column_list = cdc_config["except_column_list"]
        if not isinstance(except_column_list, list):
            errors.append(f"{prefix}: 'except_column_list' must be a list")
        else:
            for i, col in enumerate(except_column_list):
                if not isinstance(col, str):
                    errors.append(f"{prefix}: except_column_list[{i}] must be a string")

    return errors
