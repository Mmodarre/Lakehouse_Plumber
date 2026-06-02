"""Data-quality + quarantine validation for transform actions.

Module-level functions split out of :class:`TransformActionValidator` to
keep that class small (constitution §3.3).
"""

import logging
from pathlib import Path
from typing import List, Optional

from lhp.models import Action

logger = logging.getLogger(__name__)


def validate_data_quality_transform(
    action: Action, prefix: str, project_root: Optional[Path]
) -> List[str]:
    errors = []
    if not action.source:
        errors.append(f"{prefix}: Data quality transform must have 'source'")

    dq_mode = getattr(action, "mode", None)
    if dq_mode is not None and dq_mode not in ("dqe", "quarantine"):
        errors.append(
            f"{prefix}: Data quality transform 'mode' must be 'dqe' or 'quarantine', "
            f"got '{dq_mode}'"
        )

    if dq_mode == "quarantine":
        quarantine_config = getattr(action, "quarantine", None)
        if quarantine_config is None:
            errors.append(
                f"{prefix}: Data quality transform with mode='quarantine' requires "
                "a 'quarantine' configuration block with 'dlq_table' and 'source_table'"
            )
        else:
            if isinstance(quarantine_config, dict):
                for field in ("dlq_table", "source_table"):
                    value = quarantine_config.get(field)
                    if not value:
                        errors.append(f"{prefix}: quarantine.{field} is required")
                    elif isinstance(value, str) and value.count(".") != 2:
                        errors.append(
                            f"{prefix}: quarantine.{field} must be a 3-part name "
                            f"(catalog.schema.table), got '{value}'"
                        )
            else:
                for field in ("dlq_table", "source_table"):
                    value = getattr(quarantine_config, field, None)
                    if not value:
                        errors.append(f"{prefix}: quarantine.{field} is required")
                    elif isinstance(value, str) and value.count(".") != 2:
                        errors.append(
                            f"{prefix}: quarantine.{field} must be a 3-part name "
                            f"(catalog.schema.table), got '{value}'"
                        )

        validate_quarantine_expectations(action, prefix, errors, project_root)

    elif dq_mode is None or dq_mode == "dqe":
        quarantine_config = getattr(action, "quarantine", None)
        if quarantine_config is not None:
            errors.append(
                f"{prefix}: 'quarantine' configuration block is only valid "
                "when mode='quarantine'"
            )

    return errors


def validate_quarantine_expectations(
    action: Action, prefix: str, errors: List[str], project_root: Optional[Path]
) -> None:
    """Validate expectations file for quarantine mode.

    Quarantine coerces all expectations to drop, so fail/warn expectations
    are warned about and at least one expectation is required.
    """
    expectations_file = getattr(action, "expectations_file", None)
    if not expectations_file:
        return

    if not project_root:
        return

    from ....parsers.yaml_loader import load_yaml_file
    from ...loaders.external_file_loader import resolve_external_file_path
    from ...processing.dqe import DQEParser

    try:
        resolved_path = resolve_external_file_path(
            expectations_file,
            Path(project_root),
            file_type="expectations file",
        )
        data = load_yaml_file(
            resolved_path, error_context="quarantine expectations file"
        )
    except Exception as e:
        errors.append(
            f"{prefix}: Failed to load expectations file for quarantine mode: {e}"
        )
        return

    if isinstance(data, dict) and "expectations" in data:
        expectations = data["expectations"]
    elif isinstance(data, (dict, list)):
        expectations = data
    else:
        errors.append(
            f"{prefix}: Expectations file has unexpected format for quarantine mode"
        )
        return

    parser = DQEParser()
    all_as_drop = parser.get_all_expectations_as_drop(expectations)

    if not all_as_drop:
        errors.append(
            f"{prefix}: Quarantine mode requires at least one expectation, "
            f"but the expectations file '{expectations_file}' contains none. "
            "An empty expectations set would produce an invalid inverse filter."
        )
        return

    # fail/warn expectations are coerced to drop in quarantine mode.
    if isinstance(expectations, list):
        for exp in expectations:
            failure_action = exp.get("failureAction", "").lower()
            if failure_action in ("fail", "warn"):
                name = exp.get("name") or exp.get("message") or "unknown"
                logger.warning(
                    f"{prefix}: Expectation '{name}' has failureAction='{failure_action}' "
                    f"but quarantine mode coerces all expectations to 'drop'. "
                    f"The original action will be ignored."
                )
    elif isinstance(expectations, dict):
        for constraint, exp_config in expectations.items():
            action_type = exp_config.get("action", "").lower()
            if action_type in ("fail", "warn"):
                name = exp_config.get("name", constraint)
                logger.warning(
                    f"{prefix}: Expectation '{name}' has action='{action_type}' "
                    f"but quarantine mode coerces all expectations to 'drop'. "
                    f"The original action will be ignored."
                )
