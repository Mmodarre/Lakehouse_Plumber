"""Data-quality + quarantine validation for transform actions.

Module-level functions split out of :class:`TransformActionValidator` to
keep that class small (constitution §3.3).
"""

from pathlib import Path
from typing import List, Optional

from lhp.models import Action

from .._base import ValidationError
from ..field import validate_expectation_rules, validate_expectations_file
from ._name_checks import require_three_part_name


def validate_data_quality_transform(
    action: Action, prefix: str, project_root: Optional[Path]
) -> List[ValidationError]:
    errors: List[ValidationError] = []
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
                    else:
                        err = require_three_part_name(
                            value, f"quarantine.{field}", prefix
                        )
                        if err:
                            errors.append(err)
            else:
                for field in ("dlq_table", "source_table"):
                    value = getattr(quarantine_config, field, None)
                    if not value:
                        errors.append(f"{prefix}: quarantine.{field} is required")
                    else:
                        err = require_three_part_name(
                            value, f"quarantine.{field}", prefix
                        )
                        if err:
                            errors.append(err)

    elif dq_mode is None or dq_mode == "dqe":
        quarantine_config = getattr(action, "quarantine", None)
        if quarantine_config is not None:
            errors.append(
                f"{prefix}: 'quarantine' configuration block is only valid "
                "when mode='quarantine'"
            )

    quarantine = dq_mode == "quarantine"
    errors.extend(
        validate_expectations_file(
            getattr(action, "expectations_file", None),
            project_root,
            prefix,
            quarantine=quarantine,
        )
    )
    # Rules written on the action win over the file in the generator, so they
    # get the same checks.
    if action.expectations:
        errors.extend(
            validate_expectation_rules(
                action.expectations,
                "the action's 'expectations'",
                prefix,
                quarantine=quarantine,
            )
        )
    if isinstance(action.source, dict) and "expectations" in action.source:
        errors.extend(
            validate_expectation_rules(
                action.source["expectations"],
                "the action's 'source.expectations'",
                prefix,
                quarantine=quarantine,
            )
        )

    return errors
