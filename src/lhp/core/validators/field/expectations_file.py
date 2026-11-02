"""Validate an action's data-quality ``expectations_file``.

The file is read through :func:`~lhp.core.loaders.load_expectations_file`, the
loader the data_quality generator uses, so ``lhp validate`` reports a file
``lhp generate`` could not read. Every rule must be a mapping with a
constraint and a known action (see :mod:`._expectation_rules`). Quarantine
mode enforces every rule as ``drop``, so it also needs at least one rule, and
a ``fail`` or ``warn`` rule is logged as coerced. A file that cannot be read
is reported as a ``"<prefix>: <message>"`` string, a bad rule as an
``LHPError``.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lhp.errors import LHPValidationError

from ...loaders import load_expectations_file
from ...processing import DQEParser
from .._base import ValidationError
from ._expectation_rules import rule_content_errors, rule_shape_errors

logger = logging.getLogger(__name__)


def validate_expectations_file(
    expectations_file: Optional[str],
    project_root: Optional[Path],
    prefix: str,
    quarantine: bool = False,
) -> List[ValidationError]:
    """Return the problems with ``expectations_file``, or ``[]``.

    Nothing is checked when ``expectations_file`` is unset or when
    ``project_root`` is unknown (the file cannot be resolved). A file that
    cannot be loaded, or whose document is neither a mapping nor a list, is an
    error string. A rule that is not a mapping (``LHP-VAL-008``), has no
    constraint (``LHP-VAL-001``) or names an unknown action or type
    (``LHP-VAL-006``) is an ``LHPError``. With ``quarantine=True`` the messages
    name quarantine mode and the file must hold at least one rule; a file with
    none reports only that.
    """
    if not expectations_file or not project_root:
        return []

    usage = " for quarantine mode" if quarantine else ""
    error_context = (
        "quarantine expectations file" if quarantine else "expectations file"
    )
    try:
        expectations = load_expectations_file(
            expectations_file, Path(project_root), error_context=error_context
        )
    except LHPValidationError:
        return [f"{prefix}: Expectations file has unexpected format{usage}"]
    except Exception as e:
        return [f"{prefix}: Failed to load expectations file{usage}: {e}"]

    errors: List[ValidationError] = []
    errors.extend(rule_shape_errors(expectations, expectations_file, prefix))
    if errors:
        return errors
    if quarantine and not DQEParser().get_all_expectations_as_drop(expectations):
        return [
            f"{prefix}: Quarantine mode requires at least one expectation, "
            f"but the expectations file '{expectations_file}' contains none. "
            "An empty expectations set would produce an invalid inverse filter."
        ]
    errors.extend(rule_content_errors(expectations, expectations_file, prefix))
    if quarantine and not errors:
        _warn_coerced_to_drop(expectations, prefix)
    return errors


def _warn_coerced_to_drop(
    expectations: Union[List[Any], Dict[str, Any]], prefix: str
) -> None:
    if isinstance(expectations, list):
        for exp in expectations:
            failure_action = str(exp.get("failureAction") or "").lower()
            if failure_action in ("fail", "warn"):
                name = exp.get("name") or exp.get("message") or "unknown"
                logger.warning(
                    f"{prefix}: Expectation '{name}' has failureAction='{failure_action}' "
                    f"but quarantine mode coerces all expectations to 'drop'. "
                    f"The original action will be ignored."
                )
    elif isinstance(expectations, dict):
        for constraint, exp_config in expectations.items():
            action_type = str(exp_config.get("action") or "").lower()
            if action_type in ("fail", "warn"):
                name = exp_config.get("name", constraint)
                logger.warning(
                    f"{prefix}: Expectation '{name}' has action='{action_type}' "
                    f"but quarantine mode coerces all expectations to 'drop'. "
                    f"The original action will be ignored."
                )
