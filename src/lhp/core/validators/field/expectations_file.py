"""Validate an action's data-quality ``expectations_file``.

The file is read through :func:`~lhp.core.loaders.load_expectations_file`, the
loader the data_quality generator uses, so ``lhp validate`` reports a file
``lhp generate`` could not read. Quarantine mode enforces every rule as
``drop``, so it also needs at least one rule, and a ``fail`` or ``warn`` rule
is logged as coerced. Each problem is returned as a ``"<prefix>: <message>"``
string.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional, Union

from lhp.errors import LHPValidationError

from ...loaders import load_expectations_file
from ...processing import DQEParser

logger = logging.getLogger(__name__)


def validate_expectations_file(
    expectations_file: Optional[str],
    project_root: Optional[Path],
    prefix: str,
    quarantine: bool = False,
) -> List[str]:
    """Return the problems with ``expectations_file``, or ``[]``.

    Nothing is checked when ``expectations_file`` is unset or when
    ``project_root`` is unknown (the file cannot be resolved). A file that
    cannot be loaded, or whose document is neither a mapping nor a list, is an
    error. With ``quarantine=True`` the messages name quarantine mode and the
    file must hold at least one rule.
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

    if not quarantine:
        return []
    return _quarantine_errors(expectations, expectations_file, prefix)


def _quarantine_errors(
    expectations: Union[List[Any], Dict[str, Any]],
    expectations_file: str,
    prefix: str,
) -> List[str]:
    if not DQEParser().get_all_expectations_as_drop(expectations):
        return [
            f"{prefix}: Quarantine mode requires at least one expectation, "
            f"but the expectations file '{expectations_file}' contains none. "
            "An empty expectations set would produce an invalid inverse filter."
        ]
    _warn_coerced_to_drop(expectations, prefix)
    return []


def _warn_coerced_to_drop(
    expectations: Union[List[Any], Dict[str, Any]], prefix: str
) -> None:
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
