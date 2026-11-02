"""Validate data-quality expectations: a file, or rules written inline.

The file is a data_quality transform's ``expectations_file`` or a materialized
view's ``write_target.expectations_file``. It is read through
:func:`~lhp.core.loaders.load_expectations_file`, the loader both generators
use, so ``lhp validate`` reports a file ``lhp generate`` could not read. Rules
written inline on a data_quality action (``expectations`` or
``source.expectations``) get the same rule checks. Every rule must be a mapping
with text fields, a constraint and a known action (see
:mod:`._expectation_rules`). Quarantine mode enforces every rule as ``drop``,
so it also needs at least one rule, and a ``fail`` or ``warn`` rule is logged
as coerced. Rules that cannot be read are reported as a
``"<prefix>: <message>"`` string, a bad rule as an ``LHPError``.
"""

import logging
from pathlib import Path
from typing import Any, List, Optional

from lhp.errors import LHPValidationError

from ...loaders import Expectations, load_expectations_file
from ...processing import DQEParser
from .._base import ValidationError
from ._expectation_rules import (
    duplicate_name_errors,
    rule_content_errors,
    rule_shape_errors,
)

logger = logging.getLogger(__name__)


def validate_expectations_file(
    expectations_file: Optional[str],
    project_root: Optional[Path],
    prefix: str,
    quarantine: bool = False,
    unique_names: bool = False,
) -> List[ValidationError]:
    """Return the problems with ``expectations_file``, or ``[]``.

    Nothing is checked when ``expectations_file`` is unset or when
    ``project_root`` is unknown (the file cannot be resolved). A file that
    cannot be loaded, or whose document is neither a mapping nor a list, is an
    error string. A rule that is not a mapping or has a non-text field
    (``LHP-VAL-008``), has no constraint (``LHP-VAL-001``) or names an unknown
    action or type (``LHP-VAL-006``) is an ``LHPError``. With
    ``quarantine=True`` the messages name quarantine mode and the file must
    hold at least one rule; a file with none reports only that. With
    ``unique_names=True`` a name that more than one valid rule gets is an
    ``LHP-VAL-006`` too (the materialized-view path; the parsed rules are keyed
    by name, so a duplicate would be lost).
    """
    if not expectations_file or not project_root:
        return []

    usage = _usage(quarantine)
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

    return _rule_errors(
        expectations,
        f"expectations file '{expectations_file}'",
        prefix,
        quarantine,
        unique_names,
    )


def validate_expectation_rules(
    expectations: Any, source: str, prefix: str, quarantine: bool = False
) -> List[ValidationError]:
    """Return the problems with expectation rules written inline, or ``[]``.

    ``source`` names where the rules are written, e.g. ``"the action's
    'expectations'"``; error messages say ``in <source>``. The rules get the
    checks :func:`validate_expectations_file` gives a file's rules, and a value
    that is neither a list nor a mapping is an error string.
    """
    if not isinstance(expectations, (list, dict)):
        return [
            f"{prefix}: The expectations in {source} have unexpected format"
            f"{_usage(quarantine)}"
        ]
    return _rule_errors(expectations, source, prefix, quarantine, False)


def _rule_errors(
    expectations: Expectations,
    source: str,
    prefix: str,
    quarantine: bool,
    unique_names: bool,
) -> List[ValidationError]:
    errors: List[ValidationError] = []
    errors.extend(rule_shape_errors(expectations, source, prefix))
    if errors:
        return errors
    if quarantine and not DQEParser().get_all_expectations_as_drop(expectations):
        the_source = source if source.startswith("the ") else f"the {source}"
        return [
            f"{prefix}: Quarantine mode requires at least one expectation, "
            f"but {the_source} contains none. "
            "An empty expectations set would produce an invalid inverse filter."
        ]
    errors.extend(rule_content_errors(expectations, source, prefix))
    if unique_names and not errors:
        errors.extend(duplicate_name_errors(expectations, source, prefix))
    if quarantine and not errors:
        _warn_coerced_to_drop(expectations, prefix)
    return errors


def _usage(quarantine: bool) -> str:
    return " for quarantine mode" if quarantine else ""


def _warn_coerced_to_drop(expectations: Expectations, prefix: str) -> None:
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
