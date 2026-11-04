"""The error builders behind :mod:`._expectation_rules`.

Split out so the rule checks stay within the validator file-size budget. Each
builder returns one ``LHPError``; ``prefix`` names the action, ``source`` where
the rules are written.
"""

import difflib
from typing import Any, Dict, Sequence, Union

from lhp.errors import ErrorFactory, LHPError, codes

from ...processing import EXPECTATION_TYPES, FAILURE_ACTIONS


def duplicate_name_error(name: str, count: int, source: str, prefix: str) -> LHPError:
    """``LHP-VAL-071`` for a name ``count`` rules of ``source`` share."""
    return ErrorFactory.validation_error(
        codes.VAL_071,
        title="Duplicate expectation name",
        details=(
            f"{prefix}: {count} expectations in {source} are named '{name}'. "
            "An expectation name must be unique for a dataset."
        ),
        suggestions=[
            "Give each expectation its own name",
            "In a list, a rule is named by 'name' when it has one, then "
            "by 'message', then 'Constraint failed: <constraint>'",
        ],
        context={"Expectation": name, "Field": "name", "Provided": name},
    )


def not_a_mapping(prefix: str, ident: str, source: str, rule: Any) -> LHPError:
    actual = type(rule).__name__
    return ErrorFactory.validation_error(
        codes.VAL_008,
        title="Invalid expectation rule",
        details=(
            f"{prefix}: expectation '{ident}' in {source} must be a mapping, but "
            f"it is a {actual}."
        ),
        suggestions=[
            "In a list, give each rule 'name', 'constraint' and 'failureAction'",
            "In a mapping, give each constraint its 'action' and 'name'",
        ],
        context={
            "Expectation": ident,
            "Expected Type": "mapping",
            "Actual Type": actual,
        },
    )


def not_text(prefix: str, ident: str, source: str, field: str, value: Any) -> LHPError:
    actual = type(value).__name__
    return ErrorFactory.validation_error(
        codes.VAL_008,
        title=f"Invalid expectation '{field}'",
        details=(
            f"{prefix}: expectation '{ident}' in {source} has a '{field}' that is "
            f"a {actual}; it must be text."
        ),
        suggestions=[f"Write '{field}' as a string (quote it in YAML if needed)"],
        context={
            "Expectation": ident,
            "Field": field,
            "Expected Type": "str",
            "Actual Type": actual,
        },
    )


def bad_value(
    prefix: str, ident: str, where: str, field: str, value: Any, list_format: bool
) -> LHPError:
    if list_format and field == "action":
        return ErrorFactory.validation_error(
            codes.VAL_006,
            title="Invalid field 'action' in a list-format expectation",
            details=(
                f"{prefix}: expectation '{ident}' {where} sets 'action', the "
                "mapping format's key. A rule in a list sets 'failureAction'."
            ),
            suggestions=[f"Use 'failureAction: {value}' instead of 'action'"],
            context={"Expectation": ident, "Field": "action", "Provided": str(value)},
        )
    valid = EXPECTATION_TYPES if field == "type" else FAILURE_ACTIONS
    return _invalid_value(prefix, ident, where, field, value, valid)


def _invalid_value(
    prefix: str,
    ident: str,
    where: str,
    field: str,
    value: Any,
    valid: Union[Sequence[str], Dict[str, str]],
) -> LHPError:
    choices = sorted(valid)
    suggestions = [f"Use one of: {', '.join(choices)}"]
    close = difflib.get_close_matches(str(value).lower(), choices, n=1)
    if close:
        suggestions.insert(0, f"Did you mean '{close[0]}'?")
    return ErrorFactory.validation_error(
        codes.VAL_006,
        title=f"Invalid value for '{field}'",
        details=(
            f"{prefix}: expectation '{ident}' {where} has invalid value '{value}' "
            f"for '{field}'. Valid values: {', '.join(choices)}"
        ),
        suggestions=suggestions,
        context={"Expectation": ident, "Field": field, "Provided": str(value)},
    )
