"""Per-rule checks for a data-quality expectations file.

The rule semantics come from :func:`~lhp.core.processing.dqe.list_rule_category`
and :func:`~lhp.core.processing.dqe.mapping_rule_category`, the functions the
parser sorts rules with, so a rule these checks accept always parses.
"""

import difflib
from collections import Counter
from typing import Any, Dict, List, Sequence, Union

from lhp.errors import ErrorFactory, LHPError, codes

from ...processing import (
    EXPECTATION_TYPES,
    FAILURE_ACTIONS,
    expectation_names,
    list_rule_category,
    mapping_rule_category,
)

Expectations = Union[List[Any], Dict[Any, Any]]


def rule_shape_errors(
    expectations: Expectations, expectations_file: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-008`` for each rule that is not a mapping."""
    if isinstance(expectations, list):
        rules = [(f"#{i + 1}", rule) for i, rule in enumerate(expectations)]
    else:
        rules = [(f"'{constraint}'", rule) for constraint, rule in expectations.items()]
    return [
        ErrorFactory.validation_error(
            codes.VAL_008,
            title="Invalid expectation rule",
            details=(
                f"{prefix}: expectation {label} in expectations file "
                f"'{expectations_file}' must be a mapping, but it is a "
                f"{type(rule).__name__}."
            ),
            suggestions=[
                "In a list, give each rule 'name', 'constraint' and 'failureAction'",
                "In a mapping, give each constraint its 'action' and 'name'",
            ],
            context={
                "File": expectations_file,
                "Expectation": label,
                "Expected Type": "mapping",
                "Actual Type": type(rule).__name__,
            },
        )
        for label, rule in rules
        if not isinstance(rule, dict)
    ]


def rule_content_errors(
    expectations: Expectations, expectations_file: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-001`` for a rule with no constraint; ``LHP-VAL-006`` for a rule
    whose action or type is unknown, or a list rule that sets ``action``.

    Every rule must already be a mapping (:func:`rule_shape_errors`).
    """
    errors: List[LHPError] = []
    where = f"in expectations file '{expectations_file}'"
    if isinstance(expectations, list):
        for i, rule in enumerate(expectations):
            name = rule.get("name") or rule.get("message")
            label = f"'{name}'" if name else f"#{i + 1}"
            if not (rule.get("constraint") or rule.get("expression")):
                errors.append(_missing_constraint(prefix, label, where))
            field, value, category = list_rule_category(rule)
            if category is None:
                errors.append(_bad_list_value(prefix, label, where, field, value))
        return errors

    for constraint, rule in expectations.items():
        label = f"'{rule.get('name') or constraint}'"
        if not constraint:
            errors.append(_missing_constraint(prefix, label, where))
        action, category = mapping_rule_category(rule)
        if category is None:
            errors.append(
                _invalid_value(prefix, label, where, "action", action, FAILURE_ACTIONS)
            )
    return errors


def duplicate_name_errors(
    expectations: Expectations, expectations_file: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-006`` for each name that more than one rule gets.

    The names are those the parser gives (:func:`expectation_names`), so two
    unnamed list rules with the same constraint share a name too. Every rule
    must already be valid (:func:`rule_content_errors`). Names are counted as
    text, so an unhashable ``name`` value cannot crash the check.
    """
    counts = Counter(str(name) for name in expectation_names(expectations))
    return [
        ErrorFactory.validation_error(
            codes.VAL_006,
            title="Duplicate expectation name",
            details=(
                f"{prefix}: {count} expectations in expectations file "
                f"'{expectations_file}' are named '{name}'. An expectation name "
                "must be unique for a dataset."
            ),
            suggestions=[
                "Give each expectation its own name",
                "In a list, a rule is named by 'message' when it has one, then "
                "by 'name', then 'Constraint failed: <constraint>'",
            ],
            context={"Expectation": f"'{name}'", "Field": "name", "Provided": name},
        )
        for name, count in counts.items()
        if count > 1
    ]


def _missing_constraint(prefix: str, label: str, where: str) -> LHPError:
    return ErrorFactory.validation_error(
        codes.VAL_001,
        title="Missing required field 'constraint'",
        details=(
            f"{prefix}: expectation {label} {where} has no constraint, so it "
            "checks nothing."
        ),
        suggestions=[
            "Give the rule the SQL boolean expression it checks: 'constraint' "
            "(or 'expression') in a list, or the mapping key in a mapping"
        ],
        context={"Expectation": label, "Field": "constraint"},
    )


def _bad_list_value(
    prefix: str, label: str, where: str, field: str, value: Any
) -> LHPError:
    if field == "action":
        return ErrorFactory.validation_error(
            codes.VAL_006,
            title="Invalid field 'action' in a list-format expectation",
            details=(
                f"{prefix}: expectation {label} {where} sets 'action', the "
                "mapping format's key. A rule in a list sets 'failureAction'."
            ),
            suggestions=[f"Use 'failureAction: {value}' instead of 'action'"],
            context={"Expectation": label, "Field": "action", "Provided": str(value)},
        )
    valid = FAILURE_ACTIONS if field == "failureAction" else EXPECTATION_TYPES
    return _invalid_value(prefix, label, where, field, value, valid)


def _invalid_value(
    prefix: str,
    label: str,
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
            f"{prefix}: expectation {label} {where} has invalid value '{value}' "
            f"for '{field}'. Valid values: {', '.join(choices)}"
        ),
        suggestions=suggestions,
        context={"Expectation": label, "Field": field, "Provided": str(value)},
    )
