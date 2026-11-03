"""Per-rule checks for data-quality expectations, from a file or inline.

``source`` names where the rules are (e.g. ``expectations file 'x.yaml'``).
The semantics are the parser's own (``list_rule_category``,
``mapping_rule_category``, ``rule_constraint`` in :mod:`lhp.core.processing`),
so a rule these checks accept always parses. A rule is named by its ``name``
(list rules also ``message``), else its constraint key or position (``#n``).
"""

import difflib
from collections import Counter
from typing import Any, Dict, Iterator, List, Sequence, Tuple, Union

from lhp.errors import ErrorFactory, LHPError, codes

from ...loaders import Expectations
from ...processing import (
    EXPECTATION_TYPES,
    FAILURE_ACTIONS,
    expectation_names,
    list_rule_category,
    list_rule_name,
    mapping_rule_category,
    missing_constraint_error,
    rule_constraint,
)

#: Rule fields that must be text when they are set.
_TEXT_FIELDS = ("name", "message", "constraint", "expression")
#: The ``key`` :func:`_rules` yields for a list-format rule (it has none).
_IN_LIST = object()


def rule_shape_errors(
    expectations: Expectations, source: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-008`` for a rule that is not a mapping, and for a constraint
    key or a ``name``/``message``/``constraint``/``expression`` that is set but
    is not text."""
    errors: List[LHPError] = []
    for ident, key, rule in _rules(expectations):
        if key is not _IN_LIST and not isinstance(key, str):
            errors.append(_not_text(prefix, ident, source, "constraint", key))
        if not isinstance(rule, dict):
            errors.append(_not_a_mapping(prefix, ident, source, rule))
            continue
        for field in _TEXT_FIELDS:
            value = rule.get(field)
            if value is not None and not isinstance(value, str):
                errors.append(_not_text(prefix, ident, source, field, value))
    return errors


def rule_content_errors(
    expectations: Expectations, source: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-001`` for a rule with no constraint; ``LHP-VAL-006`` for an
    unknown action or type, or a list rule that sets ``action``. Every rule
    must already pass :func:`rule_shape_errors`."""
    errors: List[LHPError] = []
    where = f"in {source}"
    for ident, key, rule in _rules(expectations):
        in_list = key is _IN_LIST
        if not (rule_constraint(rule) if in_list else key):
            errors.append(missing_constraint_error(ident, prefix=prefix, where=where))
        if in_list:
            field, value, category = list_rule_category(rule)
        else:
            field, (value, category) = "action", mapping_rule_category(rule)
        if category is None:
            errors.append(_bad_value(prefix, ident, where, field, value, in_list))
    return errors


def duplicate_name_errors(
    expectations: Expectations, source: str, prefix: str
) -> List[LHPError]:
    """``LHP-VAL-006`` for each name more than one rule gets, as the parser
    names them (:func:`expectation_names`). Every rule must already be valid.
    """
    counts = Counter(expectation_names(expectations))
    return [
        ErrorFactory.validation_error(
            codes.VAL_006,
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
        for name, count in counts.items()
        if count > 1
    ]


def _rules(expectations: Expectations) -> Iterator[Tuple[str, Any, Any]]:
    """Yield ``(ident, key, rule)``; ``key`` is a mapping rule's constraint."""
    if isinstance(expectations, list):
        for i, rule in enumerate(expectations):
            yield _ident(rule, True, f"#{i + 1}"), _IN_LIST, rule
    else:
        for key, rule in expectations.items():
            yield _ident(rule, False, str(key)), key, rule


def _ident(rule: Any, in_list: bool, fallback: str) -> str:
    """The name the parser gives the rule, else ``fallback`` (for a bad rule)."""
    if isinstance(rule, dict):
        name = list_rule_name(rule) if in_list else rule.get("name")
        if isinstance(name, str) and name:
            return name
    return fallback


def _not_a_mapping(prefix: str, ident: str, source: str, rule: Any) -> LHPError:
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


def _not_text(prefix: str, ident: str, source: str, field: str, value: Any) -> LHPError:
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


def _bad_value(
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
