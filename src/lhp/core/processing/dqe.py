"""Data Quality Expectations (DQE) parser for LakehousePlumber.

:func:`list_rule_category` and :func:`mapping_rule_category` decide which DLT
category a rule belongs to, and :func:`rule_constraint` reads a list rule's
constraint. The expectation-rule validator uses them to reject a bad rule
before generation, and :class:`DQEParser` uses them to sort validated rules,
so the two can never disagree. Both raise or report a rule with no constraint
through :func:`missing_constraint_error`.
"""

import logging
from typing import TYPE_CHECKING, Any, Dict, Iterator, Optional, Tuple

from ...errors import ErrorFactory, LHPValidationError, codes

if TYPE_CHECKING:
    from ..loaders import Expectations

#: The DLT category each ``failureAction`` (list format) or ``action``
#: (mapping format) selects. Values are matched lower-cased.
FAILURE_ACTIONS: Dict[str, str] = {
    "warn": "expect",
    "drop": "expect_or_drop",
    "fail": "expect_or_fail",
}

#: The list-format ``type`` values, used when a rule has no ``failureAction``.
#: Values are matched exactly.
EXPECTATION_TYPES = ("expect", "expect_or_drop", "expect_or_fail")

_UNSET = (None, "")


def list_rule_category(rule: Dict[str, Any]) -> Tuple[str, Any, Optional[str]]:
    """Return ``(field, value, category)`` for a list-format rule.

    ``field`` is the key that decides the category and ``value`` is what the
    rule holds there. ``category`` is ``None`` when that value is not a known
    one. A set ``failureAction`` wins over ``type``; with neither, the rule is
    an ``expect``. A null or empty ``failureAction`` or ``type`` counts as
    unset. An ``action`` key (the mapping format's name for ``failureAction``)
    is never valid here, so it decides with no category.
    """
    if "action" in rule:
        return "action", rule["action"], None
    failure_action = rule.get("failureAction")
    if failure_action not in _UNSET:
        return (
            "failureAction",
            failure_action,
            FAILURE_ACTIONS.get(str(failure_action).lower()),
        )
    rule_type = rule.get("type")
    if rule_type in _UNSET:
        rule_type = "expect"
    return "type", rule_type, rule_type if rule_type in EXPECTATION_TYPES else None


def mapping_rule_category(rule_config: Dict[str, Any]) -> Tuple[Any, Optional[str]]:
    """Return ``(action, category)`` for a mapping-format rule.

    An unset (absent, null or empty) ``action`` means ``warn``. ``category``
    is ``None`` when ``action`` is not a known value.
    """
    action = rule_config.get("action")
    if action in _UNSET:
        return action, FAILURE_ACTIONS["warn"]
    return action, FAILURE_ACTIONS.get(str(action).lower())


def list_rule_name(rule: Dict[str, Any]) -> str:
    """Return a list-format rule's own name: its ``name``, else its ``message``.

    ``""`` when it has neither. The parser, the validators and quarantine mode
    all name a list rule through this, so they cannot disagree; the parser then
    falls back to ``Constraint failed: <constraint>``.
    """
    return rule.get("name") or rule.get("message") or ""


def mapping_rule_name(key: Any, rule: Dict[str, Any]) -> Any:
    """Return a mapping-format rule's name: its ``name``, else its constraint key.

    A null or empty ``name`` counts as unset, as it does in the validators.
    """
    return rule.get("name") or key


def rule_constraint(rule: Dict[str, Any]) -> Any:
    """Return a list-format rule's ``constraint``, else its ``expression``.

    The result is falsy when the rule has neither. (A mapping-format rule's
    constraint is its key.)
    """
    return rule.get("constraint") or rule.get("expression")


def missing_constraint_error(
    expectation: str, *, prefix: str = "", where: str = ""
) -> LHPValidationError:
    """The ``LHP-VAL-001`` for a rule named ``expectation`` with no constraint.

    ``prefix`` names the action and ``where`` the rule's source (e.g.
    ``in expectations file 'rules.yaml'``); the parser has neither.
    """
    lead = f"{prefix}: expectation" if prefix else "Expectation"
    location = f" {where}" if where else ""
    return ErrorFactory.validation_error(
        codes.VAL_001,
        title="Missing required field 'constraint'",
        details=(
            f"{lead} '{expectation}'{location} has no constraint, so it checks nothing."
        ),
        suggestions=[
            "Give the rule the SQL boolean expression it checks: 'constraint' "
            "(or 'expression') in a list, or the mapping key in a mapping"
        ],
        context={"Expectation": expectation, "Field": "constraint"},
    )


class DQEParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_expectations(
        self, expectations: "Expectations"
    ) -> Tuple[Dict, Dict, Dict]:
        """Split expectations into their DLT categories.

        Returns ``(expect_all, expect_all_or_drop, expect_all_or_fail)``, each
        mapping an expectation name to its constraint. Accepts both
        expectation-file formats:

        - a list of dicts with ``constraint``/``expression``, ``failureAction``
          or ``type``, and ``message``/``name``;
        - a dict mapping each constraint to its ``action`` and ``name``; the
          name defaults to the constraint.

        The rules must already be validated (``validate_expectations_file`` or
        ``validate_expectation_rules``): a rule with an unknown action is an
        internal error here, and a rule with no constraint raises
        ``LHP-VAL-001`` instead of being skipped.
        """
        if isinstance(expectations, list):
            self.logger.debug(
                f"Parsing {len(expectations)} expectation(s) into DLT categories"
            )
        categories: Dict[str, Dict[str, str]] = {t: {} for t in EXPECTATION_TYPES}
        for category, name, constraint in _categorised_rules(expectations):
            categories[category][name] = constraint

        return (
            categories["expect"],
            categories["expect_or_drop"],
            categories["expect_or_fail"],
        )

    def get_all_expectations_as_drop(
        self, expectations: "Expectations"
    ) -> Dict[str, str]:
        """Quarantine mode: all expectations enforced as drop regardless of configured failureAction.

        Handles both formats:
        - Old format: list of dicts with name/expression/failureAction keys
        - New format: dict where key is constraint, value has action/name
        """
        result: Dict[str, str] = {}
        if isinstance(expectations, list):
            for exp in expectations:
                constraint = rule_constraint(exp)
                if not constraint:
                    continue
                name = list_rule_name(exp) or f"rule_{len(result)}"
                result[name] = constraint
        elif isinstance(expectations, dict):
            for constraint, exp_config in expectations.items():
                name = mapping_rule_name(constraint, exp_config)
                result[name] = constraint
        return result


def expectation_names(expectations: "Expectations") -> list[str]:
    """Return the name :meth:`DQEParser.parse_expectations` gives each rule.

    The names come in file order, one per rule. A name listed twice keeps only
    one of its rules in the parsed result, so callers use this to find
    duplicate names. The rules must already be validated, as for
    ``parse_expectations``.
    """
    return [name for _, name, _ in _categorised_rules(expectations)]


def _categorised_rules(
    expectations: "Expectations",
) -> Iterator[Tuple[str, str, str]]:
    """Yield ``(category, name, constraint)`` for each rule, in file order."""
    if not isinstance(expectations, list):
        for key, exp_config in expectations.items():
            action, category = mapping_rule_category(exp_config)
            if category is None:
                raise AssertionError(
                    f"Unvalidated expectation action={action!r} for {key!r}"
                )
            name = mapping_rule_name(key, exp_config)
            if not key:
                raise missing_constraint_error(name or "unnamed")
            yield category, name, key
        return

    for expectation in expectations:
        field, value, category = list_rule_category(expectation)
        if category is None:
            raise AssertionError(
                f"Unvalidated expectation {field}={value!r}: {expectation}"
            )

        constraint = rule_constraint(expectation)
        message = list_rule_name(expectation)

        if not constraint:
            raise missing_constraint_error(message or "unnamed")

        if not message:
            message = f"Constraint failed: {constraint}"

        yield category, message, constraint
