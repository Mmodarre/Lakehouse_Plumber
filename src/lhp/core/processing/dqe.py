"""Data Quality Expectations (DQE) parser for LakehousePlumber.

:func:`list_rule_category` and :func:`mapping_rule_category` decide which DLT
category a rule belongs to. The expectations-file validator uses them to
reject a rule with an unknown action before generation, and
:class:`DQEParser` uses them to sort validated rules, so the two can never
disagree.
"""

import logging
from typing import Any, Dict, List, Optional, Tuple, Union

from ...errors import ErrorFactory, codes

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


def list_rule_category(rule: Dict[str, Any]) -> Tuple[str, Any, Optional[str]]:
    """Return ``(field, value, category)`` for a list-format rule.

    ``field`` is the key that decides the category and ``value`` is what the
    rule holds there. ``category`` is ``None`` when that value is not a known
    one. A set ``failureAction`` wins over ``type``; with neither, the rule is
    an ``expect``. An ``action`` key (the mapping format's name for
    ``failureAction``) is never valid here, so it decides with no category.
    """
    if "action" in rule:
        return "action", rule["action"], None
    failure_action = rule.get("failureAction")
    if failure_action not in (None, ""):
        return (
            "failureAction",
            failure_action,
            FAILURE_ACTIONS.get(str(failure_action).lower()),
        )
    rule_type = rule.get("type", "expect")
    return "type", rule_type, rule_type if rule_type in EXPECTATION_TYPES else None


def mapping_rule_category(rule_config: Dict[str, Any]) -> Tuple[Any, Optional[str]]:
    """Return ``(action, category)`` for a mapping-format rule.

    An unset (absent, null or empty) ``action`` means ``warn``. ``category``
    is ``None`` when ``action`` is not a known value.
    """
    action = rule_config.get("action")
    if action in (None, ""):
        return action, FAILURE_ACTIONS["warn"]
    return action, FAILURE_ACTIONS.get(str(action).lower())


class DQEParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_expectations(
        self, expectations: Union[List[Dict], Dict[str, Any]]
    ) -> Tuple[Dict, Dict, Dict]:
        """Split expectations into their DLT categories.

        Returns ``(expect_all, expect_all_or_drop, expect_all_or_fail)``, each
        mapping an expectation name to its constraint. Accepts both
        expectation-file formats:

        - a list of dicts with ``constraint``/``expression``, ``failureAction``
          or ``type``, and ``message``/``name``;
        - a dict mapping each constraint to its ``action`` and ``name``; the
          name defaults to the constraint.

        The rules must already be validated (``validate_expectations_file``):
        a rule with an unknown action is an internal error here, and a list
        rule with no constraint raises ``LHP-VAL-001`` instead of being skipped.
        """
        if not isinstance(expectations, list):
            return _parse_constraint_mapping(expectations)

        self.logger.debug(
            f"Parsing {len(expectations)} expectation(s) into DLT categories"
        )
        categories: Dict[str, Dict[str, str]] = {t: {} for t in EXPECTATION_TYPES}

        for expectation in expectations:
            field, value, category = list_rule_category(expectation)
            if category is None:
                raise AssertionError(
                    f"Unvalidated expectation {field}={value!r}: {expectation}"
                )

            constraint = expectation.get("constraint") or expectation.get("expression")
            message = expectation.get("message") or expectation.get("name", "")

            if not constraint:
                raise ErrorFactory.validation_error(
                    codes.VAL_001,
                    title="Missing required field 'constraint'",
                    details=(
                        f"Expectation '{message or 'unnamed'}' has no 'constraint' "
                        "(or 'expression'), so it checks nothing."
                    ),
                    suggestions=[
                        "Give the rule the SQL boolean expression it checks as "
                        "'constraint' (or 'expression')"
                    ],
                    context={
                        "Expectation": message or "unnamed",
                        "Field": "constraint",
                    },
                )

            if not message:
                message = f"Constraint failed: {constraint}"

            categories[category][message] = constraint

        return (
            categories["expect"],
            categories["expect_or_drop"],
            categories["expect_or_fail"],
        )

    def get_all_expectations_as_drop(
        self, expectations: Union[List[Dict], Dict[str, Any]]
    ) -> Dict[str, str]:
        """Quarantine mode: all expectations enforced as drop regardless of configured failureAction.

        Handles both formats:
        - Old format: list of dicts with name/expression/failureAction keys
        - New format: dict where key is constraint, value has action/name
        """
        result: Dict[str, str] = {}
        if isinstance(expectations, list):
            for exp in expectations:
                constraint = exp.get("constraint") or exp.get("expression")
                if not constraint:
                    continue
                name = exp.get("name") or exp.get("message") or f"rule_{len(result)}"
                result[name] = constraint
        elif isinstance(expectations, dict):
            for constraint, exp_config in expectations.items():
                name = exp_config.get("name", constraint)
                result[name] = constraint
        return result


def _parse_constraint_mapping(
    expectations: Dict[str, Any],
) -> Tuple[Dict[str, str], Dict[str, str], Dict[str, str]]:
    categories: Dict[str, Dict[str, str]] = {t: {} for t in EXPECTATION_TYPES}

    for constraint, exp_config in expectations.items():
        action, category = mapping_rule_category(exp_config)
        if category is None:
            raise AssertionError(
                f"Unvalidated expectation action={action!r} for {constraint!r}"
            )
        name = exp_config.get("name", constraint)
        categories[category][name] = constraint

    return (
        categories["expect"],
        categories["expect_or_drop"],
        categories["expect_or_fail"],
    )
