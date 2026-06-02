"""Data Quality Expectations (DQE) parser for LakehousePlumber."""

import logging
from typing import Any, Dict, List, Tuple, Union


class DQEParser:
    """Parse and validate Data Quality Expectations for DLT."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_expectations(self, expectations: List[Dict]) -> Tuple[Dict, Dict, Dict]:
        """Parse expectations into DLT decorator categories.

        Args:
            expectations: List of expectation dictionaries

        Returns:
            Tuple of (expect_all, expect_all_or_drop, expect_all_or_fail)
        """
        self.logger.debug(
            f"Parsing {len(expectations)} expectation(s) into DLT categories"
        )
        expect_all = {}
        expect_all_or_drop = {}
        expect_all_or_fail = {}

        for expectation in expectations:
            # Support both 'type' and 'failureAction' fields
            expectation_type = expectation.get("type", "expect")
            failure_action = expectation.get("failureAction", "").lower()

            # Map failureAction to expectation type
            if failure_action:
                if failure_action == "fail":
                    expectation_type = "expect_or_fail"
                elif failure_action == "drop":
                    expectation_type = "expect_or_drop"
                elif failure_action == "warn":
                    expectation_type = "expect"

            # Support both 'constraint' and 'expression' fields
            constraint = expectation.get("constraint") or expectation.get("expression")
            message = expectation.get("message") or expectation.get("name", "")

            if not constraint:
                self.logger.warning(
                    f"Expectation missing constraint/expression: {expectation}"
                )
                continue

            # Use constraint as message if no message provided
            if not message:
                message = f"Constraint failed: {constraint}"

            if expectation_type == "expect":
                expect_all[message] = constraint
            elif expectation_type == "expect_or_drop":
                expect_all_or_drop[message] = constraint
            elif expectation_type == "expect_or_fail":
                expect_all_or_fail[message] = constraint
            else:
                self.logger.warning(f"Unknown expectation type: {expectation_type}")

        return expect_all, expect_all_or_drop, expect_all_or_fail

    def get_all_expectations_as_drop(
        self, expectations: Union[List[Dict], Dict[str, Any]]
    ) -> Dict[str, str]:
        """Parse all expectations as drop semantics for quarantine mode.

        In quarantine mode, all expectations are enforced as drop regardless of
        their configured failureAction. This returns a single dict of
        {name: constraint} suitable for @dp.expect_all_or_drop.

        Handles both formats:
        - Old format: list of dicts with name/expression/failureAction keys
        - New format: dict where key is constraint, value has action/name

        Args:
            expectations: Expectations in either list or dict format

        Returns:
            Dict mapping expectation names to constraint expressions
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
