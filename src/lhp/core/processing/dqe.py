"""Data Quality Expectations (DQE) parser for LakehousePlumber."""

import logging
from typing import Any, Dict, List, Tuple, Union


class DQEParser:
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse_expectations(self, expectations: List[Dict]) -> Tuple[Dict, Dict, Dict]:
        self.logger.debug(
            f"Parsing {len(expectations)} expectation(s) into DLT categories"
        )
        expect_all = {}
        expect_all_or_drop = {}
        expect_all_or_fail = {}

        for expectation in expectations:
            expectation_type = expectation.get("type", "expect")
            failure_action = expectation.get("failureAction", "").lower()

            if failure_action:
                if failure_action == "fail":
                    expectation_type = "expect_or_fail"
                elif failure_action == "drop":
                    expectation_type = "expect_or_drop"
                elif failure_action == "warn":
                    expectation_type = "expect"

            constraint = expectation.get("constraint") or expectation.get("expression")
            message = expectation.get("message") or expectation.get("name", "")

            if not constraint:
                self.logger.warning(
                    f"Expectation missing constraint/expression: {expectation}"
                )
                continue

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
