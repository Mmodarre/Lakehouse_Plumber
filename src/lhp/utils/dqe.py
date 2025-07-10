"""Data Quality Expectations (DQE) parser for LakehousePlumber."""

import logging
from pathlib import Path
from typing import Dict, List, Tuple
import yaml


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

    def load_expectations_from_file(self, expectations_file: Path) -> List[Dict]:
        """Load expectations from YAML file.

        Args:
            expectations_file: Path to expectations YAML file

        Returns:
            List of expectation dictionaries
        """
        if not expectations_file.exists():
            raise FileNotFoundError(f"Expectations file not found: {expectations_file}")

        try:
            with open(expectations_file, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in expectations file: {e}")

        expectations = data.get("expectations", [])
        self.logger.info(
            f"Loaded {len(expectations)} expectations from {expectations_file}"
        )

        return expectations

    def validate_expectations(self, expectations: List[Dict]) -> List[str]:
        """Validate expectation definitions.

        Args:
            expectations: List of expectation dictionaries

        Returns:
            List of validation error messages
        """
        errors = []

        for i, expectation in enumerate(expectations):
            # Check for either 'constraint' or 'expression'
            if "constraint" not in expectation and "expression" not in expectation:
                errors.append(
                    f"Expectation {i}: Missing 'constraint' or 'expression' field"
                )

            # Support both 'type' and 'failureAction'
            expectation_type = expectation.get("type", "expect")
            failure_action = expectation.get("failureAction", "").lower()

            if failure_action:
                # Map failureAction values
                if failure_action not in ["fail", "drop", "warn"]:
                    errors.append(
                        f"Expectation {i}: Invalid failureAction '{failure_action}'. Must be one of: fail, drop, warn"
                    )
            else:
                # Validate type field
                valid_types = ["expect", "expect_or_drop", "expect_or_fail"]
                if expectation_type not in valid_types:
                    errors.append(
                        f"Expectation {i}: Invalid type '{expectation_type}'. Must be one of: {valid_types}"
                    )

            # Validate constraint is a valid SQL expression
            constraint = (
                expectation.get("constraint") or expectation.get("expression") or ""
            )
            if constraint and not self._is_valid_sql_constraint(constraint):
                self.logger.warning(
                    f"Expectation {i}: Constraint may not be valid SQL: {constraint}"
                )

        return errors

    def _is_valid_sql_constraint(self, constraint: str) -> bool:
        """Basic validation of SQL constraint.

        Args:
            constraint: SQL constraint expression

        Returns:
            True if appears to be valid SQL
        """
        # Basic checks - ensure it's not empty and has some SQL-like content
        if not constraint or not constraint.strip():
            return False

        # Should contain at least one comparison or function
        sql_keywords = [
            "=",
            ">",
            "<",
            "!=",
            "<>",
            "IS",
            "NOT",
            "NULL",
            "LIKE",
            "IN",
            "BETWEEN",
        ]
        return any(keyword in constraint.upper() for keyword in sql_keywords)
