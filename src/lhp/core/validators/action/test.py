"""Validator for test actions."""

import logging
from typing import List

from lhp.models import Action, TestActionType

from ._test_requirements import validate_test_type_requirements

logger = logging.getLogger(__name__)


class TestActionValidator:
    """Validator for test actions."""

    def __init__(self, action_registry, field_validator):
        """Initialize the test action validator.

        Args:
            action_registry: The action registry for checking valid generators
            field_validator: The field validator for strict field validation
        """
        self.action_registry = action_registry
        self.field_validator = field_validator

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate test action configuration.

        Args:
            action: The test action to validate
            prefix: Prefix for error messages (e.g., "Action[0] 'test_name'")

        Returns:
            List of validation error messages
        """
        logger.debug(f"Validating test action '{action.name}'")
        errors = []

        # 1. Validate test_type
        test_type = action.test_type
        if not test_type:
            errors.append(f"{prefix}: Test actions must have a 'test_type' field")
            return errors  # Can't continue without test_type

        # Check if test_type is valid
        if test_type not in [t.value for t in TestActionType]:
            valid_types = [t.value for t in TestActionType]
            errors.append(
                f"{prefix}: Invalid test_type '{test_type}'. Valid values are: {', '.join(valid_types)}"
            )
            return errors  # Can't continue with invalid test_type

        # 2. Validate on_violation if present
        if action.on_violation:
            if action.on_violation not in ["fail", "warn", "drop"]:
                errors.append(
                    f"{prefix}: Invalid on_violation '{action.on_violation}'. Valid values are: fail, warn, drop"
                )

        # 3. Validate test type specific requirements
        errors.extend(validate_test_type_requirements(action, prefix, test_type))

        return errors
