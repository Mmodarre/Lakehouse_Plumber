import logging
from typing import List

from lhp.models import Action, TestActionType

from ._test_requirements import validate_test_type_requirements

logger = logging.getLogger(__name__)


class TestActionValidator:
    def __init__(self, action_registry, field_validator):
        self.action_registry = action_registry
        self.field_validator = field_validator

    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating test action '{action.name}'")
        errors = []

        test_type = action.test_type
        if not test_type:
            errors.append(f"{prefix}: Test actions must have a 'test_type' field")
            return errors  # Can't continue without test_type

        if test_type not in [t.value for t in TestActionType]:
            valid_types = [t.value for t in TestActionType]
            errors.append(
                f"{prefix}: Invalid test_type '{test_type}'. Valid values are: {', '.join(valid_types)}"
            )
            return errors  # Can't continue with invalid test_type

        if action.on_violation:
            if action.on_violation not in ["fail", "warn", "drop"]:
                errors.append(
                    f"{prefix}: Invalid on_violation '{action.on_violation}'. Valid values are: fail, warn, drop"
                )

        errors.extend(validate_test_type_requirements(action, prefix, test_type))

        return errors
