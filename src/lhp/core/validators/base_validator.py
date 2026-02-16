"""Base validator class for action validation."""

import logging
from abc import ABC, abstractmethod
from typing import List, Union

from ...models.config import Action
from ...utils.error_formatter import LHPError

logger = logging.getLogger(__name__)

# Type alias for validator error items — preserves LHPError objects alongside plain strings
ValidationError = Union[str, LHPError]


class BaseActionValidator(ABC):
    """Base class for action validators."""

    def __init__(self, action_registry, field_validator):
        self.action_registry = action_registry
        self.field_validator = field_validator

    @abstractmethod
    def validate(self, action: Action, prefix: str) -> List[ValidationError]:
        """Validate an action and return list of error messages."""
        pass
