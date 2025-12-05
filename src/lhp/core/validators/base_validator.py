"""Base validator class for action validation."""

from abc import ABC, abstractmethod
from typing import List
from ...models.config import Action


class BaseActionValidator(ABC):
    """Base class for action validators."""

    def __init__(self, action_registry, field_validator):
        self.action_registry = action_registry
        self.field_validator = field_validator

    @abstractmethod
    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate an action and return list of error messages."""
        pass

