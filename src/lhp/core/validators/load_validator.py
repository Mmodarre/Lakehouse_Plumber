"""Load action validator."""

from typing import List
from ...models.config import Action, ActionType, LoadSourceType
from .base_validator import BaseActionValidator


class LoadActionValidator(BaseActionValidator):
    """Validator for load actions."""

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate load action configuration."""
        errors = []

        # Load actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Load actions must have a 'target' view name")

        # Load actions must have source configuration
        if not action.source:
            errors.append(f"{prefix}: Load actions must have a 'source' configuration")
            return errors

        # Source must be a dict for load actions
        if not isinstance(action.source, dict):
            errors.append(
                f"{prefix}: Load action source must be a configuration object"
            )
            return errors

        # Must have source type
        source_type = action.source.get("type")
        if not source_type:
            errors.append(f"{prefix}: Load action source must have a 'type' field")
            return errors

        # Validate source type is supported
        if not self.action_registry.is_generator_available(
            ActionType.LOAD, source_type
        ):
            errors.append(f"{prefix}: Unknown load source type '{source_type}'")
            return errors

        # Strict field validation for source configuration
        try:
            self.field_validator.validate_load_source(action.source, action.name)
        except Exception as e:
            errors.append(str(e))
            return errors

        # Type-specific validation
        errors.extend(self._validate_source_type(action, prefix, source_type))

        return errors

    def _validate_source_type(
        self, action: Action, prefix: str, source_type: str
    ) -> List[str]:
        """Validate specific source type requirements."""
        errors = []

        try:
            load_type = LoadSourceType(source_type)

            if load_type == LoadSourceType.CLOUDFILES:
                errors.extend(self._validate_cloudfiles_source(action, prefix))
            elif load_type == LoadSourceType.DELTA:
                errors.extend(self._validate_delta_source(action, prefix))
            elif load_type == LoadSourceType.JDBC:
                errors.extend(self._validate_jdbc_source(action, prefix))
            elif load_type == LoadSourceType.PYTHON:
                errors.extend(self._validate_python_source(action, prefix))
            elif load_type == LoadSourceType.KAFKA:
                errors.extend(self._validate_kafka_source(action, prefix))

        except ValueError:
            pass  # Already handled above

        return errors

    def _validate_cloudfiles_source(self, action: Action, prefix: str) -> List[str]:
        """Validate CloudFiles source configuration."""
        errors = []
        if not action.source.get("path"):
            errors.append(f"{prefix}: CloudFiles source must have 'path'")
        if not action.source.get("format"):
            errors.append(f"{prefix}: CloudFiles source must have 'format'")
        return errors

    def _validate_delta_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Delta source configuration."""
        errors = []
        if not action.source.get("table"):
            errors.append(f"{prefix}: Delta source must have 'table'")
        return errors

    def _validate_jdbc_source(self, action: Action, prefix: str) -> List[str]:
        """Validate JDBC source configuration."""
        errors = []
        required_fields = ["url", "user", "password", "driver"]
        for field in required_fields:
            if not action.source.get(field):
                errors.append(f"{prefix}: JDBC source must have '{field}'")

        # Must have either query or table
        if not action.source.get("query") and not action.source.get("table"):
            errors.append(f"{prefix}: JDBC source must have either 'query' or 'table'")

        return errors

    def _validate_python_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Python source configuration."""
        errors = []
        if not action.source.get("module_path"):
            errors.append(f"{prefix}: Python source must have 'module_path'")
        return errors

    def _validate_kafka_source(self, action: Action, prefix: str) -> List[str]:
        """Validate Kafka source configuration."""
        errors = []
        
        # Must have bootstrap_servers
        if not action.source.get("bootstrap_servers"):
            errors.append(f"{prefix}: Kafka source must have 'bootstrap_servers'")
        
        # Must have exactly one subscription method
        subscription_methods = [
            action.source.get("subscribe"),
            action.source.get("subscribePattern"),
            action.source.get("assign")
        ]
        
        provided_methods = [m for m in subscription_methods if m is not None]
        
        if len(provided_methods) == 0:
            errors.append(
                f"{prefix}: Kafka source must have one of: 'subscribe', 'subscribePattern', or 'assign'"
            )
        elif len(provided_methods) > 1:
            errors.append(
                f"{prefix}: Kafka source can only have ONE of: 'subscribe', 'subscribePattern', or 'assign'"
            )
        
        return errors

