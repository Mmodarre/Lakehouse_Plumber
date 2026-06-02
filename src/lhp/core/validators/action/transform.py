"""Transform action validator."""

import logging
from typing import List

from lhp.models import Action, ActionType, TransformType

from .._base import BaseActionValidator
from ._dq_transform import (
    validate_data_quality_transform,
    validate_quarantine_expectations,
)
from ._schema_transform import validate_schema_transform

logger = logging.getLogger(__name__)


class TransformActionValidator(BaseActionValidator):
    def __init__(
        self, action_registry, field_validator, project_root=None, project_config=None
    ):
        super().__init__(action_registry, field_validator)
        self.project_root = project_root
        self.project_config = project_config

    def validate(self, action: Action, prefix: str) -> List[str]:
        logger.debug(f"Validating transform action '{action.name}'")
        errors = []

        if not action.target:
            errors.append(f"{prefix}: Transform actions must have a 'target' view name")

        if not action.transform_type:
            errors.append(f"{prefix}: Transform actions must have 'transform_type'")
            return errors

        if not self.action_registry.is_generator_available(
            ActionType.TRANSFORM, action.transform_type
        ):
            errors.append(f"{prefix}: Unknown transform type '{action.transform_type}'")
            return errors

        errors.extend(self._validate_transform_type(action, prefix))

        return errors

    def _validate_transform_type(self, action: Action, prefix: str) -> List[str]:
        logger.debug(
            f"Validating transform type '{action.transform_type}' for action '{action.name}'"
        )
        errors = []

        try:
            transform_type = TransformType(action.transform_type)

            if transform_type == TransformType.SQL:
                errors.extend(self._validate_sql_transform(action, prefix))
            elif transform_type == TransformType.DATA_QUALITY:
                errors.extend(self._validate_data_quality_transform(action, prefix))
            elif transform_type == TransformType.PYTHON:
                errors.extend(self._validate_python_transform(action, prefix))
            elif transform_type == TransformType.TEMP_TABLE:
                errors.extend(self._validate_temp_table_transform(action, prefix))
            elif transform_type == TransformType.SCHEMA:
                errors.extend(self._validate_schema_transform(action, prefix))

        except ValueError as e:
            logger.debug(f"Unrecognized transform type for '{action.name}': {e}")
            pass  # Already handled above

        return errors

    def _validate_sql_transform(self, action: Action, prefix: str) -> List[str]:
        errors = []
        if not action.sql and not action.sql_path:
            errors.append(f"{prefix}: SQL transform must have 'sql' or 'sql_path'")
        if not action.source:
            errors.append(f"{prefix}: SQL transform must have 'source' view(s)")
        return errors

    def _validate_data_quality_transform(
        self, action: Action, prefix: str
    ) -> List[str]:
        return validate_data_quality_transform(action, prefix, self.project_root)

    def _validate_quarantine_expectations(
        self, action: Action, prefix: str, errors: List[str]
    ) -> None:
        validate_quarantine_expectations(action, prefix, errors, self.project_root)

    def _validate_schema_transform(self, action: Action, prefix: str) -> List[str]:
        return validate_schema_transform(
            action, prefix, self.project_config, self.project_root
        )

    def _validate_python_transform(self, action: Action, prefix: str) -> List[str]:
        errors = []

        if not hasattr(action, "source") or action.source is None:
            errors.append(
                f"{prefix}: Python transform must have 'source' (input view name)"
            )
        elif not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Python transform source must be a string or list of strings"
            )
        elif isinstance(action.source, list):
            for i, item in enumerate(action.source):
                if not isinstance(item, str):
                    errors.append(
                        f"{prefix}: Python transform source list item {i} must be a string"
                    )

        if not hasattr(action, "module_path") or not getattr(action, "module_path"):
            errors.append(f"{prefix}: Python transform must have 'module_path'")
        elif not isinstance(getattr(action, "module_path"), str):
            errors.append(f"{prefix}: Python transform module_path must be a string")
        else:
            if self.project_root:

                module_path = getattr(action, "module_path")
                source_file = self.project_root / module_path
                if not source_file.exists():
                    errors.append(
                        f"{prefix}: Python module file not found: {source_file}"
                    )

        if not hasattr(action, "function_name") or not getattr(action, "function_name"):
            errors.append(f"{prefix}: Python transform must have 'function_name'")
        elif not isinstance(getattr(action, "function_name"), str):
            errors.append(f"{prefix}: Python transform function_name must be a string")

        if hasattr(action, "parameters") and action.parameters is not None:
            if not isinstance(action.parameters, dict):
                errors.append(
                    f"{prefix}: Python transform parameters must be a dictionary"
                )

        return errors

    def _validate_temp_table_transform(self, action: Action, prefix: str) -> List[str]:
        errors = []
        if not action.source:
            errors.append(f"{prefix}: Temp table transform must have 'source'")
        return errors
