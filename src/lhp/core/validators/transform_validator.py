"""Transform action validator."""

from typing import List
from ...models.config import Action, ActionType, TransformType
from .base_validator import BaseActionValidator


class TransformActionValidator(BaseActionValidator):
    """Validator for transform actions."""

    def __init__(self, action_registry, field_validator, project_root=None, project_config=None):
        super().__init__(action_registry, field_validator)
        self.project_root = project_root
        self.project_config = project_config

    def validate(self, action: Action, prefix: str) -> List[str]:
        """Validate transform action configuration."""
        errors = []

        # Transform actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Transform actions must have a 'target' view name")

        # Must have transform_type
        if not action.transform_type:
            errors.append(f"{prefix}: Transform actions must have 'transform_type'")
            return errors

        # Validate transform type is supported
        if not self.action_registry.is_generator_available(
            ActionType.TRANSFORM, action.transform_type
        ):
            errors.append(f"{prefix}: Unknown transform type '{action.transform_type}'")
            return errors

        # Type-specific validation
        errors.extend(self._validate_transform_type(action, prefix))

        return errors

    def _validate_transform_type(self, action: Action, prefix: str) -> List[str]:
        """Validate specific transform type requirements."""
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

        except ValueError:
            pass  # Already handled above

        return errors

    def _validate_sql_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate SQL transform configuration."""
        errors = []
        # Must have SQL query
        if not action.sql and not action.sql_path:
            errors.append(f"{prefix}: SQL transform must have 'sql' or 'sql_path'")
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: SQL transform must have 'source' view(s)")
        return errors

    def _validate_data_quality_transform(
        self, action: Action, prefix: str
    ) -> List[str]:
        """Validate data quality transform configuration."""
        errors = []
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: Data quality transform must have 'source'")
        return errors

    def _validate_python_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate Python transform configuration."""
        errors = []

        # Must have source for input data
        if not hasattr(action, 'source') or action.source is None:
            errors.append(f"{prefix}: Python transform must have 'source' (input view name)")
        elif not isinstance(action.source, (str, list)):
            errors.append(
                f"{prefix}: Python transform source must be a string or list of strings"
            )
        elif isinstance(action.source, list):
            # Validate list elements are strings
            for i, item in enumerate(action.source):
                if not isinstance(item, str):
                    errors.append(
                        f"{prefix}: Python transform source list item {i} must be a string"
                    )

        # Must have module_path at action level
        if not hasattr(action, 'module_path') or not getattr(action, 'module_path'):
            errors.append(f"{prefix}: Python transform must have 'module_path'")
        elif not isinstance(getattr(action, 'module_path'), str):
            errors.append(f"{prefix}: Python transform module_path must be a string")
        else:
            # Check if module file exists (if project_root is available)
            if self.project_root:
                from pathlib import Path
                module_path = getattr(action, 'module_path')
                source_file = self.project_root / module_path
                if not source_file.exists():
                    errors.append(f"{prefix}: Python module file not found: {source_file}")

        # Must have function_name at action level
        if not hasattr(action, 'function_name') or not getattr(action, 'function_name'):
            errors.append(f"{prefix}: Python transform must have 'function_name'")
        elif not isinstance(getattr(action, 'function_name'), str):
            errors.append(f"{prefix}: Python transform function_name must be a string")

        # Validate parameters if provided
        if hasattr(action, 'parameters') and action.parameters is not None:
            if not isinstance(action.parameters, dict):
                errors.append(f"{prefix}: Python transform parameters must be a dictionary")

        return errors

    def _validate_temp_table_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate temp table transform configuration."""
        errors = []
        # Must have source
        if not action.source:
            errors.append(f"{prefix}: Temp table transform must have 'source'")
        return errors
    
    def _validate_metadata_columns_not_manipulated(
        self, 
        action: Action, 
        prefix: str,
        project_config
    ) -> List[str]:
        """Validate that operational metadata columns are not renamed or cast.
        
        Args:
            action: The schema transform action to validate
            prefix: Error message prefix
            project_config: Project configuration containing operational_metadata
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Only validate if project has operational metadata defined
        if not project_config or not project_config.operational_metadata:
            return errors
        
        # Get metadata column names from project config
        metadata_columns = set(project_config.operational_metadata.columns.keys())
        if not metadata_columns:
            return errors
        
        # Parse the schema configuration
        from pathlib import Path
        from ...utils.schema_transform_parser import SchemaTransformParser
        
        parser = SchemaTransformParser()
        schema_config = {}
        
        try:
            if action.schema_inline:
                schema_config = parser.parse_inline_schema(action.schema_inline)
            elif action.schema_file and self.project_root:
                schema_file_path = Path(self.project_root) / action.schema_file
                if schema_file_path.exists():
                    schema_config = parser.parse_file(schema_file_path)
        except Exception:
            # If parsing fails, skip this validation (other validators will catch parsing errors)
            return errors
        
        # Check column_mapping for metadata columns (source names)
        column_mapping = schema_config.get("column_mapping", {})
        for source_col in column_mapping.keys():
            if source_col in metadata_columns:
                errors.append(
                    f"{prefix}: Cannot rename operational metadata column '{source_col}'. "
                    f"Operational metadata columns defined in lhp.yaml are automatically managed "
                    f"by the framework and cannot be renamed or type-cast in schema transforms.\n"
                    f"  → To fix: Remove '{source_col}' from your schema definition."
                )
        
        # Build reverse mapping to check type_casting for renamed metadata columns (target names)
        reverse_mapping = {target: source for source, target in column_mapping.items()}
        
        # Check type_casting for both direct and renamed metadata columns
        type_casting = schema_config.get("type_casting", {})
        for col in type_casting.keys():
            # Check if it's a direct metadata column (by source name)
            if col in metadata_columns:
                errors.append(
                    f"{prefix}: Cannot type-cast operational metadata column '{col}'. "
                    f"Operational metadata columns defined in lhp.yaml are automatically managed "
                    f"by the framework and cannot be renamed or type-cast in schema transforms.\n"
                    f"  → To fix: Remove '{col}' from your schema definition."
                )
            # Check if it's a renamed metadata column (by target name)
            elif col in reverse_mapping:
                source_col = reverse_mapping[col]
                if source_col in metadata_columns:
                    errors.append(
                        f"{prefix}: Cannot type-cast operational metadata column '{source_col}' "
                        f"(renamed to '{col}'). Operational metadata columns defined in lhp.yaml "
                        f"are automatically managed by the framework and cannot be renamed or "
                        f"type-cast in schema transforms.\n"
                        f"  → To fix: Remove '{source_col} -> {col}' from your schema definition."
                    )
        
        return errors
    
    def _validate_schema_transform(self, action: Action, prefix: str) -> List[str]:
        """Validate schema transform configuration."""
        errors = []
        
        # Must have source (view name)
        if not action.source:
            errors.append(f"{prefix}: Schema transform must have 'source' (view name)")
        elif isinstance(action.source, dict):
            # Old format detected
            errors.append(
                f"{prefix}: Schema transform uses deprecated nested format. "
                "The 'source' field must be a simple view name (string). "
                "Move 'schema' or 'schema_file' to top-level action fields."
            )
        elif not isinstance(action.source, str):
            errors.append(
                f"{prefix}: Schema transform source must be a string (view name), "
                f"got {type(action.source).__name__}"
            )
        
        # Must have exactly one of schema_inline or schema_file
        has_schema_inline = hasattr(action, 'schema_inline') and action.schema_inline is not None
        has_schema_file = hasattr(action, 'schema_file') and action.schema_file is not None
        
        if has_schema_inline and has_schema_file:
            errors.append(
                f"{prefix}: Schema transform cannot specify both 'schema_inline' and 'schema_file'. "
                "Use either inline schema or external schema file, not both."
            )
        elif not has_schema_inline and not has_schema_file:
            errors.append(
                f"{prefix}: Schema transform must specify either 'schema_inline' (inline) or "
                "'schema_file' (external). Schema transforms require a schema definition."
            )
        
        # Validate enforcement if specified
        if hasattr(action, 'enforcement') and action.enforcement is not None:
            if action.enforcement not in ["strict", "permissive"]:
                errors.append(
                    f"{prefix}: Schema transform enforcement must be 'strict' or 'permissive', "
                    f"got '{action.enforcement}'"
                )
        
        # Validate schema_file exists if specified (and project_root is available)
        if has_schema_file and self.project_root:
            from pathlib import Path
            schema_file_path = Path(self.project_root) / action.schema_file
            if not schema_file_path.exists():
                errors.append(
                    f"{prefix}: Schema file '{action.schema_file}' not found at {schema_file_path}"
                )
        
        # Validate that operational metadata columns are not manipulated
        metadata_errors = self._validate_metadata_columns_not_manipulated(
            action, prefix, self.project_config
        )
        errors.extend(metadata_errors)
        
        return errors

