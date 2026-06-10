"""Schema-transform + operational-metadata validation for transform actions.

Module-level functions split out of :class:`TransformActionValidator` to
keep that class small (constitution §3.3).
"""

import logging
from pathlib import Path
from typing import List, Optional

from lhp.models import Action, ProjectConfig

logger = logging.getLogger(__name__)


def validate_metadata_columns_not_manipulated(
    action: Action,
    prefix: str,
    project_config: Optional[ProjectConfig],
    project_root: Optional[Path],
) -> List[str]:
    """Validate that operational metadata columns are not renamed or cast."""
    errors = []

    if not project_config or not project_config.operational_metadata:
        return errors

    metadata_columns = set(project_config.operational_metadata.columns.keys())
    if not metadata_columns:
        return errors

    from ....parsers.schema_transform_parser import SchemaTransformParser

    parser = SchemaTransformParser()
    schema_config = {}

    try:
        if action.schema_inline:
            schema_config = parser.parse_inline_schema(action.schema_inline)
        elif action.schema_file and project_root:
            schema_file_path = Path(project_root) / action.schema_file
            if schema_file_path.exists():
                schema_config = parser.parse_file(schema_file_path)
    except Exception as e:
        # Skip — other validators report schema parsing errors.
        logger.debug(
            f"Schema parsing failed during metadata column validation for '{action.name}': {e}"
        )
        return errors

    column_mapping = schema_config.get("column_mapping", {})
    for source_col in column_mapping.keys():
        if source_col in metadata_columns:
            errors.append(
                f"{prefix}: Cannot rename operational metadata column '{source_col}'. "
                f"Operational metadata columns defined in lhp.yaml are automatically managed "
                f"by the framework and cannot be renamed or type-cast in schema transforms.\n"
                f"  → To fix: Remove '{source_col}' from your schema definition."
            )

    reverse_mapping = {target: source for source, target in column_mapping.items()}

    type_casting = schema_config.get("type_casting", {})
    for col in type_casting.keys():
        if col in metadata_columns:
            errors.append(
                f"{prefix}: Cannot type-cast operational metadata column '{col}'. "
                f"Operational metadata columns defined in lhp.yaml are automatically managed "
                f"by the framework and cannot be renamed or type-cast in schema transforms.\n"
                f"  → To fix: Remove '{col}' from your schema definition."
            )
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


def validate_schema_transform(
    action: Action,
    prefix: str,
    project_config: Optional[ProjectConfig],
    project_root: Optional[Path],
) -> List[str]:
    errors = []

    if not action.source:
        errors.append(f"{prefix}: Schema transform must have 'source' (view name)")
    elif isinstance(action.source, dict):
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

    has_schema_inline = (
        hasattr(action, "schema_inline") and action.schema_inline is not None
    )
    has_schema_file = hasattr(action, "schema_file") and action.schema_file is not None

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

    if hasattr(action, "enforcement") and action.enforcement is not None:
        if action.enforcement not in ["strict", "permissive"]:
            errors.append(
                f"{prefix}: Schema transform enforcement must be 'strict' or 'permissive', "
                f"got '{action.enforcement}'"
            )

    if has_schema_file and project_root:
        schema_file_path = Path(project_root) / action.schema_file
        if not schema_file_path.exists():
            errors.append(
                f"{prefix}: Schema file '{action.schema_file}' not found at {schema_file_path}"
            )

    metadata_errors = validate_metadata_columns_not_manipulated(
        action, prefix, project_config, project_root
    )
    errors.extend(metadata_errors)

    return errors
