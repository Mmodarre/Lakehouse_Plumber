"""Column resolution for operational metadata.

Module-level helpers that look up the available metadata columns, validate
selections against the supported target types, and assemble the final mapping
of column name → expression. Invoked by :class:`OperationalMetadata` as thin
delegators.
"""

import logging
from typing import Any, Dict

from ....errors import ErrorCategory, LHPError
from ....models.config import MetadataColumnConfig

logger = logging.getLogger(__name__)


def _validate_target_type(meta, target_type: str) -> None:
    """Validate target type is supported."""
    del meta  # unused
    valid_types = ["streaming_table", "materialized_view", "view"]
    if target_type not in valid_types:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="008",
            title="Invalid target type for operational metadata",
            details=f"Target type '{target_type}' is not supported for operational metadata.",
            suggestions=[
                f"Use one of the supported target types: {', '.join(valid_types)}",
                "Check your action configuration",
            ],
            context={
                "Target type": target_type,
                "Valid types": valid_types,
            },
        )


def get_selected_columns(
    meta, selection: Dict[str, Any], target_type: str
) -> Dict[str, str]:
    """Get selected columns with expressions for the target type.

    Args:
        meta: :class:`OperationalMetadata` instance providing column catalog
            and substitution context.
        selection: Selection configuration from ``resolve_metadata_selection``.
        target_type: Target type (``streaming_table``, ``materialized_view``,
            or ``view``).

    Returns:
        Dictionary of column_name -> expression.
    """
    if not selection:
        return {}

    # Validate target type
    _validate_target_type(meta, target_type)

    # Get available columns (project config or defaults)
    available_columns = _get_available_columns(meta)

    # Collect selected column names additively
    selected_column_names = set()

    try:
        # Add from preset
        if "preset" in selection and selection["preset"] is not None:
            selected_column_names.update(
                _extract_column_names(meta, selection["preset"])
            )

        # Add from flowgroup
        if "flowgroup" in selection and selection["flowgroup"] is not None:
            selected_column_names.update(
                _extract_column_names(meta, selection["flowgroup"])
            )

        # Add from action
        if "action" in selection and selection["action"] is not None:
            selected_column_names.update(
                _extract_column_names(meta, selection["action"])
            )

    except Exception as e:
        # Re-raise LHPError as-is, wrap other errors
        if isinstance(e, LHPError):
            raise
        else:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="008",
                title="Error processing operational metadata selection",
                details=f"An error occurred while processing operational metadata selection: {str(e)}",
                suggestions=[
                    "Check your operational_metadata configuration syntax",
                    "Verify column names are correctly specified",
                    "Ensure selection values are proper types (list of strings)",
                ],
                context={
                    "Selection": selection,
                    "Target type": target_type,
                    "Original error": str(e),
                },
            ) from e

    # Filter by target type and build result
    result = {}
    for column_name in selected_column_names:
        if column_name in available_columns:
            column_config = available_columns[column_name]
            if target_type in column_config.applies_to:
                # Apply context substitutions
                try:
                    expression = meta._apply_substitutions(column_config.expression)
                    result[column_name] = expression
                except Exception as e:
                    raise LHPError(
                        category=ErrorCategory.CONFIG,
                        code_number="009",
                        title="Error applying substitutions to metadata column",
                        details=f"Failed to apply substitutions to column '{column_name}': {str(e)}",
                        suggestions=[
                            "Check the expression syntax in your column configuration",
                            "Verify substitution placeholders are valid (e.g., ${pipeline_name})",
                            "Ensure the expression is valid PySpark code",
                        ],
                        context={
                            "Column name": column_name,
                            "Expression": column_config.expression,
                            "Error": str(e),
                        },
                    ) from e

    return result


def _get_available_columns(meta) -> Dict[str, MetadataColumnConfig]:
    """Get available metadata columns from project config or defaults."""
    # Use locally adapted project columns if available (from adapt_expressions_for_imports)
    if (
        hasattr(meta, "_adapted_project_columns")
        and meta._adapted_project_columns is not None
    ):
        return meta._adapted_project_columns
    # Otherwise use original project config or defaults
    elif meta.project_config and meta.project_config.columns:
        return meta.project_config.columns
    else:
        return meta.default_columns


def _extract_column_names(meta, selection, context: str = "metadata") -> set:
    """Extract column names from selection configuration.

    Args:
        meta: :class:`OperationalMetadata` instance providing the catalog and
            logger.
        selection: Selection configuration (list of strings).
        context: Context for error handling (``"metadata"`` for lenient, others
            for strict).

    Returns:
        Set of column names.
    """
    if isinstance(selection, list):
        # List of specific column names - validate they exist
        available_columns = set(_get_available_columns(meta).keys())
        invalid_columns = set(selection) - available_columns

        if invalid_columns:
            if context == "metadata":
                # Lenient: Log warning and filter out unknown metadata columns
                logger.warning(
                    f"Ignoring unknown metadata columns: {', '.join(sorted(invalid_columns))}"
                )
                return set(selection) - invalid_columns
            else:
                # Strict: Throw error for other contexts
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="006",
                    title="Invalid operational metadata column references",
                    details=f"The following columns are not defined in the project configuration: {', '.join(sorted(invalid_columns))}",
                    suggestions=[
                        "Define these columns in the operational_metadata.columns section of lhp.yaml",
                        "Check for typos in column names",
                        "Verify column names are correctly spelled and case-sensitive",
                    ],
                )

        return set(selection)
    else:
        # Invalid type - return empty set
        return set()
