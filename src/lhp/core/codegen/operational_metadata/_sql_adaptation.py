"""SQL expression adaptation for wildcard-import-aware codegen.

When the generated module imports ``from pyspark.sql.functions import *``,
operational-metadata expressions can drop the ``F.`` prefix. This module
contains the module-level adapter functions invoked by
:class:`OperationalMetadata` as thin delegators.
"""

import logging
import re

from ....models.config import MetadataColumnConfig

logger = logging.getLogger(__name__)


def adapt_expressions_for_imports(meta, import_manager=None) -> None:
    """Adapt expressions based on available imports from ImportManager.

    If wildcard imports are available, use direct function calls.
    Otherwise, use the F. prefix format.

    This adaptation applies to both default columns AND project-level custom
    columns. Creates local copies to avoid mutating shared project config.

    Args:
        meta: :class:`OperationalMetadata` instance to mutate.
        import_manager: Optional ImportManager instance to check available imports.
    """
    if not import_manager:
        return  # No adaptation needed

    # Get current imports to check for wildcard patterns
    current_imports = import_manager.get_consolidated_imports()
    import_text = "\n".join(current_imports)

    # Check if wildcard imports are available
    has_functions_wildcard = "from pyspark.sql.functions import *" in import_text

    if has_functions_wildcard:
        logger.debug(
            "Wildcard imports detected - adapting expressions to use direct function calls"
        )

        # Update default columns to use direct function calls
        for column_name, wildcard_expr in meta.wildcard_expressions.items():
            if column_name in meta.default_columns:
                # Create updated config with wildcard expression
                original_config = meta.default_columns[column_name]
                meta.default_columns[column_name] = MetadataColumnConfig(
                    expression=wildcard_expr,
                    description=original_config.description,
                    applies_to=original_config.applies_to,
                    enabled=original_config.enabled,
                    additional_imports=original_config.additional_imports,
                )

        # FIXED: Create local adapted copy of project-level custom columns
        if meta.project_config and meta.project_config.columns:
            logger.debug(
                "Creating locally adapted project-level column expressions"
            )

            # Create local adapted columns dict (don't mutate original)
            meta._adapted_project_columns = {}

            for column_name, column_config in meta.project_config.columns.items():
                # Adapt F. prefix expressions to direct calls
                adapted_expression = _adapt_expression_for_wildcard(
                    meta, column_config.expression
                )

                if adapted_expression != column_config.expression:
                    logger.debug(
                        f"Adapted {column_name}: '{column_config.expression}' → '{adapted_expression}'"
                    )

                    # Create local copy with adapted expression (don't mutate original)
                    meta._adapted_project_columns[column_name] = MetadataColumnConfig(
                        expression=adapted_expression,
                        description=column_config.description,
                        applies_to=column_config.applies_to,
                        enabled=column_config.enabled,
                        additional_imports=column_config.additional_imports,
                    )
                else:
                    # No adaptation needed - use original
                    meta._adapted_project_columns[column_name] = column_config
    else:
        logger.debug(
            "No wildcard imports detected - using F. prefix expressions"
        )
        # Clear any local adaptations
        meta._adapted_project_columns = None


def _adapt_expression_for_wildcard(meta, expression: str) -> str:
    """Adapt a single expression to use direct function calls when wildcard imports are available.

    Examples:
    - "F.current_timestamp()" → "current_timestamp()"
    - "F.col('name')" → "col('name')"
    - "F.lit('value')" → "lit('value')"

    Args:
        meta: :class:`OperationalMetadata` instance (unused; kept for symmetry).
        expression: Original expression with potential F. prefix.

    Returns:
        Adapted expression with direct function calls.
    """
    del meta  # currently unused; reserved for future per-instance overrides
    # Common PySpark function patterns to adapt
    adaptations = {
        # Basic functions
        r"\bF\.current_timestamp\(\)": "current_timestamp()",
        r"\bF\.input_file_name\(\)": "input_file_name()",
        # Functions with parameters (preserve parameters)
        r"\bF\.col\(": "col(",
        r"\bF\.lit\(": "lit(",
        r"\bF\.when\(": "when(",
        r"\bF\.coalesce\(": "coalesce(",
        r"\bF\.concat\(": "concat(",
        r"\bF\.upper\(": "upper(",
        r"\bF\.lower\(": "lower(",
        r"\bF\.trim\(": "trim(",
        r"\bF\.split\(": "split(",
        r"\bF\.regexp_replace\(": "regexp_replace(",
        r"\bF\.date_format\(": "date_format(",
        r"\bF\.to_timestamp\(": "to_timestamp(",
        r"\bF\.from_unixtime\(": "from_unixtime(",
        r"\bF\.unix_timestamp\(": "unix_timestamp(",
        # Aggregate functions
        r"\bF\.sum\(": "sum(",
        r"\bF\.count\(": "count(",
        r"\bF\.max\(": "max(",
        r"\bF\.min\(": "min(",
        r"\bF\.avg\(": "avg(",
        # Window functions
        r"\bF\.row_number\(\)": "row_number()",
        r"\bF\.rank\(\)": "rank()",
        r"\bF\.dense_rank\(\)": "dense_rank()",
    }

    # Apply adaptations
    adapted_expression = expression
    for pattern, replacement in adaptations.items():
        adapted_expression = re.sub(pattern, replacement, adapted_expression)

    return adapted_expression
