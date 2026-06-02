"""Operational metadata core: the ``OperationalMetadataCatalog`` class.

Owns the catalog of built-in metadata columns, the merge of preset / flowgroup
/ action selections, the substitution context, column resolution, and the SQL
expression adaptation for wildcard-import-aware codegen.
"""

import logging
import re
from typing import Any, Dict, Optional, Set

from lhp.models import (
    Action,
    FlowGroup,
    MetadataColumnConfig,
    ProjectOperationalMetadataConfig,
)

from ....errors import ErrorFactory, LHPError, codes
from ..imports import ImportDetector

logger = logging.getLogger(__name__)


class OperationalMetadataCatalog:
    """Enhanced operational metadata handler with project-level configuration and ImportManager integration."""

    def __init__(
        self, project_config: Optional[ProjectOperationalMetadataConfig] = None
    ):
        self.logger = logging.getLogger(__name__)
        self.import_detector = ImportDetector(strategy="ast")
        self.project_config = project_config

        # Default metadata columns with adaptive expressions
        # These will be dynamically adjusted based on available imports
        self.default_columns = {
            "_ingestion_timestamp": MetadataColumnConfig(
                expression="F.current_timestamp()",  # Default format
                description="When the record was ingested",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_source_file": MetadataColumnConfig(
                expression="F.input_file_name()",  # Default format
                description="Source file path",
                applies_to=["view"],  # Only views (load actions)
            ),
            "_pipeline_run_id": MetadataColumnConfig(
                expression='F.lit(spark.conf.get("pipelines.id", "unknown"))',  # Default format
                description="Pipeline run identifier",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_pipeline_name": MetadataColumnConfig(
                expression='F.lit("${pipeline_name}")',  # Default format
                description="Pipeline name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_flowgroup_name": MetadataColumnConfig(
                expression='F.lit("${flowgroup_name}")',  # Default format
                description="FlowGroup name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
        }

        # Alternative expressions for when wildcard imports are available
        self.wildcard_expressions = {
            "_ingestion_timestamp": "current_timestamp()",
            "_source_file": "input_file_name()",
            "_pipeline_run_id": 'lit(spark.conf.get("pipelines.id", "unknown"))',
            "_pipeline_name": 'lit("${pipeline_name}")',
            "_flowgroup_name": 'lit("${flowgroup_name}")',
        }

        # Context for substitutions
        self.pipeline_name = None
        self.flowgroup_name = None

    def update_context(self, pipeline_name: str, flowgroup_name: str):
        """Update context for substitutions."""
        self.pipeline_name = pipeline_name
        self.flowgroup_name = flowgroup_name

    def adapt_expressions_for_imports(self, import_manager=None) -> None:
        """Adapt expressions based on available imports from ImportManager.

        If wildcard imports are available, use direct function calls; otherwise
        keep the F. prefix format. Applies to both default columns and
        project-level custom columns (local copies, no mutation of shared
        project config).
        """
        if not import_manager:
            return

        current_imports = import_manager.get_consolidated_imports()
        import_text = "\n".join(current_imports)
        has_functions_wildcard = "from pyspark.sql.functions import *" in import_text

        if not has_functions_wildcard:
            logger.debug("No wildcard imports detected - using F. prefix expressions")
            self._adapted_project_columns = None
            return

        logger.debug(
            "Wildcard imports detected - adapting expressions to use direct function calls"
        )

        for column_name, wildcard_expr in self.wildcard_expressions.items():
            if column_name in self.default_columns:
                original_config = self.default_columns[column_name]
                self.default_columns[column_name] = MetadataColumnConfig(
                    expression=wildcard_expr,
                    description=original_config.description,
                    applies_to=original_config.applies_to,
                    enabled=original_config.enabled,
                    additional_imports=original_config.additional_imports,
                )

        if self.project_config and self.project_config.columns:
            logger.debug("Creating locally adapted project-level column expressions")
            self._adapted_project_columns = {}

            for column_name, column_config in self.project_config.columns.items():
                adapted_expression = self._adapt_expression_for_wildcard(
                    column_config.expression
                )

                if adapted_expression != column_config.expression:
                    logger.debug(
                        f"Adapted {column_name}: '{column_config.expression}' → '{adapted_expression}'"
                    )
                    self._adapted_project_columns[column_name] = MetadataColumnConfig(
                        expression=adapted_expression,
                        description=column_config.description,
                        applies_to=column_config.applies_to,
                        enabled=column_config.enabled,
                        additional_imports=column_config.additional_imports,
                    )
                else:
                    self._adapted_project_columns[column_name] = column_config

    def _adapt_expression_for_wildcard(self, expression: str) -> str:
        """Adapt a single expression to use direct function calls when wildcard imports are available.

        Examples: ``F.current_timestamp()`` → ``current_timestamp()``,
        ``F.col('name')`` → ``col('name')``, ``F.lit('value')`` → ``lit('value')``.
        """
        adaptations = {
            r"\bF\.current_timestamp\(\)": "current_timestamp()",
            r"\bF\.input_file_name\(\)": "input_file_name()",
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
            r"\bF\.sum\(": "sum(",
            r"\bF\.count\(": "count(",
            r"\bF\.max\(": "max(",
            r"\bF\.min\(": "min(",
            r"\bF\.avg\(": "avg(",
            r"\bF\.row_number\(\)": "row_number()",
            r"\bF\.rank\(\)": "rank()",
            r"\bF\.dense_rank\(\)": "dense_rank()",
        }

        adapted_expression = expression
        for pattern, replacement in adaptations.items():
            adapted_expression = re.sub(pattern, replacement, adapted_expression)
        return adapted_expression

    def get_all_column_names(self) -> set:
        """Return union of built-in default AND project-defined column names.

        Used for defensive filtering (e.g. excluding metadata from hash calculations)
        where over-excluding is harmless.
        """
        names = set(self.default_columns.keys())
        if self.project_config and self.project_config.columns:
            names.update(self.project_config.columns.keys())
        return names

    def resolve_metadata_selection(
        self,
        flowgroup: Optional[FlowGroup],
        action: Optional[Action],
        preset_config: dict,
    ) -> Optional[Dict[str, Any]]:
        """Resolve metadata selection across preset, flowgroup, and action levels.

        Returns the combined selection dict, or ``None`` if explicitly disabled
        at the action level or no selections found.
        """
        # Explicit disable at action level wins
        if (
            action
            and hasattr(action, "operational_metadata")
            and action.operational_metadata is False
        ):
            return None

        # Additive collection from all three levels
        result = {}
        if "operational_metadata" in preset_config:
            result["preset"] = preset_config["operational_metadata"]
        if (
            flowgroup
            and hasattr(flowgroup, "operational_metadata")
            and flowgroup.operational_metadata is not None
        ):
            result["flowgroup"] = flowgroup.operational_metadata
        if (
            action
            and hasattr(action, "operational_metadata")
            and action.operational_metadata is not None
        ):
            result["action"] = action.operational_metadata

        return result if result else None

    def get_selected_columns(
        self, selection: Dict[str, Any], target_type: str
    ) -> Dict[str, str]:
        """Get selected columns with expressions for the target type.

        ``selection`` comes from ``resolve_metadata_selection``; ``target_type``
        is one of ``streaming_table``, ``materialized_view``, ``view``. Returns
        a dict of column_name -> expression.
        """
        if not selection:
            return {}

        self._validate_target_type(target_type)
        available_columns = self._get_available_columns()
        selected_column_names = set()

        try:
            for level in ("preset", "flowgroup", "action"):
                if level in selection and selection[level] is not None:
                    selected_column_names.update(
                        self._extract_column_names(selection[level])
                    )
        except Exception as e:
            if isinstance(e, LHPError):
                raise
            else:
                raise ErrorFactory.config_error(
                    codes.CFG_008,
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

        result = {}
        for column_name in selected_column_names:
            if column_name in available_columns:
                column_config = available_columns[column_name]
                if target_type in column_config.applies_to:
                    try:
                        expression = self._apply_substitutions(column_config.expression)
                        result[column_name] = expression
                    except Exception as e:
                        raise ErrorFactory.config_error(
                            codes.CFG_009,
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

    def _get_available_columns(self) -> Dict[str, MetadataColumnConfig]:
        """Get available metadata columns: adapted (from
        ``adapt_expressions_for_imports``), then project config, then defaults.
        """
        if (
            hasattr(self, "_adapted_project_columns")
            and self._adapted_project_columns is not None
        ):
            return self._adapted_project_columns
        if self.project_config and self.project_config.columns:
            return self.project_config.columns
        return self.default_columns

    def _extract_column_names(self, selection, context: str = "metadata") -> set:
        """Extract column names from selection (a list of strings).

        ``context="metadata"`` is lenient (logs and drops unknown columns);
        any other context is strict and raises ``LHPError``.
        """
        if not isinstance(selection, list):
            return set()

        available_columns = set(self._get_available_columns().keys())
        invalid_columns = set(selection) - available_columns

        if invalid_columns:
            if context == "metadata":
                logger.warning(
                    f"Ignoring unknown metadata columns: {', '.join(sorted(invalid_columns))}"
                )
                return set(selection) - invalid_columns
            raise ErrorFactory.config_error(
                codes.CFG_006,
                title="Invalid operational metadata column references",
                details=f"The following columns are not defined in the project configuration: {', '.join(sorted(invalid_columns))}",
                suggestions=[
                    "Define these columns in the operational_metadata.columns section of lhp.yaml",
                    "Check for typos in column names",
                    "Verify column names are correctly spelled and case-sensitive",
                ],
            )

        return set(selection)

    def _validate_target_type(self, target_type: str) -> None:
        valid_types = ["streaming_table", "materialized_view", "view"]
        if target_type not in valid_types:
            raise ErrorFactory.config_error(
                codes.CFG_008,
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

    def _apply_substitutions(self, expression: str) -> str:
        """Apply ``${pipeline_name}`` / ``${flowgroup_name}`` context substitutions."""
        if self.pipeline_name:
            expression = expression.replace("${pipeline_name}", self.pipeline_name)
        if self.flowgroup_name:
            expression = expression.replace("${flowgroup_name}", self.flowgroup_name)
        return expression

    def get_required_imports(self, columns: Dict[str, str]) -> Set[str]:
        """Get the union of imports required by selected columns.

        ``columns`` is a dict of column_name -> expression as returned from
        ``get_selected_columns``.
        """
        if not columns:
            return set()

        imports = set()
        available_columns = self._get_available_columns()

        for column_name, expression in columns.items():
            imports.update(self.import_detector.detect_imports(expression))
            if column_name in available_columns:
                column_config = available_columns[column_name]
                if column_config.additional_imports:
                    imports.update(column_config.additional_imports)

        return imports
