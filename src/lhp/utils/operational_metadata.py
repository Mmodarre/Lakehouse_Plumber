"""Enhanced operational metadata handling for LakehousePlumber.

Provides functionality to add operational metadata columns to DLT tables
with project-level configuration and automatic import detection using AST parsing.
"""

import ast
import re
import logging
from typing import Dict, List, Any, Optional, Set, Union
from ..models.config import (
    FlowGroup,
    Action,
    MetadataColumnConfig,
    ProjectOperationalMetadataConfig,
)
from ..utils.error_formatter import LHPError, ErrorCategory


class ImportDetector:
    """Detects required imports from PySpark expressions using AST parsing."""

    def __init__(self, strategy: str = "ast"):
        self.logger = logging.getLogger(__name__)
        self.strategy = strategy

        # Fallback regex patterns for when AST parsing fails
        self.fallback_patterns = {
            r"\bF\.": "from pyspark.sql import functions as F",
            r"\budf\(": "from pyspark.sql.functions import udf",
            r"\bpandas_udf\(": "from pyspark.sql.functions import pandas_udf",
            r"\bbroadcast\(": "from pyspark.sql.functions import broadcast",
            r"\bStringType\(\)": "from pyspark.sql.types import StringType",
            r"\bIntegerType\(\)": "from pyspark.sql.types import IntegerType",
            r"\bDoubleType\(\)": "from pyspark.sql.types import DoubleType",
            r"\bBooleanType\(\)": "from pyspark.sql.types import BooleanType",
            r"\bTimestampType\(\)": "from pyspark.sql.types import TimestampType",
        }

        # Function to import mapping for AST parsing
        self.function_imports = {
            ("F", "*"): "from pyspark.sql import functions as F",
            ("udf", None): "from pyspark.sql.functions import udf",
            ("pandas_udf", None): "from pyspark.sql.functions import pandas_udf",
            ("broadcast", None): "from pyspark.sql.functions import broadcast",
            ("StringType", None): "from pyspark.sql.types import StringType",
            ("IntegerType", None): "from pyspark.sql.types import IntegerType",
            ("DoubleType", None): "from pyspark.sql.types import DoubleType",
            ("BooleanType", None): "from pyspark.sql.types import BooleanType",
            ("TimestampType", None): "from pyspark.sql.types import TimestampType",
        }

    def detect_imports(self, expression: str) -> Set[str]:
        """Detect required imports from a PySpark expression.

        Args:
            expression: PySpark expression string

        Returns:
            Set of import statements required
        """
        if self.strategy == "ast":
            return self._detect_imports_ast(expression)
        else:
            return self._detect_imports_regex(expression)

    def _detect_imports_ast(self, expression: str) -> Set[str]:
        """Detect imports using AST parsing with regex fallback."""
        try:
            # Try to parse as an expression
            tree = ast.parse(expression, mode="eval")
            visitor = FunctionCallVisitor()
            visitor.visit(tree)

            imports = set()
            for func_call in visitor.function_calls:
                if len(func_call) == 2:
                    module, function = func_call
                    if function is None:
                        # Direct function call like udf(), StringType()
                        if (module, None) in self.function_imports:
                            imports.add(self.function_imports[(module, None)])
                    else:
                        # Attribute access like F.current_timestamp
                        if (module, "*") in self.function_imports:
                            imports.add(self.function_imports[(module, "*")])

            return imports

        except (SyntaxError, ValueError) as e:
            # Fallback to regex detection
            self.logger.debug(
                f"AST parsing failed for expression '{expression}': {e}. Using regex fallback."
            )
            return self._detect_imports_regex(expression)

    def _detect_imports_regex(self, expression: str) -> Set[str]:
        """Detect imports using regex patterns."""
        imports = set()

        for pattern, import_statement in self.fallback_patterns.items():
            if re.search(pattern, expression):
                imports.add(import_statement)

        return imports


class FunctionCallVisitor(ast.NodeVisitor):
    """AST visitor to collect function calls."""

    def __init__(self):
        self.function_calls = []

    def visit_Call(self, node):
        """Visit function calls like udf(), StringType(), F.current_timestamp()."""
        if isinstance(node.func, ast.Name):
            # Direct function call: udf(), StringType(), etc.
            self.function_calls.append((node.func.id, None))
        elif isinstance(node.func, ast.Attribute):
            # Method call: F.current_timestamp(), obj.method(), etc.
            if isinstance(node.func.value, ast.Name):
                self.function_calls.append((node.func.value.id, node.func.attr))

        self.generic_visit(node)

    def visit_Attribute(self, node):
        """Visit attribute access (e.g., F.current_timestamp)."""
        if isinstance(node.value, ast.Name):
            # This is a simple attribute access like F.current_timestamp
            self.function_calls.append((node.value.id, node.attr))

        self.generic_visit(node)

    def visit_Name(self, node):
        """Visit standalone function names."""
        if isinstance(node.ctx, ast.Load):
            # This is a function name being loaded
            self.function_calls.append((node.id, None))

        self.generic_visit(node)


class OperationalMetadata:
    """Enhanced operational metadata handler with project-level configuration."""

    def __init__(
        self, project_config: Optional[ProjectOperationalMetadataConfig] = None
    ):
        self.logger = logging.getLogger(__name__)
        self.import_detector = ImportDetector(strategy="ast")
        self.project_config = project_config

        # Default metadata columns (backward compatibility)
        self.default_columns = {
            "_ingestion_timestamp": MetadataColumnConfig(
                expression="F.current_timestamp()",
                description="When the record was ingested",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_source_file": MetadataColumnConfig(
                expression="F.input_file_name()",
                description="Source file path",
                applies_to=["view"],  # Only views (load actions)
            ),
            "_pipeline_run_id": MetadataColumnConfig(
                expression='F.lit(spark.conf.get("pipelines.id", "unknown"))',
                description="Pipeline run identifier",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_pipeline_name": MetadataColumnConfig(
                expression='F.lit("${pipeline_name}")',
                description="Pipeline name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
            "_flowgroup_name": MetadataColumnConfig(
                expression='F.lit("${flowgroup_name}")',
                description="FlowGroup name",
                applies_to=["streaming_table", "materialized_view", "view"],
            ),
        }

        # Context for substitutions
        self.pipeline_name = None
        self.flowgroup_name = None

    def update_context(self, pipeline_name: str, flowgroup_name: str):
        """Update context for template substitutions."""
        self.pipeline_name = pipeline_name
        self.flowgroup_name = flowgroup_name

    def resolve_metadata_selection(
        self, flowgroup: FlowGroup, action: Action, preset_config: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Resolve metadata selection across preset, flowgroup, and action levels.

        Args:
            flowgroup: FlowGroup configuration
            action: Action configuration
            preset_config: Preset configuration dictionary

        Returns:
            Resolved metadata selection or None if disabled
        """
        # Check for explicit disable at action level first
        if (
            hasattr(action, "operational_metadata")
            and action.operational_metadata is False
        ):
            # Explicitly disabled at action level - no metadata at all
            return None

        # Always collect from all levels for additive behavior
        result = {}

        # Add preset level selection
        if "operational_metadata" in preset_config:
            result["preset"] = preset_config["operational_metadata"]

        # Add flowgroup level selection
        if (
            hasattr(flowgroup, "operational_metadata")
            and flowgroup.operational_metadata is not None
        ):
            result["flowgroup"] = flowgroup.operational_metadata

        # Add action level selection (unless it's False, which we already handled)
        if (
            hasattr(action, "operational_metadata")
            and action.operational_metadata is not None
        ):
            result["action"] = action.operational_metadata

        # Return combined result or None if no selections found
        return result if result else None

    def _extract_column_names(
        self, selection: Union[bool, List[str]], context: str = "metadata"
    ) -> Set[str]:
        """Extract column names from selection configuration.

        Args:
            selection: Selection configuration (bool or list of strings)
            context: Context for error handling ("metadata" for lenient, others for strict)

        Returns:
            Set of column names
        """
        if selection is True:
            # Boolean true means all available columns
            return set(self._get_available_columns().keys())
        elif isinstance(selection, list):
            # List of specific column names - validate they exist
            available_columns = set(self._get_available_columns().keys())
            invalid_columns = set(selection) - available_columns

            if invalid_columns:
                if context == "metadata":
                    # Lenient: Log warning and filter out unknown metadata columns
                    self.logger.warning(
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
                        example="""Add missing columns to lhp.yaml:

operational_metadata:
  columns:
    _ingestion_timestamp:
      expression: "F.current_timestamp()"
      description: "When record was ingested"
    _your_custom_column:
      expression: "F.lit('your_value')"
      description: "Your custom metadata" """,
                        context={
                            "Invalid columns": list(invalid_columns),
                            "Available columns": list(available_columns),
                        },
                    )

            # List of specific column names
            return set(selection)
        else:
            # Invalid or empty selection
            return set()

    def _validate_target_type(self, target_type: str):
        """Validate target type is supported.

        Args:
            target_type: Target type to validate
        """
        valid_types = ["streaming_table", "materialized_view", "view"]
        if target_type not in valid_types:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="007",
                title="Invalid target type for operational metadata",
                details=f"Target type '{target_type}' is not supported for operational metadata",
                suggestions=[
                    f"Use one of the supported target types: {', '.join(valid_types)}",
                    "Check your target configuration",
                ],
                context={
                    "Provided target type": target_type,
                    "Valid target types": valid_types,
                },
            )

    def get_selected_columns(
        self, selection: Dict[str, Any], target_type: str
    ) -> Dict[str, str]:
        """Get selected columns with expressions for the target type.

        Args:
            selection: Selection configuration from resolve_metadata_selection
            target_type: Target type ('streaming_table' or 'materialized_view')

        Returns:
            Dictionary of column_name -> expression
        """
        if not selection:
            return {}

        # Validate target type
        self._validate_target_type(target_type)

        # Get available columns (project config or defaults)
        available_columns = self._get_available_columns()

        # Collect selected column names
        selected_column_names = set()

        try:
            # Add from preset
            if "preset" in selection and selection["preset"] is not None:
                selected_column_names.update(
                    self._extract_column_names(selection["preset"])
                )

            # Add from flowgroup
            if "flowgroup" in selection and selection["flowgroup"] is not None:
                selected_column_names.update(
                    self._extract_column_names(selection["flowgroup"])
                )

            # Add from action
            if "action" in selection and selection["action"] is not None:
                selected_column_names.update(
                    self._extract_column_names(selection["action"])
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
                        "Ensure selection values are proper types (bool or list of strings)",
                    ],
                    context={
                        "Selection": selection,
                        "Target type": target_type,
                        "Original error": str(e),
                    },
                )

        # Filter by target type and enabled status, then build result
        result = {}
        for column_name in selected_column_names:
            if column_name in available_columns:
                column_config = available_columns[column_name]
                # Check if column is enabled and applies to target type
                if column_config.enabled and target_type in column_config.applies_to:
                    # Apply context substitutions
                    try:
                        expression = self._apply_substitutions(column_config.expression)
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
                        )

        return result

    def _get_available_columns(self) -> Dict[str, MetadataColumnConfig]:
        """Get available metadata columns from project config or defaults."""
        if self.project_config and self.project_config.columns:
            return self.project_config.columns
        else:
            return self.default_columns

    def _apply_substitutions(self, expression: str) -> str:
        """Apply context substitutions to expression.

        Args:
            expression: Expression with possible substitutions

        Returns:
            Expression with substitutions applied
        """
        if self.pipeline_name:
            expression = expression.replace("${pipeline_name}", self.pipeline_name)
        if self.flowgroup_name:
            expression = expression.replace("${flowgroup_name}", self.flowgroup_name)

        return expression

    def get_required_imports(self, columns: Dict[str, str]) -> Set[str]:
        """Get required imports for selected columns.

        Args:
            columns: Dictionary of column_name -> expression

        Returns:
            Set of import statements required
        """
        if not columns:
            return set()

        imports = set()
        available_columns = self._get_available_columns()

        for column_name, expression in columns.items():
            # Get imports from expression
            imports.update(self.import_detector.detect_imports(expression))

            # Add additional imports from configuration
            if column_name in available_columns:
                column_config = available_columns[column_name]
                if column_config.additional_imports:
                    imports.update(column_config.additional_imports)

        return imports
