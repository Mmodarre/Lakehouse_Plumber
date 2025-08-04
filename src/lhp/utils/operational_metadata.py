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
        """
        Adapt expressions based on available imports from ImportManager.
        
        If wildcard imports are available, use direct function calls.
        Otherwise, use the F. prefix format.
        
        This adaptation applies to both default columns AND project-level custom columns.
        Creates local copies to avoid mutating shared project config.
        
        Args:
            import_manager: Optional ImportManager instance to check available imports
        """
        if not import_manager:
            return  # No adaptation needed
        
        # Get current imports to check for wildcard patterns
        current_imports = import_manager.get_consolidated_imports()
        import_text = "\n".join(current_imports)
        
        # Check if wildcard imports are available
        has_functions_wildcard = "from pyspark.sql.functions import *" in import_text
        
        if has_functions_wildcard:
            self.logger.debug("Wildcard imports detected - adapting expressions to use direct function calls")
            
            # Update default columns to use direct function calls
            for column_name, wildcard_expr in self.wildcard_expressions.items():
                if column_name in self.default_columns:
                    # Create updated config with wildcard expression
                    original_config = self.default_columns[column_name]
                    self.default_columns[column_name] = MetadataColumnConfig(
                        expression=wildcard_expr,
                        description=original_config.description,
                        applies_to=original_config.applies_to,
                        enabled=original_config.enabled,
                        additional_imports=original_config.additional_imports
                    )
            
            # FIXED: Create local adapted copy of project-level custom columns
            if self.project_config and self.project_config.columns:
                self.logger.debug("Creating locally adapted project-level column expressions")
                
                # Create local adapted columns dict (don't mutate original)
                self._adapted_project_columns = {}
                
                for column_name, column_config in self.project_config.columns.items():
                    # Adapt F. prefix expressions to direct calls
                    adapted_expression = self._adapt_expression_for_wildcard(column_config.expression)
                    
                    if adapted_expression != column_config.expression:
                        self.logger.debug(f"Adapted {column_name}: '{column_config.expression}' → '{adapted_expression}'")
                        
                        # Create local copy with adapted expression (don't mutate original)
                        self._adapted_project_columns[column_name] = MetadataColumnConfig(
                            expression=adapted_expression,
                            description=column_config.description,
                            applies_to=column_config.applies_to,
                            enabled=column_config.enabled,
                            additional_imports=column_config.additional_imports
                        )
                    else:
                        # No adaptation needed - use original
                        self._adapted_project_columns[column_name] = column_config
        else:
            self.logger.debug("No wildcard imports detected - using F. prefix expressions")
            # Clear any local adaptations
            self._adapted_project_columns = None
    
    def _adapt_expression_for_wildcard(self, expression: str) -> str:
        """
        Adapt a single expression to use direct function calls when wildcard imports are available.
        
        Examples:
        - "F.current_timestamp()" → "current_timestamp()"
        - "F.col('name')" → "col('name')"
        - "F.lit('value')" → "lit('value')"
        
        Args:
            expression: Original expression with potential F. prefix
            
        Returns:
            Adapted expression with direct function calls
        """
        # Common PySpark function patterns to adapt
        adaptations = {
            # Basic functions
            r'\bF\.current_timestamp\(\)': 'current_timestamp()',
            r'\bF\.input_file_name\(\)': 'input_file_name()',
            
            # Functions with parameters (preserve parameters)
            r'\bF\.col\(': 'col(',
            r'\bF\.lit\(': 'lit(',
            r'\bF\.when\(': 'when(',
            r'\bF\.coalesce\(': 'coalesce(',
            r'\bF\.concat\(': 'concat(',
            r'\bF\.upper\(': 'upper(',
            r'\bF\.lower\(': 'lower(',
            r'\bF\.trim\(': 'trim(',
            r'\bF\.split\(': 'split(',
            r'\bF\.regexp_replace\(': 'regexp_replace(',
            r'\bF\.date_format\(': 'date_format(',
            r'\bF\.to_timestamp\(': 'to_timestamp(',
            r'\bF\.from_unixtime\(': 'from_unixtime(',
            r'\bF\.unix_timestamp\(': 'unix_timestamp(',
            
            # Aggregate functions
            r'\bF\.sum\(': 'sum(',
            r'\bF\.count\(': 'count(',
            r'\bF\.max\(': 'max(',
            r'\bF\.min\(': 'min(',
            r'\bF\.avg\(': 'avg(',
            
            # Window functions
            r'\bF\.row_number\(\)': 'row_number()',
            r'\bF\.rank\(\)': 'rank()',
            r'\bF\.dense_rank\(\)': 'dense_rank()',
        }
        
        # Apply adaptations
        adapted_expression = expression
        for pattern, replacement in adaptations.items():
            adapted_expression = re.sub(pattern, replacement, adapted_expression)
        
        return adapted_expression

    def resolve_metadata_selection(
        self, flowgroup: Optional[FlowGroup], action: Optional[Action], preset_config: dict
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
        if action and hasattr(action, 'operational_metadata') and action.operational_metadata is False:
            # Explicitly disabled at action level - no metadata at all
            return None
        
        # Always collect from all levels for additive behavior
        result = {}
        
        # Add preset level selection
        if 'operational_metadata' in preset_config:
            result['preset'] = preset_config['operational_metadata']
        
        # Add flowgroup level selection
        if flowgroup and hasattr(flowgroup, 'operational_metadata') and flowgroup.operational_metadata is not None:
            result['flowgroup'] = flowgroup.operational_metadata
        
        # Add action level selection (unless it's False, which we already handled)
        if action and hasattr(action, 'operational_metadata') and action.operational_metadata is not None:
            result['action'] = action.operational_metadata
        
        # Return combined result or None if no selections found
        return result if result else None

    def _validate_target_type(self, target_type: str):
        """Validate target type is supported."""
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
        self, selection: Dict[str, Any], target_type: str
    ) -> Dict[str, str]:
        """Get selected columns with expressions for the target type.
        
        Args:
            selection: Selection configuration from resolve_metadata_selection
            target_type: Target type ('streaming_table', 'materialized_view', or 'view')
            
        Returns:
            Dictionary of column_name -> expression
        """
        if not selection:
            return {}
        
        # Validate target type
        self._validate_target_type(target_type)
        
        # Get available columns (project config or defaults)
        available_columns = self._get_available_columns()
        
        # Collect selected column names additively
        selected_column_names = set()
        
        try:
            # Add from preset
            if 'preset' in selection and selection['preset'] is not None:
                selected_column_names.update(self._extract_column_names(selection['preset']))
            
            # Add from flowgroup
            if 'flowgroup' in selection and selection['flowgroup'] is not None:
                selected_column_names.update(self._extract_column_names(selection['flowgroup']))
            
            # Add from action
            if 'action' in selection and selection['action'] is not None:
                selected_column_names.update(self._extract_column_names(selection['action']))
        
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
                        "Ensure selection values are proper types (list of strings)"
                    ],
                    context={
                        "Selection": selection,
                        "Target type": target_type,
                        "Original error": str(e)
                    }
                )
        
        # Filter by target type and build result
        result = {}
        for column_name in selected_column_names:
            if column_name in available_columns:
                column_config = available_columns[column_name]
                if target_type in column_config.applies_to:
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
                                "Ensure the expression is valid PySpark code"
                            ],
                            context={
                                "Column name": column_name,
                                "Expression": column_config.expression,
                                "Error": str(e)
                            }
                        )
        
        return result

    def _get_available_columns(self) -> Dict[str, MetadataColumnConfig]:
        """Get available metadata columns from project config or defaults."""
        # Use locally adapted project columns if available (from adapt_expressions_for_imports)
        if hasattr(self, '_adapted_project_columns') and self._adapted_project_columns is not None:
            return self._adapted_project_columns
        # Otherwise use original project config or defaults
        elif self.project_config and self.project_config.columns:
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

    def _extract_column_names(self, selection, context: str = "metadata") -> set:
        """Extract column names from selection configuration.
        
        Args:
            selection: Selection configuration (list of strings)
            context: Context for error handling ("metadata" for lenient, others for strict)
            
        Returns:
            Set of column names
        """
        if isinstance(selection, list):
            # List of specific column names - validate they exist
            available_columns = set(self._get_available_columns().keys())
            invalid_columns = set(selection) - available_columns
            
            if invalid_columns:
                if context == "metadata":
                    # Lenient: Log warning and filter out unknown metadata columns
                    self.logger.warning(f"Ignoring unknown metadata columns: {', '.join(sorted(invalid_columns))}")
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
                            "Verify column names are correctly spelled and case-sensitive"
                        ]
                    )
            
            return set(selection)
        else:
            # Invalid type - return empty set
            return set()

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
