"""Streaming table write generator"""

import ast
import logging
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.dqe import DQEParser
from ...utils.error_formatter import ErrorCategory, ErrorFormatter, LHPError
from ...utils.external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)
from ...utils.schema_parser import SchemaParser

logger = logging.getLogger(__name__)

# Allowed types for source_function parameter values
_ALLOWED_PARAM_TYPES = (str, int, float, bool, list, dict, type(None))


class SourceFunctionResult(NamedTuple):
    """Result of processing a source_function configuration."""

    code: str
    name: str
    parameters: Optional[Dict[str, Any]] = None


class StreamingTableWriteGenerator(BaseActionGenerator):
    """Generate streaming table write actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.schema_parser = SchemaParser()

    def generate(self, action: Action, context: dict) -> str:
        """Generate streaming table code."""
        target_config = action.write_target
        if not target_config:
            raise ErrorFormatter.missing_required_field(
                field_name="write_target",
                component_type="Streaming table write action",
                component_name=action.name,
                field_description="The write_target configuration is required for streaming table actions.",
                example_config="""actions:
  - name: write_data
    type: write
    sub_type: streaming_table
    source: v_transformed
    write_target:
      table: my_table
      catalog: my_catalog
      schema: my_schema""",
            )
        logger.debug(f"Generating streaming table write for action '{action.name}'")

        # Extract source views as a list
        source_views = self._extract_source_views(action.source)

        # Get readMode from action or default to stream
        readMode = action.readMode or "stream"

        # Extract configuration
        mode = target_config.get(
            "mode", "standard"
        )  # Valid modes: "standard" (default), "cdc", "snapshot_cdc"
        catalog = target_config.get("catalog")
        schema = target_config.get("schema")
        table = target_config.get("table")

        # Snapshot CDC still requires a dedicated table; other modes honor the flag.
        if mode == "snapshot_cdc":
            create_table = True
        else:
            create_table = target_config.get("create_table", True)

        # Build full table name (normalizer guarantees catalog/schema are present)
        full_table_name = f"{catalog}.{schema}.{table}" if catalog and schema else table
        logger.debug(
            f"Streaming table '{action.name}': target='{full_table_name}', mode='{mode}', readMode='{readMode}', sources={source_views}"
        )

        # Table properties
        properties = {}
        if target_config.get("table_properties"):
            properties.update(target_config["table_properties"])

        # Spark configuration
        spark_conf = target_config.get("spark_conf", {})

        # Schema definition (SQL DDL string or StructType)
        schema_value = target_config.get("table_schema")
        schema = None

        if schema_value:
            # Check if it's a file path
            if is_file_path(schema_value):
                # Load from external file
                project_root = context.get("project_root", Path.cwd())
                file_ext = Path(schema_value).suffix.lower()

                if file_ext in [".yaml", ".yml", ".json"]:
                    # YAML/JSON schema - parse and convert to DDL
                    resolved_path = resolve_external_file_path(
                        schema_value, project_root, file_type="table schema file"
                    )
                    schema_data = self.schema_parser.parse_schema_file(resolved_path)
                    schema = self.schema_parser.to_schema_hints(schema_data)
                else:
                    # DDL/SQL file - load as plain text
                    schema = load_external_file_text(
                        schema_value, project_root, file_type="table schema file"
                    ).strip()
            else:
                # Inline DDL
                schema = schema_value

        # Row filter clause
        row_filter = target_config.get("row_filter")

        # Temporary table flag
        temporary = target_config.get("temporary", False)

        # Handle CDC configuration for auto_cdc mode
        cdc_config = target_config.get("cdc_config", {}) if mode == "cdc" else {}

        # Check if we need struct import for sequence_by
        if (
            mode == "cdc"
            and cdc_config.get("sequence_by")
            and isinstance(cdc_config["sequence_by"], list)
        ):
            self.add_import("from pyspark.sql.functions import struct")

        # Handle snapshot CDC configuration for snapshot_cdc mode
        snapshot_cdc_config = (
            target_config.get("snapshot_cdc_config", {})
            if mode == "snapshot_cdc"
            else {}
        )

        # Process source function code for snapshot_cdc mode
        source_function_code = None
        source_expression = None
        if mode == "snapshot_cdc" and snapshot_cdc_config.get("source_function"):
            result = self._process_source_function(
                snapshot_cdc_config["source_function"], context
            )
            source_function_code = result.code
            source_expression = self._build_source_expression(result)

        # Process data quality expectations
        expectations = context.get("expectations", [])
        expect_all = {}
        expect_all_or_drop = {}
        expect_all_or_fail = {}

        if expectations:
            dqe_parser = DQEParser()
            expect_all, expect_all_or_drop, expect_all_or_fail = (
                dqe_parser.parse_expectations(expectations)
            )

        # NOTE: Operational metadata support removed from write actions
        # Metadata should be added at load level and flow through naturally
        metadata_columns = {}
        flowgroup = context.get("flowgroup")

        # Per-flow CDC config for the single-action path (empty for non-CDC).
        flow_cdc_config = self._build_flow_cdc_config(mode, cdc_config)

        # Check if this is a combined action with individual metadata
        if hasattr(action, "_action_metadata") and action._action_metadata:
            # Use new action metadata structure for individual append flows
            action_metadata = action._action_metadata
            flow_name = action_metadata[0][
                "flow_name"
            ]  # Use first flow name for template compatibility
            flow_names = [meta["flow_name"] for meta in action_metadata]
        elif hasattr(action, "_flow_names") and action._flow_names:
            # Legacy combined actions - convert to new structure
            flow_names = action._flow_names
            flow_name = flow_names[0]
            action_metadata = []
            for i, (source_view, flow_name_item) in enumerate(
                zip(source_views, flow_names)
            ):
                action_metadata.append(
                    {
                        "action_name": f"{action.name}_{i+1}",
                        "source_view": source_view,
                        "once": action.once or False,  # Legacy: same once flag for all
                        "flow_name": flow_name_item,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
        else:
            # Single action - create metadata structure for each source view
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]  # Remove "write_" prefix
            base_flow_name = (
                f"f_{base_flow_name}"
                if not base_flow_name.startswith("f_")
                else base_flow_name
            )

            action_metadata = []
            flow_names = []

            if len(source_views) > 1:
                # Multiple sources: create separate append flow for each
                for i, source_view in enumerate(source_views):
                    flow_name = f"{base_flow_name}_{i+1}"
                    action_metadata.append(
                        {
                            "action_name": f"{action.name}_{i+1}",
                            "source_view": source_view,
                            "once": action.once or False,
                            "readMode": action.readMode,  # Preserve readMode
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {full_table_name} from {source_view}",
                            "flow_cdc_config": flow_cdc_config,
                        }
                    )
                    flow_names.append(flow_name)
            else:
                # Single source: create one append flow
                flow_name = base_flow_name
                action_metadata.append(
                    {
                        "action_name": action.name,
                        "source_view": source_views[0] if source_views else "",
                        "once": action.once or False,
                        "readMode": action.readMode,  # Preserve readMode
                        "flow_name": flow_name,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
                        "flow_cdc_config": flow_cdc_config,
                    }
                )
                flow_names.append(flow_name)

            # Set flow_name for backward compatibility (use first flow name)
            flow_name = flow_names[0] if flow_names else base_flow_name

        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),  # Function name safe
            "full_table_name": full_table_name,
            "source_views": source_views,  # Keep for backward compatibility
            "source_view": (
                source_views[0] if source_views and mode == "cdc" else None
            ),  # CDC only supports single source
            "flow_name": flow_name,  # Keep for backward compatibility
            "mode": mode,
            "create_table": create_table,  # Pass create_table flag to template
            "properties": properties,
            "spark_conf": spark_conf,
            "schema": schema,
            "row_filter": row_filter,
            "temporary": temporary,
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "comment": target_config.get("comment", f"Streaming table: {table}"),
            "table_path": target_config.get("path"),
            "cdc_config": cdc_config,
            "snapshot_cdc_config": snapshot_cdc_config,
            "source_function_code": source_function_code,
            "source_expression": source_expression,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
            "description": action.description or f"Append flow to {full_table_name}",
            "once": action.once or False,  # Keep for backward compatibility
            "action_metadata": action_metadata,  # New: individual action metadata
            "readMode": readMode,
        }

        return self.render_template("write/streaming_table.py.j2", template_context)

    def _build_flow_cdc_config(
        self, mode: str, cdc_config: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Build the per-flow CDC config dict for a single CDC action.

        Returns an empty dict for non-CDC modes so the template can safely
        index into it without branching on mode.
        """
        if mode != "cdc":
            return {}
        return {
            "ignore_null_updates": cdc_config.get("ignore_null_updates"),
            "apply_as_deletes": cdc_config.get("apply_as_deletes"),
            "apply_as_truncates": cdc_config.get("apply_as_truncates"),
            "column_list": cdc_config.get("column_list"),
            "except_column_list": cdc_config.get("except_column_list"),
        }

    def _extract_source_views(self, source) -> List[str]:
        """Extract source views as a list from action source."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            result = []
            for item in source:
                if isinstance(item, str):
                    result.append(item)
                else:
                    logger.warning(
                        f"Unexpected source item type {type(item).__name__}, skipping"
                    )
            return result
        else:
            logger.warning(
                f"Unexpected source type {type(source).__name__}, returning empty list"
            )
            return []

    def _build_source_expression(self, result: SourceFunctionResult) -> str:
        """Build the source= expression for snapshot CDC.

        Returns a bare function name when no parameters, or a partial()
        expression with keyword arguments when parameters are present.
        """
        if not result.parameters:
            return result.name

        param_parts = [f"{k}={repr(v)}" for k, v in result.parameters.items()]
        params_str = ",\n        ".join(param_parts)
        self.add_import("from functools import partial")
        return f"partial(\n        {result.name},\n        {params_str}\n    )"

    @staticmethod
    def _find_function_node(
        tree: ast.Module, function_name: str
    ) -> "ast.FunctionDef | None":
        """Find a top-level FunctionDef by name in the AST."""
        for node in tree.body:
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                return node
        return None

    def _process_source_function(
        self, source_function_config: Dict[str, Any], context: Dict[str, Any] = None
    ) -> SourceFunctionResult:
        """Process source_function configuration and return function code, name, and parameters.

        Args:
            source_function_config: Dict with 'file', 'function', and optional 'parameters' keys
            context: Generation context containing substitution_manager and other data

        Returns:
            SourceFunctionResult with code, name, and optional parameters
        """
        from pathlib import Path

        file_name = source_function_config.get("file")
        function_name = source_function_config.get("function")
        parameters = source_function_config.get("parameters")

        if not file_name or not function_name:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="002",
                title="Incomplete source_function configuration",
                details="The source_function configuration is missing required fields.",
                suggestions=[
                    "Specify both 'file' and 'function' in your source_function config",
                    "Check your YAML syntax and indentation",
                ],
                example="""Correct configuration:
snapshot_cdc_config:
  source_function:
    file: "functions/my_snapshots.py"    # ← Required
    function: "my_snapshot_function"     # ← Required
  keys: ["id"]
  stored_as_scd_type: 2""",
                context={
                    "Provided file": file_name,
                    "Provided function": function_name,
                },
            )

        # Find the function file - try multiple locations
        project_root = (
            context.get("project_root", Path.cwd()) if context else Path.cwd()
        )
        possible_paths = [
            # Relative to project root (most common)
            project_root / file_name,
            # As absolute/relative path directly
            Path(file_name),
            # In current working directory
            Path.cwd() / file_name,
        ]

        function_file_path = None
        for path in possible_paths:
            if path.exists():
                function_file_path = path
                break

        if not function_file_path:
            # Convert paths to relative project paths for better readability
            project_root = Path.cwd()
            relative_paths = []
            for path in possible_paths:
                try:
                    rel_path = path.relative_to(project_root)
                    relative_paths.append(str(rel_path))
                except ValueError:
                    # If path is outside project, show as absolute
                    relative_paths.append(str(path))

            raise LHPError(
                category=ErrorCategory.IO,
                code_number="002",
                title="Snapshot function file not found",
                details=f"Cannot locate the Python file containing your snapshot function: '{file_name}'",
                suggestions=[
                    "Create the function file in one of these locations:",
                    f"   • {file_name} (relative to project root)",
                    "",
                    "Ensure the file contains your snapshot function definition",
                    "Check the file path in your YAML configuration for typos",
                ],
                example=f"""1. Create the file: {file_name}

2. Add your function:
   from typing import Optional, Tuple
   from pyspark.sql import DataFrame
   
   def your_function_name(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
       # Your snapshot logic here
       if latest_version is None:
           df = spark.read.table("your_snapshot_table")
           return (df, 1)
       # More logic...
       return None

3. Reference it in YAML:
   snapshot_cdc_config:
     source_function:
       file: "{file_name}"
       function: "your_function_name" """,
                context={"File": file_name, "Searched Locations": relative_paths},
            )

        # Read and parse the Python file
        with open(function_file_path, "r", encoding="utf-8") as f:
            source_code = f.read()

        # Apply substitutions to the source code and parameters
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            source_code = substitution_mgr._process_string(source_code)

            if parameters:
                parameters = substitution_mgr.substitute_yaml(parameters)

            # Single collection point after ALL substitutions
            secret_refs = substitution_mgr.get_secret_references()
            if (
                "secret_references" in context
                and context["secret_references"] is not None
            ):
                context["secret_references"].update(secret_refs)

        try:
            tree = ast.parse(source_code)
        except SyntaxError as e:
            raise LHPError(
                category=ErrorCategory.IO,
                code_number="003",
                title="Python syntax error in function file",
                details=f"The function file '{file_name}' contains invalid Python syntax: {e}",
                suggestions=[
                    "Check the Python syntax in your function file",
                    "Ensure proper indentation (use spaces, not tabs)",
                    "Verify all parentheses, brackets, and quotes are properly closed",
                    "Test the file independently: python -m py_compile your_file.py",
                ],
                example="""Valid function file example:
from typing import Optional, Tuple
from pyspark.sql import DataFrame

def my_snapshot_function(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    if latest_version is None:
        df = spark.read.table("my_table")
        return (df, 1)
    return None""",
                context={"File": file_name, "Syntax Error": str(e)},
            )

        # Find the function node once for both validation and extraction
        func_node = self._find_function_node(tree, function_name)

        # Validate parameters against function signature if provided
        if parameters:
            self._validate_function_parameters(func_node, function_name, parameters)

        # Extract the specific function
        function_code = self._extract_function_code(
            source_code, tree, function_name, func_node
        )

        if not function_code:
            raise LHPError(
                category=ErrorCategory.IO,
                code_number="004",
                title=f"Function '{function_name}' not found in file",
                details=f"The function '{function_name}' is not defined in the file '{file_name}'",
                suggestions=[
                    f"Define a function named '{function_name}' in your file",
                    "Check for typos in the function name",
                    "Ensure the function is defined at the top level (not nested inside another function)",
                    "Verify the function name matches exactly (case-sensitive)",
                ],
                example=f"""Add this function to {file_name}:

def {function_name}(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
    \"\"\"
    Your snapshot processing logic here.

    Args:
        latest_version: Most recent version processed, or None for first run

    Returns:
        Tuple of (DataFrame, version_number) or None if no more data
    \"\"\"
    if latest_version is None:
        # First run logic
        df = spark.read.table("your_snapshot_table")
        return (df, 1)

    # Subsequent runs logic
    return None  # No more snapshots""",
                context={"File": file_name, "Expected Function": function_name},
            )

        return SourceFunctionResult(function_code, function_name, parameters or None)

    def _validate_function_parameters(
        self,
        func_node: "ast.FunctionDef | None",
        function_name: str,
        parameters: Dict[str, Any],
    ) -> None:
        """Validate that parameters match the function's keyword-only arguments.

        Skips name validation if the function accepts **kwargs.

        Args:
            func_node: The AST node for the function, or None if not found
            function_name: Name of the target function (for error messages)
            parameters: Parameter dict from YAML config

        Raises:
            LHPError: If parameter names don't match keyword-only args,
                      or if parameter values have unsupported types
        """
        # Type guard: validate parameter values
        for key, value in parameters.items():
            if not isinstance(value, _ALLOWED_PARAM_TYPES):
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="005",
                    title="Unsupported parameter type in source_function",
                    details=(
                        f"Parameter '{key}' has type '{type(value).__name__}', "
                        f"which is not supported."
                    ),
                    suggestions=[
                        "Supported types: str, int, float, bool, list, dict, None",
                        f"Convert '{key}' to one of the supported types",
                    ],
                )

        if func_node is None:
            # Function not found — skip name validation,
            # _extract_function_code will raise its own error
            return

        # If function accepts **kwargs, skip name validation
        if func_node.args.kwarg:
            logger.debug(
                f"Function '{function_name}' accepts **kwargs, "
                f"skipping parameter name validation"
            )
            return

        # Collect keyword-only argument names
        kw_only_names = {arg.arg for arg in func_node.args.kwonlyargs}

        # Check for unknown parameter names
        unknown = set(parameters.keys()) - kw_only_names
        if unknown:
            raise LHPError(
                category=ErrorCategory.CONFIG,
                code_number="006",
                title="Unknown parameters for source_function",
                details=(
                    f"Parameters {sorted(unknown)} are not keyword-only arguments "
                    f"of function '{function_name}'."
                ),
                suggestions=[
                    (
                        f"Available keyword-only arguments: {sorted(kw_only_names)}"
                        if kw_only_names
                        else f"Function '{function_name}' has no keyword-only arguments. "
                        f"Add a '*' separator before the parameters you want to bind."
                    ),
                    "Ensure parameter names in YAML match the function signature",
                    f"Example function signature: def {function_name}("
                    f"latest_version, *, {', '.join(sorted(parameters.keys()))})",
                ],
            )

    def _extract_function_code(
        self,
        source_code: str,
        tree: ast.Module,
        function_name: str,
        func_node: "ast.FunctionDef | None" = None,
    ) -> str:
        """Extract function code and its dependencies from the AST.

        Args:
            source_code: Original source code
            tree: Parsed AST
            function_name: Name of function to extract
            func_node: Pre-found FunctionDef node (avoids redundant traversal)

        Returns:
            Complete function code with imports and dependencies
        """
        source_lines = source_code.split("\n")
        function_lines = []
        imports = []

        # Extract only top-level imports (not nested within functions)
        for node in tree.body:
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                import_line = source_lines[node.lineno - 1].strip()
                imports.append(import_line)

        # Use pre-found node or fall back to search
        if func_node is None:
            func_node = self._find_function_node(tree, function_name)

        if func_node is not None:
            start_line = func_node.lineno - 1
            end_line = (
                func_node.end_lineno
                if hasattr(func_node, "end_lineno")
                else len(source_lines)
            )
            function_lines = source_lines[start_line:end_line]

        if not function_lines:
            return ""

        # Combine imports and function
        result = []

        # Add necessary imports (filter out duplicates and imports truly available in DLT context)
        unique_imports = []
        for imp in imports:
            # Skip ONLY the imports that are truly available in DLT context
            # Keep pyspark.sql.functions, pyspark.sql.types, pyspark.sql, and other specific imports
            skip_import = False

            # Skip base pyspark session imports (these are redundant in DLT)
            if imp.startswith("from pyspark import") or imp.startswith(
                "import pyspark"
            ):
                skip_import = True
            # Skip spark session imports (spark is available in DLT)
            elif "SparkSession" in imp or "getOrCreate" in imp:
                skip_import = True

            if not skip_import and imp not in unique_imports:
                unique_imports.append(imp)

        if unique_imports:
            result.extend(unique_imports)
            result.append("")  # Empty line after imports

        # Add function code
        result.extend(function_lines)

        return "\n".join(result)
