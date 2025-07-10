"""Streaming table write generator - adapted from BurrowBuilder."""

import ast
from typing import Dict, List, Tuple
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.dqe import DQEParser
from ...utils.error_formatter import LHPError, ErrorCategory


class StreamingTableWriteGenerator(BaseActionGenerator):
    """Generate streaming table write actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate streaming table code."""
        target_config = action.write_target
        if not target_config:
            raise ValueError(
                "Streaming table action must have write_target configuration"
            )

        # Extract source views as a list
        source_views = self._extract_source_views(action.source)

        # Extract configuration
        mode = target_config.get(
            "mode", "standard"
        )  # "cdc" and "snapshot_cdc" are special modes
        database = target_config.get("database")
        table = target_config.get("table") or target_config.get("name")

        # For CDC modes, always create the table since CDC flows need dedicated tables
        if mode in ["cdc", "snapshot_cdc"]:
            create_table = True
        else:
            create_table = target_config.get(
                "create_table", True
            )  # Default to True for standard mode

        # Build full table name
        full_table_name = f"{database}.{table}" if database else table

        # Table properties with defaults
        properties = {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.enableChangeDataFeed": "true",
        }
        if target_config.get("table_properties"):
            properties.update(target_config["table_properties"])

        # Spark configuration
        spark_conf = target_config.get("spark_conf", {})

        # Schema definition (SQL DDL string or StructType)
        schema = target_config.get("table_schema") or target_config.get("schema")

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
        source_function_name = None
        if mode == "snapshot_cdc" and snapshot_cdc_config.get("source_function"):
            source_function_code, source_function_name = self._process_source_function(
                snapshot_cdc_config["source_function"]
            )

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
                            "flow_name": flow_name,
                            "description": action.description
                            or f"Append flow to {full_table_name} from {source_view}",
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
                        "flow_name": flow_name,
                        "description": action.description
                        or f"Append flow to {full_table_name}",
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
            "source_function_name": source_function_name,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
            "description": action.description or f"Append flow to {full_table_name}",
            "once": action.once or False,  # Keep for backward compatibility
            "action_metadata": action_metadata,  # New: individual action metadata
        }

        # Enable stream readMode for CDC
        if (
            mode == "cdc"
            and isinstance(action.source, dict)
            and action.source.get("type") == "delta"
        ):
            action.source["readMode"] = "stream"
            action.source["read_change_feed"] = True

        return self.render_template("write/streaming_table.py.j2", template_context)

    def _extract_source_views(self, source) -> List[str]:
        """Extract source views as a list from action source."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            # Handle list of sources - each can be string or dict
            result = []
            for item in source:
                if isinstance(item, str):
                    result.append(item)
                elif isinstance(item, dict):
                    # Handle database field in source configuration
                    database = item.get("database")
                    table = (
                        item.get("table") or item.get("view") or item.get("name", "")
                    )

                    if database and table:
                        result.append(f"{database}.{table}")
                    elif table:
                        result.append(table)
            return result
        elif isinstance(source, dict):
            # Handle database field in source configuration
            database = source.get("database")
            table = source.get("table") or source.get("view") or source.get("name", "")

            if database and table:
                return [f"{database}.{table}"]
            elif table:
                return [table]
            else:
                return []
        else:
            return []

    def _process_source_function(
        self, source_function_config: Dict[str, str]
    ) -> Tuple[str, str]:
        """Process source_function configuration and return function code and function name.

        Args:
            source_function_config: Dict with 'file' and 'function' keys

        Returns:
            Tuple of (function_code, function_name)
        """
        from pathlib import Path

        file_name = source_function_config.get("file")
        function_name = source_function_config.get("function")

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
        possible_paths = [
            # Relative to current pipeline directory
            Path("pipelines") / "bronze_dimensions" / file_name,
            # Relative to project root
            Path(file_name),
            # In current working directory
            Path.cwd() / file_name,
            # In tpch_lakehouse directory (if we're in subdirectory)
            Path.cwd() / "tpch_lakehouse" / file_name,
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
                    f"   • pipelines/bronze_dimensions/{file_name}",
                    f"   • {file_name} (project root)",
                    "",
                    "Ensure the file contains your snapshot function definition",
                    "Check the file path in your YAML configuration for typos",
                ],
                example=f"""1. Create the file: pipelines/bronze_dimensions/{file_name}

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
        with open(function_file_path, "r") as f:
            source_code = f.read()

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

        # Extract the specific function
        function_code = self._extract_function_code(source_code, tree, function_name)

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

        return function_code, function_name

    def _extract_function_code(
        self, source_code: str, tree: ast.Module, function_name: str
    ) -> str:
        """Extract function code and its dependencies from the AST.

        Args:
            source_code: Original source code
            tree: Parsed AST
            function_name: Name of function to extract

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

        # Find the function definition
        for node in ast.walk(tree):
            if isinstance(node, ast.FunctionDef) and node.name == function_name:
                # Extract the function lines
                start_line = node.lineno - 1
                end_line = (
                    node.end_lineno
                    if hasattr(node, "end_lineno")
                    else len(source_lines)
                )

                function_lines = source_lines[start_line:end_line]
                break

        if not function_lines:
            return ""

        # Combine imports and function
        result = []

        # Add necessary imports (filter out duplicates and common ones)
        unique_imports = []
        for imp in imports:
            # Skip imports that are usually already available in DLT context
            if not any(skip in imp for skip in ["pyspark", "spark"]):
                if imp not in unique_imports:
                    unique_imports.append(imp)

        # Add DataFrame import if it's referenced in type hints
        function_text = "\n".join(function_lines)
        if "DataFrame" in function_text and not any(
            "DataFrame" in imp for imp in unique_imports
        ):
            unique_imports.insert(0, "from pyspark.sql import DataFrame")

        if unique_imports:
            result.extend(unique_imports)
            result.append("")  # Empty line after imports

        # Add function code
        result.extend(function_lines)

        return "\n".join(result)
