"""Snapshot-CDC source-function loader.

Reads a user-supplied Python file, locates the named function via AST,
validates its parameters, and returns the extracted function source
code, name, and parameters as a :class:`SourceFunctionResult`.

This module is pure: it does not mutate any generator instance state.
The companion ``_build_source_expression`` (in
``streaming_table.py``) consumes the result and is the only piece that
needs to mutate generator state (to register a ``functools.partial``
import).
"""

import ast
import logging
from pathlib import Path
from typing import Any, Dict, NamedTuple, Optional

from ...core.loaders.external_file_loader import load_external_file_text
from ...errors import ErrorCategory, LHPError

logger = logging.getLogger(__name__)

# Allowed types for source_function parameter values
_ALLOWED_PARAM_TYPES = (str, int, float, bool, list, dict, type(None))


class SourceFunctionResult(NamedTuple):
    """Result of processing a source_function configuration."""

    code: str
    name: str
    parameters: Optional[Dict[str, Any]] = None


def load_source_function(
    source_function_config: Dict[str, Any],
    context: Optional[Dict[str, Any]] = None,
) -> SourceFunctionResult:
    """Process source_function configuration and return function code, name, and parameters.

    Args:
        source_function_config: Dict with 'file', 'function', and optional 'parameters' keys
        context: Generation context containing substitution_manager and other data

    Returns:
        SourceFunctionResult with code, name, and optional parameters
    """
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

    # Locate and read the function file using the shared external-file
    # loader. ``load_external_file_text`` internally calls
    # ``resolve_external_file_path`` (which raises a rich LHPFileError via
    # ``ErrorFormatter.file_not_found`` with structured search locations
    # when the file is missing) then reads the file with consistent
    # UnicodeDecodeError / PermissionError handling.
    project_root = context.get("project_root", Path.cwd()) if context else Path.cwd()
    source_code = load_external_file_text(
        file_name, project_root, file_type="snapshot source function file"
    )

    # Apply substitutions to the source code and parameters
    if context and "substitution_manager" in context:
        substitution_mgr = context["substitution_manager"]
        source_code = substitution_mgr._process_string(source_code)

        if parameters:
            parameters = substitution_mgr.substitute_yaml(parameters)

        # Single collection point after ALL substitutions
        secret_refs = substitution_mgr.secret_references
        if "secret_references" in context and context["secret_references"] is not None:
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
        ) from e

    # Find the function node once for both validation and extraction
    func_node = _find_function_node(tree, function_name)

    # Validate parameters against function signature if provided
    if parameters:
        _validate_function_parameters(func_node, function_name, parameters)

    # Extract the specific function
    function_code = _extract_function_code(source_code, tree, function_name, func_node)

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


def _find_function_node(
    tree: ast.Module, function_name: str
) -> "ast.FunctionDef | None":
    """Find a top-level FunctionDef by name in the AST."""
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == function_name:
            return node
    return None


def _validate_function_parameters(
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
        func_node = _find_function_node(tree, function_name)

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

        # __future__ imports must be hoisted to the top of the assembled
        # module per PEP 236; CodeAssembler.assemble (invoked by
        # CodeGenerationService.generate_flowgroup_code) handles this
        # centrally. Strip here so the inlined source-function block does
        # not embed a duplicate the chokepoint would then have to harvest.
        if imp.startswith("from __future__"):
            skip_import = True
        # Skip base pyspark session imports (these are redundant in DLT)
        elif imp.startswith("from pyspark import") or imp.startswith("import pyspark"):
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
