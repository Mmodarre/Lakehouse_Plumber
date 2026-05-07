"""Python transformation generator."""

import logging
from pathlib import Path

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import (
    ErrorCategory,
    ErrorFormatter,
    LHPValidationError,
)
from .python_file_copier import (  # noqa: F401  (re-exported for test compatibility)
    PythonFunctionConflictError,
)

logger = logging.getLogger(__name__)


class PythonTransformGenerator(BaseActionGenerator):
    """Generate Python transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: dict) -> str:
        """Generate Python transform code."""
        logger.debug(
            f"Generating Python transform for target '{action.target}', action '{action.name}'"
        )
        # Extract module configuration from action level
        module_path = getattr(action, "module_path", None)
        function_name = getattr(action, "function_name", None)
        parameters = getattr(action, "parameters", {})

        # Apply substitution to module_path, function_name, and parameters if available
        if "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            if module_path:
                module_path = substitution_mgr._process_string(module_path)
            if function_name:
                function_name = substitution_mgr._process_string(function_name)
            if parameters:
                parameters = substitution_mgr.substitute_yaml(parameters)

        if not module_path:
            raise ErrorFormatter.missing_required_field(
                field_name="module_path",
                component_type="Python transform action",
                component_name=action.name,
                field_description="This field specifies the Python module containing the transform function.",
                example_config="""actions:
  - name: transform_data
    type: transform
    sub_type: python
    source: v_raw_data
    target: v_transformed
    module_path: "custom_python/my_transform.py"  # Required
    function_name: "transform"                     # Required""",
            )
        if not function_name:
            raise ErrorFormatter.missing_required_field(
                field_name="function_name",
                component_type="Python transform action",
                component_name=action.name,
                field_description="This field specifies the name of the Python function to call for the transformation.",
                example_config="""actions:
  - name: transform_data
    type: transform
    sub_type: python
    source: v_raw_data
    target: v_transformed
    module_path: "custom_python/my_transform.py"  # Required
    function_name: "transform"                     # Required""",
            )

        logger.debug(
            f"Python transform '{action.name}': module_path='{module_path}', function='{function_name}', readMode='{action.readMode or 'batch'}'"
        )

        # Resolve and copy Python file
        project_root = context.get("spec_dir") or Path.cwd()
        copied_module_name = self._copy_python_file(module_path, project_root, context)

        # Determine source view(s) from action.source directly
        source_views = self._extract_source_views_from_action_source(action.source)

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_views": source_views,
            "readMode": readMode,
            "module_path": module_path,
            "module_name": copied_module_name,
            "function_name": function_name,
            "parameters": parameters,
            "description": action.description
            or f"Python transform: {copied_module_name}.{function_name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        # Add import for the copied module
        self.add_import(
            f"from custom_python_functions.{copied_module_name} import {function_name}"
        )

        return self.render_template("transform/python.py.j2", template_context)

    def _extract_source_views_from_action_source(self, source) -> list:
        """Extract source view names from action.source field."""
        if source is None:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="014",
                title="Missing source for Python transform",
                details="Python transform source cannot be None - transforms require input data.",
                suggestions=[
                    "Add a 'source' field specifying the input view name(s)",
                    "Use a string for single source or a list for multiple sources",
                ],
                context={"Source": "None"},
            )
        elif isinstance(source, str):
            return [source]  # Single source view
        elif isinstance(source, list):
            return source  # Multiple source views
        else:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="014",
                title="Invalid source type for Python transform",
                details=(
                    f"Python transform source must be a string or list of strings, "
                    f"got {type(source).__name__}."
                ),
                suggestions=[
                    "Use a string for single source: source: v_raw_data",
                    "Use a list for multiple sources: source: [v_data1, v_data2]",
                ],
                context={"Source Type": type(source).__name__},
            )

    def _copy_python_file(
        self, module_path: str, project_root: Path, context: dict
    ) -> str:
        """Resolve the source file then delegate to ``PythonFileCopier``."""
        source_file = project_root / module_path

        if not source_file.exists():
            raise ErrorFormatter.file_not_found(
                file_path=str(source_file),
                search_locations=[
                    f"Relative to project root: {project_root / module_path}",
                ],
                file_type="Python module file",
            )

        flowgroup = context.get("flowgroup")
        if not flowgroup:
            raise LHPValidationError(
                category=ErrorCategory.VALIDATION,
                code_number="015",
                title="Missing flowgroup context for Python file copying",
                details="Flowgroup context is required for Python file copying but was not provided.",
                suggestions=[
                    "Ensure the action is executed within a flowgroup context",
                    "Check that the flowgroup configuration is valid",
                ],
                context={"Module Path": module_path},
            )

        module_name = Path(module_path).stem
        output_dir = context.get("output_dir")
        if output_dir is None:
            return module_name

        custom_functions_dir = output_dir / "custom_python_functions"

        python_copier = context.get("python_file_copier")
        if python_copier is None:
            from .python_file_copier import PythonFileCopier

            python_copier = PythonFileCopier()

        return python_copier.copy_user_module(
            source_file, module_path, custom_functions_dir, context
        )
