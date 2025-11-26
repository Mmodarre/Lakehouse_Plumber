"""Python load generator for LakehousePlumber."""

import logging
from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter


class PythonLoadGenerator(BaseActionGenerator):
    """Generate Python function load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)

    def generate(self, action: Action, context: dict) -> str:
        """Generate Python load code."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ValueError("Python source must be a configuration object")

        # Extract module and function information
        module_path = source_config.get("module_path")
        function_name = source_config.get("function_name", "get_df")
        parameters = source_config.get("parameters", {})

        if not module_path:
            raise ErrorFormatter.missing_required_field(
                field_name="module_path",
                component_type="Python load action",
                component_name=action.name,
                field_description="This field specifies the Python module containing the data loading function.",
                example_config="""actions:
  - name: load_custom_data
    type: load
    sub_type: python
    target: v_custom_data
    source:
      module_path: "transformations/custom_loader.py"  # Required
      function_name: "load_data"                       # Optional (defaults to 'get_df')
      parameters:                                      # Optional
        start_date: "2023-01-01"
        end_date: "2023-12-31" """,
            )

        # Extract module name from path
        # For dotted paths like "my_project.loaders.customer_loader", use the full path
        if "." in module_path:
            # Full dotted path provided
            module_parts = module_path.split(".")
            module_name = module_parts[-1]  # Last part is the module name
            import_path = module_path
        else:
            # Simple module name
            module_name = Path(module_path).stem
            import_path = module_name

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "module_path": module_path,
            "module_name": module_name,
            "function_name": function_name,
            "parameters": parameters,
            "description": action.description
            or f"Python source: {module_name}.{function_name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        # Add import for the module
        self.add_import(f"from {import_path} import {function_name}")

        return self.render_template("load/python.py.j2", template_context)
