"""Python load generator for LakehousePlumber."""

import logging

from ...core.registry import BaseActionGenerator
from lhp.models import Action
from ...errors import ErrorFormatter
from lhp.core.codegen import copy_user_module_for_pipeline


class PythonLoadGenerator(BaseActionGenerator):
    """Generate Python function load actions via copy-and-import.

    Mirrors :class:`PythonTransformGenerator`: the user's module file is
    copied into ``custom_python_functions/`` next to the generated pipeline,
    and the generated code imports the loader function from there.
    """

    def __init__(self):
        super().__init__(use_import_manager=True)
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)

    def generate(self, action: Action, context: dict) -> str:
        """Generate Python load code."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ErrorFormatter.invalid_source_format(
                action_name=action.name,
                action_type="python load",
                expected_formats=[
                    "A configuration object (dict) with 'module_path'",
                    "source:\n  module_path: 'transformations/custom_loader.py'\n  function_name: 'load_data'",
                ],
            )
        self.logger.debug(
            f"Generating Python load for target '{action.target}', action '{action.name}'"
        )

        # Process source config through substitution manager first if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )

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

        # Hard-require a .py file path. The legacy dotted-import and
        # bare-module-name forms are no longer accepted; this generator
        # now follows the same copy-and-import convention as the rest of
        # the family (transform/python, custom_datasource, custom_sink).
        if not module_path.endswith(".py"):
            raise ErrorFormatter.file_not_found(
                file_path=module_path,
                search_locations=[
                    "module_path must be a path to a Python file ending in .py",
                ],
                file_type="Python load action module file",
            )

        self.logger.debug(
            f"Python load '{action.name}': module_path='{module_path}', function='{function_name}'"
        )

        # Resolve and copy the user's Python file via the shared helper.
        copied_module_name = copy_user_module_for_pipeline(
            module_path, context, component_label="Python load action"
        )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "module_path": module_path,
            "module_name": copied_module_name,
            "function_name": function_name,
            "parameters": parameters,
            "description": action.description
            or f"Python source: {copied_module_name}.{function_name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        # Import the loader function from the copied module.
        self.add_import(
            f"from custom_python_functions.{copied_module_name} import {function_name}"
        )

        return self.render_template("load/python.py.j2", template_context)
