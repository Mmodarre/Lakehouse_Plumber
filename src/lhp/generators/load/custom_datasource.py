"""Custom data source load generator for LakehousePlumber."""

import logging
import re
from pathlib import Path

from ...core.loaders.external_file_loader import load_external_file_text
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFormatter
from lhp.models import Action
from lhp.core.codegen import copy_user_module_for_pipeline


class CustomDataSourceLoadGenerator(BaseActionGenerator):
    """Generate custom data source load actions via copy-and-import.

    Copies the user's PySpark ``DataSource`` source file into a
    ``custom_python_functions/`` sibling directory of the generated pipeline
    file, then registers the package with PySpark's *vendored* cloudpickle
    so the class survives serialization to executors. The class is imported
    by name and registered on the local Spark session at module load.
    """

    def __init__(self):
        # Enable ImportManager for advanced import handling
        super().__init__(use_import_manager=True)
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)

    def _extract_datasource_format_name(self, source_code: str, class_name: str) -> str:
        """Extract the format name from the DataSource class name() method."""
        try:
            # Look for the class definition and its name() method
            class_pattern = rf"class\s+{re.escape(class_name)}\s*\([^)]*\):"
            class_match = re.search(class_pattern, source_code, re.MULTILINE)

            if not class_match:
                self.logger.warning(f"Could not find class {class_name} in source code")
                return class_name  # Fallback to class name

            # Find the name() method within the class
            class_start = class_match.end()

            # Look for the name() method after the class definition
            name_method_pattern = (
                r'@classmethod\s+def\s+name\s*\([^)]*\):\s*return\s+["\']([^"\']+)["\']'
            )
            name_match = re.search(
                name_method_pattern, source_code[class_start:], re.MULTILINE | re.DOTALL
            )

            if name_match:
                format_name = name_match.group(1)
                self.logger.info(
                    f"Extracted format name '{format_name}' from {class_name}.name() method"
                )
                return format_name
            else:
                self.logger.warning(
                    f"Could not find name() method in {class_name}, using class name as fallback"
                )
                return class_name  # Fallback to class name

        except (re.error, AttributeError, IndexError) as e:
            self.logger.warning(
                f"Error extracting format name from {class_name}: {e}, using class name as fallback"
            )
            return class_name  # Fallback to class name

    def generate(self, action: Action, context: dict) -> str:
        """Generate custom data source load code via copy-and-import."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ErrorFormatter.invalid_source_format(
                action_name=action.name,
                action_type="custom_datasource",
                expected_formats=[
                    "A configuration object (dict) with 'module_path' and 'custom_datasource_class'",
                    "source:\n  type: custom_datasource\n  module_path: 'path/to/source.py'\n  custom_datasource_class: 'MyDataSource'",
                ],
            )
        self.logger.debug(
            f"Generating custom datasource load for target '{action.target}', action '{action.name}'"
        )

        # Process source config through substitution manager first if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )

        module_path = source_config.get("module_path")
        custom_datasource_class = source_config.get("custom_datasource_class")
        parameters = source_config.get("options", {})

        # Validate required fields
        if not module_path:
            raise ErrorFormatter.missing_required_field(
                field_name="module_path",
                component_type="Custom data source load action",
                component_name=action.name,
                field_description="This field specifies the Python module containing the custom DataSource class.",
                example_config="""actions:
  - name: load_custom_api
    type: load
    source:
      type: custom_datasource
      module_path: "data_sources/api_source.py"       # Required
      custom_datasource_class: "APIDataSource"        # Required
      options:                                         # Optional
        apiKey: "your-api-key"
        endpoint: "https://api.example.com" """,
            )

        if not custom_datasource_class:
            raise ErrorFormatter.missing_required_field(
                field_name="custom_datasource_class",
                component_type="Custom data source load action",
                component_name=action.name,
                field_description="This field specifies the name of the custom DataSource class to register and use.",
                example_config="""actions:
  - name: load_custom_api
    type: load
    source:
      type: custom_datasource
      module_path: "data_sources/api_source.py"       # Required
      custom_datasource_class: "APIDataSource"        # Required
      options:                                         # Optional
        apiKey: "your-api-key"
        endpoint: "https://api.example.com" """,
            )

        # Read the user's source via the shared external-file loader so that
        # missing-file / permission / encoding errors match every other
        # generator. The copy itself happens later via
        # ``copy_user_module_for_pipeline``.
        project_root = context.get("spec_dir") or Path.cwd()
        raw_source_code = load_external_file_text(
            module_path, project_root, file_type="custom data source file"
        )

        # Substitute so format-name extraction sees resolved tokens (e.g.
        # ``${datasource_format}`` in the user's ``name()`` return value).
        # ``copy_user_module`` re-substitutes when copying — idempotent for
        # resolved tokens.
        if "substitution_manager" in context:
            raw_source_code = context["substitution_manager"]._process_string(
                raw_source_code
            )

        # Extract the format name against the substituted source so the
        # generated ``format=...`` value matches the user's post-substitution
        # ``name()`` return value.
        datasource_format_name = self._extract_datasource_format_name(
            raw_source_code, custom_datasource_class
        )

        # Copy the user's module into custom_python_functions/ alongside the
        # generated pipeline file. Skipped in dry-run (output_dir is None).
        module_name = copy_user_module_for_pipeline(
            module_path, context, component_label="Custom data source"
        )

        # Plumb the three imports + the cloudpickle registration statement.
        # Dedupe across actions in the same flowgroup is handled by
        # ImportManager (imports) and the assembler (pre-pipeline statements).
        self.add_import("import custom_python_functions")
        self.add_import("from pyspark import cloudpickle as _lhp_cloudpickle")
        self.add_import(
            f"from custom_python_functions.{module_name} import {custom_datasource_class}"
        )
        self.add_pre_pipeline_statement(
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
        )

        # Get readMode from action or default to stream
        readMode = action.readMode or "stream"
        self.logger.debug(
            f"Custom datasource '{action.name}': class='{custom_datasource_class}', "
            f"format='{datasource_format_name}', readMode='{readMode}', module='{module_name}'"
        )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "custom_datasource_class": custom_datasource_class,
            "datasource_format_name": datasource_format_name,
            "readMode": readMode,
            "options": parameters or {},  # Ensure it's never None
            "description": action.description
            or f"Load data from custom data source: {custom_datasource_class}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/custom_datasource.py.j2", template_context)
