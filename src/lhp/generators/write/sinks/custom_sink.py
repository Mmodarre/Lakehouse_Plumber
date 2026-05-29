"""Custom Python sink generator."""

import logging
import re
from pathlib import Path
from typing import Any, Dict

from ....core.loaders.external_file_loader import load_external_file_text
from ....errors import ErrorFormatter
from lhp.models import Action
from ....core.python_file_copier import copy_user_module_for_pipeline
from .base_sink import BaseSinkWriteGenerator


class CustomSinkWriteGenerator(BaseSinkWriteGenerator):
    """Generate custom Python sink write actions via copy-and-import.

    Mirrors :class:`CustomDataSourceLoadGenerator`: copies the user's
    ``DataSink`` source into ``custom_python_functions/`` next to the generated
    pipeline, registers the package with PySpark's vendored cloudpickle, and
    imports + registers the class on the local Spark session.
    """

    def __init__(self):
        # Enable ImportManager for advanced import handling
        super().__init__()
        self.logger = logging.getLogger(__name__)

    def _extract_datasink_format_name(self, source_code: str, class_name: str) -> str:
        """Extract the format name from the DataSink class name() method."""
        try:
            # Look for the class definition and its name() method
            # Make parentheses optional to handle classes with and without inheritance
            class_pattern = rf"class\s+{re.escape(class_name)}\s*(?:\([^)]*\))?:"
            class_match = re.search(class_pattern, source_code, re.MULTILINE)

            if not class_match:
                self.logger.warning(f"Could not find class {class_name} in sink code")
                return class_name  # Fallback to class name

            # Find the name() method within the class
            class_start = class_match.end()

            # Look for the name() method after the class definition
            # Pattern allows for docstrings and whitespace between method definition and return
            name_method_pattern = (
                r'@classmethod\s+def\s+name\s*\([^)]*\):.*?return\s+["\']([^"\']+)["\']'
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

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate custom sink code via copy-and-import."""
        sink_config = action.write_target

        # Extract configuration
        module_path = sink_config.get("module_path")
        custom_sink_class = sink_config.get("custom_sink_class")
        sink_name = sink_config.get("sink_name")
        options = sink_config.get("options", {})
        self.logger.debug(
            f"Generating custom sink for action '{action.name}': class='{custom_sink_class}', sink_name='{sink_name}'"
        )

        # Validate required fields
        if not module_path:
            raise ErrorFormatter.missing_required_field(
                field_name="module_path",
                component_type="Custom sink write action",
                component_name=action.name,
                field_description="This field specifies the Python module containing the custom DataSink class.",
                example_config=f"""actions:
  - name: {action.name}
    type: write
    source: v_data
    write_target:
      type: sink
      sink_type: custom
      module_path: "sinks/my_sink.py"       # Required
      custom_sink_class: "MyCustomDataSink"  # Required
      sink_name: "my_custom_sink"            # Required
      options:                                # Optional
        endpoint: "https://api.example.com"
        api_key: "your-api-key" """,
            )

        if not custom_sink_class:
            raise ErrorFormatter.missing_required_field(
                field_name="custom_sink_class",
                component_type="Custom sink write action",
                component_name=action.name,
                field_description="This field specifies the name of the custom DataSink class to register and use.",
                example_config=f"""actions:
  - name: {action.name}
    type: write
    source: v_data
    write_target:
      type: sink
      sink_type: custom
      module_path: "sinks/my_sink.py"       # Required
      custom_sink_class: "MyCustomDataSink"  # Required
      sink_name: "my_custom_sink"            # Required
      options:                                # Optional
        endpoint: "https://api.example.com"
        api_key: "your-api-key" """,
            )

        # Read the user's sink module via the shared external-file loader so
        # that missing-file / permission / encoding errors match every other
        # generator. The copy itself happens later via
        # ``copy_user_module_for_pipeline``.
        project_root = context.get("spec_dir") or Path.cwd()
        raw_sink_code = load_external_file_text(
            module_path, project_root, file_type="custom sink file"
        )

        # Substitute so format-name extraction sees resolved tokens (e.g.
        # ``${sink_format}`` in the user's ``name()`` return value).
        # ``copy_user_module`` re-substitutes when copying — idempotent for
        # resolved tokens.
        if "substitution_manager" in context:
            raw_sink_code = context["substitution_manager"]._process_string(
                raw_sink_code
            )

        # Extract the format name against the substituted source so the
        # generated ``format=...`` value matches the user's post-substitution
        # ``name()`` return value.
        datasink_format_name = self._extract_datasink_format_name(
            raw_sink_code, custom_sink_class
        )

        # Copy the user's module into custom_python_functions/ alongside the
        # generated pipeline file. Skipped in dry-run (output_dir is None).
        module_name = copy_user_module_for_pipeline(
            module_path, context, component_label="Custom sink"
        )

        # Plumb the three imports + the cloudpickle registration statement.
        self.add_import("import custom_python_functions")
        self.add_import("from pyspark import cloudpickle as _lhp_cloudpickle")
        self.add_import(
            f"from custom_python_functions.{module_name} import {custom_sink_class}"
        )
        self.add_pre_pipeline_statement(
            "_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)"
        )

        # Extract source views
        source_views = self._extract_source_views(action.source)

        # Get operational metadata configuration
        add_metadata, metadata_columns = self._get_operational_metadata(action, context)

        # Build comment
        comment = (
            sink_config.get("comment")
            or action.description
            or f"Custom sink: {custom_sink_class}"
        )

        # Build template context
        template_context = {
            "action_name": action.name,
            "sink_name": sink_name,
            "custom_sink_class": custom_sink_class,
            "datasink_format_name": datasink_format_name,
            "options": options,
            "source_views": source_views,
            "comment": comment,
            "description": action.description or comment,
            "add_operational_metadata": add_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("write/sinks/custom_sink.py.j2", template_context)
