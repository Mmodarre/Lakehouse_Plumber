"""Custom data source load generator for LakehousePlumber."""

import logging
import re
from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter


class CustomDataSourceLoadGenerator(BaseActionGenerator):
    """Generate custom data source load actions with unified import management."""

    def __init__(self):
        # Enable ImportManager for advanced import handling
        super().__init__(use_import_manager=True)
        self.add_import("from pyspark import pipelines as dp")
        self.logger = logging.getLogger(__name__)
        self.custom_source_code = None  # Store for later appending by orchestrator
        self.source_file_path = None    # Track source file

    def _extract_datasource_format_name(self, source_code: str, class_name: str) -> str:
        """Extract the format name from the DataSource class name() method."""
        try:
            # Look for the class definition and its name() method
            class_pattern = rf'class\s+{re.escape(class_name)}\s*\([^)]*\):'
            class_match = re.search(class_pattern, source_code, re.MULTILINE)
            
            if not class_match:
                self.logger.warning(f"Could not find class {class_name} in source code")
                return class_name  # Fallback to class name
            
            # Find the name() method within the class
            class_start = class_match.end()
            
            # Look for the name() method after the class definition
            name_method_pattern = r'@classmethod\s+def\s+name\s*\([^)]*\):\s*return\s+["\']([^"\']+)["\']'
            name_match = re.search(name_method_pattern, source_code[class_start:], re.MULTILINE | re.DOTALL)
            
            if name_match:
                format_name = name_match.group(1)
                self.logger.info(f"Extracted format name '{format_name}' from {class_name}.name() method")
                return format_name
            else:
                self.logger.warning(f"Could not find name() method in {class_name}, using class name as fallback")
                return class_name  # Fallback to class name
                
        except Exception as e:
            self.logger.warning(f"Error extracting format name from {class_name}: {e}, using class name as fallback")
            return class_name  # Fallback to class name

    def generate(self, action: Action, context: dict) -> str:
        """Generate custom data source load code with unified import management."""
        # Extract configuration from source (following cloudfiles pattern)
        source_config = action.source
        if isinstance(source_config, str):
            raise ValueError("Custom data source must be a configuration object")
        
        # Process source config through substitution manager first if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )
        
        module_path = source_config.get('module_path')
        custom_datasource_class = source_config.get('custom_datasource_class')
        parameters = source_config.get('options', {})

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

        # Read custom source file
        project_root = context.get("spec_dir") or Path.cwd()
        source_path = project_root / module_path
        
        if not source_path.exists():
            raise FileNotFoundError(f"Custom data source file not found: {source_path}")
        
        raw_source_code = source_path.read_text()
        self.source_file_path = Path(module_path).as_posix()

        # Apply substitutions to the raw source code if substitution_manager is available
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            raw_source_code = substitution_mgr._process_string(raw_source_code)
            
            # Track secret references if they exist
            secret_refs = substitution_mgr.get_secret_references()
            if "secret_references" in context and context["secret_references"] is not None:
                context["secret_references"].update(secret_refs)

        # Use ImportManager to extract imports and get cleaned source
        self.custom_source_code = self.add_imports_from_file(raw_source_code)
        
        self.logger.info(f"Extracted imports from custom source file: {source_path}")

        # Extract the actual format name from the DataSource class
        datasource_format_name = self._extract_datasource_format_name(
            raw_source_code, custom_datasource_class
        )

        # Get readMode from action or default to stream
        readMode = action.readMode or "stream"

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