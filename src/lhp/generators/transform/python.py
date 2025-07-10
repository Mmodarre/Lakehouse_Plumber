"""Python transformation generator."""

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


class PythonTransformGenerator(BaseActionGenerator):
    """Generate Python transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate Python transform code."""
        # Extract configuration
        transform_config = action.source if isinstance(action.source, dict) else {}

        # Get module and function information
        module_path = transform_config.get("module_path")
        function_name = transform_config.get("function_name", "transform")
        parameters = transform_config.get("parameters", {})

        if not module_path:
            raise ValueError("Python transform must have 'module_path'")

        # Extract module name from path
        # For dotted paths like "my_project.transformers.enrich_customers", use the full path
        if "." in module_path:
            # Full dotted path provided
            module_parts = module_path.split(".")
            module_name = module_parts[-1]  # Last part is the module name
            import_path = module_path
        else:
            # Simple module name
            module_name = Path(module_path).stem
            import_path = module_name

        # Determine source view(s)
        source_views = self._extract_source_views(transform_config)

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_views": source_views,
            "readMode": readMode,
            "module_path": module_path,
            "module_name": module_name,
            "function_name": function_name,
            "parameters": parameters,
            "description": action.description
            or f"Python transform: {module_name}.{function_name}",
        }

        # Add import for the module
        self.add_import(f"from {import_path} import {function_name}")

        return self.render_template("transform/python.py.j2", template_context)

    def _extract_source_views(self, config) -> list:
        """Extract source view names from configuration."""
        # Look for sources, views, or view field
        sources = config.get("sources") or config.get("views") or config.get("view", [])
        if isinstance(sources, str):
            return [sources]
        elif isinstance(sources, list):
            return sources
        else:
            return []
