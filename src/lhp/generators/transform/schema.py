"""Schema transformation generator."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action

class SchemaTransformGenerator(BaseActionGenerator):
    """Generate schema application transformations."""
    
    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
        self.add_import("from pyspark.sql import functions as F")
        self.add_import("from pyspark.sql.types import StructType")
    
    def generate(self, action: Action, context: dict) -> str:
        """Generate schema transform code."""
        schema_config = action.source.get('schema', {}) if isinstance(action.source, dict) else {}
        
        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"
        
        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_view": self._extract_source_view(action.source),
            "readMode": readMode,
            "schema_enforcement": schema_config.get('enforcement', 'strict'),
            "type_casting": schema_config.get('type_casting', {}),
            "column_mapping": schema_config.get('column_mapping', {}),
            "description": action.description or f"Schema application: {action.name}"
        }
        
        return self.render_template("transform/schema.py.j2", template_context)
    
    def _extract_source_view(self, source) -> str:
        """Extract source view name from source configuration."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get('view', source.get('source'))
        else:
            raise ValueError("Invalid source configuration for schema transform") 