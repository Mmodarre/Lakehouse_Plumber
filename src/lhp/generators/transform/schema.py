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
        schema_config = (
            action.source.get("schema", {}) if isinstance(action.source, dict) else {}
        )

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Get metadata columns to preserve from project config
        project_config = context.get("project_config")
        metadata_columns = set()
        if project_config and project_config.operational_metadata:
            metadata_columns = set(project_config.operational_metadata.columns.keys())

        # Filter out metadata columns from schema operations
        filtered_column_mapping = {}
        filtered_type_casting = {}

        # Only apply column mapping to non-metadata columns
        for old_col, new_col in schema_config.get("column_mapping", {}).items():
            if old_col not in metadata_columns:
                filtered_column_mapping[old_col] = new_col

        # Only apply type casting to non-metadata columns
        for col, new_type in schema_config.get("type_casting", {}).items():
            if col not in metadata_columns:
                filtered_type_casting[col] = new_type

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "source_view": self._extract_source_view(action.source),
            "readMode": readMode,
            "schema_enforcement": schema_config.get("enforcement", "strict"),
            "type_casting": filtered_type_casting,
            "column_mapping": filtered_column_mapping,
            "description": action.description or f"Schema application: {action.name}",
            "metadata_columns": list(
                metadata_columns
            ),  # Pass to template for reference
        }

        return self.render_template("transform/schema.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        """Extract source view name from source configuration."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get("view", source.get("source"))
        else:
            raise ValueError("Invalid source configuration for schema transform")
