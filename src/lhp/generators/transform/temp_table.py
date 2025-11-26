"""Temporary table transformation generator."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


class TempTableTransformGenerator(BaseActionGenerator):
    """Generate temporary streaming table transformations."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: dict) -> str:
        """Generate temporary table transform code."""
        # Extract source view
        source_view = self._extract_source_view(action.source)

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Target table name (use exact target from YAML)
        target_table = action.target

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_table": target_table,
            "source_view": source_view,
            "readMode": readMode,
            "comment": f"Temporary table for {action.target}",
            "table_properties": {},
            "description": action.description or f"Temporary table: {target_table}",
            "sql": action.sql.replace("{source}", source_view) if action.sql else None,
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
        }

        return self.render_template("transform/temp_table.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        """Extract source view name from action source."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get("view", source.get("source", ""))
        else:
            raise ValueError("Temp table transform must have a source view")
