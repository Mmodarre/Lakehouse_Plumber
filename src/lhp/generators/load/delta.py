"""Delta load generator """

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from typing import Dict, Any


class DeltaLoadGenerator(BaseActionGenerator):
    """Generate Delta table load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: Dict[str, Any]) -> str:
        """Generate Delta load code."""
        source_config = action.source if isinstance(action.source, dict) else {}

        # Extract configuration
        table = source_config.get("table")
        catalog = source_config.get("catalog")
        database = source_config.get("database")

        # Build table reference
        if catalog and database:
            table_ref = f"{catalog}.{database}.{table}"
        elif database:
            table_ref = f"{database}.{table}"
        else:
            table_ref = table

        # Check for CDC configuration
        cdf_enabled = source_config.get("cdf_enabled", False) or source_config.get(
            "read_change_feed", False
        )
        cdc_options = source_config.get("cdc_options", {})

        # Determine readMode - CDC requires streaming
        # First check action.readMode, then source config, then default
        readMode = action.readMode or source_config.get(
            "readMode", "stream" if cdf_enabled else "batch"
        )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )
        
        # Apply additional context substitutions for Delta source
        # Replace ${source_table} placeholder with actual table reference
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", table_ref
            )

        template_context = {
            "target": action.target,
            "table_ref": table_ref,
            "readMode": readMode,
            "cdf_enabled": cdf_enabled,
            "starting_version": (
                cdc_options.get("starting_version", 0) if cdf_enabled else None
            ),
            "starting_timestamp": cdc_options.get("starting_timestamp"),
            "where_clauses": source_config.get("where_clause", []),
            "select_columns": source_config.get("select_columns"),
            "reader_options": source_config.get("reader_options", {}),
            "description": action.description or f"Delta source: {table_ref}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/delta.py.j2", template_context)
