"""Delta load generator - adapted from BurrowBuilder."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata
from typing import Dict, Any


class DeltaLoadGenerator(BaseActionGenerator):
    """Generate Delta table load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

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
        flowgroup = context.get("flowgroup")
        preset_config = context.get("preset_config", {})
        project_config = context.get("project_config")

        # Initialize operational metadata handler
        operational_metadata = OperationalMetadata(
            project_config=(
                project_config.operational_metadata if project_config else None
            )
        )

        # Update context for substitutions (source table substitution)
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)

        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, "view"
        )

        # Apply additional context substitutions for Delta source
        # Replace ${source_table} placeholder with actual table reference
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", table_ref
            )

        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

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
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        return self.render_template("load/delta.py.j2", template_context)
