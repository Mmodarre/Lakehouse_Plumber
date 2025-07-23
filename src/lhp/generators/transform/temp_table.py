"""Temporary table transformation generator."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata


class TempTableTransformGenerator(BaseActionGenerator):
    """Generate temporary streaming table transformations."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate temporary table transform code."""
        # Extract source view
        source_view = self._extract_source_view(action.source)

        # Get readMode from action or default to batch
        readMode = action.readMode or "batch"

        # Target table name (use exact target from YAML)
        target_table = action.target

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

        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)

        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(
            flowgroup, action, preset_config
        )
        metadata_columns = operational_metadata.get_selected_columns(
            selection or {}, "view"
        )

        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

        template_context = {
            "action_name": action.name,
            "target_table": target_table,
            "source_view": source_view,
            "readMode": readMode,
            "comment": f"Temporary table for {action.target}",
            "table_properties": {},
            "description": action.description or f"Temporary table: {target_table}",
            "sql": action.sql.replace("{source}", source_view) if action.sql else None,
            "add_operational_metadata": bool(metadata_columns),
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
