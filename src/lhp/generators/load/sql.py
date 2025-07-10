"""SQL load generator - adapted from BurrowBuilder."""

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata
from ...utils.error_formatter import ErrorFormatter


class SQLLoadGenerator(BaseActionGenerator):
    """Generate SQL query load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate SQL load code."""
        source_config = action.source

        # Get SQL query
        if isinstance(source_config, str):
            sql_query = source_config
        elif isinstance(source_config, dict):
            sql_query = self._get_sql_query(source_config, context.get("spec_dir"))
        else:
            raise ValueError("SQL source must be a string or configuration object")

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
            "target_view": action.target,
            "sql_query": sql_query,
            "description": action.description or f"SQL source: {action.name}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        return self.render_template("load/sql.py.j2", template_context)

    def _get_sql_query(self, source_config: dict, spec_dir: Path = None) -> str:
        """Extract SQL query from configuration."""
        if "sql" in source_config:
            return source_config["sql"]
        elif "sql_path" in source_config:
            sql_file = Path(source_config["sql_path"])
            if not sql_file.is_absolute() and spec_dir:
                sql_file = spec_dir / sql_file

            if not sql_file.exists():
                # Build search locations for error message
                search_locations = []
                if source_config["sql_path"].startswith("/"):
                    search_locations.append(f"Absolute path: {sql_file}")
                else:
                    search_locations.append(
                        f"Relative to YAML: {spec_dir / source_config['sql_path']}"
                    )
                    search_locations.append(
                        f"Working directory: {Path.cwd() / source_config['sql_path']}"
                    )

                raise ErrorFormatter.file_not_found(
                    file_path=str(source_config["sql_path"]),
                    search_locations=search_locations,
                    file_type="SQL file",
                )

            return sql_file.read_text().strip()
        else:
            raise ValueError("SQL source must have 'sql' or 'sql_path'")
