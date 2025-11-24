"""SQL load generator """

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata
from ...utils.external_file_loader import load_external_file_text


class SQLLoadGenerator(BaseActionGenerator):
    """Generate SQL query load actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: dict) -> str:
        """Generate SQL load code."""
        source_config = action.source

        # Get SQL query
        if isinstance(source_config, str):
            sql_query = source_config
        elif isinstance(source_config, dict):
            sql_query = self._get_sql_query(source_config, context.get("spec_dir"), context)
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

    def _get_sql_query(self, source_config: dict, spec_dir: Path = None, context: dict = None) -> str:
        """Extract SQL query from configuration."""
        sql_content = None
        
        if "sql" in source_config:
            sql_content = source_config["sql"]
        elif "sql_path" in source_config:
            # Use common utility for file loading
            project_root = context.get("project_root") if context else (spec_dir or Path.cwd())
            sql_content = load_external_file_text(
                source_config["sql_path"],
                project_root,
                file_type="SQL file"
            ).strip()
        else:
            raise ValueError("SQL source must have 'sql' or 'sql_path'")
        
        # Apply substitutions to the SQL content if substitution_manager is available
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            sql_content = substitution_mgr._process_string(sql_content)
            
            # Track secret references if they exist
            secret_refs = substitution_mgr.get_secret_references()
            if "secret_references" in context and context["secret_references"] is not None:
                context["secret_references"].update(secret_refs)
        
        return sql_content
