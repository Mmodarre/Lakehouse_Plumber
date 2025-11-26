"""SQL load generator """

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
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
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "sql_query": sql_query,
            "description": action.description or f"SQL source: {action.name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        return self.render_template("load/sql.py.j2", template_context)

    def _get_sql_query(self, source_config: dict, spec_dir: Path = None, context: dict = None) -> str:
        """Extract SQL query from configuration."""
        sql_content = None
        
        if "sql" in source_config:
            sql_content = source_config["sql"]
        elif "sql_path" in source_config:
            # Use common utility for file loading
            project_root = context.get("project_root", Path.cwd()) if context else (spec_dir or Path.cwd())
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
