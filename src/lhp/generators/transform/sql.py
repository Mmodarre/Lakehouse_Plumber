"""SQL transformation generator for LakehousePlumber."""

import logging
from pathlib import Path

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter
from ...utils.external_file_loader import load_external_file_text

logger = logging.getLogger(__name__)


class SQLTransformGenerator(BaseActionGenerator):
    """Generate SQL transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: dict) -> str:
        """Generate SQL transform code."""
        sql_type = "sql_path" if action.sql_path else "inline sql"
        logger.debug(
            f"Generating SQL transform for target '{action.target}', action '{action.name}', sql_type='{sql_type}'"
        )

        # Get SQL query from action
        sql_query = self._get_sql_query(action, context.get("spec_dir"), context)

        # Determine if this creates a view or table
        is_final_target = context.get("is_final_target", False)
        target_table = context.get("target_table")
        logger.debug(
            f"SQL transform '{action.name}': is_final_target={is_final_target}, source_refs={self._extract_source_refs(action.source)}"
        )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "sql_query": sql_query,
            "source_refs": self._extract_source_refs(action.source),
            "is_final_target": is_final_target,
            "target_table": target_table,
            "description": action.description or f"SQL transform: {action.name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
        }

        return self.render_template("transform/sql.py.j2", template_context)

    def _get_sql_query(
        self, action: Action, spec_dir: Path = None, context: dict = None
    ) -> str:
        """Get SQL query from action configuration."""
        sql_content = None

        if action.sql:
            sql_content = action.sql.strip()
        elif action.sql_path:
            # Use common utility for file loading
            project_root = (
                context.get("project_root", Path.cwd())
                if context
                else (spec_dir or Path.cwd())
            )
            sql_content = load_external_file_text(
                action.sql_path, project_root, file_type="SQL file"
            ).strip()
        else:
            raise ErrorFormatter.missing_required_field(
                field_name="sql/sql_path",
                component_type="SQL transform action",
                component_name=action.name,
                field_description="SQL transform requires either inline 'sql' or an external 'sql_path'.",
                example_config="""actions:
  - name: transform_data
    type: transform
    sub_type: sql
    source: v_raw_data
    target: v_transformed
    sql: | "SELECT * FROM $source WHERE status = 'active'"
    # OR
    # sql_path: "queries/transform.sql" """,
            )

        # Apply substitutions to the SQL content if substitution_manager is available
        if context and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            sql_content = substitution_mgr._process_string(sql_content)

            # Track secret references if they exist
            secret_refs = substitution_mgr.get_secret_references()
            if (
                "secret_references" in context
                and context["secret_references"] is not None
            ):
                context["secret_references"].update(secret_refs)

        return sql_content

    def _extract_source_refs(self, source) -> list:
        """Extract source references for DLT read calls."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            return source
        else:
            return []
