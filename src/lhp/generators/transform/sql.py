"""SQL transformation generator for LakehousePlumber."""

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action


class SQLTransformGenerator(BaseActionGenerator):
    """Generate SQL transformation actions."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate SQL transform code."""
        # Get SQL query from action
        sql_query = self._get_sql_query(action, context.get("spec_dir"))

        # Determine if this creates a view or table
        is_final_target = context.get("is_final_target", False)
        target_table = context.get("target_table")

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "sql_query": sql_query,
            "source_refs": self._extract_source_refs(action.source),
            "is_final_target": is_final_target,
            "target_table": target_table,
            "description": action.description or f"SQL transform: {action.name}",
        }

        return self.render_template("transform/sql.py.j2", template_context)

    def _get_sql_query(self, action: Action, spec_dir: Path = None) -> str:
        """Get SQL query from action configuration."""
        if action.sql:
            return action.sql.strip()
        elif action.sql_path:
            sql_file = Path(action.sql_path)
            if not sql_file.is_absolute() and spec_dir:
                sql_file = spec_dir / sql_file

            if not sql_file.exists():
                raise FileNotFoundError(f"SQL file not found: {sql_file}")

            return sql_file.read_text().strip()
        else:
            raise ValueError(f"SQL transform '{action.name}' must have sql or sql_path")

    def _extract_source_refs(self, source) -> list:
        """Extract source references for DLT read calls."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            return source
        else:
            return []
