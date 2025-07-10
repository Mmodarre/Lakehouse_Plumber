"""JDBC load generator with secret support."""

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.operational_metadata import OperationalMetadata


class JDBCLoadGenerator(BaseActionGenerator):
    """Generate JDBC load actions with secret support."""

    def __init__(self):
        super().__init__()
        self.add_import("import dlt")

    def generate(self, action: Action, context: dict) -> str:
        """Generate JDBC load code with secret substitution."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ValueError("JDBC source must be a configuration object")

        # Process source config through substitution manager first if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
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

        # Apply additional context substitutions for JDBC source
        table_name = source_config.get("table", "unknown_table")
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", table_name
            )

        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "jdbc_url": source_config.get("url"),
            "jdbc_user": source_config.get("user"),
            "jdbc_password": source_config.get("password"),
            "jdbc_driver": source_config.get("driver"),
            "jdbc_query": source_config.get("query"),
            "jdbc_table": source_config.get("table"),
            "description": action.description or f"JDBC source: {action.name}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        code = self.render_template("load/jdbc.py.j2", template_context)

        # Secret processing is now handled centrally by ActionOrchestrator using SecretCodeGenerator
        # No need to process secrets here - just return the code with placeholders

        return code
