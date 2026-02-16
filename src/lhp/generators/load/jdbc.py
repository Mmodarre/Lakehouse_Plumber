"""JDBC load generator with secret support."""

import logging

from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.error_formatter import ErrorFormatter

logger = logging.getLogger(__name__)


class JDBCLoadGenerator(BaseActionGenerator):
    """Generate JDBC load actions with secret support."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")

    def generate(self, action: Action, context: dict) -> str:
        """Generate JDBC load code with secret substitution."""
        source_config = action.source
        if isinstance(source_config, str):
            raise ErrorFormatter.invalid_source_format(
                action_name=action.name,
                action_type="jdbc load",
                expected_formats=[
                    "A configuration object (dict) with JDBC connection details",
                    "source:\n  type: jdbc\n  url: 'jdbc:postgresql://host:5432/db'\n  table: 'my_table'\n  driver: 'org.postgresql.Driver'",
                ],
            )
        logger.debug(
            f"Generating JDBC load for target '{action.target}', action '{action.name}'"
        )

        # Process source config through substitution manager first if available
        if "substitution_manager" in context:
            source_config = context["substitution_manager"].substitute_yaml(
                source_config
            )

        # Handle operational metadata
        add_operational_metadata, metadata_columns = self._get_operational_metadata(
            action, context
        )

        # Apply additional context substitutions for JDBC source
        table_name = source_config.get("table", "unknown_table")
        for col_name, expression in metadata_columns.items():
            metadata_columns[col_name] = expression.replace(
                "${source_table}", table_name
            )

        jdbc_table = source_config.get("table")
        jdbc_driver = source_config.get("driver")
        logger.debug(
            f"JDBC load '{action.name}': table='{jdbc_table}', driver='{jdbc_driver}'"
        )

        template_context = {
            "action_name": action.name,
            "target_view": action.target,
            "jdbc_url": source_config.get("url"),
            "jdbc_user": source_config.get("user"),
            "jdbc_password": source_config.get("password"),
            "jdbc_driver": jdbc_driver,
            "jdbc_query": source_config.get("query"),
            "jdbc_table": jdbc_table,
            "description": action.description or f"JDBC source: {action.name}",
            "add_operational_metadata": add_operational_metadata,
            "metadata_columns": metadata_columns,
            "flowgroup": context.get("flowgroup"),
        }

        code = self.render_template("load/jdbc.py.j2", template_context)

        # Secret processing is now handled centrally by ActionOrchestrator using SecretCodeGenerator
        # No need to process secrets here - just return the code with placeholders

        return code
