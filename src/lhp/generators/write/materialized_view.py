"""Materialized view write generator for LakehousePlumber."""

import logging
from pathlib import Path

from lhp.models import Action

from ...core.loaders.external_file_loader import (
    is_file_path,
    load_external_file_text,
    resolve_external_file_path,
)
from ...core.registry import BaseActionGenerator
from ...errors import ErrorFactory, codes
from ...parsers.schema_parser import SchemaParser

logger = logging.getLogger(__name__)


class MaterializedViewWriteGenerator(BaseActionGenerator):
    """Uses @dp.materialized_view decorator."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import DataFrame")
        self.schema_parser = SchemaParser()

    def generate(self, action: Action, context: dict) -> str:
        target_config = action.write_target
        if not target_config:
            raise ErrorFactory.missing_required_field(
                field_name="write_target",
                component_type="Materialized view write action",
                component_name=action.name,
                field_description="The write_target configuration is required for materialized view actions.",
                example_config="""actions:
  - name: write_mv
    type: write
    sub_type: materialized_view
    source: v_transformed
    write_target:
      table: my_materialized_view
      catalog: my_catalog
      schema: my_schema""",
            )

        catalog = target_config.get("catalog")
        schema = target_config.get("schema")
        table = target_config.get("table")

        # Build full table name (normalizer guarantees catalog/schema are present)
        full_table_name = f"{catalog}.{schema}.{table}" if catalog and schema else table
        logger.debug(
            f"Generating materialized view write for target '{full_table_name}', action '{action.name}'"
        )

        properties = target_config.get("table_properties", {})
        spark_conf = target_config.get("spark_conf", {})
        schema_value = target_config.get("table_schema")
        schema = None

        if schema_value:
            if is_file_path(schema_value):
                project_root = context.get("project_root", Path.cwd())
                file_ext = Path(schema_value).suffix.lower()

                if file_ext in [".yaml", ".yml", ".json"]:
                    resolved_path = resolve_external_file_path(
                        schema_value, project_root, file_type="table schema file"
                    )
                    schema_data = self.schema_parser.parse_schema_file(resolved_path)
                    schema = self.schema_parser.to_schema_hints(schema_data)
                else:
                    schema = load_external_file_text(
                        schema_value, project_root, file_type="table schema file"
                    ).strip()
            else:
                schema = schema_value

        row_filter = target_config.get("row_filter")
        temporary = target_config.get("temporary", False)
        refresh_schedule = target_config.get("refresh_schedule")
        has_sql = "sql" in target_config or "sql_path" in target_config
        logger.debug(
            f"Materialized view '{action.name}': has_sql={has_sql}, has_schema={schema is not None}, refresh_schedule={refresh_schedule}"
        )

        sql_query = None
        if "sql" in target_config:
            sql_query = target_config["sql"].strip()
        elif "sql_path" in target_config:
            project_root = context.get("project_root", Path.cwd())
            sql_query = load_external_file_text(
                target_config["sql_path"], project_root, file_type="SQL file"
            ).strip()

        if sql_query and "substitution_manager" in context:
            substitution_mgr = context["substitution_manager"]
            sql_query = substitution_mgr._process_string(sql_query)

            secret_refs = substitution_mgr.secret_references
            if (
                "secret_references" in context
                and context["secret_references"] is not None
            ):
                context["secret_references"].update(secret_refs)

        # No SQL: must have a source view.
        source_view = None
        if not sql_query:
            source_view = self._extract_source_view(action.source)

        # Metadata is added at load level and flows through naturally — write
        # actions intentionally have no operational-metadata columns of their own.
        metadata_columns = {}
        flowgroup = context.get("flowgroup")

        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),  # Function name safe
            "full_table_name": full_table_name,
            "source_view": source_view,
            "sql_query": sql_query,
            "properties": properties,
            "spark_conf": spark_conf,
            "schema": schema,
            "row_filter": row_filter,
            "temporary": temporary,
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "table_path": target_config.get("path"),
            "comment": target_config.get("comment", f"Materialized view: {table}"),
            "refresh_schedule": refresh_schedule,
            "description": action.description
            or f"Write to {full_table_name} from multiple sources",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup,
        }

        return self.render_template("write/materialized_view.py.j2", template_context)

    def _extract_source_view(self, source) -> str:
        if isinstance(source, str):
            return source
        if isinstance(source, list) and source:
            first_item = source[0]
            if isinstance(first_item, str):
                return first_item
            logger.warning(
                f"Unexpected source list item type {type(first_item).__name__}, "
                f"converting to string"
            )
            return str(first_item)
        raise ErrorFactory.validation_error(
            codes.VAL_017,
            title="Invalid source configuration for materialized view write",
            details=(
                "Materialized view write requires a valid source configuration. "
                "Source must be a string (view name) or non-empty list of strings."
            ),
            suggestions=[
                "Provide a source view name: source: v_transformed",
                "Or provide a list of source views",
            ],
            context={"Source Type": type(source).__name__},
        )
