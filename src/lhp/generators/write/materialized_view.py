"""Materialized view write generator for LakehousePlumber."""

from pathlib import Path
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.external_file_loader import load_external_file_text


class MaterializedViewWriteGenerator(BaseActionGenerator):
    """Generate materialized view write actions using @dp.materialized_view decorator."""

    def __init__(self):
        super().__init__()
        self.add_import("from pyspark import pipelines as dp")
        self.add_import("from pyspark.sql import DataFrame")

    def generate(self, action: Action, context: dict) -> str:
        """Generate materialized view code."""
        target_config = action.write_target
        if not target_config:
            raise ValueError(
                "Materialized view action must have write_target configuration"
            )

        # Extract configuration
        database = target_config.get("database")
        table = target_config.get("table") or target_config.get("name")

        # Build full table name
        full_table_name = f"{database}.{table}" if database else table

        # Table properties with defaults
        properties = target_config.get("table_properties", {})

        # Spark configuration
        spark_conf = target_config.get("spark_conf", {})

        # Schema definition (SQL DDL string or StructType)
        schema_value = target_config.get("table_schema") or target_config.get("schema")
        schema = None
        
        if schema_value:
            # Check if it's a file path
            if self._is_table_schema_file(schema_value):
                # Load from external file
                project_root = context.get("project_root")
                if project_root:
                    schema = load_external_file_text(
                        schema_value,
                        project_root,
                        file_type="table schema file"
                    ).strip()
                else:
                    # Fallback if project_root not in context
                    schema = schema_value
            else:
                # Inline DDL
                schema = schema_value

        # Row filter clause
        row_filter = target_config.get("row_filter")

        # Temporary table flag
        temporary = target_config.get("temporary", False)

        # Refresh schedule
        refresh_schedule = target_config.get("refresh_schedule")

        # Get SQL query if provided directly in write_target
        sql_query = None
        if "sql" in target_config:
            sql_query = target_config["sql"].strip()
        elif "sql_path" in target_config:
            # Load from external SQL file
            project_root = context.get("project_root", Path.cwd())
            sql_query = load_external_file_text(
                target_config["sql_path"],
                project_root,
                file_type="SQL file"
            ).strip()
            
            # Apply substitutions if available
            if "substitution_manager" in context:
                substitution_mgr = context["substitution_manager"]
                sql_query = substitution_mgr._process_string(sql_query)
                
                # Track secret references
                secret_refs = substitution_mgr.get_secret_references()
                if "secret_references" in context and context["secret_references"] is not None:
                    context["secret_references"].update(secret_refs)

        # If no SQL in write_target, must have source view
        source_view = None
        if not sql_query:
            source_view = self._extract_source_view(action.source)

        # NOTE: Operational metadata support removed from write actions
        # Metadata should be added at load level and flow through naturally
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
            "flowgroup": flowgroup,  # Add flowgroup to context for template
        }

        return self.render_template("write/materialized_view.py.j2", template_context)
    
    def _is_table_schema_file(self, value: str) -> bool:
        """Check if value is a schema file path."""
        if not value:
            return False
        value_lower = value.lower()
        return (".ddl" in value_lower or 
                ".sql" in value_lower or 
                "/" in value or 
                "\\" in value)

    def _extract_source_view(self, source) -> str:
        """Extract source view name from action source."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            # Handle database field in source configuration
            database = source.get("database")
            table = source.get("table") or source.get("view") or source.get("name", "")

            if database and table:
                return f"{database}.{table}"
            else:
                return table
        elif isinstance(source, list) and source:
            # Handle first item in list - can be string or dict
            first_item = source[0]
            if isinstance(first_item, str):
                return first_item
            elif isinstance(first_item, dict):
                # Handle database field in source configuration
                database = first_item.get("database")
                table = (
                    first_item.get("table")
                    or first_item.get("view")
                    or first_item.get("name", "")
                )

                if database and table:
                    return f"{database}.{table}"
                else:
                    return table
            else:
                return str(first_item)
        else:
            raise ValueError("Invalid source configuration for materialized view write")
