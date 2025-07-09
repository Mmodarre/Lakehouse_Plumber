"""Materialized view write generator for LakehousePlumber."""

from typing import Dict, Any, Optional
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.dqe import DQEParser
from ...utils.operational_metadata import OperationalMetadata


class MaterializedViewWriteGenerator(BaseActionGenerator):
    """Generate materialized view write actions."""
    
    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
        self.add_import("from pyspark.sql import DataFrame")
    
    def generate(self, action: Action, context: dict) -> str:
        """Generate materialized view code."""
        target_config = action.write_target
        if not target_config:
            raise ValueError("Materialized view action must have write_target configuration")
        
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
        schema = target_config.get("table_schema") or target_config.get("schema")
        
        # Row filter clause
        row_filter = target_config.get("row_filter")
        
        # Temporary table flag
        temporary = target_config.get("temporary", False)
        
        # Refresh schedule
        refresh_schedule = target_config.get("refresh_schedule")
        
        # Get SQL query if provided directly in write_target
        sql_query = target_config.get("sql")
        
        # If no SQL in write_target, must have source view
        source_view = None
        if not sql_query:
            source_view = self._extract_source_view(action.source)
        
        # NEW: Handle operational metadata with simplified system
        flowgroup = context.get('flowgroup')
        preset_config = context.get('preset_config', {})
        project_config = context.get('project_config')
        
        # Initialize operational metadata handler
        operational_metadata = OperationalMetadata(
            project_config=project_config.operational_metadata if project_config else None
        )
        
        # Update context for substitutions
        if flowgroup:
            operational_metadata.update_context(flowgroup.pipeline, flowgroup.flowgroup)
        
        # Resolve metadata selection
        selection = operational_metadata.resolve_metadata_selection(flowgroup, action, preset_config)
        metadata_columns = operational_metadata.get_selected_columns(selection or {}, 'materialized_view')
        
        # Get required imports for metadata
        metadata_imports = operational_metadata.get_required_imports(metadata_columns)
        for import_stmt in metadata_imports:
            self.add_import(import_stmt)
        
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
            "description": action.description or f"Write to materialized view: {full_table_name}",
            "add_operational_metadata": bool(metadata_columns),
            "metadata_columns": metadata_columns,
            "flowgroup": flowgroup  # Add flowgroup to context for template
        }
        
        return self.render_template("write/materialized_view.py.j2", template_context)
    
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
                table = first_item.get("table") or first_item.get("view") or first_item.get("name", "")
                
                if database and table:
                    return f"{database}.{table}"
                else:
                    return table
            else:
                return str(first_item)
        else:
            raise ValueError("Invalid source configuration for materialized view write") 