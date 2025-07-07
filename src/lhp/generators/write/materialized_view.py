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
        self.operational_metadata = OperationalMetadata()
    
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
        schema = target_config.get("schema")
        
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
        
        # Check if operational metadata is enabled
        flowgroup = context.get('flowgroup')
        preset_config = context.get('preset_config', {})
        add_metadata = self.operational_metadata.should_add_metadata(flowgroup, action, preset_config)
        
        if add_metadata:
            # Add required imports
            for import_stmt in self.operational_metadata.get_required_imports():
                self.add_import(import_stmt)
            
            # Update metadata context
            if flowgroup:
                self.operational_metadata.update_context(
                    flowgroup.pipeline, 
                    flowgroup.flowgroup
                )
        
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
            "add_operational_metadata": add_metadata,
            "metadata_code": self.operational_metadata.generate_metadata_code(action) if add_metadata else "",
            "flowgroup": flowgroup  # Add flowgroup to context for template
        }
        
        return self.render_template("write/materialized_view.py.j2", template_context)
    
    def _extract_source_view(self, source) -> str:
        """Extract source view name from action source."""
        if isinstance(source, str):
            return source
        elif isinstance(source, dict):
            return source.get("view", source.get("name", ""))
        elif isinstance(source, list) and source:
            return source[0]  # Take first source for write
        else:
            raise ValueError("Invalid source configuration for materialized view write") 