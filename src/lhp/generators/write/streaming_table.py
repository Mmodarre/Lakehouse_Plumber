"""Streaming table write generator - adapted from BurrowBuilder."""

from typing import Dict, Any, List, Optional
from ...core.base_generator import BaseActionGenerator
from ...models.config import Action
from ...utils.dqe import DQEParser
from ...utils.operational_metadata import OperationalMetadata


class StreamingTableWriteGenerator(BaseActionGenerator):
    """Generate streaming table write actions."""
    
    def __init__(self):
        super().__init__()
        self.add_import("import dlt")
        self.operational_metadata = OperationalMetadata()
    
    def generate(self, action: Action, context: dict) -> str:
        """Generate streaming table code."""
        target_config = action.write_target
        if not target_config:
            raise ValueError("Streaming table action must have write_target configuration")
        
        # Extract source views as a list
        source_views = self._extract_source_views(action.source)
        
        # Extract configuration
        mode = target_config.get("mode", "standard")  # Only "cdc" is a special mode
        database = target_config.get("database")
        table = target_config.get("table") or target_config.get("name")
        
        # Build full table name
        full_table_name = f"{database}.{table}" if database else table
        
        # Table properties with defaults
        properties = {
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.enableChangeDataFeed": "true"
        }
        if target_config.get("table_properties"):
            properties.update(target_config["table_properties"])
        
        # Handle CDC configuration for auto_cdc mode
        cdc_config = target_config.get("cdc_config", {}) if mode == "cdc" else {}
        
        # Process data quality expectations
        expectations = context.get("expectations", [])
        expect_all = {}
        expect_all_or_drop = {}
        expect_all_or_fail = {}
        
        if expectations:
            dqe_parser = DQEParser()
            expect_all, expect_all_or_drop, expect_all_or_fail = dqe_parser.parse_expectations(expectations)
        
        # Add operational metadata imports if needed
        flowgroup = context.get('flowgroup')
        preset_config = context.get('preset_config', {})
        add_operational_metadata = self.operational_metadata.should_add_metadata(flowgroup, action, preset_config)
        
        if add_operational_metadata:
            # Add required imports
            for import_stmt in self.operational_metadata.get_required_imports():
                self.add_import(import_stmt)
            
            # Update metadata context
            if flowgroup:
                self.operational_metadata.update_context(
                    flowgroup.pipeline, 
                    flowgroup.flowgroup
                )
        
        # Generate flow name from action name
        flow_name = action.name.replace("-", "_").replace(" ", "_")
        if flow_name.startswith("write_"):
            flow_name = flow_name[6:]  # Remove "write_" prefix
        flow_name = f"f_{flow_name}" if not flow_name.startswith("f_") else flow_name
        
        template_context = {
            "action_name": action.name,
            "table_name": table.replace(".", "_"),  # Function name safe
            "full_table_name": full_table_name,
            "source_views": source_views,
            "source_view": source_views[0] if source_views and mode == "cdc" else None,  # CDC only supports single source
            "flow_name": flow_name,
            "mode": mode,
            "properties": properties,
            "partitions": target_config.get("partition_columns"),
            "cluster_by": target_config.get("cluster_columns"),
            "comment": target_config.get("comment", f"Streaming table: {table}"),
            "table_path": target_config.get("path"),
            "cdc_config": cdc_config,
            "expect_all": expect_all,
            "expect_all_or_drop": expect_all_or_drop,
            "expect_all_or_fail": expect_all_or_fail,
            "add_operational_metadata": add_operational_metadata,
            "metadata_code": self.operational_metadata.generate_metadata_code(action) if add_operational_metadata else "",
            "flowgroup": flowgroup,
            "description": action.description or f"Append flow to {full_table_name}",
            "once": action.once or False
        }
        
        # Enable stream readMode for CDC
        if mode == "cdc" and isinstance(action.source, dict) and action.source.get("type") == "delta":
            action.source["readMode"] = "stream"
            action.source["read_change_feed"] = True
        
        return self.render_template("write/streaming_table.py.j2", template_context)
    
    def _extract_source_views(self, source) -> List[str]:
        """Extract source views as a list from action source."""
        if isinstance(source, str):
            return [source]
        elif isinstance(source, list):
            return source
        elif isinstance(source, dict):
            # For dict sources, extract the view field
            view = source.get("view", source.get("name", ""))
            return [view] if view else []
        else:
            return [] 