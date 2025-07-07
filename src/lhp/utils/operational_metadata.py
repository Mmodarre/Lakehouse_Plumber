"""Operational metadata handling for LakehousePlumber.

Provides functionality to add operational metadata columns to DLT tables
when the operational_metadata flag is enabled.
"""

import logging
from typing import Dict, List, Any, Optional, Set
from datetime import datetime
from pathlib import Path

from ..models.config import FlowGroup, Action, ActionType


class OperationalMetadata:
    """Handle operational metadata for DLT pipelines.
    
    When enabled, adds lineage columns directly to data tables:
    - _ingestion_timestamp
    - _source_file
    - _pipeline_run_id
    - _pipeline_name
    - _flowgroup_name
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.metadata_columns = {
            '_ingestion_timestamp': 'current_timestamp()',
            '_source_file': 'input_file_name()',
            '_pipeline_run_id': 'metadata.pipeline_run_id',
            '_pipeline_name': 'metadata.pipeline_name',
            '_flowgroup_name': 'metadata.flowgroup_name'
        }
    
    def should_add_metadata(self, flowgroup: FlowGroup, action: Action, 
                          preset_config: Dict[str, Any]) -> bool:
        """Determine if operational metadata should be added.
        
        Args:
            flowgroup: The flowgroup being processed
            action: The specific action
            preset_config: Resolved preset configuration
            
        Returns:
            True if metadata should be added
        """
        # Check action-level flag first
        if hasattr(action, 'operational_metadata') and action.operational_metadata is not None:
            return bool(action.operational_metadata)
        
        # Check flowgroup-level flag
        if hasattr(flowgroup, 'operational_metadata') and flowgroup.operational_metadata is not None:
            return bool(flowgroup.operational_metadata)
        
        # Check preset configuration
        if 'operational_metadata' in preset_config:
            return bool(preset_config['operational_metadata'])
        
        # Default is False
        return False
    
    def get_required_imports(self) -> Set[str]:
        """Get imports required for operational metadata.
        
        Returns:
            Set of import statements
        """
        return {
            'from pyspark.sql import functions as F',
            'from pyspark.sql.functions import current_timestamp, input_file_name'
        }
    
    def generate_metadata_columns(self, target_type: str) -> Dict[str, str]:
        """Generate metadata column expressions based on target type.
        
        Args:
            target_type: Type of target (streaming_table or materialized_view)
            
        Returns:
            Dictionary of column name to expression
        """
        columns = {
            '_ingestion_timestamp': 'F.current_timestamp()',
            '_pipeline_name': f'F.lit("{self.pipeline_name}")' if hasattr(self, 'pipeline_name') else 'F.lit("unknown")',
            '_flowgroup_name': f'F.lit("{self.flowgroup_name}")' if hasattr(self, 'flowgroup_name') else 'F.lit("unknown")'
        }
        
        # Add source file for streaming tables
        if target_type == 'streaming_table':
            columns['_source_file'] = 'F.input_file_name()'
        
        # Add pipeline run ID if available
        columns['_pipeline_run_id'] = 'F.lit(spark.conf.get("pipelines.id", "unknown"))'
        
        return columns
    
    def add_metadata_to_dataframe(self, df_variable: str, columns: Dict[str, str]) -> str:
        """Generate code to add metadata columns to a DataFrame.
        
        Args:
            df_variable: Name of the DataFrame variable
            columns: Dictionary of column names to expressions
            
        Returns:
            Code to add metadata columns
        """
        code_lines = []
        
        code_lines.append(f"# Add operational metadata columns")
        for col_name, expression in columns.items():
            code_lines.append(f"{df_variable} = {df_variable}.withColumn('{col_name}', {expression})")
        
        return '\n'.join(code_lines)
    
    def generate_metadata_code(self, action: Action, df_variable: str = "df") -> str:
        """Generate code to add operational metadata.
        
        Args:
            action: The action to generate metadata for
            df_variable: Name of the DataFrame variable
            
        Returns:
            Generated code string
        """
        if action.type == ActionType.WRITE:
            # Determine target type
            if isinstance(action.source, dict):
                target_type = action.source.get('type', 'streaming_table')
            else:
                target_type = 'streaming_table'
            
            columns = self.generate_metadata_columns(target_type)
            return self.add_metadata_to_dataframe(df_variable, columns)
        
        return ""
    
    def update_context(self, pipeline_name: str, flowgroup_name: str):
        """Update metadata context.
        
        Args:
            pipeline_name: Name of the pipeline
            flowgroup_name: Name of the flowgroup
        """
        self.pipeline_name = pipeline_name
        self.flowgroup_name = flowgroup_name 