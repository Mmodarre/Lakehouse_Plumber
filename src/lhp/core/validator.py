"""Configuration validator for LakehousePlumber."""

import logging
from typing import List, Dict, Any, Optional
from pathlib import Path

from ..models.config import FlowGroup, Action, ActionType, LoadSourceType, TransformType, WriteTargetType
from .action_registry import ActionRegistry
from .dependency_resolver import DependencyResolver


class ConfigValidator:
    """Validate LakehousePlumber configurations."""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.action_registry = ActionRegistry()
        self.dependency_resolver = DependencyResolver()
    
    def validate_flowgroup(self, flowgroup: FlowGroup) -> List[str]:
        """Step 4.4.2: Validate flowgroups and actions.
        
        Args:
            flowgroup: FlowGroup to validate
            
        Returns:
            List of validation error messages
        """
        errors = []
        
        # Validate basic fields
        if not flowgroup.pipeline:
            errors.append("FlowGroup must have a 'pipeline' name")
        
        if not flowgroup.flowgroup:
            errors.append("FlowGroup must have a 'flowgroup' name")
        
        if not flowgroup.actions:
            errors.append("FlowGroup must have at least one action")
        
        # Validate each action
        action_names = set()
        target_names = set()
        
        for i, action in enumerate(flowgroup.actions):
            action_errors = self.validate_action(action, i)
            errors.extend(action_errors)
            
            # Check for duplicate action names
            if action.name in action_names:
                errors.append(f"Duplicate action name: '{action.name}'")
            action_names.add(action.name)
            
            # Check for duplicate target names
            if action.target and action.target in target_names:
                errors.append(f"Duplicate target name: '{action.target}' in action '{action.name}'")
            if action.target:
                target_names.add(action.target)
        
        # Validate dependencies
        if flowgroup.actions:
            dependency_errors = self.dependency_resolver.validate_relationships(flowgroup.actions)
            errors.extend(dependency_errors)
        
        # Validate template usage
        if flowgroup.use_template and not flowgroup.template_parameters:
            self.logger.warning(f"FlowGroup uses template '{flowgroup.use_template}' but no parameters provided")
        
        return errors
    
    def validate_action(self, action: Action, index: int) -> List[str]:
        """Step 4.4.2: Validate action types and required fields.
        
        Args:
            action: Action to validate
            index: Action index in the flowgroup
            
        Returns:
            List of validation error messages
        """
        errors = []
        prefix = f"Action[{index}] '{action.name}'"
        
        # Basic validation
        if not action.name:
            errors.append(f"Action[{index}]: Missing 'name' field")
            return errors  # Can't continue without name
        
        if not action.type:
            errors.append(f"{prefix}: Missing 'type' field")
            return errors  # Can't continue without type
        
        # Type-specific validation
        if action.type == ActionType.LOAD:
            errors.extend(self._validate_load_action(action, prefix))
        
        elif action.type == ActionType.TRANSFORM:
            errors.extend(self._validate_transform_action(action, prefix))
        
        elif action.type == ActionType.WRITE:
            errors.extend(self._validate_write_action(action, prefix))
        
        else:
            errors.append(f"{prefix}: Unknown action type '{action.type}'")
        
        return errors
    
    def _validate_load_action(self, action: Action, prefix: str) -> List[str]:
        """Validate load action configuration."""
        errors = []
        
        # Load actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Load actions must have a 'target' view name")
        
        # Load actions must have source configuration
        if not action.source:
            errors.append(f"{prefix}: Load actions must have a 'source' configuration")
            return errors
        
        # Source must be a dict for load actions
        if not isinstance(action.source, dict):
            errors.append(f"{prefix}: Load action source must be a configuration object")
            return errors
        
        # Must have source type
        source_type = action.source.get("type")
        if not source_type:
            errors.append(f"{prefix}: Load action source must have a 'type' field")
            return errors
        
        # Validate source type is supported
        if not self.action_registry.is_generator_available(ActionType.LOAD, source_type):
            errors.append(f"{prefix}: Unknown load source type '{source_type}'")
            return errors
        
        # Type-specific validation
        try:
            load_type = LoadSourceType(source_type)
            
            if load_type == LoadSourceType.CLOUDFILES:
                if not action.source.get("path"):
                    errors.append(f"{prefix}: CloudFiles source must have 'path'")
                if not action.source.get("format"):
                    errors.append(f"{prefix}: CloudFiles source must have 'format'")
            
            elif load_type == LoadSourceType.DELTA:
                if not action.source.get("table"):
                    errors.append(f"{prefix}: Delta source must have 'table'")
            
            elif load_type == LoadSourceType.JDBC:
                required_fields = ["url", "user", "password", "driver"]
                for field in required_fields:
                    if not action.source.get(field):
                        errors.append(f"{prefix}: JDBC source must have '{field}'")
                # Must have either query or table
                if not action.source.get("query") and not action.source.get("table"):
                    errors.append(f"{prefix}: JDBC source must have either 'query' or 'table'")
            
            elif load_type == LoadSourceType.PYTHON:
                if not action.source.get("module_path"):
                    errors.append(f"{prefix}: Python source must have 'module_path'")
            
        except ValueError:
            pass  # Already handled above
        
        return errors
    
    def _validate_transform_action(self, action: Action, prefix: str) -> List[str]:
        """Validate transform action configuration."""
        errors = []
        
        # Transform actions must have a target
        if not action.target:
            errors.append(f"{prefix}: Transform actions must have a 'target' view name")
        
        # Must have transform_type
        if not action.transform_type:
            errors.append(f"{prefix}: Transform actions must have 'transform_type'")
            return errors
        
        # Validate transform type is supported
        if not self.action_registry.is_generator_available(ActionType.TRANSFORM, action.transform_type):
            errors.append(f"{prefix}: Unknown transform type '{action.transform_type}'")
            return errors
        
        # Type-specific validation
        try:
            transform_type = TransformType(action.transform_type)
            
            if transform_type == TransformType.SQL:
                # Must have SQL query
                if not action.sql and not action.sql_path:
                    errors.append(f"{prefix}: SQL transform must have 'sql' or 'sql_path'")
                # Must have source
                if not action.source:
                    errors.append(f"{prefix}: SQL transform must have 'source' view(s)")
            
            elif transform_type == TransformType.DATA_QUALITY:
                # Must have source
                if not action.source:
                    errors.append(f"{prefix}: Data quality transform must have 'source'")
            
            elif transform_type == TransformType.PYTHON:
                # Must have source with module_path
                if not isinstance(action.source, dict):
                    errors.append(f"{prefix}: Python transform source must be a configuration object")
                elif not action.source.get("module_path"):
                    errors.append(f"{prefix}: Python transform must have 'module_path' in source")
            
            elif transform_type == TransformType.TEMP_TABLE:
                # Must have source
                if not action.source:
                    errors.append(f"{prefix}: Temp table transform must have 'source'")
            
        except ValueError:
            pass  # Already handled above
        
        return errors
    
    def _validate_write_action(self, action: Action, prefix: str) -> List[str]:
        """Validate write action configuration."""
        errors = []
        
        # Write actions should not have a target (they are the final output)
        if action.target:
            self.logger.warning(f"{prefix}: Write actions typically don't have 'target' field")
        
        # Write actions must have write_target configuration
        if not action.write_target:
            errors.append(f"{prefix}: Write actions must have 'write_target' configuration")
            return errors
        
        # write_target must be a dict
        if not isinstance(action.write_target, dict):
            errors.append(f"{prefix}: Write action write_target must be a configuration object")
            return errors
        
        # Must have target type
        target_type = action.write_target.get("type")
        if not target_type:
            errors.append(f"{prefix}: Write action write_target must have a 'type' field")
            return errors
        
        # Validate target type is supported
        if not self.action_registry.is_generator_available(ActionType.WRITE, target_type):
            errors.append(f"{prefix}: Unknown write target type '{target_type}'")
            return errors
        
        # Type-specific validation
        try:
            write_type = WriteTargetType(target_type)
            
            if write_type in [WriteTargetType.STREAMING_TABLE, WriteTargetType.MATERIALIZED_VIEW]:
                # Must have database and table/name
                if not action.write_target.get("database"):
                    errors.append(f"{prefix}: {target_type} must have 'database'")
                if not action.write_target.get("table") and not action.write_target.get("name"):
                    errors.append(f"{prefix}: {target_type} must have 'table' or 'name'")
                
                # Must have source (view to read from)
                if write_type == WriteTargetType.STREAMING_TABLE:
                    # Check if this is snapshot_cdc mode, which defines source differently
                    mode = action.write_target.get("mode", "standard")
                    if mode != "snapshot_cdc":
                        if not action.source:
                            errors.append(f"{prefix}: Streaming table must have 'source' to read from")
                        # Validate source is string or list
                        elif not isinstance(action.source, (str, list)):
                            errors.append(f"{prefix}: Streaming table source must be a string or list of view names")
                elif write_type == WriteTargetType.MATERIALIZED_VIEW:
                    # Materialized view can have either source view or SQL
                    if not action.source and not action.write_target.get("sql"):
                        errors.append(f"{prefix}: Materialized view must have either 'source' or 'sql' in write_target")
                    # If source is provided, it should be string or list
                    elif action.source and not isinstance(action.source, (str, list)):
                        errors.append(f"{prefix}: Materialized view source must be a string or list of view names")
                
                # Validate new @dlt.table options
                self._validate_dlt_table_options(action, prefix, errors)
                
                # Validate mode-specific configurations
                if write_type == WriteTargetType.STREAMING_TABLE:
                    mode = action.write_target.get("mode", "standard")
                    if mode == "snapshot_cdc":
                        self._validate_snapshot_cdc_config(action, prefix, errors)
            
        except ValueError:
            pass  # Already handled above
        
        return errors
    
    def _validate_dlt_table_options(self, action: Action, prefix: str, errors: List[str]):
        """Validate DLT table options (spark_conf, table_properties, schema, etc.)."""
        if not action.write_target:
            return
        
        # Validate spark_conf
        spark_conf = action.write_target.get("spark_conf")
        if spark_conf is not None:
            if not isinstance(spark_conf, dict):
                errors.append(f"{prefix}: 'spark_conf' must be a dictionary")
            else:
                # Validate spark_conf keys (should be strings)
                for key, value in spark_conf.items():
                    if not isinstance(key, str):
                        errors.append(f"{prefix}: spark_conf key '{key}' must be a string")
        
        # Validate table_properties
        table_properties = action.write_target.get("table_properties")
        if table_properties is not None:
            if not isinstance(table_properties, dict):
                errors.append(f"{prefix}: 'table_properties' must be a dictionary")
            else:
                # Validate table_properties keys (should be strings)
                for key, value in table_properties.items():
                    if not isinstance(key, str):
                        errors.append(f"{prefix}: table_properties key '{key}' must be a string")
        
        # Validate schema
        schema = action.write_target.get("schema")
        if schema is not None:
            if not isinstance(schema, str):
                errors.append(f"{prefix}: 'schema' must be a string (SQL DDL or StructType)")
        
        # Validate row_filter
        row_filter = action.write_target.get("row_filter")
        if row_filter is not None:
            if not isinstance(row_filter, str):
                errors.append(f"{prefix}: 'row_filter' must be a string")
        
        # Validate temporary
        temporary = action.write_target.get("temporary")
        if temporary is not None:
            if not isinstance(temporary, bool):
                errors.append(f"{prefix}: 'temporary' must be a boolean")
        
        # Validate partition_columns
        partition_columns = action.write_target.get("partition_columns")
        if partition_columns is not None:
            if not isinstance(partition_columns, list):
                errors.append(f"{prefix}: 'partition_columns' must be a list")
            else:
                for i, col in enumerate(partition_columns):
                    if not isinstance(col, str):
                        errors.append(f"{prefix}: partition_columns[{i}] must be a string")
        
        # Validate cluster_columns
        cluster_columns = action.write_target.get("cluster_columns")
        if cluster_columns is not None:
            if not isinstance(cluster_columns, list):
                errors.append(f"{prefix}: 'cluster_columns' must be a list")
            else:
                for i, col in enumerate(cluster_columns):
                    if not isinstance(col, str):
                        errors.append(f"{prefix}: cluster_columns[{i}] must be a string")
    
    def _validate_snapshot_cdc_config(self, action: Action, prefix: str, errors: List[str]):
        """Validate snapshot CDC configuration."""
        if not action.write_target:
            return
        
        snapshot_cdc_config = action.write_target.get("snapshot_cdc_config")
        if not snapshot_cdc_config:
            errors.append(f"{prefix}: snapshot_cdc mode requires 'snapshot_cdc_config'")
            return
        
        if not isinstance(snapshot_cdc_config, dict):
            errors.append(f"{prefix}: 'snapshot_cdc_config' must be a dictionary")
            return
        
        # Validate source configuration (mutually exclusive)
        has_source = snapshot_cdc_config.get("source") is not None
        has_source_function = snapshot_cdc_config.get("source_function") is not None
        
        if not has_source and not has_source_function:
            errors.append(f"{prefix}: snapshot_cdc_config must have either 'source' or 'source_function'")
        elif has_source and has_source_function:
            errors.append(f"{prefix}: snapshot_cdc_config cannot have both 'source' and 'source_function'")
        
        # Validate source_function if provided
        if has_source_function:
            source_function = snapshot_cdc_config["source_function"]
            if not isinstance(source_function, dict):
                errors.append(f"{prefix}: 'source_function' must be a dictionary")
            else:
                if not source_function.get("file"):
                    errors.append(f"{prefix}: source_function must have 'file'")
                if not source_function.get("function"):
                    errors.append(f"{prefix}: source_function must have 'function'")
        
        # Validate required keys parameter
        keys = snapshot_cdc_config.get("keys")
        if not keys:
            errors.append(f"{prefix}: snapshot_cdc_config must have 'keys'")
        elif not isinstance(keys, list):
            errors.append(f"{prefix}: 'keys' must be a list")
        elif not keys:  # Empty list
            errors.append(f"{prefix}: 'keys' cannot be empty")
        else:
            for i, key in enumerate(keys):
                if not isinstance(key, str):
                    errors.append(f"{prefix}: keys[{i}] must be a string")
        
        # Validate stored_as_scd_type
        scd_type = snapshot_cdc_config.get("stored_as_scd_type")
        if scd_type is not None:
            if not isinstance(scd_type, int) or scd_type not in [1, 2]:
                errors.append(f"{prefix}: 'stored_as_scd_type' must be 1 or 2")
        
        # Validate track history options (mutually exclusive)
        has_track_list = snapshot_cdc_config.get("track_history_column_list") is not None
        has_track_except = snapshot_cdc_config.get("track_history_except_column_list") is not None
        
        if has_track_list and has_track_except:
            errors.append(f"{prefix}: cannot have both 'track_history_column_list' and 'track_history_except_column_list'")
        
        # Validate track_history_column_list
        if has_track_list:
            track_list = snapshot_cdc_config["track_history_column_list"]
            if not isinstance(track_list, list):
                errors.append(f"{prefix}: 'track_history_column_list' must be a list")
            else:
                for i, col in enumerate(track_list):
                    if not isinstance(col, str):
                        errors.append(f"{prefix}: track_history_column_list[{i}] must be a string")
        
        # Validate track_history_except_column_list
        if has_track_except:
            except_list = snapshot_cdc_config["track_history_except_column_list"]
            if not isinstance(except_list, list):
                errors.append(f"{prefix}: 'track_history_except_column_list' must be a list")
            else:
                for i, col in enumerate(except_list):
                    if not isinstance(col, str):
                        errors.append(f"{prefix}: track_history_except_column_list[{i}] must be a string")
    
    def validate_action_references(self, actions: List[Action]) -> List[str]:
        """Validate that all action references are valid."""
        errors = []
        
        # Build set of all available views/targets
        available_views = set()
        for action in actions:
            if action.target:
                available_views.add(action.target)
        
        # Check all references
        for action in actions:
            sources = self._extract_all_sources(action)
            for source in sources:
                # Skip external sources
                if not source.startswith("v_") and "." in source:
                    continue  # Likely an external table like bronze.customers
                
                if source.startswith("v_") and source not in available_views:
                    errors.append(
                        f"Action '{action.name}' references view '{source}' which is not defined"
                    )
        
        return errors
    
    def _extract_all_sources(self, action: Action) -> List[str]:
        """Extract all source references from an action."""
        sources = []
        
        if isinstance(action.source, str):
            sources.append(action.source)
        elif isinstance(action.source, list):
            sources.extend(action.source)
        elif isinstance(action.source, dict):
            # Check various fields that might contain source references
            for field in ["view", "source", "views", "sources"]:
                value = action.source.get(field)
                if isinstance(value, str):
                    sources.append(value)
                elif isinstance(value, list):
                    sources.extend(value)
        
        return sources 