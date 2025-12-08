from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
from enum import Enum


class ActionType(str, Enum):
    LOAD = "load"
    TRANSFORM = "transform"
    WRITE = "write"
    TEST = "test"


class TestActionType(str, Enum):
    """Types of test actions available."""
    __test__ = False  # Tell pytest this is not a test class
    ROW_COUNT = "row_count"
    UNIQUENESS = "uniqueness"
    REFERENTIAL_INTEGRITY = "referential_integrity"
    COMPLETENESS = "completeness"
    RANGE = "range"
    SCHEMA_MATCH = "schema_match"
    ALL_LOOKUPS_FOUND = "all_lookups_found"
    CUSTOM_SQL = "custom_sql"
    CUSTOM_EXPECTATIONS = "custom_expectations"


class ViolationAction(str, Enum):
    """Actions to take when test expectations are violated."""
    FAIL = "fail"
    WARN = "warn"


class LoadSourceType(str, Enum):
    CLOUDFILES = "cloudfiles"
    DELTA = "delta"
    SQL = "sql"
    PYTHON = "python"
    JDBC = "jdbc"
    CUSTOM_DATASOURCE = "custom_datasource"
    KAFKA = "kafka"


class TransformType(str, Enum):
    SQL = "sql"
    PYTHON = "python"
    DATA_QUALITY = "data_quality"
    TEMP_TABLE = "temp_table"
    SCHEMA = "schema"


class WriteTargetType(str, Enum):
    STREAMING_TABLE = "streaming_table"
    MATERIALIZED_VIEW = "materialized_view"
    SINK = "sink"


class MetadataColumnConfig(BaseModel):
    """Configuration for a single metadata column."""

    expression: str
    description: Optional[str] = None
    applies_to: List[str] = ["streaming_table", "materialized_view"]
    additional_imports: Optional[List[str]] = None
    enabled: bool = True


class MetadataPresetConfig(BaseModel):
    """Configuration for a metadata column preset."""

    columns: List[str]
    description: Optional[str] = None


class OperationalMetadataSelection(BaseModel):
    """Operational metadata selection configuration (used in flowgroups/actions/presets)."""

    enabled: bool = True
    preset: Optional[str] = None  # Reference to project-defined preset
    columns: Optional[List[str]] = None  # Explicit column selection
    include_columns: Optional[List[str]] = None  # Alternative syntax
    exclude_columns: Optional[List[str]] = None  # Alternative syntax


class ProjectOperationalMetadataConfig(BaseModel):
    """Project-level operational metadata configuration (definitions only)."""

    columns: Dict[str, MetadataColumnConfig]
    presets: Optional[Dict[str, MetadataPresetConfig]] = None
    defaults: Optional[Dict[str, Any]] = None


class ProjectConfig(BaseModel):
    """Project-level configuration loaded from lhp.yaml."""

    name: str
    version: str = "1.0"
    description: Optional[str] = None
    author: Optional[str] = None
    created_date: Optional[str] = None
    include: Optional[List[str]] = None
    operational_metadata: Optional[ProjectOperationalMetadataConfig] = None
    required_lhp_version: Optional[str] = None


class WriteTarget(BaseModel):
    """Write target configuration for streaming tables, materialized views, and sinks."""

    type: WriteTargetType
    
    # Streaming table and materialized view fields
    database: Optional[str] = None
    table: Optional[str] = None
    create_table: bool = (
        True  # Default to True - optional, only set to False when needed
    )
    comment: Optional[str] = None
    table_properties: Optional[Dict[str, Any]] = None
    partition_columns: Optional[List[str]] = None
    cluster_columns: Optional[List[str]] = None
    spark_conf: Optional[Dict[str, Any]] = None
    table_schema: Optional[str] = None
    row_filter: Optional[str] = None
    temporary: bool = False
    path: Optional[str] = None
    # Materialized view specific
    refresh_schedule: Optional[str] = None
    sql: Optional[str] = None
    
    # Sink-specific fields
    sink_type: Optional[str] = None  # delta, kafka, custom
    sink_name: Optional[str] = None
    
    # Kafka/Event Hubs sink fields
    bootstrap_servers: Optional[str] = None
    topic: Optional[str] = None
    
    # Custom sink fields
    module_path: Optional[str] = None
    custom_sink_class: Optional[str] = None
    
    # Common sink options
    options: Optional[Dict[str, Any]] = None

    # Backward compatibility property for 'schema' field
    @property
    def schema(self) -> Optional[str]:
        """Legacy property for backward compatibility. Use table_schema instead."""
        return self.table_schema

    @schema.setter
    def schema(self, value: Optional[str]) -> None:
        """Legacy setter for backward compatibility. Use table_schema instead."""
        self.table_schema = value


class Action(BaseModel):
    name: str
    type: ActionType
    source: Optional[Union[str, List[Union[str, Dict[str, Any]]], Dict[str, Any]]] = (
        None
    )
    target: Optional[str] = None
    description: Optional[str] = None
    readMode: Optional[str] = Field(
        None,
        description="Read mode: 'batch' or 'stream'. Controls spark.read vs spark.readStream",
    )
    # Write-specific target configuration
    write_target: Optional[Union[WriteTarget, Dict[str, Any]]] = None
    # Action-specific configurations
    transform_type: Optional[TransformType] = None
    sql: Optional[str] = None
    sql_path: Optional[str] = None
    operational_metadata: Optional[Union[bool, List[str]]] = (
        None  # Simplified: bool or list of column names
    )
    expectations_file: Optional[str] = None  # For data quality transforms
    # Schema transform specific fields
    schema_inline: Optional[str] = None  # Inline schema definition (arrow or YAML format)
    schema_file: Optional[str] = None  # External schema file path
    enforcement: Optional[str] = None  # Schema enforcement mode: strict or permissive
    # Python transform specific fields
    module_path: Optional[str] = None  # Path to Python module (relative to project root)
    function_name: Optional[str] = None  # Python function name to call
    parameters: Optional[Dict[str, Any]] = None  # Parameters passed to Python function
    # Custom data source specific fields
    custom_datasource_class: Optional[str] = None  # Custom DataSource class name
    # Write action specific
    once: Optional[bool] = None  # For one-time flows/backfills
    # Test action specific fields
    test_type: Optional[str] = None  # Test type (row_count, uniqueness, etc.)
    on_violation: Optional[str] = None  # Action on violation (fail, warn)
    tolerance: Optional[int] = None  # Tolerance for row_count tests
    columns: Optional[List[str]] = None  # Columns for uniqueness/completeness tests
    filter: Optional[str] = None  # Optional WHERE clause filter for uniqueness tests
    reference: Optional[str] = None  # Reference table for referential integrity
    source_columns: Optional[List[str]] = None  # Source columns for joins
    reference_columns: Optional[List[str]] = None  # Reference columns for joins
    required_columns: Optional[List[str]] = None  # Required columns for completeness
    column: Optional[str] = None  # Column for range tests
    min_value: Optional[Any] = None  # Min value for range tests
    max_value: Optional[Any] = None  # Max value for range tests
    lookup_table: Optional[str] = None  # Lookup table for ALL_LOOKUPS_FOUND
    lookup_columns: Optional[List[str]] = None  # Lookup columns
    lookup_result_columns: Optional[List[str]] = None  # Expected result columns
    expectations: Optional[List[Dict[str, Any]]] = None  # Custom expectations

    def model_post_init(self, __context: Any) -> None:
        """Post-initialization processing - normalize all path fields for cross-platform compatibility."""
        # List of path fields that need normalization
        path_fields = ['module_path', 'sql_path', 'expectations_file', 'schema_file']
        
        # Normalize direct path fields
        for field in path_fields:
            value = getattr(self, field, None)
            if value and isinstance(value, str):
                setattr(self, field, value.replace('\\', '/'))
        
        # Normalize paths in source dict if present
        if isinstance(self.source, dict):
            for field in path_fields:
                if field in self.source and isinstance(self.source[field], str):
                    self.source[field] = self.source[field].replace('\\', '/')
        
        # Normalize paths in write_target dict if present
        if isinstance(self.write_target, dict):
            # Handle snapshot_cdc source function file paths
            if 'snapshot_cdc_config' in self.write_target:
                snapshot_config = self.write_target['snapshot_cdc_config']
                if isinstance(snapshot_config, dict) and 'source_function' in snapshot_config:
                    source_func = snapshot_config['source_function']
                    if isinstance(source_func, dict) and 'file' in source_func:
                        if isinstance(source_func['file'], str):
                            source_func['file'] = source_func['file'].replace('\\', '/')
            
            # Handle table_schema and schema paths
            for schema_field in ['table_schema', 'schema', 'sql_path', 'module_path']:
                if schema_field in self.write_target and isinstance(self.write_target[schema_field], str):
                    self.write_target[schema_field] = self.write_target[schema_field].replace('\\', '/')


class FlowGroup(BaseModel):
    pipeline: str
    flowgroup: str
    job_name: Optional[str] = None
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[Union[bool, List[str]]] = (
        None  # Simplified: bool or list of column names
    )


class Template(BaseModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    presets: List[str] = []  # List of preset names to apply to template actions
    parameters: List[Dict[str, Any]] = []
    actions: Union[List[Action], List[Dict[str, Any]]] = []
    _raw_actions: bool = False  # Internal flag to track if actions are raw dictionaries
    
    def has_raw_actions(self) -> bool:
        """Check if template contains raw action dictionaries (not validated Action objects)."""
        return self._raw_actions
    
    def get_actions_as_dicts(self) -> List[Dict[str, Any]]:
        """Get actions as dictionaries, converting from Action objects if needed."""
        if self._raw_actions:
            return self.actions
        else:
            return [action.model_dump(mode="json") for action in self.actions]


class Preset(BaseModel):
    name: str
    version: str = "1.0"
    extends: Optional[str] = None
    description: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None
