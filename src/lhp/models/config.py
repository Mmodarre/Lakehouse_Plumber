from pydantic import BaseModel, Field
from typing import List, Dict, Any, Optional, Union
from enum import Enum

class ActionType(str, Enum):
    LOAD = "load"
    TRANSFORM = "transform"
    WRITE = "write"

class LoadSourceType(str, Enum):
    CLOUDFILES = "cloudfiles"
    DELTA = "delta"
    SQL = "sql"
    PYTHON = "python"
    JDBC = "jdbc"

class TransformType(str, Enum):
    SQL = "sql"
    PYTHON = "python"
    DATA_QUALITY = "data_quality"
    TEMP_TABLE = "temp_table"
    SCHEMA = "schema"

class WriteTargetType(str, Enum):
    STREAMING_TABLE = "streaming_table"
    MATERIALIZED_VIEW = "materialized_view"

class WriteTarget(BaseModel):
    """Write target configuration for streaming tables and materialized views."""
    type: WriteTargetType
    database: str
    table: str
    create_table: bool = False  # Default to False - explicit table creation required
    comment: Optional[str] = None
    table_properties: Optional[Dict[str, Any]] = None
    partition_columns: Optional[List[str]] = None
    cluster_columns: Optional[List[str]] = None
    spark_conf: Optional[Dict[str, Any]] = None
    schema: Optional[str] = None
    row_filter: Optional[str] = None
    temporary: bool = False
    path: Optional[str] = None
    # Materialized view specific
    refresh_schedule: Optional[str] = None
    sql: Optional[str] = None

class Action(BaseModel):
    name: str
    type: ActionType
    source: Optional[Union[str, List[str], Dict[str, Any]]] = None
    target: Optional[str] = None
    description: Optional[str] = None
    readMode: Optional[str] = Field(None, description="Read mode: 'batch' or 'stream'. Controls spark.read vs spark.readStream")
    # Write-specific target configuration
    write_target: Optional[Union[WriteTarget, Dict[str, Any]]] = None
    # Action-specific configurations
    transform_type: Optional[TransformType] = None
    sql: Optional[str] = None
    sql_path: Optional[str] = None
    operational_metadata: Optional[bool] = None
    expectations_file: Optional[str] = None  # For data quality transforms
    # Write action specific
    once: Optional[bool] = None  # For one-time flows/backfills

class FlowGroup(BaseModel):
    pipeline: str
    flowgroup: str
    presets: List[str] = []
    use_template: Optional[str] = None
    template_parameters: Optional[Dict[str, Any]] = None
    actions: List[Action] = []
    operational_metadata: Optional[bool] = None

class Template(BaseModel):
    name: str
    version: str = "1.0"
    description: Optional[str] = None
    parameters: List[Dict[str, Any]] = []
    actions: List[Action] = []

class Preset(BaseModel):
    name: str
    version: str = "1.0"
    extends: Optional[str] = None
    description: Optional[str] = None
    defaults: Optional[Dict[str, Any]] = None 