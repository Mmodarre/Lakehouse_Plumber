"""Event-log and monitoring-pipeline configuration models."""

from typing import List, Optional

from pydantic import BaseModel, ConfigDict, Field


class EventLogConfig(BaseModel):
    """Project-level event log configuration for pipeline resource generation."""

    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = True
    catalog: Optional[str] = None
    schema_: Optional[str] = Field(None, alias="schema")
    name_prefix: str = ""
    name_suffix: str = ""


class MonitoringMaterializedViewConfig(BaseModel):
    """Configuration for a single monitoring materialized view."""

    name: str
    sql: Optional[str] = None
    sql_path: Optional[str] = None


class MonitoringConfig(BaseModel):
    """Project-level monitoring pipeline configuration.

    Generates two artifacts:

    1. A standalone notebook that runs N independent streaming queries (one per
       pipeline event log) appending into a user-created Delta table.
    2. A DLT pipeline with materialized views only, reading from that Delta table.

    A Databricks Workflow job chains: notebook_task (union) → pipeline_task (MVs).
    """

    model_config = ConfigDict(populate_by_name=True)

    enabled: bool = True
    pipeline_name: Optional[str] = None  # default: {project_name}_event_log_monitoring
    catalog: Optional[str] = None  # default: event_log.catalog
    schema_: Optional[str] = Field(None, alias="schema")  # default: event_log.schema
    streaming_table: str = "all_pipelines_event_log"  # user-created Delta table
    checkpoint_path: str = ""  # streaming checkpoint base path (required when enabled)
    job_config_path: Optional[str] = (
        None  # relative path to monitoring job config YAML (required when enabled)
    )
    max_concurrent_streams: int = 10  # ThreadPoolExecutor max_workers
    materialized_views: Optional[List[MonitoringMaterializedViewConfig]] = None
    enable_job_monitoring: bool = False
