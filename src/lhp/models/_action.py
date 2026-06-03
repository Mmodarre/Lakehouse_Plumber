"""Action and write-target models."""

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from ._enums import ActionType, TransformType, WriteTargetType
from ._quarantine import QuarantineConfig


class WriteTarget(BaseModel):
    """Write target configuration for streaming tables, materialized views, and sinks."""

    model_config = ConfigDict(populate_by_name=True)

    type: WriteTargetType

    # Streaming table and materialized view fields
    catalog: Optional[str] = None
    schema: Optional[str] = (
        None  # UC namespace schema (not DDL — use table_schema for DDL)
    )
    database: Optional[str] = None  # REMOVE_AT_V1.0.0: deprecated, use catalog + schema
    table: Optional[str] = None
    create_table: bool = True
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
    sql_path: Optional[str] = None

    # Sink-specific fields
    sink_type: Optional[str] = None  # delta, kafka, custom, foreachbatch
    sink_name: Optional[str] = None

    # Kafka/Event Hubs sink fields
    bootstrap_servers: Optional[str] = None
    topic: Optional[str] = None

    # Custom sink fields
    module_path: Optional[str] = None
    custom_sink_class: Optional[str] = None

    # ForEachBatch sink fields
    batch_handler: Optional[str] = None

    # Common sink options
    options: Optional[Dict[str, Any]] = None


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
    write_target: Optional[Union[WriteTarget, Dict[str, Any]]] = None
    transform_type: Optional[TransformType] = None
    sql: Optional[str] = None
    sql_path: Optional[str] = None
    operational_metadata: Optional[Union[bool, List[str]]] = None
    expectations_file: Optional[str] = None
    mode: Optional[str] = Field(
        None,
        description="Data quality mode: 'dqe' (default) or 'quarantine' (DLQ recycling)",
    )
    quarantine: Optional[QuarantineConfig] = Field(
        None,
        description="Quarantine configuration (required when mode is 'quarantine')",
    )
    # Schema transform specific fields
    schema_inline: Optional[str] = (
        None  # Inline schema definition (arrow or YAML format)
    )
    schema_file: Optional[str] = None
    enforcement: Optional[str] = None  # Schema enforcement mode: strict or permissive
    # Python transform specific fields
    module_path: Optional[str] = (
        None  # Path to Python module (relative to project root)
    )
    function_name: Optional[str] = None
    parameters: Optional[Dict[str, Any]] = None
    # Custom data source specific fields
    custom_datasource_class: Optional[str] = None
    # Write action specific
    once: Optional[bool] = None
    # Test action specific fields
    test_type: Optional[str] = None
    on_violation: Optional[str] = None
    tolerance: Optional[int] = None
    columns: Optional[List[str]] = None
    filter: Optional[str] = None  # Optional WHERE clause filter for uniqueness tests
    reference: Optional[str] = None
    source_columns: Optional[List[str]] = None
    reference_columns: Optional[List[str]] = None
    required_columns: Optional[List[str]] = None
    column: Optional[str] = None
    min_value: Optional[Any] = None
    max_value: Optional[Any] = None
    lookup_table: Optional[str] = None  # Lookup table for ALL_LOOKUPS_FOUND
    lookup_columns: Optional[List[str]] = None
    lookup_result_columns: Optional[List[str]] = None
    expectations: Optional[List[Dict[str, Any]]] = None
    test_id: Optional[str] = None

    @property
    def resolved_test_target(self) -> str:
        """Canonical target name for test actions: explicit target or tmp_test_{name}."""
        return self.target or f"tmp_test_{self.name}"

    def model_post_init(self, __context: Any) -> None:
        """Normalize path fields for cross-platform compatibility."""
        path_fields = ["module_path", "sql_path", "expectations_file", "schema_file"]

        for field in path_fields:
            value = getattr(self, field, None)
            if value and isinstance(value, str):
                setattr(self, field, value.replace("\\", "/"))

        if isinstance(self.source, dict):
            for field in path_fields:
                if field in self.source and isinstance(self.source[field], str):
                    self.source[field] = self.source[field].replace("\\", "/")

        if isinstance(self.write_target, dict):
            if "snapshot_cdc_config" in self.write_target:
                snapshot_config = self.write_target["snapshot_cdc_config"]
                if (
                    isinstance(snapshot_config, dict)
                    and "source_function" in snapshot_config
                ):
                    source_func = snapshot_config["source_function"]
                    if isinstance(source_func, dict) and "file" in source_func:
                        if isinstance(source_func["file"], str):
                            source_func["file"] = source_func["file"].replace("\\", "/")

            for schema_field in ["table_schema", "schema", "sql_path", "module_path"]:
                if schema_field in self.write_target and isinstance(
                    self.write_target[schema_field], str
                ):
                    self.write_target[schema_field] = self.write_target[
                        schema_field
                    ].replace("\\", "/")
