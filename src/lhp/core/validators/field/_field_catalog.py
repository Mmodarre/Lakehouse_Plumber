"""Known-field catalogs for config-field validation.

Field-name data consumed by ``ConfigFieldValidator``: valid fields for each
load-source type, each write-target type, and the always-valid action-level
fields. Split from ``_field_suggestions`` to stay within the per-file line
budget.
"""

LOAD_SOURCE_FIELDS = {
    "cloudfiles": {
        "type",
        "path",
        "format",
        "options",
        "reader_options",
        "format_options",
        "schema",
        "schema_file",
        "readMode",
        "schema_location",
        "schema_infer_column_types",
        "max_files_per_trigger",
        "schema_evolution_mode",
        "rescue_data_column",
    },
    "delta": {
        "type",
        "table",
        "catalog",
        "schema",
        "database",  # REMOVE_AT_V1.0.0: deprecated, use catalog + schema
        "readMode",
        "options",
        "where_clause",
        "select_columns",
    },
    "sql": {"type", "sql", "sql_path"},
    "jdbc": {"type", "url", "user", "password", "driver", "query", "table"},
    "python": {"type", "module_path", "function_name", "parameters"},
    "kafka": {
        "type",
        "bootstrap_servers",
        "subscribe",
        "subscribePattern",
        "assign",
        "options",
        "readMode",
    },
    "custom_datasource": {
        "type",
        "module_path",
        "custom_datasource_class",
        "options",
    },
}

WRITE_TARGET_FIELDS = {
    "streaming_table": {
        "type",
        "catalog",
        "schema",
        "database",  # REMOVE_AT_V1.0.0: deprecated, use catalog + schema
        "table",
        "create_table",
        "comment",
        "description",
        "table_properties",
        "partition_columns",
        "cluster_columns",
        "spark_conf",
        # UC namespace schema name
        "table_schema",  # DDL table schema definition
        "row_filter",
        "temporary",
        "path",
        "mode",
        "cdc_config",
        "snapshot_cdc_config",
    },
    "materialized_view": {
        "type",
        "catalog",
        "schema",
        "database",  # REMOVE_AT_V1.0.0: deprecated, use catalog + schema
        "table",
        "create_table",
        "comment",
        "description",
        "table_properties",
        "partition_columns",
        "cluster_columns",
        "spark_conf",
        # UC namespace schema name
        "table_schema",  # DDL table schema definition
        "row_filter",
        "temporary",
        "path",
        "refresh_schedule",
        "sql",
        "sql_path",
    },
    "sink": {
        "type",
        "sink_type",
        "sink_name",
        "comment",
        "description",
        # Kafka/Event Hubs fields
        "bootstrap_servers",
        "topic",
        # Custom sink fields
        "module_path",
        "custom_sink_class",
        # ForEachBatch sink fields
        "batch_handler",
        # Common fields
        "options",
    },
}

ACTION_FIELDS = {
    "name",
    "type",
    "source",
    "target",
    "description",
    "readMode",
    "write_target",
    "transform_type",
    "sql",
    "sql_path",
    "operational_metadata",
    "expectations_file",
    "mode",
    "quarantine",
    "once",
    # Python transform specific fields
    "module_path",
    "function_name",
    "parameters",
    # Custom data source specific fields
    "custom_datasource_class",
    # Schema transform specific fields
    "schema_inline",
    "schema_file",
    "enforcement",
    # Test action specific fields
    "test_type",
    "on_violation",
    "tolerance",
    "columns",
    "filter",
    "reference",
    "source_columns",
    "reference_columns",
    "required_columns",
    "column",
    "min_value",
    "max_value",
    "lookup_table",
    "lookup_columns",
    "lookup_result_columns",
    "expectations",
    "test_id",
}


__all__ = [
    "ACTION_FIELDS",
    "LOAD_SOURCE_FIELDS",
    "WRITE_TARGET_FIELDS",
]
