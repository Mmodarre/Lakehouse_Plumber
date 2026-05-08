# Write Actions Reference

Write actions persist data to streaming tables, materialized views, or external sinks.

## Common Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | **req** | Unique name within the FlowGroup |
| `type` | **req** | Must be `write` |
| `source` | **req** | Source view (string or list for multi-source append) |
| `write_target` | **req** | Target configuration (dict) |
| `description` | opt | Documentation for the action |

---

## Streaming Table (Standard Append)

```yaml
- name: write_bronze
  type: write
  source: v_cleaned_data          # req — string or list (multi-source append)
  write_target:
    type: streaming_table         # req
    database: "{catalog}.{bronze_schema}"  # req
    table: "customer"             # req
    create_table: true            # opt (default: true)
    table_properties:             # opt
      delta.enableChangeDataFeed: "true"
      delta.autoOptimize.optimizeWrite: "true"
    partition_columns: ["region"] # opt
    cluster_columns: ["customer_id"]  # opt
    spark_conf: {}                # opt — streaming Spark config
    table_schema: |               # opt — inline DDL or file path (.ddl/.sql/.yaml)
      customer_id BIGINT NOT NULL,
      name STRING
    row_filter: "ROW FILTER catalog.schema.filter_func ON (region)"  # opt
    comment: "Bronze customer table"  # opt
  description: "Write to bronze table"
```

**Multi-source append flows:**
```yaml
source:
  - v_source_a
  - v_source_b
```

**table_schema formats** (auto-detected):
- Inline DDL (multiline string)
- External file: `"schemas/table.ddl"`, `"schemas/table.sql"`, `"schemas/table.yaml"`

---

## Streaming Table (CDC — SCD Type 1/2)

```yaml
- name: write_scd2
  type: write
  source: v_changes               # req
  write_target:
    type: streaming_table         # req
    database: "{catalog}.{silver_schema}"  # req
    table: "dim_customer"         # req
    mode: "cdc"                   # req for CDC
    cdc_config:                   # req for CDC
      keys: ["customer_id"]       # req — natural business keys
      sequence_by: "_commit_timestamp"  # req — ordering column
      scd_type: 2                 # req — 1 or 2
      track_history_column_list: ["name", "address", "phone"]  # opt (mutually exclusive with except)
      # track_history_except_column_list: ["_processing_timestamp"]  # opt (mutually exclusive with above)
      ignore_null_updates: true   # opt
      except_column_list: ["_sequence_timestamp"]  # opt — exclude from history tracking
    table_properties:
      delta.enableChangeDataFeed: "true"
```

**cdc_config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `keys` | **req** | List of natural business key columns |
| `sequence_by` | **req** | Column for ordering changes |
| `scd_type` | **req** | `1` (overwrite) or `2` (history tracking) |
| `track_history_column_list` | opt | Specific columns to track (mutually exclusive with except) |
| `track_history_except_column_list` | opt | Columns to exclude from tracking |
| `ignore_null_updates` | opt | Skip updates where all tracked columns are null |
| `except_column_list` | opt | Columns excluded from change detection |

---

## Streaming Table (Snapshot CDC)

For full-snapshot CDC using `create_auto_cdc_from_snapshot_flow()`.

### Table Source

```yaml
- name: write_snapshot
  type: write
  write_target:
    type: streaming_table
    database: "{catalog}.{silver_schema}"
    table: "dim_product"
    mode: "snapshot_cdc"          # req
    snapshot_cdc_config:          # req
      source: "catalog.bronze.product_snapshots"  # req (one of source or source_function)
      keys: ["product_id"]        # req
      stored_as_scd_type: 2       # req — 1 or 2
      track_history_except_column_list: ["_processing_timestamp"]  # opt
```

### Function Source (Self-Contained)

```yaml
- name: write_part_snapshot
  type: write
  # No source field — source_function provides data internally
  write_target:
    type: streaming_table
    database: "{catalog}.{silver_schema}"
    table: "part_dim"
    mode: "snapshot_cdc"
    snapshot_cdc_config:
      source_function:            # req (one of source or source_function)
        file: "py_functions/part_snapshot_func.py"  # req
        function: "next_snapshot_and_version"        # req
      keys: ["part_id"]
      stored_as_scd_type: 2
      track_history_except_column_list: ["_source_file_path", "_processing_timestamp"]
```

**snapshot_cdc_config fields:**

| Field | Required | Description |
|-------|----------|-------------|
| `source` | req* | Source table name (*mutually exclusive with source_function) |
| `source_function.file` | req* | Path to Python file (*mutually exclusive with source) |
| `source_function.function` | req* | Function name in the file |
| `keys` | **req** | Primary key columns (list) |
| `stored_as_scd_type` | **req** | `1` or `2` |
| `track_history_column_list` | opt | Columns to track (mutually exclusive with except) |
| `track_history_except_column_list` | opt | Columns to exclude from tracking |

**Key rules:**
- With `source_function`: action is **self-contained** — no `source` field at action level needed
- Self-contained snapshot CDC is **exempt** from "must have Load action" requirement
- `source` and `source_function` are **mutually exclusive**

---

## Materialized View

```yaml
# From source view
- name: write_mv
  type: write
  source: v_aggregated            # opt (if sql/sql_path provided)
  write_target:
    type: materialized_view       # req
    database: "{catalog}.{gold_schema}"  # req
    table: "customer_summary"     # req
    table_properties: {}          # opt
    partition_columns: ["region"] # opt
    cluster_columns: ["customer_segment"]  # opt
    row_filter: "ROW FILTER catalog.schema.filter ON (region)"  # opt
    comment: "Daily summary"      # opt

# Inline SQL (no source field needed)
- name: write_mv_sql
  type: write
  write_target:
    type: materialized_view
    database: "{catalog}.{gold_schema}"
    table: "daily_sales"
    sql: |                        # opt — alternative to source
      SELECT region, DATE(txn_date) as day, SUM(amount) as total
      FROM {catalog}.{silver_schema}.transactions
      GROUP BY region, DATE(txn_date)

# External SQL file
- name: write_mv_file
  type: write
  write_target:
    type: materialized_view
    database: "{catalog}.{gold_schema}"
    table: "daily_sales"
    sql_path: "sql/gold/daily_sales.sql"  # opt — alternative to source or sql
```

**Three query sources** (use one):
1. `source` field — read from existing view
2. `sql` in write_target — inline SQL
3. `sql_path` in write_target — external SQL file

---

## Sink Actions (External Destinations)

Sinks write to external systems beyond DLT-managed tables.

### Delta Sink

```yaml
- name: export_to_external
  type: write
  source: v_data
  write_target:
    type: sink
    sink_type: delta              # req
    sink_name: analytics_export   # req — unique identifier
    comment: "Export to analytics"
    options:
      tableName: "analytics_catalog.reporting.daily_summary"  # req (one of tableName or path)
      # path: "/mnt/delta/table"  # req (one of tableName or path) — mutually exclusive
      mergeSchema: "true"         # opt
```

**Key:** `tableName` and `path` are mutually exclusive — use one.

### Kafka Sink

```yaml
- name: stream_to_kafka
  type: write
  source: v_kafka_ready           # must have key and value columns
  write_target:
    type: sink
    sink_type: kafka              # req
    sink_name: order_events       # req
    bootstrap_servers: "broker:9092"  # req
    topic: "output-topic"         # req
    comment: "Stream events"
    options:
      kafka.security.protocol: "SASL_SSL"
      kafka.batch.size: "16384"
      kafka.compression.type: "snappy"
```

**Key:** Source must have explicit `key` (opt) and `value` (req) columns. Create them in a transform action before writing.

### Custom Sink

```yaml
- name: push_to_api
  type: write
  source: v_api_ready
  write_target:
    type: sink
    sink_type: custom             # req
    sink_name: crm_api            # req
    module_path: "sinks/api_sink.py"  # req — path to Python DataSink class
    custom_sink_class: "APIDataSource"  # req — class name
    comment: "Push to CRM API"
    options:
      endpoint: "${CRM_API_ENDPOINT}"
      apiKey: "${CRM_API_KEY}"
      batchSize: "100"
```

**Implementation:** Class must implement `DataSource` interface with `name()` classmethod and `writer()` method returning a `DataSink` instance. The `DataSink` implements `write(iterator)` for batch processing.

### ForEachBatch Sink

For advanced streaming — custom Python logic per micro-batch (merges, multi-destination writes, complex upserts).

```yaml
- name: merge_updates
  type: write
  source: v_changes
  write_target:
    type: sink
    sink_type: foreachbatch       # req
    sink_name: customer_merge     # req
    module_path: "batch_handlers/merge.py"  # req — Python file with batch handler
    comment: "Merge updates"
```
