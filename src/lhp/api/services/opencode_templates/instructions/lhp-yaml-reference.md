# LHP YAML Configuration Reference

This is the complete reference for writing Lakehouse Plumber flowgroup YAML files.

## FlowGroup Structure

### Single FlowGroup

```yaml
pipeline: bronze_raw          # Required: pipeline name
flowgroup: customer_ingestion # Required: flowgroup name
job_name: NCR                 # Optional: orchestration job assignment
presets: [cloudfiles_defaults] # Optional: preset names to merge
use_template: csv_ingestion   # Optional: template to apply
template_parameters:          # Required if use_template is set
  table_name: customer
variables:                    # Optional: local vars (%{var} syntax)
  entity: customer
operational_metadata: true    # Optional: add metadata columns
actions:                      # Required: list of actions
  - name: load_customer
    type: load
    # ... action config
```

### Multi-FlowGroup (Array Syntax)

```yaml
pipeline: bronze_raw
presets: [shared_preset]       # Inherited by all children
flowgroups:
  - flowgroup: customer_bronze
    actions: [...]
  - flowgroup: orders_bronze
    actions: [...]
```

### Multi-Document Syntax

Separate YAML documents in one file using `---`:

```yaml
pipeline: bronze
flowgroup: customers
actions: [...]
---
pipeline: bronze
flowgroup: orders
actions: [...]
```

---

## Load Actions

All loads require: `name`, `type: load`, `source`, `target` (view name).

### CloudFiles Source

```yaml
- name: load_raw_customers
  type: load
  readMode: stream           # stream (default) or batch
  source:
    type: cloudfiles
    path: /mnt/landing/customers/
    format: csv              # csv, json, parquet, avro, text, binaryFile
    options:
      cloudFiles.maxFilesPerTrigger: 100
      cloudFiles.inferColumnTypes: "true"
      header: "true"
      delimiter: ","
    schema_location: /mnt/schema/customers/
    schema_evolution_mode: addNewColumns  # addNewColumns, rescue, failOnNewColumns, none
    rescue_data_column: _rescued_data
  target: raw_customers_view
```

**schemaHints** options:
- Inline DDL: `"customer_id BIGINT, name STRING, created_at TIMESTAMP"`
- YAML file path: `"schemas/customer_schema.yaml"`
- DDL file path: `"schemas/customer.ddl"`

### Delta Source

```yaml
- name: load_delta_customers
  type: load
  readMode: stream
  source:
    type: delta
    database: "{catalog}.{bronze_schema}"
    table: raw_customers
    options:
      readChangeFeed: "true"
      startingVersion: "1"
  target: customers_cdf_view
```

### SQL Source

```yaml
- name: load_sql_data
  type: load
  source:
    type: sql
    sql: "SELECT * FROM catalog.schema.reference_table"
    # OR: sql_path: "sql/reference_query.sql"
  target: reference_view
```

### JDBC Source

```yaml
- name: load_jdbc
  type: load
  readMode: batch
  source:
    type: jdbc
    url: "${secret:jdbc/url}"
    driver: com.mysql.cj.jdbc.Driver
    user: "${secret:jdbc/user}"
    password: "${secret:jdbc/password}"
    query: "SELECT * FROM customers WHERE active = 1"
  target: jdbc_customers_view
```

### Kafka Source

```yaml
- name: load_kafka_events
  type: load
  readMode: stream
  source:
    type: kafka
    bootstrap_servers: "${secret:kafka/brokers}"
    subscribe: customer_events
    options:
      startingOffsets: earliest
      kafka.security.protocol: SASL_SSL
  target: raw_events_view
```

### Python Source

```yaml
- name: load_python_data
  type: load
  source:
    type: python
    module_path: py_functions/custom_loader.py
    function_name: load_data
    parameters:
      api_endpoint: "https://api.example.com/data"
  target: api_data_view
```

---

## Transform Actions

All transforms require: `name`, `type: transform`, `transform_type`.

### SQL Transform

```yaml
- name: clean_customers
  type: transform
  transform_type: sql
  source: raw_customers_view      # single source
  # source: [view_a, view_b]     # multi-source
  sql: |
    SELECT
      customer_id,
      TRIM(name) AS name,
      LOWER(email) AS email
    FROM stream(raw_customers_view)  -- stream() required for streaming sources
  target: cleaned_customers_view
```

### Python Transform

```yaml
- name: enrich_customers
  type: transform
  transform_type: python
  source: cleaned_customers_view
  module_path: py_functions/enrichment.py
  function_name: enrich
  parameters:
    lookup_table: reference_data
  readMode: stream
  target: enriched_customers_view
```

Function signature (single source): `def enrich(df, spark, parameters) -> DataFrame`
Function signature (multi-source): `def enrich(dataframes, spark, parameters) -> DataFrame`

### Schema Transform

```yaml
- name: apply_schema
  type: transform
  transform_type: schema
  source: raw_view
  readMode: stream
  schema_file: schemas/customer_schema.yaml  # OR schema_inline
  # schema_inline:
  #   - "old_id -> customer_id: BIGINT"
  #   - "full_name -> name: STRING"
  #   - "created: TIMESTAMP"
  enforcement: strict           # strict (drop unmapped) or permissive (keep all)
  target: schema_applied_view
```

Arrow notation:
- `"old -> new: TYPE"` — rename and cast
- `"old -> new"` — rename only
- `"col: TYPE"` — cast only
- `"col"` — pass-through (strict mode only)

### Data Quality Transform

```yaml
- name: dq_check
  type: transform
  transform_type: data_quality
  source: cleaned_view
  readMode: stream
  expectations_file: expectations/customer_dq.json
  target: validated_view
```

Expectations JSON format:
```json
[
  {"name": "valid_id", "expression": "customer_id IS NOT NULL", "failureAction": "drop"},
  {"name": "valid_email", "expression": "email LIKE '%@%'", "failureAction": "warn"}
]
```
`failureAction`: `fail` (abort pipeline), `warn` (log, keep row), `drop` (remove row).

### Temp Table Transform

```yaml
- name: intermediate_agg
  type: transform
  transform_type: temp_table
  source: cleaned_view
  sql: "SELECT region, COUNT(*) as cnt FROM stream(cleaned_view) GROUP BY region"
  target: region_counts_temp
```

---

## Write Actions

All writes require: `name`, `type: write`, `write_target`.

### Streaming Table (Append)

```yaml
- name: write_customers
  type: write
  source: enriched_customers_view
  write_target:
    type: streaming_table
    database: "{catalog}.{bronze_schema}"
    table: customers
    create_table: true           # default true
    table_properties:
      delta.enableChangeDataFeed: "true"
      quality: "bronze"
    partition_columns: [region]
    cluster_columns: [customer_id]
    comment: "Bronze customer data from CloudFiles ingestion"
```

### Streaming Table (CDC)

```yaml
- name: write_customers_cdc
  type: write
  source: customers_cdf_view
  write_target:
    type: streaming_table
    database: "{catalog}.{silver_schema}"
    table: customers_silver
    mode: cdc
    cdc_config:
      keys: [customer_id]
      sequence_by: _commit_timestamp
      stored_as_scd_type: "1"     # "1" (overwrite) or "2" (history)
      ignore_null_updates: true
      apply_as_deletes: "operation = 'DELETE'"
      except_column_list: [_commit_timestamp, _commit_version]
```

### Streaming Table (Snapshot CDC)

```yaml
- name: write_snapshot_cdc
  type: write
  write_target:
    type: streaming_table
    database: "{catalog}.{silver_schema}"
    table: customers_snapshot
    mode: snapshot_cdc
    snapshot_cdc_config:
      source: "{catalog}.{bronze_schema}.customer_snapshots"
      keys: [customer_id]
      stored_as_scd_type: "2"
```

### Materialized View

```yaml
- name: write_customer_summary
  type: write
  write_target:
    type: materialized_view
    database: "{catalog}.{gold_schema}"
    table: customer_summary
    sql: |
      SELECT
        region,
        COUNT(*) AS total_customers,
        SUM(revenue) AS total_revenue
      FROM catalog.silver.customers
      GROUP BY region
    comment: "Gold-level customer aggregation by region"
```

### Sink (External Write)

```yaml
# Delta sink
- name: write_to_external
  type: write
  source: processed_view
  write_target:
    type: sink
    sink_type: delta
    sink_name: external_delta
    options:
      tableName: "external_catalog.schema.table"

# Kafka sink
- name: publish_events
  type: write
  source: outbound_events_view
  write_target:
    type: sink
    sink_type: kafka
    sink_name: event_publisher
    bootstrap_servers: "${secret:kafka/brokers}"
    topic: customer_updates
```

---

## Substitution System

Resolution order (first match wins):

1. **Local variables** `%{var}`: Defined in `variables:` block, resolved first
2. **Template parameters** `{{ param }}`: From `template_parameters:` block
3. **Environment tokens** `{token}` or `${token}`: From `substitutions/<env>.yaml`
4. **Secrets** `${secret:scope/key}`: Converted to `dbutils.secrets.get()` in generated code

---

## Operational Metadata

Set at flowgroup level or per-action. Additive across preset + flowgroup + action.

```yaml
operational_metadata: true      # Adds all standard columns
# OR selective:
operational_metadata:
  - _processing_timestamp       # current_timestamp()
  - _source_file_path           # CloudFiles _metadata.file_path
  - _source_file_size           # CloudFiles _metadata.file_size
  - _source_file_modification_time
  - _record_hash                # Hash of all columns
```

---

## Presets

Reusable defaults in `presets/` directory. Matched implicitly by action type.

```yaml
# presets/cloudfiles_defaults.yaml
name: cloudfiles_defaults
version: "1.0"
extends: base_preset          # optional inheritance
defaults:
  load_actions:
    cloudfiles:               # matches any cloudfiles load
      options:
        cloudFiles.maxFilesPerTrigger: 200
        cloudFiles.inferColumnTypes: "true"
  write_actions:
    streaming_table:           # matches any streaming_table write
      table_properties:
        delta.enableChangeDataFeed: "true"
```

Merge rule: preset values are deep-merged with explicit config. Explicit always wins on conflict.

---

## Special Flags

- `once: true` on an action — marks as one-time backfill (not continuously triggered)
- `job_name` — assigns flowgroup to an orchestration job. If any flowgroup uses it, ALL must.
