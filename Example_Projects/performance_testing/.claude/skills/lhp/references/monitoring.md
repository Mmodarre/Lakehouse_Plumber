# Monitoring & Event Log Reference

Centralized pipeline observability — event log injection and monitoring pipeline, configured entirely through `lhp.yaml`.

**Prerequisites:** Databricks Asset Bundles enabled (`databricks.yml` exists), Unity Catalog, at least one pipeline.

---

## Event Log Configuration

### lhp.yaml `event_log` Section

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable/disable event log injection |
| `catalog` | string | **(required)** | Unity Catalog name (supports `${token}` substitution) |
| `schema` | string | **(required)** | Schema name (supports `${token}` substitution) |
| `name_prefix` | string | `""` | Prefix for generated event log table name |
| `name_suffix` | string | `""` | Suffix for generated event log table name |

**Table naming formula:** `{name_prefix}{pipeline_name}{name_suffix}`

**Examples:**

| Pipeline | name_prefix | name_suffix | Generated Table Name |
|----------|-------------|-------------|---------------------|
| `bronze_load` | `""` | `_event_log` | `bronze_load_event_log` |
| `silver_transform` | `el_` | `""` | `el_silver_transform` |

**Minimal example:**
```yaml
# lhp.yaml
event_log:
  catalog: "${catalog}"
  schema: _meta
  name_suffix: "_event_log"
```

All fields support LHP token substitution from `substitutions/<env>.yaml`.

### Pipeline-Level Overrides (pipeline_config.yaml)

Individual pipelines can override or opt out via `pipeline_config.yaml`.

**Full replace** (NOT merge — entire project-level event_log is ignored for that pipeline):
```yaml
# config/pipeline_config.yaml
---
pipeline: silver_analytics
event_log:
  name: custom_event_log
  catalog: analytics_catalog
  schema: monitoring
```

**Pipeline opt-out:**
```yaml
---
pipeline: temp_debug_pipeline
event_log: false
```

Project-level event logging does NOT require the `-pc` flag — it's applied automatically during `lhp generate`.

### Generated Resource Output

Event log blocks are injected into `resources/lhp/*.pipeline.yml`:
```yaml
# resources/lhp/bronze_load.pipeline.yml (excerpt)
channel: CURRENT
event_log:
  name: bronze_load_event_log
  schema: _meta
  catalog: acme_edw_dev
```

---

## Monitoring Pipeline Configuration

### lhp.yaml `monitoring` Section

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable/disable monitoring pipeline |
| `pipeline_name` | string | `${project_name}_event_log_monitoring` | Custom monitoring pipeline name |
| `catalog` | string | Inherits from `event_log.catalog` | Override catalog for monitoring tables |
| `schema` | string | Inherits from `event_log.schema` | Override schema for monitoring tables |
| `streaming_table` | string | `all_pipelines_event_log` | Name of centralized Delta table populated by the union notebook |
| `checkpoint_path` | string | **(required)** | Base path for per-pipeline streaming checkpoints (typically a UC volume) |
| `job_config_path` | string | **(required)** | Path (relative to project root) to monitoring job YAML used for cluster, schedule, notifications |
| `max_concurrent_streams` | int | `10` | `ThreadPoolExecutor` `max_workers` for the union notebook |
| `materialized_views` | list | One default `events_summary` MV | Custom MV definitions (see below) |
| `enable_job_monitoring` | boolean | `false` | Adds the `jobs_stats` MV that pulls Databricks Jobs run metadata via the SDK |

Enabling monitoring requires both `checkpoint_path` and `job_config_path` (LHP-CFG-008 if missing). The `event_log` block must also be present and enabled (LHP-CFG-008 if missing).

### Materialized Views

Three behaviors:

| Setting | Behavior |
|---------|----------|
| Omitted / `null` | Default `events_summary` MV is created |
| `[]` (empty list) | No MVs — streaming table only |
| Explicit list | Only the specified MVs are created |

**Inline SQL example:**
```yaml
monitoring:
  materialized_views:
    - name: "error_events"
      sql: "SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"
    - name: "pipeline_latency"
      sql: >-
        SELECT _source_pipeline, avg(duration_ms) as avg_duration
        FROM all_pipelines_event_log GROUP BY _source_pipeline
```

**External SQL file:**
```yaml
monitoring:
  materialized_views:
    - name: "custom_analysis"
      sql_path: "sql/monitoring_custom_analysis.sql"
```

**Validation rules:**
- Each MV must have a `name` field
- MV names must be unique
- Each MV must specify `sql` XOR `sql_path` (not both)

### `__eventlog_monitoring` Alias

Use in `pipeline_config.yaml` to target the monitoring pipeline without knowing its exact name:

```yaml
# config/pipeline_config.yaml
---
pipeline: __eventlog_monitoring
serverless: false
edition: ADVANCED
clusters:
  - label: default
    node_type_id: Standard_D4ds_v5
    autoscale:
      min_workers: 1
      max_workers: 4
```

**Rules:**
- Resolves to the actual monitoring pipeline name at generation time
- Must be a **standalone** pipeline entry, NOT in a pipeline list (LHP-VAL-011)
- If both the alias and the real name appear, error is raised (LHP-VAL-010)
- If monitoring is not configured, alias is silently ignored with a warning

```yaml
# WRONG — alias in a list (LHP-VAL-011)
pipeline:
  - bronze_pipeline
  - __eventlog_monitoring

# CORRECT — separate documents
---
pipeline: bronze_pipeline
serverless: false
---
pipeline: __eventlog_monitoring
serverless: false
```

---

## Common Patterns

### Minimal Setup

```yaml
# lhp.yaml
name: my_project
version: "1.0"

event_log:
  catalog: "${catalog}"
  schema: _meta
  name_suffix: "_event_log"

monitoring:
  checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
  job_config_path: "config/monitoring_job_config.yaml"
```

Creates: event log injection on all pipelines + monitoring pipeline with default `events_summary` MV + Workflow job chaining the union notebook and the MVs pipeline.

### Full Customization

```yaml
# lhp.yaml
event_log:
  catalog: "${catalog}"
  schema: _meta
  name_suffix: "_event_log"

monitoring:
  pipeline_name: "central_observability"
  catalog: "analytics_catalog"
  schema: "_monitoring"
  streaming_table: "unified_event_stream"
  checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
  job_config_path: "config/monitoring_job_config.yaml"
  max_concurrent_streams: 20
  materialized_views:
    - name: "error_events"
      sql: "SELECT * FROM unified_event_stream WHERE event_type = 'error'"
    - name: "daily_analysis"
      sql_path: "sql/monitoring_custom_analysis.sql"
```

### Selective Pipeline Monitoring

Opt out specific pipelines from event logging:
```yaml
# config/pipeline_config.yaml
---
pipeline: temp_debug_pipeline
event_log: false

---
pipeline: experimental_pipeline
event_log: false
```

Opted-out pipelines are excluded from the union notebook's `SOURCES` list and contribute no rows to the union Delta table.

### Environment-Specific Configuration

```yaml
# lhp.yaml
event_log:
  catalog: "${catalog}"
  schema: "${monitoring_schema}"
  name_suffix: "_event_log"

monitoring:
  checkpoint_path: "/Volumes/${catalog}/${monitoring_schema}/checkpoints/event_logs"
  job_config_path: "config/monitoring_job_config.yaml"
```

```yaml
# substitutions/dev.yaml         # substitutions/prod.yaml
dev:                              prod:
  catalog: acme_edw_dev             catalog: acme_edw_prod
  monitoring_schema: _meta          monitoring_schema: _monitoring
```

---

## Troubleshooting

| Issue | Error Code | Solution |
|-------|-----------|---------|
| `event_log` is not a YAML mapping | LHP-CFG-006 | Define as mapping with `catalog` and `schema` |
| `event_log` missing `catalog` or `schema` | LHP-CFG-007 | Add both required fields, or set `enabled: false` |
| `monitoring` is not a YAML mapping | LHP-CFG-008 | Define `monitoring:` as a mapping with `checkpoint_path` and `job_config_path` |
| Monitoring missing `checkpoint_path` | LHP-CFG-008 | Add `checkpoint_path` (typically a UC volume) or disable monitoring |
| Monitoring missing `job_config_path` | LHP-CFG-008 | Add `job_config_path` pointing to a job YAML, or disable monitoring |
| Monitoring `job_config_path` file not found | LHP-CFG-008 / LHP-IO-001 | File at resolved path does not exist; create it or fix the path |
| `materialized_views` is not a list | LHP-CFG-008 | Use YAML list: `materialized_views: [...]` |
| Monitoring enabled without `event_log` | LHP-CFG-008 | Add `event_log` section, or disable monitoring |
| Duplicate MV names | LHP-CFG-008 | Each MV must have unique `name` |
| Both `sql` and `sql_path` on same MV | LHP-CFG-008 | Use one or the other |
| Alias + real name both in config | LHP-VAL-010 | Use only `__eventlog_monitoring` or the real name |
| Alias used in a pipeline list | LHP-VAL-011 | Must be standalone `pipeline:` entry |
