# Write — streaming_table (standard append)

`type: write`, `write_target.type: streaming_table` with `mode: standard` (the default). Handler: `StreamingTableWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `type` | string | — | Must be `streaming_table`. |
| `mode` | string | `standard` | One of `standard`, `cdc`, `snapshot_cdc`. |
| `catalog` | string | — | Target catalog. |
| `schema` | string | — | Target schema. |
| `table` | string | — | Target table name. |
| `create_table` | bool | `true` | — |
| `temporary` | bool | `false` | — |
| `comment` | string | — | — |
| `table_properties` | dict | — | — |
| `tags` | dict | — | UC tags `{key: value}`; value `""`/`~`/null = key-only. Applied to all managed tables/columns in one pass when the pipeline reaches a terminal state (update_progress) by a generated `_uc_tagging_hook.py` (REST API; existing state read once from `system.information_schema`), not in the table DDL. Failures raise into the pipeline event log. **On by default** — declaring `tags` opts in; set `uc_tagging.enabled: false` in `lhp.yaml` to disable. `tag_update_concurrency` (default 16) tunes the thread pool. Column tags live in a YAML/JSON `table_schema`. |
| `spark_conf` | dict | — | — |
| `table_schema` | string | — | Inline schema or schema-file path. |
| `row_filter` | string | — | — |
| `partition_columns` | list | — | — |
| `cluster_columns` | list | — | — |
| `cluster_by_auto` | bool | — | Auto liquid clustering; renders `cluster_by_auto=True`. Mutually exclusive with `cluster_columns`. Omitted when false/unset. |
| `path` | string | — | — |

## Minimal YAML

```yaml
- name: write_customer_silver
  type: write
  source: v_customer_bronze
  write_target:
    type: streaming_table
    mode: standard
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: customer_dim
```

## Key rules

- `source` may be a single view or a list (multi-source append flow into one table).
- `table_schema` auto-detects inline DDL vs. a `.ddl`/`.sql`/`.yaml` file path.
