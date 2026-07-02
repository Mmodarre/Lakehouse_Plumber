# Write — materialized_view

`type: write`, `write_target.type: materialized_view`. Handler: `MaterializedViewWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `catalog` / `schema` / `table` | string | — | Three-part target name. |
| `sql` | string | — | Inline query (one of `sql` / `sql_path` / action `source`). |
| `sql_path` | string | — | External query file. |
| `refresh_schedule` | string | — | Cron / schedule. |
| `table_properties` | dict | `{}` | — |
| `tags` | dict | — | UC tags `{key: value}`; value `""`/`~`/null = key-only. Applied during the run by a generated `_uc_tagging_hook.py` (REST API, not the view DDL): on `update_progress` `RUNNING` (streaming tables) and on the terminal state (materialized views, which materialize later — so MVs are tagged on the terminal pass), each entity tagged at most once. Tagging on `RUNNING` keeps tag-write failures visible as event-log warnings while the run is live; it never fails the pipeline. Existing tag state is read once at module import from `system.information_schema` (best-effort — a read failure is re-raised as a warning on the first `RUNNING` event, then tagging proceeds create-only; no init crash). **On by default** — declaring `tags` opts in; set `uc_tagging.enabled: false` in `lhp.yaml` to disable. `tag_update_concurrency` (default 16) tunes the thread pool. |
| `spark_conf` | dict | `{}` | — |
| `table_schema` | string | — | Inline or file. |
| `row_filter` | string | — | — |
| `temporary` | bool | `false` | — |
| `partition_columns` | list | — | — |
| `cluster_columns` | list | — | — |
| `cluster_by_auto` | bool | — | Auto liquid clustering; renders `cluster_by_auto=True`. Mutually exclusive with `cluster_columns`. Omitted when false/unset. |
| `refresh_policy` | string | — | One of `auto`, `incremental`, `incremental_strict`, `full`; any other value fails validation. Renders `refresh_policy="incremental"`. MV-only. |
| `path` | string | — | — |
| `comment` | string | `Materialized view: {table}` | — |

## Minimal YAML

```yaml
- name: write_customer_summary
  type: write
  write_target:
    type: materialized_view
    catalog: "${catalog}"
    schema: "${gold_schema}"
    table: customer_summary
    sql: "SELECT customer_id, COUNT(*) AS orders FROM v_orders GROUP BY customer_id"
```

## Key rules

- Exactly one query source: action `source` field, `sql`, or `sql_path`.
- No action-level `source` needed when `sql`/`sql_path` is provided.
- `cluster_columns` and `cluster_by_auto` are mutually exclusive (XOR) — setting both fails validation with `'cluster_columns' and 'cluster_by_auto' are mutually exclusive`.
