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
| `tags` | dict | — | UC tags `{key: value}`; value `""`/`~`/null = key-only. Applied during the run by a generated `_uc_tagging_hook.py` (REST API, not the view DDL): on `update_progress` `RUNNING` (streaming tables) and on the terminal state (materialized views, which materialize later — so MVs are tagged on the terminal pass), each entity tagged at most once. Tagging on `RUNNING` keeps tag-write failures visible as event-log warnings while the run is live; it never fails the pipeline. Existing tag state is read once at module import from `system.information_schema` (best-effort — a read failure is re-raised as a warning on the first `RUNNING` event, then tagging proceeds create-only; no init crash). **On by default** — declaring `tags` opts in; set `uc_tagging.enabled: false` in `lhp.yaml` to disable. `tag_update_concurrency` (default 16, range 1–20) tunes the thread pool; `remove_undeclared_tags` (default false) is additive, `true` reconciles to the declared set (deletes undeclared keys). |
| `tags_file` | string | — | Path to an external UC tags file (convention `uc_tags/<table>.yaml`); mutually exclusive with `tags` (both set → `cannot specify both 'tags' and 'tags_file'`). The single source of column-level tags. Strict format: a mapping whose only keys are `version` (optional, `"1.0"`/`"1.0.0"`; absent ⇒ 1.0), `table` (required — the write target's unqualified, post-substitution table name; `name` accepted as an alias), `tags` (optional, table-level mapping, may be empty), and `columns` (optional, column-level — a **list** of `{name, tags}` entries: each `name` a non-empty, unique column name; each entry's `tags` a mapping that is required but may be empty); at least one of `tags`/`columns` required. Value `""`/`~`/null = key-only at either level. Any unknown top-level key (the former `column_tags` key now errors as unknown), a missing identifier (neither `table` nor `name`), bad `version`, wrong-typed `table`/`name`/`tags`, a `columns` that is not a list, an entry that is not a `{name, tags}` mapping or has an unknown key, a missing/empty/duplicate `name`, a missing or non-mapping per-entry `tags`, or neither `tags` nor `columns` → `LHP-CFG-067`. The sidecar's identifier (`table`/`name`) should equal the write target's table; a mismatch logs an `LHP-CFG-068` warning and generation proceeds using the write target's table (check skipped under `--sandbox`, which renames the table). Missing file → `LHP-IO-001`. |
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
- UC tags apply only to the table-creating action; temporary tables and sinks are excluded. Column-level tags come only from the `tags_file` `columns:` list — a schema file (`table_schema`) must not carry a column `tags:` key (`lhp generate` raises `LHP-VAL-016` if it does; generate-time only, since validate runs no codegen).
- UC tagging permissions (enforced by Unity Catalog, not LHP): the pipeline's run-as identity needs `APPLY TAG` on the table and `ASSIGN` on required governed tags to write tags via the REST API, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on `system.information_schema` to read existing tag state.
