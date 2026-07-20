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
| `tags_file` | string | — | Path to a unified schema/tags file (convention `schemas/<table>.yaml`); may be the SAME file as `table_schema`. Mutually exclusive with an inline `tags` mapping (both set → `cannot specify both 'tags' and 'tags_file'`). Supplies UC tags: a table-level `tags:` mapping and per-column `tags:`. File keys: optional identifier `table` (canonical) or its alias `name`; a table-level `tags:` mapping; and a `columns:` **list** of `{name, type?, nullable?, comment?, tags?}` entries — `name` required (non-empty, unique); `type`/`nullable`/`comment` are schema-only (ignored by the tags reader); each `tags` is that column's UC tags (a mapping, may be empty). Legacy `version`/`description`/`primary_key` top-level keys are tolerated and ignored. Value `""`/`~`/null = key-only tag. `LHP-CFG-067`: non-mapping file, unknown top-level key (the retired `column_tags` key now errors), `columns` not a list, a non-mapping/unknown-key column entry; as a tags_file also wrong-typed `table`/`name`/`tags`, a missing/empty/duplicate column `name`, or a non-mapping per-column `tags`. Identifier is **optional**; when set as a tags_file it should equal the write target's table — a mismatch (or `table`≠`name`) logs an `LHP-CFG-068` warning and generation uses the write target's table (check skipped under `--sandbox`, which renames the table). Missing file → `LHP-IO-001`. |
| `spark_conf` | dict | `{}` | — |
| `table_schema` | string | — | Inline schema, or a `.ddl`/`.sql`/`.yaml`/`.json` file path. A `.yaml`/`.json` file is the unified schema/tags file — `type`/`nullable`/`comment` are read here. UC tags in that file are ignored unless the same file is also set as `tags_file` (else `LHP-CFG-069` warns at generate time). |
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
- UC tags apply only to the table-creating action; temporary tables and sinks are excluded. Column-level tags come from a `tags_file`'s per-column `tags:`. A schema file may carry those same tags — point BOTH `table_schema` and `tags_file` at it. A `table_schema` file that carries tags but is NOT also wired as `tags_file` has them dropped; `lhp generate` warns `LHP-CFG-069` (streaming-table + MV writes only; generate-time, since validate runs no codegen).
- UC tagging permissions (enforced by Unity Catalog, not LHP): the pipeline's run-as identity needs `APPLY TAG` on the table and `ASSIGN` on required governed tags to write tags via the REST API, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on `system.information_schema` to read existing tag state.
