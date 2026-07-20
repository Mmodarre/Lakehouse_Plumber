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
| `tags` | dict | — | UC tags `{key: value}`; value `""`/`~`/null = key-only. Applied during the run by a generated `_uc_tagging_hook.py` (REST API, not the table DDL): on `update_progress` `RUNNING` (streaming tables) and on the terminal state (MVs, which materialize later), each entity tagged at most once; failures surface as event-log warnings and never fail the pipeline. Existing tag state is read once at module import from `system.information_schema` (best-effort; read failure → warning on first `RUNNING`, then create-only). **On by default** — declaring `tags` opts in; set `uc_tagging.enabled: false` in `lhp.yaml` to disable. `tag_update_concurrency` (default 16, range 1–20) tunes the thread pool; `remove_undeclared_tags` (default false) is additive, `true` reconciles to the declared set. |
| `tags_file` | string | — | Path to a unified schema/tags file (convention `schemas/<table>.yaml`); may be the SAME file as `table_schema`. Mutually exclusive with an inline `tags` mapping (both set → `cannot specify both 'tags' and 'tags_file'`). Supplies UC tags: a table-level `tags:` mapping and per-column `tags:`. File keys: optional identifier `table` (canonical) or its alias `name`; a table-level `tags:` mapping; and a `columns:` **list** of `{name, type?, nullable?, comment?, tags?}` entries — `name` required (non-empty, unique); `type`/`nullable`/`comment` are schema-only (ignored by the tags reader); each `tags` is that column's UC tags (a mapping, may be empty). Legacy `version`/`description`/`primary_key` top-level keys are tolerated and ignored. Value `""`/`~`/null = key-only tag. `LHP-CFG-067`: non-mapping file, unknown top-level key (the retired `column_tags` key now errors), `columns` not a list, a non-mapping/unknown-key column entry; as a tags_file also wrong-typed `table`/`name`/`tags`, a missing/empty/duplicate column `name`, or a non-mapping per-column `tags`. Identifier is **optional**; when set as a tags_file it should equal the write target's table — a mismatch (or `table`≠`name`) logs an `LHP-CFG-068` warning and generation uses the write target's table (check skipped under `--sandbox`, which renames the table). Missing file → `LHP-IO-001`. |
| `spark_conf` | dict | — | — |
| `table_schema` | string | — | Inline schema, or a `.ddl`/`.sql`/`.yaml`/`.json` file path (auto-detected). A `.yaml`/`.json` file is the unified schema/tags file — `type`/`nullable`/`comment` are read here. UC tags in that file are ignored unless the same file is also set as `tags_file` (else `LHP-CFG-069` warns at generate time; streaming-table + MV writes only). |
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
- UC tags apply only to the table-creating action (`create_table: true`); temporary tables and sinks are excluded. Column-level tags come from a `tags_file`'s per-column `tags:`. A schema file may carry those same tags — point BOTH `table_schema` and `tags_file` at it. A `table_schema` file that carries tags but is NOT also wired as `tags_file` has them dropped; `lhp generate` warns `LHP-CFG-069` (streaming-table + MV writes only; generate-time, since validate runs no codegen).
- UC tagging permissions (enforced by Unity Catalog, not LHP): the pipeline's run-as identity needs `APPLY TAG` on the table and `ASSIGN` on required governed tags to write tags via the REST API, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on `system.information_schema` to read existing tag state.
