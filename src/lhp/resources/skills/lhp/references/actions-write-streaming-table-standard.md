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
| `tags_file` | string | — | Path to an external UC tags file (convention `uc_tags/<table>.yaml`); mutually exclusive with `tags` (both set → `cannot specify both 'tags' and 'tags_file'`). The single source of column-level tags. Strict format: a mapping whose only keys are `version` (required, `"1.0"`/`"1.0.0"`), `table` (required, the fully-qualified write target), `tags` (optional, table-level mapping, may be empty), and `column_tags` (optional, column-level — a **list** of `{name, tags}` entries: each `name` a non-empty, unique column name; each entry's `tags` a mapping that is required but may be empty); at least one of `tags`/`column_tags` required. Value `""`/`~`/null = key-only at either level. Any unknown/missing top-level key (the removed `columns` key now errors as unknown), bad `version`, wrong-typed `table`/`tags`, a `column_tags` that is not a list, an entry that is not a `{name, tags}` mapping or has an unknown key, a missing/empty/duplicate `name`, a missing or non-mapping per-entry `tags`, or neither `tags` nor `column_tags` → `LHP-CFG-067`. The sidecar's `table:` must equal the write target's table (mismatch → `LHP-CFG-067`; check skipped under `--sandbox`, which renames the table). Missing file → `LHP-IO-001`. |
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
- UC tags apply only to the table-creating action (`create_table: true`); temporary tables and sinks are excluded. Column-level tags come only from the `tags_file` `column_tags:` list — a schema file (`table_schema`) must not carry a column `tags:` key (`lhp generate` raises `LHP-VAL-016` if it does; generate-time only, since validate runs no codegen).
- UC tagging permissions (enforced by Unity Catalog, not LHP): the pipeline's run-as identity needs `APPLY TAG` on the table and `ASSIGN` on required governed tags to write tags via the REST API, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on `system.information_schema` to read existing tag state.
