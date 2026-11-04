# Write — materialized_view

`type: write`, `write_target.type: materialized_view`. Handler: `MaterializedViewWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `catalog` / `schema` / `table` | string | — | Three-part target name. |
| `sql` | string | — | Inline query (one of `sql` / `sql_path` / action `source`). |
| `sql_path` | string | — | External query file. `${token}` / `${secret:scope/key}` substituted; a plain `${name}` left after substitution → `LHP-CFG-010` from `lhp validate` and `lhp generate` (dotted/prefixed Spark variables such as `${mypipeline.start}` are left as written; a pipeline `configuration:` parameter read at runtime needs a dotted key, e.g. `${mypipeline.start_date}` — a plain `${start_date}` fails even when the configuration defines it). In a template action, a file containing `{{`, `{%` or `{#` is first rendered with the template parameters at template expansion (strict; `LHP-CFG-071`) — see templates-presets.md. |
| `refresh_schedule` | string | — | Cron / schedule. |
| `table_properties` | dict | `{}` | — |
| `tags` | dict | — | UC tags `{key: value}`; value `""`/`~`/null = key-only. Applied during the run by a generated `_uc_tagging_hook.py` (REST API, not the view DDL): on `update_progress` `RUNNING` (streaming tables) and on the terminal state (materialized views, which materialize later — so MVs are tagged on the terminal pass), each entity tagged at most once. Tagging on `RUNNING` keeps tag-write failures visible as event-log warnings while the run is live; it never fails the pipeline. Existing tag state is read once at module import from `system.information_schema` (best-effort — a read failure is re-raised as a warning on the first `RUNNING` event, then tagging proceeds create-only; no init crash). **On by default** — declaring `tags` opts in; set `uc_tagging.enabled: false` in `lhp.yaml` to disable. `tag_update_concurrency` (default 16, range 1–20) tunes the thread pool; `remove_undeclared_tags` (default false) is additive, `true` reconciles to the declared set (deletes undeclared keys). `max_allowable_consecutive_failures` (default null) is the hook's `@dp.on_event_hook` failure budget — an integer >= 0 or null; null means no limit and the hook is never disabled. |
| `tags_file` | string | — | Path to a unified schema/tags file (convention `schemas/<table>.yaml`); may be the SAME file as `table_schema`. Mutually exclusive with an inline `tags` mapping (both set → `cannot specify both 'tags' and 'tags_file'`). Supplies UC tags: a table-level `tags:` mapping and per-column `tags:`. File keys: optional identifier `table` (canonical) or its alias `name`; a table-level `tags:` mapping; and a `columns:` **list** of `{name, type?, nullable?, comment?, tags?}` entries — `name` required (non-empty, unique); `type`/`nullable`/`comment` are schema-only (ignored by the tags reader); each `tags` is that column's UC tags (a mapping, may be empty). Legacy `version`/`description`/`primary_key` top-level keys are tolerated and ignored. Value `""`/`~`/null = key-only tag. `LHP-CFG-067`: non-mapping file, unknown top-level key (the retired `column_tags` key now errors), `columns` not a list, a non-mapping/unknown-key column entry; as a tags_file also wrong-typed `table`/`name`/`tags`, a missing/empty/duplicate column `name`, or a non-mapping per-column `tags`. Identifier is **optional**; when set as a tags_file it should equal the write target's table — a mismatch (or `table`≠`name`) logs an `LHP-CFG-068` warning and generation uses the write target's table (check skipped under `--sandbox`, which renames the table). Missing file → `LHP-IO-001`. Tag keys/values substitute `${token}`; a plain `${name}` left after substitution → `LHP-CFG-010` (dotted/prefixed references pass). `lhp validate` reads every tags_file the tagging hook would read (none when `uc_tagging.enabled: false`), so it reports these errors as well as `lhp generate`. |
| `spark_conf` | dict | `{}` | — |
| `table_schema` | string | — | Inline schema, or a `.ddl`/`.sql`/`.yaml`/`.json` file path. A `.yaml`/`.json` file is the unified schema/tags file — `type`/`nullable`/`comment` are read here. UC tags in that file are ignored unless the same file is also set as `tags_file` (else `LHP-CFG-069` warns at generate time). |
| `row_filter` | string | — | — |
| `temporary` | bool | `false` | — |
| `private` | bool | `false` | Emitted as `private=`. Creates the table for the pipeline's lifetime without publishing it to the metastore; visible only inside the pipeline. |
| `partition_columns` | list | — | Mutually exclusive with `cluster_columns` and `cluster_by_auto: true` — Databricks does not allow partitioning together with liquid clustering; both set → validation error `cannot specify both 'partition_columns' and liquid clustering`. `[]` counts as unset. |
| `cluster_columns` | list | — | Liquid clustering; renders `cluster_by=[...]`. Mutually exclusive with `partition_columns`. `[]` counts as unset. |
| `cluster_by_auto` | bool | — | Auto liquid clustering; renders `cluster_by_auto=True`. Can be combined with `cluster_columns` (both kwargs emitted): the columns become the initial clustering keys and Databricks may later change them based on the workload. Omitted when false/unset. `true` is mutually exclusive with `partition_columns`. |
| `refresh_policy` | string | — | One of `auto`, `incremental`, `incremental_strict`, `full`; any other value fails validation. Renders `refresh_policy="incremental"`. MV-only. |
| `path` | string | — | — |
| `expectations_file` | string | — | Data-quality expectations file (project-root relative; same list/mapping formats as a `data_quality` transform, see below). Rules become `@dp.expect_all` / `@dp.expect_all_or_drop` / `@dp.expect_all_or_fail` decorators on the view. Valid only on a `materialized_view` write target: on a `streaming_table` or `sink` write target it is `LHP-CFG-001` (unknown field); set on the action itself it is `LHP-CFG-073`. |
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
- `cluster_columns` and `cluster_by_auto` may be set together — renders `cluster_by=[...]` plus `cluster_by_auto=True`; the columns are the initial clustering keys, which Databricks may later change based on the workload.
- `partition_columns` cannot be combined with `cluster_columns` or `cluster_by_auto: true` (Databricks does not allow partitioning together with liquid clustering) — validation fails; remove one.
- UC tags apply only to the table-creating action; temporary tables and sinks are excluded. Column-level tags come from a `tags_file`'s per-column `tags:`. A schema file may carry those same tags — point BOTH `table_schema` and `tags_file` at it. A `table_schema` file that carries tags but is NOT also wired as `tags_file` has them dropped; `lhp generate` warns `LHP-CFG-069` (streaming-table + MV writes only; generate-time, since validate runs no codegen).
- UC tagging permissions (enforced by Unity Catalog, not LHP): the pipeline's run-as identity needs `APPLY TAG` on the table and `ASSIGN` on required governed tags to write tags via the REST API, plus `USE CATALOG`, `USE SCHEMA`, and `SELECT` on `system.information_schema` to read existing tag state.
- **Expectations, shape 1:** `write_target.expectations_file` works with `sql`, `sql_path` or a `source` view. List format: `name`, `constraint` (or `expression`), `failureAction` (`warn`|`drop`|`fail`) or `type` (`expect`|`expect_or_drop`|`expect_or_fail`). Mapping format: `"<constraint>": {action: warn|drop|fail, name: ...}`. A wrapping top-level `expectations:` key is accepted. Names must be unique within the file (`LHP-VAL-071`). Bad rules: `LHP-VAL-001` (no constraint), `LHP-VAL-006` (unknown action/type, or `action` in a list entry — use `failureAction`), `LHP-VAL-008` (non-text name/constraint, non-mapping rule). The MV keeps incremental refresh, unless its query is non-deterministic (e.g. `F.current_timestamp()` anywhere in its chain, operational metadata included): Databricks then recomputes it in full.
- **Expectations, shape 2 (fusion):** a `data_quality` action in `dqe` mode that reads in batch and whose target is read by exactly one action, a pass-through MV (`source` = the DQ target as a string or a one-element list, no `sql`/`sql_path`/`expectations_file`, the only write to its table), is fused. A test action reading the DQ view does not count as a reader, so fusion is the same with and without `--include-tests`. An MV with its own `expectations_file` is NOT fused (put all its rules in that file); names repeated within the fused DQ's own rules are `LHP-VAL-071`. Fused: the MV gets the DQ's expectations and operational-metadata columns and reads the DQ's *source* view; the DQ view is still emitted but unread, so it never runs. Reason: expectations on a view upstream of an MV force a full recompute on every refresh; on the MV they do not.
- **Product gap:** an MV whose own `sql`/`sql_path` reads a `data_quality` view is NOT fused. It works, but is fully recomputed on every refresh. To stay incremental, put the rules on the MV with `write_target.expectations_file`, or make the MV pass-through (without an `expectations_file` of its own).
- Example (both shapes): `tests/e2e/fixtures/testing_project/pipelines/04_gold/mv_write_expectations.yaml`.
