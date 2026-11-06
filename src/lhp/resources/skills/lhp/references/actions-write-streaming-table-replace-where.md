# Write — streaming_table (REPLACE WHERE)

`type: write`, `write_target.type: streaming_table` with `mode: replace_where`. Requires `replace_where_config`. Handler: `StreamingTableWriteGenerator` (validated by `ReplaceWhereConfigValidator`). Emits a single `@dp.table(..., replace_where=...)` over a batch read — Databricks defines a REPLACE WHERE table and its flow in one statement, and the flow takes the table's name. There is no `dp.create_streaming_table` and no flow decorator.

On every refresh the flow deletes the target rows matching the predicate and recomputes them by re-running the source query for that window; rows outside the predicate are left untouched. The platform applies the predicate to the source read, so it does not need repeating in the source. This is the batch counterpart of `mode: replace` (REPLACE USING), mirroring `cdc` → `snapshot_cdc`.

## Options (under `write_target.replace_where_config:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `replace_where` | string | required | Non-blank SQL predicate; emitted verbatim as `replace_where="..."`. `${token}` substitutions are resolved first; `${secret:...}` is rejected. No other key is allowed in the block. |

## Minimal YAML

```yaml
- name: write_orders_recent
  type: write
  source: v_orders_source
  write_target:
    type: streaming_table
    mode: replace_where
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: orders_recent
    replace_where_config:
      replace_where: "order_date >= date_add(current_date(), -7)"
```

## Generated code

```python
@dp.table(
    name="acme.silver.orders_recent",
    comment="Streaming table: orders_recent",
    table_properties={"pipelines.reset.allowed": "false"},
    replace_where="order_date >= date_add(current_date(), -7)",
)
def orders_recent():
    """Replace-where flow to acme.silver.orders_recent"""
    df = spark.read.table("v_orders_source")

    return df
```

`table_properties` is always emitted. `spark_conf`, `partition_columns` (→ `partition_cols=`), `cluster_columns` (→ `cluster_by=`), `cluster_by_auto`, `path`, `table_schema` (→ `schema=`), `row_filter` and `private` are emitted only when set.

## Key rules

- **Reset guard:** LHP defaults `table_properties["pipelines.reset.allowed"]` to `"false"` so a full refresh cannot wipe the rows outside the predicate window. An explicit value from the YAML or a preset always wins; the key is matched case-insensitively, so a differently cased key is never duplicated.
- Source must be a **batch** read of a **single** view (a string or a one-element list; more than one source view is rejected); `readMode: stream` on the write is rejected (Databricks: `REPLACE_WHERE_WITH_STREAMING_SOURCE`). A streaming source view (`cloudfiles`/`kafka` load, `delta` load with `readMode: stream`) fails LHP-VAL-070; a `delta` load is batch by default.
- The table is always created by this action: `create_table: false` is rejected, and the target must be served by this **single** flow — no other write action may share its `catalog.schema.table` (no fan-in, no combining with other modes). Checked per pipeline: a second creator fails LHP-CFG-004; a `create_table: false` writer fails LHP-VAL-010 naming the REPLACE WHERE flow (Databricks: `UNSUPPORTED_FEATURE.MULTI_FLOW_REPLACE_WHERE_TARGET`).
- `temporary: true` is rejected; `temporary: false` passes (use `private: true` for a pipeline-private table).
- Action-level `expectations` are rejected — Databricks does not support expectations on a REPLACE WHERE target (`UNSUPPORTED_FEATURE.REPLACE_WHERE_FLOW_WITH_EXPECTATIONS`), so no `@dp.expect*` decorator is ever emitted.
- An unknown mode such as `replace-where` fails with LHP-VAL-006 and a did-you-mean of `replace_where`.
- `catalog` + `schema` + `table` (also accepts `database` as the combined `catalog.schema` form in older configs).
- Config-block and write-setting errors are listed inside LHP-VAL-007 (`lhp validate` and `lhp generate`), and so are LHP-VAL-006 and LHP-VAL-070; only LHP-CFG-004 / LHP-VAL-010 are reported on their own.
- In the event log the flow is named after the table's full `catalog.schema.table` name (the flow takes the table's name).

## Full refresh and the reset guard

- A full refresh re-runs the source query for the **current predicate only**; unguarded, every row outside it is deleted.
- With `pipelines.reset.allowed: "false"` (the LHP default): a **pipeline-wide** full refresh succeeds, does not reset the table (no RESET `dataset_life_cycle` event for it — the only sign it was protected), keeps the rows outside the predicate and recomputes the rows inside it. A **selective** full refresh naming the table fails the whole update at analysis: `Table <catalog.schema.table> is not resettable, so it cannot be contained in fullRefreshTables in the refresh selection.`
- Deliberate reset: set `table_properties: {pipelines.reset.allowed: "true"}` on the write, generate + deploy, run the full refresh, then remove the setting (LHP re-adds `"false"`) and deploy again — or drop the table. After a reset the table holds only the rows the current predicate matches.

## Incremental refresh

- Planner techniques observed (event log `planning_information`; not exhaustive — Databricks has others, e.g. `PARTITION_OVERWRITE`): `NO_OP` / `APPEND_ONLY` / `ROW_BASED` are incremental, chosen by the change set (none / inserts only / updates and deletes); `COMPLETE_REPLACE_WHERE` deletes and re-inserts every row inside the predicate. The first refresh is always a full computation.
- Databricks conditions (if one fails, that refresh recomputes every row inside the predicate): serverless compute only; supported query shape (a deterministic query whose operators Databricks can refresh incrementally); deterministic predicate (`current_date()` allowed, `rand()` not); predicate on **base columns of a source table** (not aggregate/window outputs); no external DML inside the predicate; the predicate does not cover rows the previous predicate excluded.
- Predicate guidance: a moving lower bound (`order_date >= date_add(current_date(), -7)`) stays eligible; widening the predicate costs one full recompute (later refreshes can be incremental again); a moving upper bound (`BETWEEN date_add(current_date(), -7) AND current_date()`) can trigger a one-off full recompute; narrowing keeps the rows that no longer match.
- Operational metadata is added on the load / supported transform (never on the write), so it is part of the flow's query. A non-deterministic column (`F.current_timestamp()`) makes **every** refresh `COMPLETE_REPLACE_WHERE` (planner: `PLAN_NOT_DETERMINISTIC`, `expression_name: current_timestamp`, `plan_not_deterministic_sub_type: TIME_FUNCTION`). Deterministic columns (`xxhash64` record hash) are fine.
- Expectations on an upstream view (e.g. a batch `dqe` `data_quality` transform) are accepted and `drop` works, but every refresh recomputes in full and the flow's event-log metrics do not count dropped rows.
- Reading the event log: `planning_information` event, `origin.flow_name` = FQ table name, message `Flow '<fq>' has been planned to be executed as <TECHNIQUE>.`; `details.planning_information.technique_information[]` — the entry with `"is_chosen": true` ran; blockers under `incrementalization_issues`. `flow_progress` `num_output_rows` / `num_deleted_rows` are predicate-sized either way — they cannot show incrementality.

## LHP-VAL-073 (warning, category `validation`)

- For each `mode: replace_where` write, walks upstream over declared `source:` edges **within the same flowgroup**, resolves the operational-metadata columns of every action that adds them, and flags expressions that call `current_timestamp`, `current_date`, `localtimestamp`, `now`, `curdate`, `rand`, `randn`, `random`, `uuid`, `shuffle`, `monotonically_increasing_id`, `input_file_name`, `input_file_block_start`, `input_file_block_length`, `spark_partition_id`, use the bare keyword `current_timestamp` / `current_date` / `localtimestamp`, or call `unix_timestamp()` with no argument (case-insensitive).
- One warning per flowgroup listing every `write '<w>' <- action '<a>': column '<c>' = <expr>`; full text with `--show-details`. Does not fail the run; `--strict` does. Fix: deselect the columns (`operational_metadata: false`, or deterministic columns only) or make the expressions deterministic.
- Scope limits: not followed — views read only inside SQL text (an undeclared `JOIN`), views produced by another flowgroup of the same pipeline. Advisory: a miss only loses incremental refresh, never data.
- Dedup is per `(code, YAML file)`: two flowgroups in one file that both trigger it show only the warning of the flowgroup whose name sorts first; `--strict` still fails.

## LHP-VAL-072 (error): streaming reads of a REPLACE WHERE table

- Why: every refresh rewrites rows inside the predicate. A stream of the table fails on Databricks with `DELTA_SOURCE_TABLE_IGNORE_CHANGES`; with `skipChangeCommits` it silently skips every rewritten commit (inserts included).
- Checked, project-wide across every pipeline of the run, by `lhp validate` and `lhp generate` (generate writes no files): a `delta` load whose read is a stream (`skipChangeCommits` does **not** exempt); `STREAM(<table>)` in a SQL load or SQL transform (inline or `sql_path`, detected after substitution); `STREAM(<table>)` in a `custom_sql` test (only with `--include-tests`).
- Exempt: a change-feed read — `delta` load with `options.readChangeFeed` = boolean `true` or string `"true"` in any case. Batch reads are not checked.
- Matching: the read must name the same table, compared without regard to case or backticks. `catalog.schema.table` or `schema.table`; `schema.table` matches only when a single catalog in the run produces it, and only outside a filtered run (see Limits); same name in another catalog is a different table.
- SQL sqlglot cannot parse (`WATERMARK`, `STREAM(t) WITH (...)`): `STREAM` arguments are read lexically — a 2/3-part name is checked; `$source`, a `${token}` placeholder, a 1-part name (view) and a table-valued function (`STREAM read_files(...)`) are not table reads; anything else (e.g. a subquery) is unidentifiable and, when the run writes a REPLACE WHERE table, is reported with message 2. SQL LHP cannot read is reported with message 2 too.
- Message 1: `<p>.<f>.<a> streams from '<table>', a REPLACE WHERE table written by <p>.<f>.<a>. Each refresh rewrites rows, which a streaming read cannot follow (skipChangeCommits silently drops data). Use readMode: batch, or stream its change feed with options.readChangeFeed: "true" (see docs).`
- Message 2: `<p>.<f>.<a> may stream from a REPLACE WHERE table: LHP cannot tell which table a STREAM read in its SQL names (the SQL does not parse and the STREAM argument is not a plain table name, or the SQL cannot be read), and the project writes REPLACE WHERE tables (<tables>). ... Name the streamed table with its full catalog.schema.table, as STREAM(catalog.schema.table), or read it through a delta load action (see docs).`
- Fix: read in batch (`readMode: batch`, SQL without `STREAM()`, or a materialized view) or follow the change feed (recipe below); for message 2, write `STREAM(catalog.schema.table)` or use a `delta` load.
- Reported against the **reading pipeline with no flowgroup**: `lhp validate` shows it on the pipeline row (`✗ <pipeline>  LHP-VAL-072  Streaming read of a REPLACE WHERE table`); in the `--show-details` panel the context line names only the pipeline, while the message names each read and the write behind it as `pipeline.flowgroup.action`.
- Limits: Python code is not inspected (`spark.readStream` in a Python load/transform is not detected). **Filtered runs** (`-p/--pipeline`, sandbox profile pipeline scope) check only the selected pipelines: a read whose REPLACE WHERE producer is in an unselected pipeline is not reported, and `schema.table` reads are not checked at all — run a full `lhp validate` before deploy. Only tables LHP generates with `mode: replace_where` are known. `STREAM(IDENTIFIER(...))` is not detected.

## Mirror through the change feed

Enable CDF on the REPLACE WHERE write (adding it to an existing table does not reset it): `table_properties: {delta.enableChangeDataFeed: "true"}`. Then, typically in a downstream pipeline:

```yaml
- name: load_orders_recent_changes
  type: load
  readMode: stream
  source:
    type: delta
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: orders_recent              # the mode: replace_where table
    options:
      readChangeFeed: "true"          # exempt from LHP-VAL-072
    where_clause:
      - "_change_type != 'update_preimage'"
  target: v_orders_recent_changes

- name: write_orders_recent_mirror
  type: write
  source: v_orders_recent_changes
  write_target:
    type: streaming_table
    mode: cdc
    catalog: "${catalog}"
    schema: "${gold_schema}"
    table: orders_recent_mirror
    cdc_config:
      keys: ["order_id"]
      sequence_by: ["_commit_version", "_change_type"]   # REQUIRED tie-break
      scd_type: 1
      apply_as_deletes: "_change_type = 'delete'"
      except_column_list: ["_change_type", "_commit_version", "_commit_timestamp"]
```

- **Tie-break:** a rewritten key has a `delete` and an `insert` in the same commit. `sequence_by: "_commit_version"` alone kept the `delete` in every observed tie — keys silently vanished, no error. `["_commit_version", "_change_type"]` makes the `insert` win within a commit (`"insert"` > `"delete"`); `_commit_version` still orders commits.
- The `update_preimage` filter is insurance (no update images were observed; `"update_preimage"` > `"update_postimage"` would win the tie).
- Under `COMPLETE_REPLACE_WHERE` every refresh emits delete + insert for every row inside the predicate; the mirror stays correct.
- A raw change log (same load into a plain append streaming table) works; Databricks warns it cannot enable CDF on that table (reserved `_change_type` / `_commit_version` / `_commit_timestamp` columns).
- Tested on Databricks over several refreshes (rewrites, deletes, inserts, a narrowed predicate) with hand-written REPLACE WHERE producers and LHP-generated consumers: the mirror matched after every refresh. With an LHP-generated producer: one update. Not tested: resets, schema changes, CDF retention gaps, readers started from `startingVersion`.

## Backfills

One flow per target, so a backfill cannot be a second flow. Use Databricks' one-update predicate override: `replace_where_overrides` when starting the update, entries `{flow_name, predicate_override}`. `flow_name` is the table's full name (e.g. `dev_catalog.silver.orders_recent`). LHP does not generate overrides; later updates use the generated predicate. `replace_where_overrides` itself was not exercised in LHP's Databricks tests.
