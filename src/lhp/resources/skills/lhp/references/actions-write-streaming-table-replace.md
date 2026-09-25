# Write — streaming_table (REPLACE USING)

`type: write`, `write_target.type: streaming_table` with `mode: replace`. Requires `replace_config`. Handler: `StreamingTableWriteGenerator` (validated by `ReplaceFlowConfigValidator`). Emits `dp.create_streaming_table(...)` plus a single `@dp.replace_flow(...)`.

REPLACE USING is a **replace, not a merge**. For each `replace_using` key value in a batch it deletes that key's target rows and writes the batch's rows for that key in their place (`sequence_by` orders competing batches; highest wins); keys absent from a batch are untouched. `replace_using` is a **grouping key, not a unique primary key** — one key may own many rows and every row the source sends for it is kept (appended, never collapsed). Contrast `mode: cdc`, which merges change events onto a unique key into a single current row (or SCD 2 history). Use `replace` when the source resends complete row sets per changed key (partial snapshots); use `cdc` for per-row change deltas against a real primary key.

## Options (under `write_target.replace_config:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `replace_using` | list[string] | required | Non-empty key columns identifying rows to replace; emitted as `replace_using=`. |
| `sequence_by` | string | required | Exactly one ordering column (highest wins); emitted as `sequence_by=`. A list is **not** allowed (unlike `cdc_config`). |

## Minimal YAML

```yaml
- name: write_orders_current
  type: write
  source: v_order_updates
  write_target:
    type: streaming_table
    mode: replace
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: orders_current
    replace_config:
      replace_using: ["order_id"]
      sequence_by: "updated_at"
```

## Key rules

- Source must be a **streaming** read; `readMode: batch` is rejected (the flow reads `spark.readStream`).
- `create_table` is forced `true` — LHP always emits `dp.create_streaming_table` for the target.
- The target must be served by this **single** replace flow: a `replace` action cannot share its `catalog.schema.table` with any other write action (no fan-in, no combining with other modes).
- `replace_using` is a non-empty list; `sequence_by` is a single string column.
- `catalog` + `schema` + `table` (also accepts `database` as the combined `catalog.schema` form in older configs).
- Future sibling (not yet implemented): `mode: replace_where` with a `replace_where_config.replace_where` predicate — the batch-read counterpart, mirroring `cdc` → `snapshot_cdc`.
