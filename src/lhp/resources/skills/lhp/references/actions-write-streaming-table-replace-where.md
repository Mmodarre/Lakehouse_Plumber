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
- Source must be a **batch** read of a **single** view (a string or a one-element list; more than one source view is rejected); `readMode: stream` on the write is rejected (Databricks: `REPLACE_WHERE_WITH_STREAMING_SOURCE`).
- The table is always created by this action: `create_table: false` is rejected, and the target must be served by this **single** flow — no other write action may share its `catalog.schema.table` (no fan-in, no combining with other modes).
- `temporary` is rejected (use `private: true` for a pipeline-private table).
- Action-level `expectations` are rejected — Databricks does not support expectations on a REPLACE WHERE target, so no `@dp.expect*` decorator is ever emitted.
- An unknown mode such as `replace-where` fails with LHP-VAL-006 and a did-you-mean of `replace_where`.
- `catalog` + `schema` + `table` (also accepts `database` as the combined `catalog.schema` form in older configs).
