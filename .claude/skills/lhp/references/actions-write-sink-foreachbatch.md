# Write — sink (foreachbatch)

`type: write`, `write_target.type: sink` with `sink_type: foreachbatch`. Runs custom Python per micro-batch (merges, multi-destination writes, complex upserts). Requires exactly one of `module_path` / `batch_handler`; `source` must be a single view string. Handler: `ForEachBatchSinkWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `sink_name` | string | required | Unique identifier. |
| `module_path` | string | one-of | External handler file (one of `module_path` / `batch_handler`). |
| `batch_handler` | string | one-of | Inline handler body (one of `module_path` / `batch_handler`). |
| `comment` | string | derived | — |

## Minimal YAML

```yaml
- name: merge_customer_updates
  type: write
  source: v_customer_changes
  write_target:
    type: sink
    sink_type: foreachbatch
    sink_name: customer_merge_sink
    batch_handler: |
      df.createOrReplaceTempView("batch_view")
      df.sparkSession.sql("""
          MERGE INTO ${catalog}.${gold_schema}.dim_customer AS tgt
          USING batch_view AS src
          ON tgt.customer_id = src.customer_id
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
      """)
```

## Key rules

- Exactly one of `module_path` / `batch_handler`.
- `source` must be a single view string (not a list).
- Local helper imports (external `module_path`) follow the standard rules (`LHP-VAL-023/024/025`, `LHP-IO-003`).
