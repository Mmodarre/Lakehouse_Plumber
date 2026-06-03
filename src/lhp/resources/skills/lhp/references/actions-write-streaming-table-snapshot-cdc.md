# Write — streaming_table (snapshot CDC)

`type: write`, `write_target.type: streaming_table` with `mode: snapshot_cdc`. Full-snapshot CDC via `create_auto_cdc_from_snapshot_flow()`. Requires `snapshot_cdc_config`. `create_table` is forced `true` in this mode. Handler: `StreamingTableWriteGenerator` (validated by `SnapshotCDCValidator`).

## Options (under `write_target.snapshot_cdc_config:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | one-of | Exactly one of `source` / `source_function`. |
| `source_function` | dict | one-of | `{file, function, parameters?}`; `file` and `function` required. |
| `keys` | list[string] | required | Non-empty. |
| `stored_as_scd_type` | int | — | `1` or `2`. |
| `track_history_column_list` | list[string] | — | Mutually exclusive with `track_history_except_column_list`. |
| `track_history_except_column_list` | list[string] | — | Mutually exclusive with `track_history_column_list`. |

## Minimal YAML

```yaml
- name: write_customer_snapshot
  type: write
  write_target:
    type: streaming_table
    mode: snapshot_cdc
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: customer_dim
    snapshot_cdc_config:
      source: "${catalog}.${bronze_schema}.customer_snapshot"
      keys: ["customer_id"]
      stored_as_scd_type: 2
```

## Key rules

- `source` and `source_function` are mutually exclusive.
- With `source_function`, the action is **self-contained** — no action-level `source` field needed, and it is exempt from the "must have a Load action" requirement. `source_function.file` is resolved relative to project root.
- Local helper imports (snapshot `source_function`) follow the standard rules: `LHP-VAL-023/024/025`, `LHP-IO-003`.
