# Write — streaming_table (CDC, SCD Type 1/2)

`type: write`, `write_target.type: streaming_table` with `mode: cdc`. Requires `cdc_config`. Handler: `StreamingTableWriteGenerator` (validated by `CDCValidator`).

## Options (under `write_target.cdc_config:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `keys` | list[string] | required | Non-empty; natural business keys. |
| `sequence_by` | string / list[string] | — | Ordering column(s). |
| `scd_type` | int | — | `1` or `2`. |
| `ignore_null_updates` | bool | — | — |
| `apply_as_deletes` | string (expr) | — | — |
| `apply_as_truncates` | string (expr) | — | Not allowed with SCD Type 2. |
| `track_history_column_list` | list[string] | — | Mutually exclusive with `track_history_except_column_list`. |
| `track_history_except_column_list` | list[string] | — | Mutually exclusive with `track_history_column_list`. |
| `column_list` | list[string] | — | Mutually exclusive with `except_column_list`. |
| `except_column_list` | list[string] | — | Mutually exclusive with `column_list`. |

## Minimal YAML

```yaml
- name: write_customer_silver
  type: write
  source: v_customer_bronze
  write_target:
    type: streaming_table
    mode: cdc
    catalog: "${catalog}"
    schema: "${silver_schema}"
    table: customer_dim
    cdc_config:
      keys: ["customer_id"]
      sequence_by: "last_modified_dt"
      scd_type: 2
```

## Key rules

- `scd_type: 1` overwrites; `scd_type: 2` tracks history.
- `track_history_column_list` / `track_history_except_column_list` are mutually exclusive; so are `column_list` / `except_column_list`.
- `catalog` + `schema` + `table` (also accepts `database` as the catalog.schema combined form in older configs).
