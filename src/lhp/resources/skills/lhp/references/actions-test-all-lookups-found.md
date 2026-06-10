# Test — all_lookups_found

`type: test`, `test_type: all_lookups_found`. Validates that dimension lookups succeed (surrogate keys present after joins). Fields are flat on the action. Handler: `AllLookupsFoundTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view holding the lookup keys. |
| `lookup_table` | string | required | Three-part name (`catalog.schema.table`) of the lookup table. |
| `lookup_columns` | list | required | Key columns in `source`; equal length to `lookup_result_columns`. |
| `lookup_result_columns` | list | required | Matching columns in `lookup_table`; equal length to `lookup_columns`. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_product_lookup
  type: test
  test_type: all_lookups_found
  source: v_orders_clean
  lookup_table: main.sales.products
  lookup_columns: [product_id]
  lookup_result_columns: [id]
```
