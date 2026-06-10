# Test — referential_integrity

`type: test`, `test_type: referential_integrity`. Checks foreign-key relationships between tables. Fields are flat on the action. Handler: `ReferentialIntegrityTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view holding the foreign-key values. |
| `reference` | string | required | Three-part name (`catalog.schema.table`) of the referenced table. |
| `source_columns` | list | required | Foreign-key columns in `source`; equal length to `reference_columns`. |
| `reference_columns` | list | required | Key columns in `reference`; equal length to `source_columns`. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_customer_fk
  type: test
  test_type: referential_integrity
  source: v_orders_clean
  reference: main.sales.customers
  source_columns: [customer_id]
  reference_columns: [id]
```
