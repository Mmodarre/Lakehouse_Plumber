# Test — completeness

`type: test`, `test_type: completeness`. Ensures specific columns are not null. Fields are flat on the action. Handler: `CompletenessTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view to check. |
| `required_columns` | list | required | Columns that must be non-null. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_complete
  type: test
  test_type: completeness
  source: v_orders_clean
  required_columns: [order_id, customer_id, order_date]
```
