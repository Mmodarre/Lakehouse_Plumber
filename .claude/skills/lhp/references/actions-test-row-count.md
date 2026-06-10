# Test — row_count

`type: test`, `test_type: row_count`. Compares record counts between two sources. Fields are flat on the action. Handler: `RowCountTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | list | required | Exactly two tables to compare. |
| `tolerance` | int | `0` | Allowed absolute difference in row counts. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_row_count
  type: test
  test_type: row_count
  source: [v_orders_raw, v_orders_clean]
  tolerance: 0
```
