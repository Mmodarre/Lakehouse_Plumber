# Test — range

`type: test`, `test_type: range`. Validates a column falls within min/max bounds. Fields are flat on the action. Handler: `RangeTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view to check. |
| `column` | string | required | Column whose values are range-checked. |
| `min_value` | number | — | Inclusive lower bound; at least one of `min_value` / `max_value` required. |
| `max_value` | number | — | Inclusive upper bound; at least one of `min_value` / `max_value` required. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_amount_range
  type: test
  test_type: range
  source: v_orders_clean
  column: amount
  min_value: 0
```
