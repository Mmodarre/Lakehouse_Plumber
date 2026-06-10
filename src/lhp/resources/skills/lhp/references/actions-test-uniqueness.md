# Test — uniqueness

`type: test`, `test_type: uniqueness`. Validates a unique constraint over one or more columns. Fields are flat on the action. Handler: `UniquenessTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string | required | Table or view to check. |
| `columns` | list | required | Columns whose combined value must be unique. |
| `filter` | string | — | WHERE-clause predicate restricting rows checked. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_unique
  type: test
  test_type: uniqueness
  source: v_orders_clean
  columns: [order_id]
```

## Key rules

- Use `filter` for SCD2 active rows (for example `"__END_AT IS NULL"`).
