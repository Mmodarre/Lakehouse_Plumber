# Test — custom_sql

`type: test`, `test_type: custom_sql`. Runs a SQL query whose returned rows represent violations. Fields are flat on the action. Handler: `CustomSqlTestGenerator`. Tests run only with `lhp generate --include-tests`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `sql` | string | required | Query whose returned rows represent violations. |
| `source` | string | — | Table or view referenced by the query; one of `source` or `sql` must be present. |
| `on_violation` | string | `fail` | `fail`, `warn`, or `drop`; invalid values coerced to `fail`. |

## Minimal YAML

```yaml
- name: orders_negative_amount
  type: test
  test_type: custom_sql
  source: v_orders_clean
  sql: "SELECT * FROM v_orders_clean WHERE amount < 0"
```

## Key rules

- Optional `expectations` list may attach named expectations with their own `on_violation`.
