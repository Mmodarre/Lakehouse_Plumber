# Transform — sql

`type: transform` with `transform_type: sql`. Fields are flat on the action. Requires exactly one of `sql` or `sql_path`. Handler: `SQLTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string / list | — | Input view(s). |
| `sql` | string | — | Inline SQL (one of `sql` / `sql_path`). |
| `sql_path` | string | — | External SQL file (one of `sql` / `sql_path`). |

## Minimal YAML

```yaml
- name: transform_orders
  type: transform
  transform_type: sql
  source: v_orders_raw
  sql: "SELECT * FROM v_orders_raw WHERE amount > 0"
  target: v_orders
```

## Key rules

- Use `stream(view_name)` in SQL for streaming sources; without it the query runs in batch mode.
- Substitution variables work in inline SQL and external files (`${token}`, `${secret:scope/key}`); files are processed for substitutions before execution.
