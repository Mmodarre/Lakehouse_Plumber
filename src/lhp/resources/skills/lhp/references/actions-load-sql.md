# Load — sql

`type: load` with `source.type: sql`. Materializes a SQL query into a temporary view. Handler: `SQLLoadGenerator`.

`source:` must be a mapping with `type: sql` and exactly one of `sql` or `sql_path`. The public validation path rejects a scalar SQL source.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `type` | string | required | Must be `sql`. |
| `sql` | string | — | Inline SQL query (one of `sql` / `sql_path`). |
| `sql_path` | string | — | External SQL file (one of `sql` / `sql_path`). |

## Minimal YAML

```yaml
- name: load_summary
  type: load
  source:
    type: sql
    sql: "SELECT * FROM ${catalog}.${bronze_schema}.orders"
  target: v_orders
```

## Key rules

- Exactly one of `sql` / `sql_path`.
- Substitution variables work in both inline SQL and external SQL files (`${token}`, `${secret:scope/key}`).
