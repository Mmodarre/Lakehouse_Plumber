# Load — sql

`type: load` with `source.type: sql`. Materializes a SQL query into a temporary view. Handler: `SQLLoadGenerator`.

`source:` may be an inline SQL string, or a dict carrying `sql` or `sql_path`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string / dict | — | Inline SQL string, or a dict with `sql` or `sql_path`. |
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
