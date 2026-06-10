# Load — jdbc

`type: load` with `source.type: jdbc`. Reads from an external database over JDBC. Handler: `JDBCLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `url` | string | — | JDBC URL. |
| `user` | string | — | Secret-substitutable. |
| `password` | string | — | Secret-substitutable. |
| `driver` | string | — | JDBC driver class. |
| `table` | string | `unknown_table` | One of `table` / `query`. |
| `query` | string | — | One of `table` / `query`. |

## Minimal YAML

```yaml
- name: load_jdbc
  type: load
  source:
    type: jdbc
    url: "jdbc:postgresql://host:5432/db"
    user: "${secret:db/user}"
    password: "${secret:db/password}"
    driver: org.postgresql.Driver
    table: public.orders
  target: v_orders
```

## Key rules

- `query` and `table` are mutually exclusive — use one.
- Always supply credentials via `${secret:scope/key}`; never inline.
