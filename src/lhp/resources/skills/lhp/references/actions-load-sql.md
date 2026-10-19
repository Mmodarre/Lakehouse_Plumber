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
- Substitution variables work in both inline SQL and external SQL files (`${token}`, `${secret:scope/key}`). A plain `${name}` still in the `sql_path` file after substitution → `LHP-CFG-010` from `lhp validate` and `lhp generate`; dotted/prefixed Spark variables (`${mypipeline.start}`, `${hiveconf:x}`) are left as written. A pipeline `configuration:` parameter read at runtime needs a dotted key (`mypipeline.start_date` → `${mypipeline.start_date}`); a plain `${start_date}` fails even when the configuration defines it. In a template action, a `sql_path` file containing `{{`, `{%` or `{#` is first rendered with the template parameters at template expansion (strict; `LHP-CFG-071`) — see templates-presets.md.
