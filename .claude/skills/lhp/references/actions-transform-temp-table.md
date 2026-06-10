# Transform — temp_table

`type: transform` with `transform_type: temp_table`. Materializes an intermediate temp table; cleaned up when the pipeline completes. Fields are flat on the action. Handler: `TempTableTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `source` | string / dict | required | View-name string, or a dict with `view` / `source`. |
| `sql` | string | — | Optional; `{source}` placeholder is substituted. |
| `readMode` | string | `batch` | Read mode. |

## Minimal YAML

```yaml
- name: stage_orders
  type: transform
  transform_type: temp_table
  source: v_orders
  sql: "SELECT * FROM {source} WHERE status = 'open'"
  target: tmp_orders
```

## Key rules

- Without `sql`, it is a passthrough materialization of `source`.
- Use `stream({source})` in `sql` when `readMode: stream`.
- Useful for multi-step transforms where intermediate materialization improves performance.
