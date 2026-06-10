# Write — sink (delta)

`type: write`, `write_target.type: sink` with `sink_type: delta`. Format is fixed `delta`. Handler: `DeltaSinkWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `sink_name` | string | — | Unique identifier. |
| `options` | dict | `{}` | Sink options (for example `tableName`). |
| `comment` | string | derived | — |

## Minimal YAML

```yaml
- name: write_orders_to_delta_sink
  type: write
  source: v_orders
  write_target:
    type: sink
    sink_type: delta
    sink_name: orders_delta_sink
    options:
      tableName: "${catalog}.${gold_schema}.orders_export"
```

## Key rules

- In `options`, `tableName` and `path` are mutually exclusive — use one.
- `mergeSchema: "true"` and other Delta writer options pass through `options`.
