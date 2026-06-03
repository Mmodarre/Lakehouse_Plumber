# Transform — data_quality (expectations)

`type: transform` with `transform_type: data_quality`. Applies expectation rules. Fields are flat on the action. Handler: `DataQualityTransformGenerator`.

## Options (flat on action)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `expectations_file` | string | required | YAML rules file. |
| `mode` | string | `dqe` | `dqe` or `quarantine` (`DQMode`). |
| `quarantine` | object | — | Required when `mode: quarantine`; fields `dlq_table` and `source_table` (both required). |
| `readMode` | string | `stream` | Must be `stream`. |

## Minimal YAML

```yaml
- name: dq_orders
  type: transform
  transform_type: data_quality
  source: v_orders
  expectations_file: "expectations/orders.yaml"
  mode: dqe
  readMode: stream
  target: v_orders_validated
```

## Key rules

- `expectations_file` is a **YAML** rules file. Each rule has a per-rule `action`: `warn` (default), `drop`, or `fail`.
- `mode: dqe` applies expectations inline; `mode: quarantine` routes violations to a DLQ — requires a `quarantine` block with `dlq_table` and `source_table`.
