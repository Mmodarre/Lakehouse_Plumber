# Write — sink (kafka)

`type: write`, `write_target.type: sink` with `sink_type: kafka`. Format is fixed `kafka`. Handler: `KafkaSinkWriteGenerator`.

## Options (under `write_target:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `bootstrap_servers` | string | — | Feeds `kafka.bootstrap.servers`. |
| `topic` | string | — | Feeds the `topic` option. |
| `sink_name` | string | — | Unique identifier. |
| `options` | dict | — | Extra `kafka.*` options via `KafkaOptionsValidator`. |
| `comment` | string | derived | — |

## Minimal YAML

```yaml
- name: write_orders_to_kafka_sink
  type: write
  source: v_orders_for_kafka
  write_target:
    type: sink
    sink_type: kafka
    sink_name: order_events_kafka
    bootstrap_servers: "${kafka_bootstrap_cluster}"
    topic: "acme.orders.fulfillment"
    options:
      kafka.security.protocol: "SASL_SSL"
      kafka.sasl.mechanism: "PLAIN"
      checkpointLocation: "/Volumes/${catalog}/_meta/checkpoints/orders_kafka"
```

## Key rules

- Source must carry a `value` column (and optionally `key`); create them in a transform before writing.
- For Azure Event Hubs over the Kafka protocol, see `actions-write-sink-eventhubs.md`.
