# Write — sink (Azure Event Hubs, via Kafka sink)

Azure Event Hubs is targeted **through the Kafka sink** (`type: write`, `write_target.type: sink`, `sink_type: kafka`) via its Kafka-protocol-compatible endpoint. There is no dedicated Event Hubs handler — the backing handler is `KafkaSinkWriteGenerator`. Event Hubs mode is detected when `options.kafka.sasl.mechanism` is `OAUTHBEARER` (`kafka_sink.py:is_event_hubs`).

## Options (under `write_target:`, scoped to the Event Hubs connection shape)

| Key | Type | Default | Event Hubs connection shape |
|-----|------|---------|------------------------------|
| `bootstrap_servers` | string | — | `<namespace>.servicebus.windows.net:9093`. |
| `topic` | string | — | Event Hub name. |
| `sink_name` | string | — | Unique identifier. |
| `options` | dict | — | Set `kafka.sasl.mechanism: OAUTHBEARER` (triggers Event Hubs mode), plus `kafka.security.protocol`, `kafka.sasl.jaas.config`, and other `kafka.*` options via `KafkaOptionsValidator`. |
| `comment` | string | `Event Hubs sink to {topic}` when EH detected | — |

## Minimal YAML

```yaml
- name: write_orders_to_eventhubs
  type: write
  source: v_orders_for_eventhubs
  write_target:
    type: sink
    sink_type: kafka
    sink_name: order_events_eventhubs
    bootstrap_servers: "${eh_namespace}.servicebus.windows.net:9093"
    topic: "acme-orders"
    options:
      kafka.security.protocol: "SASL_SSL"
      kafka.sasl.mechanism: "OAUTHBEARER"
      kafka.sasl.jaas.config: "kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;"
      checkpointLocation: "/Volumes/${catalog}/_meta/checkpoints/orders_eventhubs"
```

## Key rules

- Same options surface as the Kafka sink; only the connection shape differs (namespace endpoint on port 9093, Event Hub name as `topic`, SASL/OAUTHBEARER auth).
- `OAUTHBEARER` is what flips the generator into Event Hubs mode (sets the EH-style derived comment); no EH-only YAML keys exist.
