# Load — kafka

`type: load` with `source.type: kafka`. Streams from Apache Kafka (or Kafka-protocol-compatible endpoints) into a temporary view. Handler: `KafkaLoadGenerator`.

## Options (under `source:`)

| Key | Type | Default | Accepted / constraints |
|-----|------|---------|------------------------|
| `bootstrap_servers` | string | required | Maps to `kafka.bootstrap.servers`. |
| `subscribe` | string | — | Exactly one of `subscribe` / `subscribePattern` / `assign`. |
| `subscribePattern` | string | — | Topic regex; one of the subscription methods. |
| `assign` | string (JSON) | — | Partitions; one of the subscription methods. |
| `readMode` | string | `stream` | Must be `stream`. |
| `options` | dict | — | Extra `kafka.*` options. |

## Minimal YAML

```yaml
- name: load_events
  type: load
  source:
    type: kafka
    bootstrap_servers: "broker1:9092,broker2:9092"
    subscribe: orders
    readMode: stream
  target: v_events
```

## Key rules

- Exactly one subscription method (`subscribe` / `subscribePattern` / `assign`).
- Kafka returns binary `key`/`value` columns — deserialize in a transform action.
- **AWS MSK IAM:** `kafka.security.protocol: SASL_SSL`, `kafka.sasl.mechanism: AWS_MSK_IAM`, plus the MSK IAM login module / callback handler in `kafka.sasl.jaas.config`; port 9098; IAM role needs `kafka-cluster:*`.
- **Azure Event Hubs (OAuth):** bootstrap `<namespace>.servicebus.windows.net:9093`, `kafka.sasl.mechanism: OAUTHBEARER` with the OAuthBearer login module + token endpoint; port 9093; SP needs the "Azure Event Hubs Data Receiver" role.
