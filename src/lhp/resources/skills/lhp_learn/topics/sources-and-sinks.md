# Topic — Other sources and sinks

Outcome: select and configure an adapter while distinguishing generation from
connectivity. Start with the learner's named source/destination; if unnamed, offer
two relevant options from this table. Read only the selected leaf. Pause each step.

| Source | Reference |
|---|---|
| Files / Auto Loader | [cloudfiles](../references/actions-load-cloudfiles.md) |
| Delta / CDF | [delta](../references/actions-load-delta.md) |
| SQL query | [SQL](../references/actions-load-sql.md) |
| Database | [JDBC](../references/actions-load-jdbc.md) |
| Event stream | [Kafka](../references/actions-load-kafka.md) |
| Python function | [Python](../references/actions-load-python.md) |
| Custom Spark source | [custom source](../references/actions-load-custom-datasource.md) |

| Sink | Reference |
|---|---|
| Delta | [Delta sink](../references/actions-write-sink-delta.md) |
| Kafka | [Kafka sink](../references/actions-write-sink-kafka.md) |
| Azure Event Hubs | [Event Hubs](../references/actions-write-sink-eventhubs.md) |
| Custom sink | [custom](../references/actions-write-sink-custom.md) |
| Microbatch function | [foreachbatch](../references/actions-write-sink-foreachbatch.md) |

## adapters.1 — Identify the data contract

Clarify batch/stream, format or schema, and source/destination role from the request.
Explain the selected adapter's LHP fields. Activity: learner locates required fields
in its minimal example. Secrets use references; unavailable values can be clearly
labelled examples for generation. Pause.

## adapters.2 — Connect the adapter

Use a separate learning flowgroup. Ask the learner to adapt the reference fragment
into a complete flow with a compatible source/transform/write and unique names.
For Kafka, explain binary key/value decoding or encoding where relevant. For a sink,
explain how external output differs from a managed Lakeflow table. They save the
YAML and any required module/config files. Pause without writing their answer.

## adapters.3 — Validate and inspect

Read the saved attempt and validate. Give targeted feedback and let the learner
repair errors. Pause. On readiness generate and locate the selected reader/writer,
options and secret lookups. Record the substep; generated code is not a connection
test. Pause.

## adapters.4 — Optional connectivity exercise

If the learner wants execution, verify actual tools, permissions, credentials and
source availability without printing secrets. Use a small approved dataset/target,
inspect real results, and report what remains unverified. Otherwise recap the
configuration boundary and return to the saved course checkpoint when requested.
Pause.
