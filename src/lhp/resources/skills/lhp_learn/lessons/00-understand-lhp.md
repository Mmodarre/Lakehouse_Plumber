# Lesson 0 — Understand LHP and its capabilities

Outcome: explain LHP's role, recognise its building blocks and know which feature
addresses a need. No project scaffold or execution is needed. Teach one checkpoint
at a time and wait after each; the capability groups below are separate checkpoints,
not a catalogue to deliver in one message. Keep each capability to one short
sentence in the learner-facing overview. Expand only when the learner asks.
Use a simple orders story throughout. No prerequisite quiz on Databricks or SQL.

## 0.1 — Why LHP exists

Explain that LHP is a YAML-driven ETL framework that generates Databricks Lakeflow
Spark Declarative Pipelines. One orders pipeline is manageable by hand; many similar
tables across environments repeat the same plumbing. LHP lets the author express
patterns and settings consistently and inspect the resulting Python.
Activity: show the same source → clean → table story for orders and customers;
invite the learner to notice which parts repeat. Pause for readiness.

## 0.2 — How it works

Show this text diagram (render it visually if supported):

    YAML + environment values → LHP validation/generation
                              → Lakeflow Python + bundle resources
                              → deploy → execute on Databricks → inspect results

LHP runs during generation; Databricks executes the generated pipelines. Explain
that validation checks configuration, generation writes code, and neither proves
data was processed. Ordinary generated pipelines do not need LHP as a runtime
interpreter; their sources and custom code may still have runtime dependencies.
YAML is the maintained source when continuing to regenerate. Activity: ask the
learner to locate where data processing happens in the diagram. Pause.

## 0.3 — What the pieces mean

- Project: the files and settings that describe related pipelines.
- Pipeline: the grouping that becomes a Lakeflow pipeline deployment resource.
- Flowgroup: a named group of related actions, often for one entity.
- Action: a load, transform, write or test operation within a flowgroup.

Sketch project → ingestion pipeline → orders flowgroup → load/transform/write.
Add test as an assertion on data, without implying it must reshape the chain.
A named view connects work within a pipeline; a persisted table can connect
pipelines. Dependencies determine execution, not YAML's position as a script.
Activity: have the learner identify the flowgroup and the action in this story.
Pause; keep schema keys for the later file lesson.

## 0.4a — Bring data in

- Auto Loader: incrementally ingest files with configurable formats, schema hints and reader options.
- Delta: read tables in batch or streaming mode, including Change Data Feed and supported time-travel options.
- SQL sources: load data through inline queries or SQL files with explicit input dependencies where needed.
- JDBC: ingest from databases using connection, query and partition options.
- Kafka: consume event streams with configurable subscriptions and source options.
- Python sources: call a Python function to produce data using logic specific to the source.
- Custom data sources: integrate a supported custom Spark data source and its options.

Give one orders example for files and one for change events; ask which resembles
the learner's work, allowing 'continue' with no project interview. Pause.

## 0.4b — Shape and check data

- SQL transforms: filter, join and aggregate data through inline SQL or external SQL files.
- Python transforms: apply a reusable Python function when the transformation needs custom code.
- Schema transforms: rename, cast and enforce columns using a shared schema definition.
- Temporary tables: materialize an intermediate pipeline result for downstream actions.
- Data quality: apply named expectations with warn, drop or fail behaviour.
- Quarantine: retain rejected rows, recycle operator-corrected records automatically, and preserve a deduplicated recovery ledger for replay after a full refresh.
- Data tests: check row counts, uniqueness, relationships, completeness, ranges, schemas, lookups and custom assertions.
- Test reporting: send test results through a custom provider to a Delta audit table or an external system.

For quarantine, explain if asked: recovery needs user-managed inbox/outbox tables,
CDF and row tracking, corrected rows marked fixed, and supported triggered execution.
Full-refresh recovery depends on retaining those external tables and replayable
source data; it is not a guarantee for arbitrary deletion or changing source history.
See [quarantine](../references/quarantine.md). Do not claim to have tested runtime
recovery from generation alone. Activity: contrast dropping a bad row with keeping
it for correction. Pause.

## 0.4c — Choose the output behaviour

- Streaming tables: maintain incremental outputs from one or more append flows.
- CDC: apply ordered changes and deletes using keys, with current-state or historical dimension handling.
- Snapshot CDC: derive changes from successive full snapshots using a table or Python snapshot source.
- Replacement writes: replace matching key groups from an incoming stream using REPLACE USING.
- Materialized views: maintain query results such as joins and business aggregates.
- Sinks: write through Delta, Kafka/Event Hubs, custom sinks or foreach-batch code.
- Table settings: configure supported properties, comments, partitioning and clustering for generated targets.

Use an order event stream, a customer history table and a revenue summary to give
these names a purpose. Activity: let the learner point to the relevant output for
one of those needs. Pause; defer detailed runtime semantics to the write lessons.

## 0.5a — Reuse and configure

- Presets: share action defaults while keeping each flowgroup's own structure.
- Templates: instantiate a repeated flowgroup pattern with different parameters.
- Blueprints: expand parameterized groups of flowgroups across tenants, regions or source systems.
- Multiple flowgroups: keep related definitions together while generating their individual pipeline files.
- Environments: resolve catalog, schema, path and other settings without duplicating pipeline logic.
- Local variables: reuse values within a flowgroup to keep names and expressions consistent.
- Secret references: generate runtime secret lookups without embedding credentials in the YAML.
- Operational metadata: add consistent processing timestamps and source metadata to data flows.
- Unity Catalog tags: apply configured table and column tags to supported generated assets.

Activity: match shared settings, a repeated table pattern and a regional rollout
to preset, template and blueprint. Offer an explanation rather than a scored quiz.
Pause. Only introduce placeholder syntax later when opening actual files.

## 0.5b — Develop, ship and operate

- Scaffolding: create an empty project or a complete bundled sample to explore.
- Validation: detect configuration, reference and dependency problems before generating code.
- Generation and diff: produce readable Python and inspect proposed changes before writing them.
- Dependency analysis: inspect how actions, flowgroups and pipelines depend on each other.
- Bundle integration: generate pipeline deployment resources alongside the project's Databricks bundle.
- Jobs: create orchestration definitions from pipeline dependencies and job configuration.
- Pipeline wheels: package supported generated pipeline code as versioned Python artifacts.
- CI workflows: automate validation, generation and deployment with the project's chosen CI system.
- Monitoring: consolidate configured event logs into generated monitoring pipelines and views.
- Personal sandboxes: generate a scoped, namespaced set of outputs while reading shared upstream data.
- LHP Web: optionally inspect graphs, edit forms and validate or generate from a local browser interface.
- Coding-agent support: install versioned offline references for development or this separate guided course.
- Python API: invoke supported LHP operations from Python tools and notebooks as well as the CLI.
- Diagnostics: use structured error codes and project inspection commands to investigate problems.
- Telemetry controls: inspect or disable LHP's anonymous usage reporting through its CLI settings.

These are the capabilities of this release, not promises that every integration
is already configured in the learner's environment. Activity: identify one useful
future topic and record it without starting that topic yet. Pause.

## 0.6 — The learning journey

Show the sample's ingestion → silver cleansing/dimensions → gold sales summary.
Explain that we will open files, follow one simple flow, configure an environment,
validate, inspect generated Python and make learner-authored changes. Deployment
is a separate choice; advanced features have their own lessons. LHP Web is optional.
Activity: invite any remaining concept question. Save progress and wait for readiness
before [Lesson 1](01-meet-your-project.md). Do not claim the sample covers every
capability: it does not exercise all sources, sinks, tests, blueprints or quarantine.
