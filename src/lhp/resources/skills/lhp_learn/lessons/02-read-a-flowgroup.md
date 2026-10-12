# Lesson 2 — Read one flowgroup

Prerequisite: accessible project files. Outcome: trace data through actions and
distinguish a view from a table. Read [Delta load](../references/actions-load-delta.md)
and [standard writes](../references/actions-write-streaming-table-standard.md).
For the sample use `pipelines/01_ingest/nation_region.yaml`; focus on the nation
pair first. Each checkpoint ends with a pause.

## 2.1 — Find the boundaries

Ask the learner to open the file and locate `pipeline`, `flowgroup` and `actions`.
Explain their values using the Lesson 0 hierarchy. Point out the preset and
metadata names only as settings we will revisit. A flowgroup can contain more
than one load/write pair; don't equate a file with exactly one table.
Activity: identify the pipeline and flowgroup names in the actual file. Pause.

## 2.2 — Follow the load

Read only `load_nation`. Follow `source.type`, catalog, schema, table and read mode,
then its `target: v_nation`. Explain that the named view is an intermediate input
for another action; it is not the persisted destination table.
Activity: find the source table and the view name without changing the file. Pause.

## 2.3 — Follow the write

Read `write_nation`. Match its `source` to the load's target, then locate
`write_target.type`, catalog, schema and table. Briefly identify `${...}` as values
supplied by the environment, leaving resolution to the next lesson.
Activity: trace source table → named view → destination table in their own file.
Answer confusion about `source` on a load versus a write with those two snippets.
Pause; do not generate yet.

## 2.4 — Recognise the repeated pattern

Ask the learner to inspect the region pair using the same approach. Offer a hint
if needed; do not answer both locations before they have looked. Explain that
these connections also feed dependency analysis and runtime scheduling.
Recap the distinction between views and tables, record the inspection and wait
before [Lesson 3](03-understand-configuration.md).
