# Topic — Choose a write strategy

Outcome: match the source's meaning to an output strategy. Read [standard](../references/actions-write-streaming-table-standard.md),
[replace](../references/actions-write-streaming-table-replace.md) and
[materialized views](../references/actions-write-materialized-view.md). For event
merging or full snapshots, also use [CDC](cdc.md). Pause at each checkpoint.

## writes.1 — State the source contract

Use three labelled examples: appended order events, complete line sets resent for
selected order IDs, and a revenue aggregation. Explain streaming append, REPLACE
USING and materialized-view purposes. Activity: learner selects a strategy for
one example and explains the source assumption. Give feedback and pause.

## writes.2 — Inspect an existing write

Open a sample ingestion write and `pipelines/03_gold/sales_by_nation.yaml` with its
SQL. Compare the source view path with the materialized view's SQL path. Activity:
locate the target type and query definition. Do not force every write to have an
action-level source; supported subtypes differ. Pause.

## writes.3 — Configure a replacement example

Use an isolated example with a streaming source containing an order/group key and
ordering column. Let the learner adapt the replace reference fragment into its
containing flowgroup: `mode: replace`, `replace_config.replace_using`, and
`sequence_by`. Use a distinct target, served by this single write. Explain that
matching key groups are replaced, while missing groups stay; it is not per-row
CDC or an entire-table overwrite. Activity: learner edits and predicts the result.
Pause. Avoid presenting a fragment as a complete runnable project.

## writes.4 — Verify the chosen contract

Read the saved attempt and validate with the project's actual environment and
pipeline config. Pause. After readiness generate, locate `replace_flow` and its
parameters, and compare with the append or materialized-view output. No local
command proves replacement happened on data. If execution is requested, verify
runtime support first and use isolated data. Save the current substep and pause.
