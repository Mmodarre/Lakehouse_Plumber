# Topic — Quality, quarantine and recycling

Outcome: choose row handling, configure one expectation and trace the recovery
path. Read [data quality](../references/actions-transform-data-quality.md) and
[quarantine contract and DDL](../references/quarantine.md). Pause each checkpoint.

## quality.1 — Locate one rule

Open `pipelines/02_silver/orders_clean.yaml`, its schema transformation and
`expectations/orders.yaml`. Explain that expectations use post-rename column names.
Compare warn/drop/fail on one rule. Activity: learner identifies the current rule
and predicts the treatment of a violating row. Pause.

## quality.2 — The learner changes row handling

In an isolated learning copy, let the learner change one rule's `action` and keep
the rest unchanged. Inspect their saved attempt, validate, and pause. After
readiness generate and locate the corresponding expectation decorator. Explain
that the code reveals configured handling; runtime metrics need execution.
Save the active substep and wait.

## quality.3 — Choose quarantine

Introduce clean rows → normal output and rejected rows → external DLQ inbox.
Explain operator correction/marking fixed → CDF → deduplicated outbox → revalidation
→ output. It does not automatically correct bad business data. Read the external
inbox/outbox DDL and triggered-execution requirements in the local reference.
Activity: learner locates where a correction is stored and where it is rechecked.
Pause; do not create workspace tables as part of this explanation.

## quality.4 — Configure and inspect the recovery path

Let the learner change the quality transform in their learning copy to
`mode: quarantine`, with `quarantine.dlq_table` and `quarantine.source_table`.
Choose consistent fully qualified labelled example names for generation, or real
approved names for execution. All rules become drop within this mode; explain any
coercion warning. Read and validate the saved attempt; pause. On readiness generate
and trace the clean, quarantine, recycle and final union paths. Save the substep.

## quality.5 — Understand replay and full refresh

Explain the insert-only MERGEs keyed by `_dlq_sk` and filtered to the action's
`_dlq_source_table`, and the external outbox ledger.
Retained tables support correction replay after resetting the pipeline; clearing
those tables discards that history. Full-refresh outcomes also depend on retained
source data and stable row identity, so do not promise blanket deduplication across
arbitrary source/rule changes. Activity: learner explains why keeping the outbox
matters. Do not run a full refresh to demonstrate this without their choice. Pause.

## quality.6 — Optional runtime recovery

Only if requested, create the required tables in the selected development target,
verify their required properties and run an isolated example with one known bad
row. Pause to inspect its failed rules. Have the learner correct the payload and
mark it fixed; pause before the next run. Inspect actual recycled results after
execution, allowing subsequent triggered runs as needed. Record observed evidence
and unresolved timing rather than inventing an exact count or completion. Pause.
