# Topic — CDC and SCD2

Outcome: choose change events versus snapshots and trace keys, ordering, deletes
and history into generated code. Read [CDC patterns](../references/cdc-patterns.md),
[CDC writes](../references/actions-write-streaming-table-cdc.md) and
[snapshot CDC](../references/actions-write-streaming-table-snapshot-cdc.md).
Use the sample's customer and supplier dimensions; wait after every checkpoint.

## cdc.1 — Understand the incoming changes

Open `pipelines/02_silver/dim_customer.yaml`. Trace the Delta CDF option and the
filter excluding update preimages. Explain why before/after images at the same
key and sequence need care. Activity: learner identifies insert/update/delete
information and the sequence column in the actual file. Pause.

## cdc.2 — Read the history contract

Trace `cdc_config.keys`, `sequence_by`, `scd_type`, `apply_as_deletes` and excluded
CDF columns. Explain current state versus SCD2 with two small illustrative versions
of a customer; label these examples, not query results. Activity: learner predicts
which values identify a customer and which order their changes. Pause.

## cdc.3 — Make one history choice

In an isolated learning copy, ask the learner to change this target to SCD1 using
the documented `scd_type` setting and a distinct learning destination. They save
the edit; you inspect it and validate. Pause before generating. After readiness,
generate and inspect the actual CDC call and the changed history setting. Record
these as edit/validation and generation substeps. Do not deploy over the SCD2 demo.

## cdc.4 — Contrast a snapshot source

Open `dim_supplier.yaml` and `functions/supplier_snapshot.py`. Explain the
(DataFrame, version) contract, its parameters and `stored_as_scd_type`. Unlike the
CDF example, this write has no upstream action-level source. Activity: learner
finds the version progression and the tracked history columns. Pause.

## cdc.5 — Observe or explain history

If the learner chooses runtime work and the sample is deployed, inspect the two
actual dimension tables before and after a separately requested demo update.
Otherwise use the generated calls and labelled examples. Discuss deletes and
history columns, record exactly what was observed, and ask whether to return to
the course or choose another topic. Pause.
