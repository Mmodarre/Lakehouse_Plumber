=================================================
Quarantine bad rows instead of dropping them
=================================================

.. meta::
   :description: Keep failed rows in a dead-letter table instead of dropping them — declare mode: quarantine on a Lakehouse Plumber data_quality transform and LHP generates the whole DLQ recycling subsystem: sink, inverse-filter routing, CDF recycle flow, and the clean-plus-recycled UNION view.

Let's take a stream of raw order rows where some fail your expectations — a null
``order_id``, a negative ``amount`` — and instead of throwing those rows away,
keep them. Route each failed row into a dead-letter table you can query, fix in
place, and recycle back into the pipeline on the next run. The clean rows never
stop flowing.

The Get Started course drops bad rows with ``mode: dqe``: one
``@dp.expect_all_or_drop`` decorator, and a row that fails is gone. That is the
right default when bad data is noise. But when a failed row is a customer order
you cannot silently lose — because someone has to investigate it, backfill it,
and get it into the table — dropping is the wrong verb. You want to *quarantine*
it.

You could hand-write the machinery for that: a ``foreachBatch`` sink that
``MERGE``\s failed rows into a Delta table on a deterministic key so retries do
not double-insert; an inverse filter that selects exactly the rows that failed
*any* rule; a Change Data Feed reader that notices when an operator marks a row
fixed; a windowed dedup so a row edited twice is ingested once; and a
VARIANT-to-columns reconstruction that re-validates recycled rows before they
rejoin the stream. Or you declare ``mode: quarantine`` with a two-field
``quarantine`` block, and let Lakehouse Plumber write all of it. That is the idea
on every page: **declare your ETL, don't hand-write it.**

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for ``lhp
init``) and a streaming source of rows — quarantine generates streaming code, so
the ``data_quality`` action runs with ``readMode: stream``. This guide reuses the
orders ingest from the course: an Auto Loader ``load`` that streams raw CSV into
``v_orders_raw``.

Quarantine also depends on two tables that you create yourself, not Lakehouse
Plumber. The generated code reads and writes them by name; it does not run their
DDL:

- The **DLQ table** (the inbox) named in ``dlq_table``. It must have Change Data
  Feed and row tracking enabled — ``delta.enableChangeDataFeed = 'true'`` — or
  the recycle stream fails at run time.
- The **outbox table**, whose name Lakehouse Plumber derives by appending
  ``_outbox`` to ``dlq_table``. No YAML configures it.

The exact column schema for both tables, and the one-time ``CREATE TABLE`` DDL,
live in the Quarantine reference. Create them once per catalog and schema before
the pipeline runs.

Declare the quarantine
======================

Take the ``data_quality`` action and change one field. Where drop mode sets
``mode: dqe`` (or omits ``mode`` entirely), quarantine mode sets ``mode:
quarantine`` and adds a ``quarantine`` block. That block has exactly two required
fields — ``dlq_table`` and ``source_table``:

.. literalinclude:: ../../_fixtures/guide_qo_quarantine/pipelines/bronze_orders.yaml
   :language: yaml
   :caption: pipelines/bronze_orders.yaml
   :emphasize-lines: 25-28

The two fields carry the whole contract:

- ``dlq_table`` — the fully-qualified dead-letter table failed rows are written
  to. Here ``${catalog}.${bronze_schema}.orders_dlq``, which resolves per
  environment from ``substitutions/dev.yaml``.
- ``source_table`` — the fully-qualified name this flowgroup tags its rows with
  in the DLQ. It is written into the ``_dlq_source_table`` column and used to
  filter the recycle stream, so one shared DLQ table can serve many flowgroups.
  Point it at the table the write action targets: ``${catalog}.${bronze_schema}.orders``.

The expectations file is the same YAML you would write for drop mode:

.. literalinclude:: ../../_fixtures/guide_qo_quarantine/expectations/orders_quality.yaml
   :language: yaml
   :caption: expectations/orders_quality.yaml

.. important::

   In quarantine mode every expectation is coerced to ``drop``, whatever each
   rule's ``action`` says. A row that fails any rule is held out of the clean
   stream and routed to the DLQ. If a rule is written as ``warn`` or ``fail``,
   ``lhp validate`` emits a warning telling you the action was coerced — the
   per-rule ``warn`` and ``fail`` semantics do not apply here. Both rules above
   are already ``drop``, so there is nothing to coerce.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  1 file
   ✓ generate (0.33s)
   ✓ format (0.06s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` enforces the symmetry between the two sides: ``mode: quarantine``
requires the ``quarantine`` block, and a ``quarantine`` block without ``mode:
quarantine`` is rejected. It also fails an empty expectations file, because with
no rules the inverse filter that selects failed rows would be meaningless.

Read the dead-letter subsystem
==============================

Open ``generated/dev/bronze_orders/orders_quarantine.py``. This is the entire
output — nothing is hidden behind a runtime. The ``load`` view and the ``write``
target at the ends are ordinary; everything between them is the dead-letter
subsystem that the one ``mode: quarantine`` switch generated:

.. literalinclude:: ../../_fixtures/guide_qo_quarantine/generated/dev/bronze_orders/orders_quarantine.py
   :language: python
   :caption: generated/dev/bronze_orders/orders_quarantine.py
   :emphasize-lines: 44-49, 87-91, 185-195

Read it top to bottom and the inbox/outbox design is all there:

- **Rules and constants.** ``_EXPECTATIONS_v_orders_raw`` holds every rule as a
  drop. ``_INVERSE_FILTER_v_orders_raw`` is the negation —
  ``NOT ((order_id IS NOT NULL) AND (amount >= 0))`` — which matches exactly the
  rows that failed at least one rule. ``_FAILED_RULE_EXPRS_v_orders_raw`` builds,
  per row, the ``{name, rule}`` structs recording *which* rules it broke. The
  resolved table names sit in ``DLQ_TABLE_``, ``DLQ_OUTBOX_TABLE_`` (the
  auto-derived ``orders_dlq_outbox``), and ``SOURCE_TABLE_`` constants.
- **The clean path.** ``_clean_v_orders_raw`` applies ``@dp.expect_all_or_drop``
  to the source view. Passing rows flow on; this is also what surfaces the
  pass/fail metrics in the Databricks event log.
- **The quarantine path.** ``quarantine_flow_v_orders_raw`` reads the same source,
  applies the inverse filter to select only failed rows, and annotates each with
  ``_dlq_failed_rules``. Its target, ``dlq_sink_v_orders_raw``, is a
  ``@dp.foreach_batch_sink`` that hashes each row to a deterministic ``_dlq_sk``
  (``xxhash64`` of the source table plus the row JSON) and ``MERGE``\s it into the
  DLQ, so a retry never double-inserts the same row.
- **The recycle path.** ``recycle_flow_v_orders_raw`` reads the DLQ through Change
  Data Feed, filtered to rows an operator has marked ``_dlq_status = 'fixed'``.
  ``recycle_sink_v_orders_raw`` dedups them by keeping the latest
  ``_commit_version`` per ``_dlq_sk`` and merges them into the outbox — the
  permanent "already recycled" ledger that stops a re-edited row being ingested
  twice.
- **The final view.** ``_recycled_v_orders_raw`` reconstructs each recycled row
  from the VARIANT ``_row_data`` column with ``try_variant_get``, re-runs the
  expectations on it, and ``v_orders_validated`` — the view your write reads —
  ``UNION``\s the clean stream with the validated recycled stream.

.. note::

   The recycle loop has a one-run lag by design: a row you mark ``fixed`` during
   run *N* is read from the CDF, deduped into the outbox, and re-joined to the
   output on run *N+1*. This follows from the streaming checkpoint model, and it
   is why quarantine runs in triggered (non-continuous) pipelines. The full list
   of limitations is in the Quarantine reference.

To operate the queue — query quarantined rows, see which rule each one broke,
and mark a corrected row ``fixed`` so it recycles — you work directly against the
DLQ table in SQL. Those recipes, with the ``_dlq_sk`` / ``_dlq_status`` /
``_dlq_failed_rules`` column details, are in the Quarantine reference; this guide
stays on turning the mode on and reading what it wrote.

What you just did
=================

You changed one field — ``mode: dqe`` became ``mode: quarantine`` — and added a
four-line ``quarantine`` block. In drop mode that same ``data_quality`` action
compiles to a single ``@dp.expect_all_or_drop`` decorator on one view. In
quarantine mode it expands into a full dead-letter subsystem: **39 lines of YAML
became a 296-line Lakeflow file, and roughly 230 of those lines are DLQ plumbing
you did not write** — the ``foreach_batch_sink`` MERGE, the inverse-filter
routing flow, the CDF recycle reader, the windowed outbox dedup, and the
VARIANT reconstruction that re-validates recycled rows.

Not one ``MERGE``, ``foreachBatch``, ``readChangeFeed``, or ``xxhash64`` came
from you. You declared where failed rows go — ``dlq_table`` and ``source_table``
— and Lakehouse Plumber owns the recycling machinery. The generated Python is
still code you own: version it, diff it, open it in the Databricks editor like
any other file.

What's next
===========

- **Create the DLQ tables and operate the queue.** The Quarantine reference has
  the one-time ``CREATE TABLE`` DDL (with Change Data Feed and row tracking), the
  full column schema for the inbox and outbox, and the SQL recipes for querying
  and marking rows fixed.
- **Stay with drop when you don't need the row back.** If a failed row is noise
  rather than a record to recover, ``mode: dqe`` and a single
  ``@dp.expect_all_or_drop`` is the lighter choice — covered in the Get Started
  course and the data-quality guide.
- **Share one DLQ across flowgroups.** Because every row is tagged with its
  ``source_table``, several flowgroups can point ``dlq_table`` at the same
  dead-letter table and still recycle independently. Set the shared ``dlq_table``
  in a preset and let each flowgroup supply its own ``source_table``.
