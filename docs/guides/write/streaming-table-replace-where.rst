================================================
Recompute the recent rows of a streaming table
================================================

.. meta::
   :description: Recompute the last days of a table on every refresh with a Lakehouse Plumber replace_where write — a Databricks REPLACE WHERE flow over a batch source that leaves older rows as they are.

Some data keeps changing for a while after it lands. Orders are corrected,
cancelled and re-priced for a few days after they are placed, late records
arrive, and an upstream system reprocesses yesterday. After that the rows
settle. You want a table that keeps the whole history, where the recent days
are recomputed from the source on every refresh and the older days are left
alone.

That is what a Databricks REPLACE WHERE flow does, and ``mode: replace_where``
generates one.

Recompute a window, keep the history
====================================

You give the write a SQL predicate, such as "orders from the last 7 days". On
every refresh the REPLACE WHERE flow deletes the target rows that match the
predicate and recomputes them by re-running the source query for that range.
Rows that don't match are left exactly as they are. The source is read in
**batch**, so there is no streaming checkpoint to manage.

A worked example. The predicate is ``order_date >= date_add(current_date(),
-7)``, and the refresh runs on 2026-06-10, so it covers 2026-06-03 onwards:

.. code-block:: text

   BEFORE — target rows
     order_id  order_date  amount
     101       2026-05-20  40
     205       2026-06-05  12
     206       2026-06-06  30
     207       2026-06-08  18

   SOURCE — what the bronze table holds now
     order_id  order_date  amount
     101       2026-05-20  45     # corrected, but outside the predicate
     205       2026-06-05  12
     207       2026-06-08  20     # corrected
     208       2026-06-10  25     # new
     # 206 was deleted upstream

   AFTER — target rows, via REPLACE WHERE
     order_id  order_date  amount
     101       2026-05-20  40     # untouched: outside the predicate
     205       2026-06-05  12
     207       2026-06-08  20
     208       2026-06-10  25
     # 206 is gone: the rows inside the predicate were recomputed from the source

Inside the predicate the target now matches the source: ``207`` was corrected,
``208`` was added and ``206`` was deleted, with no delete event needed. Outside
it, nothing changed, including the correction to order ``101``. That is the
trade-off: once a row is older than the window, the table stops following the
source for it. Pick a window that covers how long your data keeps changing.

The batch read is what sets it apart from its streaming sibling,
``mode: replace``:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Aspect
     - ``mode: replace_where`` (REPLACE WHERE)
     - ``mode: replace`` (REPLACE USING)
   * - Rows replaced
     - Every target row that matches a predicate
     - Every target row of each key that a batch carries
   * - Source read
     - Batch; the predicate's range is recomputed
     - Stream; the new rows are applied
   * - Rows outside
     - Never touched
     - Keys absent from the batch are never touched

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a source table. This guide reads a bronze ``orders`` table
that another pipeline lands, with an ``order_date`` column for the predicate.

Three rules are worth knowing before you write it:

- The source is read in **batch**. A streaming source is rejected.
- The target is served by this **single** flow. No other write action can use
  its table, and Lakehouse Plumber always creates the table for you.
- Databricks refreshes the flow incrementally only on **serverless** compute.

Declare the REPLACE WHERE write
===============================

The flowgroup has two actions: a ``load`` that reads the orders in batch into a
view, and a ``write`` whose ``write_target`` is a ``streaming_table`` in
``mode: replace_where``. Create ``pipelines/orders_recent.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_replace_where/pipelines/orders_recent.yaml
   :language: yaml
   :caption: pipelines/orders_recent.yaml
   :emphasize-lines: 15,34,37-38

- ``readMode: batch`` on the load makes ``v_orders`` a batch view. A ``delta``
  load reads in batch by default; it is spelled out here because a streaming
  view would be rejected.
- ``mode: replace_where`` is the switch.
- ``replace_where_config.replace_where`` is the predicate, a SQL string that
  Lakehouse Plumber writes into the generated code as it is. ``${...}`` tokens
  in it are resolved per environment first.
- ``table_properties`` turns on the change data feed. Nothing in this pipeline
  needs it; a downstream pipeline uses it at the end of this guide.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.29s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.31s)
   ✓ format (0.01s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.3s

``validate`` resolves the tokens and checks the write: that the
``replace_where_config`` block holds a non-blank predicate, that the view it
reads is a batch view, and that no other write action in the pipeline targets
the table.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/silver_orders/orders_recent.py``:

.. literalinclude:: ../../_fixtures/guide_write_st_replace_where/generated/dev/silver_orders/orders_recent.py
   :language: python
   :caption: generated/dev/silver_orders/orders_recent.py
   :emphasize-lines: 30-43

The target is one ``@dp.table`` statement, which Databricks reads as both the
streaming table and its REPLACE WHERE flow. There is no
``dp.create_streaming_table`` and no flow decorator. The predicate is the
``replace_where=`` argument, exactly as you wrote it, and the flow reads its
view with ``spark.read``. ``table_properties`` holds your change-data-feed
setting and one more that you did not write, ``pipelines.reset.allowed``.

Keep the history safe
=====================

A full refresh of a REPLACE WHERE flow re-runs the source query for the
current predicate only. Without protection it would delete every row older
than 7 days. Lakehouse Plumber therefore sets ``pipelines.reset.allowed`` to
``"false"`` on every REPLACE WHERE table, unless you set it yourself:

- A full refresh of the whole pipeline still runs. The table keeps its older
  rows, and only the last 7 days are recomputed.
- A full refresh that selects this table fails the update, because the table
  is not resettable.

When you do want to start the table over, see the reset recipe in the
:ref:`write reference <replace-where-reset>`.

Keep refreshes incremental
==========================

Databricks can refresh the flow incrementally, processing only what changed
inside the predicate instead of recomputing all of it. That needs serverless
compute, a deterministic predicate on columns of the source table, and a
deterministic query. Two things in your YAML affect it:

- **The predicate.** A moving lower bound, like the one above, stays
  eligible. Widening the predicate costs one full recomputation of the window,
  and so can a moving upper bound.
- **Operational metadata.** A metadata column computed with a non-deterministic
  function, such as ``F.current_timestamp()``, makes every refresh recompute the
  whole window. ``lhp validate`` warns about it with ``LHP-VAL-073``.

The :ref:`write reference <replace-where-incremental>` explains how to check in
the event log which way each refresh ran.

Read the table downstream
=========================

Every refresh rewrites rows inside the predicate, so the table is not an
append-only source. Suppose a gold pipeline streams it, as it would any other
table:

.. code-block:: yaml
   :caption: pipelines/orders_recent_copy.yaml (rejected)

   pipeline: gold_orders
   flowgroup: orders_recent_copy

   actions:
     - name: load_orders_recent
       type: load
       readMode: stream
       source:
         type: delta
         catalog: "${catalog}"
         schema: "${silver_schema}"
         table: orders_recent
       target: v_orders_recent

     - name: write_orders_recent_copy
       type: write
       source: v_orders_recent
       write_target:
         type: streaming_table
         catalog: "${catalog}"
         schema: "${gold_schema}"
         table: orders_recent_copy

On Databricks that stream would fail as soon as a refresh rewrote a row, and
with ``skipChangeCommits`` it would silently skip the rewritten rows instead.
``lhp validate`` stops it first:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✗ gold_orders  LHP-VAL-072  Streaming read of a REPLACE WHERE table
   ✓ silver_orders  0 files
   ✗ validate (0.32s)
   1 validated · 1 failed · 0.3s
   gold_orders  LHP-VAL-072  Streaming read of a REPLACE WHERE table
   Re-run with details: lhp validate --env dev --show-details

The check covers the whole run, so it catches a reader in another pipeline.
``--show-details`` names the read and the write behind it:

.. code-block:: text

   gold_orders.orders_recent_copy.load_orders_recent streams from 'dev_catalog.silver.orders_recent', a REPLACE WHERE table written by silver_orders.orders_recent.write_orders_recent. Each refresh rewrites rows, which a streaming read cannot follow (skipChangeCommits silently drops data). Use readMode: batch, or stream its change feed with options.readChangeFeed: "true" (see docs).

A batch read fixes it. To keep a streaming copy instead, follow the table's
change data feed, which records each rewrite as ``delete`` and ``insert``
events, and apply those events with AUTO CDC. Replace the file with
``pipelines/orders_recent_mirror.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_replace_where/pipelines/orders_recent_mirror.yaml
   :language: yaml
   :caption: pipelines/orders_recent_mirror.yaml
   :emphasize-lines: 21-24,41

- ``readChangeFeed: "true"`` streams the table's change events instead of its
  rows. Lakehouse Plumber does not report a change-feed read as
  ``LHP-VAL-072``.
- ``sequence_by: ["_commit_version", "_change_type"]`` is required. A rewritten
  row has a ``delete`` and an ``insert`` in the same commit. Ordered by
  ``_commit_version`` alone, AUTO CDC can keep the ``delete`` and drop the row
  from the copy without any error; adding ``_change_type`` makes the ``insert``
  win.
- The ``where_clause`` drops update pre-images, and ``except_column_list``
  keeps the change-feed columns out of the copy.

Validate and generate again; both pipelines pass:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ gold_orders  0 files
   ✓ silver_orders  0 files
   ✓ validate (0.32s)
   2 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ gold_orders  1 file
   ✓ silver_orders  1 file
   ✓ generate (0.34s)
   ✓ format (0.01s)
   ✓ monitoring (0.00s)
   2 pipelines generated · 2 files · 0.3s

The mirror's flow is an AUTO CDC flow over the change feed:

.. literalinclude:: ../../_fixtures/guide_write_st_replace_where/generated/dev/gold_orders/orders_recent_mirror.py
   :language: python
   :caption: generated/dev/gold_orders/orders_recent_mirror.py (excerpt)
   :lines: 39-49

What you just did
=================

A short ``replace_where_config`` block compiled to one ``@dp.table`` with a
``replace_where=`` predicate, a batch read and a reset guard. You declared
*which rows to keep recomputing*; Databricks deletes and recomputes them on
each refresh, and Lakehouse Plumber checked the setup around it: a batch
source, a single flow, and downstream pipelines that follow the table through
its change feed instead of streaming its rows.

What's next
===========

- **Replace by key from a stream instead.** When the source resends complete
  sets of rows per key, use ``mode: replace`` (REPLACE USING), covered in
  :doc:`streaming-table-replace`.
- **See the field reference.** The rules, the reset recipe, the incremental
  refresh conditions, ``LHP-VAL-072`` and ``LHP-VAL-073`` and backfills are
  described in the write action reference.

Syntax and options
------------------

See :doc:`/reference/actions/write/streaming-table-replace-where` for all supported settings and defaults.
