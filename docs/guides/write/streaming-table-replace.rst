==========================================
Replace all rows that match a grouping key
==========================================

.. meta::
   :description: Replace a grouping key's whole set of rows from partial snapshots with a Lakehouse Plumber replace-mode write — REPLACE USING replaces (not merges) and keeps multiple rows per key, unlike AUTO CDC.

Some sources don't emit row-level change events. Instead, whenever something
about an entity changes, they resend the entity's **complete current set of
rows**. For example, when a subset of records in a source file are changed and
the whole file is reingested. Or when an order-management system re-exports
*every line* of an order the moment any line is added, repriced, or removed -
you want the target table to hold, for each order, exactly the lines the source
most recently sent, swapping the old set for the new one, and leaving orders
that weren't included in the latest partial snapshot alone.

That is a **replace**, not a merge, and it is the one thing AUTO CDC can't do.

Replace, not merge
==================

``mode: replace`` generates a Databricks **REPLACE USING** flow. For every key
value present in an incoming batch, it deletes the target rows for that key and
writes the batch's rows for that key in their place. The crucial part:
``replace_using`` is a **grouping key, not a primary key**. It does not have to
be unique, one key can own many rows, and REPLACE USING keeps **every** row the
source sent for that key; it never collapses them into one.

That is different from ``mode: cdc`` (AUTO CDC), which *merges* a stream of
insert/update/delete events onto a **unique primary key** and resolves all the
changes for a key into a **single** current row (or a versioned SCD Type 2
history). Same streaming-table target, fundamentally different operation:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Aspect
     - ``mode: replace`` (REPLACE USING)
     - ``mode: cdc`` (AUTO CDC)
   * - Operation
     - Replace a key's rows as a whole set
     - Merge change events onto a key
   * - Key
     - Grouping key — **need not be unique**
     - Primary key — one logical row per key
   * - Rows per key
     - **Many**, kept exactly as sent (appended, not combined)
     - **One** current row (SCD 1) or a versioned history (SCD 2)
   * - Several source rows for one key
     - All are written to the target
     - Collapsed / merged into a single row
   * - Source shape
     - Partial snapshots — all rows for each key the batch carries (changed or not)
     - Change deltas (explicit insert / update / delete)
   * - Removing rows
     - Implicit — a row missing from a key's new set is deleted; absent keys are untouched
     - Explicit only — needs a delete event (``apply_as_deletes``)

A worked example makes the difference concrete — watch the line that gets
*removed*. Order ``42`` has three lines in the target. Upstream, one line's
quantity changes, one new line is added, and one line is **deleted**, so the
source resends order ``42`` with its complete current set (a newer
``updated_at``):

.. code-block:: text

   BEFORE — target rows for order 42
     order_id  product  qty
     42        widget   2
     42        gadget   5
     42        bolt     10

   BATCH — the complete current set the source resends (updated_at = 2024-06-02)
     order_id  product  qty
     42        widget   3      # qty changed
     42        bolt     10     # unchanged
     42        cable    1      # new line
     # gadget is absent — it was deleted upstream

   AFTER — target rows for order 42, via REPLACE USING (replace_using=[order_id])
     order_id  product  qty
     42        widget   3
     42        bolt     10
     42        cable    1
     # the whole set was replaced; gadget is gone — deleted with no delete event,
     # purely by being absent from the batch

Three things happened in one step: ``widget`` was updated, ``cable`` inserted,
and ``gadget`` **deleted**, and the delete is the part an upsert can't do. AUTO
CDC applies only the rows a change feed carries: hand it the same batch and it
would upsert ``widget`` and ``cable`` but never learn that ``gadget`` was
removed, so the stale line would linger until the source emitted an explicit
delete event (``apply_as_deletes``). **Even with a unique per-line key, AUTO CDC
can't infer a delete from an absence.** REPLACE USING can, because a partial
snapshot states the whole truth for its keys — so a row that drops out of the
set drops out of the table.

Reach for ``mode: replace`` when the source resends complete sets keyed by a
grouping column; reach for ``mode: cdc`` when it emits per-row changes against a
real primary key.

.. note::

   REPLACE USING maintains only the **current state** of each key; the target
   always reflects the latest snapshot. It cannot keep a versioned **SCD Type 2**
   history (validity windows, superseded rows preserved over time). When you need
   history, use ``mode: cdc`` with ``scd_type: 2``.

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a **streaming** source of partial snapshots. This guide reads
a bronze ``order_line_updates`` table that another pipeline lands — each batch
carries the full set of current lines for the ``order_id`` values it contains
(whether or not they changed), plus an ``updated_at`` that orders competing
batches for an order.

Two rules are worth knowing before you write it. The source must be a streaming
read (``readMode: stream``); a batch read is rejected. And the target must be
served by this **single** replace flow: it cannot share its table with any other
write action, so Lakehouse Plumber always creates the table for you
(``create_table`` is forced ``true``).

Declare the replace write
=========================

The flowgroup has two actions: a ``load`` that streams the snapshots into a
view, and a ``write`` whose ``write_target`` is a ``streaming_table`` in
``mode: replace``. Create ``pipelines/order_lines_current.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_replace/pipelines/order_lines_current.yaml
   :language: yaml
   :caption: pipelines/order_lines_current.yaml
   :emphasize-lines: 34-37

``mode: replace`` is the switch; ``replace_config`` is the whole contract, and
both fields map straight to the Databricks REPLACE USING parameters:

- ``replace_using`` — the grouping key columns. Every row in a batch that
  carries a given key replaces that key's rows in the target; keys not present
  in the batch are left untouched. Here a single ``order_id``, but it need not be
  unique — an order owns many line rows and they are all kept.
- ``sequence_by`` — a single column that orders competing batches for a key, so
  a late-arriving stale batch never overwrites a newer one; the highest value
  wins. Unlike ``cdc_config``, which also accepts a list, REPLACE USING takes
  exactly **one** sequence column.

The ``load`` reads with ``readMode: stream``, so each new batch of snapshots
flows through continuously. The ``${...}`` tokens resolve per environment from
``substitutions/dev.yaml``, so the same flowgroup targets dev, staging, and prod
unchanged.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ order_lines_current  0 files
   ✓ validate (0.29s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ order_lines_current  1 file
   ✓ generate (0.29s)
   ✓ format (0.01s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.3s

``validate`` resolves the tokens and checks the ``replace_config`` — that
``replace_using`` is a non-empty list and ``sequence_by`` is a single column —
and that the source is a streaming read, before you commit to generating.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/order_lines_current/order_lines_current.py``. This is the
entire output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_st_replace/generated/dev/order_lines_current/order_lines_current.py
   :language: python
   :caption: generated/dev/order_lines_current/order_lines_current.py
   :emphasize-lines: 29-42

There is no ``MERGE`` and no ``foreachBatch`` — and, unlike AUTO CDC, no
apply-changes call either. LHP wrote two things: a ``dp.create_streaming_table``
that declares the target, and one ``@dp.replace_flow`` that replaces each key's
rows into it. Both fields of your ``replace_config`` came through as arguments —
``replace_using=["order_id"]`` and ``sequence_by="updated_at"`` — and the
``${...}`` tokens resolved into the three-part name
``dev_catalog.silver.order_lines_current``. The flow reads ``spark.readStream``,
because REPLACE USING requires a streaming source.

.. note::

   ``@dp.replace_flow`` is Databricks' REPLACE USING flow. Lakehouse Plumber
   decides *how the flow is wired and declared*; Databricks performs the
   per-key replacement at run time. Because the target is served by this single
   flow, LHP always emits the ``dp.create_streaming_table`` alongside it — you
   cannot add a second flow to this table.

What you just did
=================

A short ``replace_config`` block compiled to a ``dp.create_streaming_table``
plus a ``@dp.replace_flow``, and **zero lines of set-replacement logic came
from you**: not the delete-the-old-set-then-insert, not the grouping by key, not
the sequencing that keeps a stale batch from clobbering fresh data. You declared
*which key groups the rows* and *which column orders batches*, and LHP owns the
rest.

What's next
===========

- **Merge change events onto a primary key instead.** When the source emits
  per-row inserts/updates/deletes against a unique key and you want one current
  row (or SCD history) per key, use ``mode: cdc`` (AUTO CDC) — covered in its own
  guide.
- **See the field reference.** ``replace_using`` and ``sequence_by``, the forced
  table creation, and the single-flow and streaming-source constraints are all
  listed in the write action reference.
