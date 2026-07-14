==========================================
Apply CDC changes to a streaming table
==========================================

.. meta::
   :description: Apply a stream of change rows into an SCD streaming table with a declarative Lakehouse Plumber CDC write — LHP generates dp.create_auto_cdc_flow, so you never hand-write MERGE or apply-changes logic.

Let's turn a stream of customer change rows — inserts, updates, and deletes,
arriving out of order — into a clean, current-state silver **dimension**. The
latest change per key wins, and a delete removes the row. And let's do it
without hand-writing a single line of merge logic.

You could hand-write this: a ``foreachBatch`` that runs a ``MERGE``, matching
each change to its target row on the key, ordering out-of-order changes so a
stale update never clobbers a fresh one, and branching to delete instead of
upsert when a row is a tombstone. Or you declare ``mode: cdc``, hand Lakehouse
Plumber the keys and the ordering column, and let it write the apply-changes
flow. That is the idea on every page: **declare your ETL, don't hand-write it.**

The change in intent is one block. A standard streaming table *appends* every
incoming row; add ``mode: cdc`` with a ``cdc_config`` and the same write target
*applies* each row as a change instead. LHP writes the rest.

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a source of change rows. This guide reads a bronze
``customer_changes`` table that another pipeline lands — each row is one change
record carrying an ``operation`` marker (``INSERT`` / ``UPDATE`` / ``DELETE``)
and a ``sequence_num`` that orders changes per customer. A CDC write *applies*
changes; it does not produce them.

Declare the CDC write
=====================

The flowgroup has two actions: a ``load`` that streams the change table into a
view, and a ``write`` whose ``write_target`` is a ``streaming_table`` in
``mode: cdc``. Create ``pipelines/silver_customers.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_st_cdc/pipelines/silver_customers.yaml
   :language: yaml
   :caption: pipelines/silver_customers.yaml
   :emphasize-lines: 32-38

``mode: cdc`` is the switch; ``cdc_config`` is the whole contract, and every
field maps to how the change is applied:

- ``keys`` — the business key AUTO CDC matches each change against. Here a
  single ``customer_id``; it can be several columns.
- ``sequence_by`` — the column that orders competing changes for a key, so a
  late-arriving stale row never overwrites a newer value. A single column name
  here; it also accepts a list.
- ``scd_type`` — ``1`` keeps only current state (overwrite on update); ``2``
  keeps full history. This guide starts with Type 1.
- ``apply_as_deletes`` — a SQL expression; rows that match it remove the key
  instead of upserting it. Here, tombstone rows where ``operation`` is
  ``'DELETE'``.
- ``except_column_list`` — columns the flow uses but does not store in the
  dimension. The two control columns, ``operation`` and ``sequence_num``, drive
  the apply but are not part of the customer record.

The ``load`` reads with ``readMode: stream``, so each new batch of changes flows
through continuously. The ``${...}`` tokens resolve per environment from
``substitutions/dev.yaml``, so the same flowgroup targets dev, staging, and prod
unchanged. The write action reference covers the rest of ``cdc_config`` —
``track_history_column_list``, ``ignore_null_updates``, ``apply_as_truncates``,
``column_list`` — and their constraints. Snapshot-based CDC, where the source is
periodic full snapshots rather than a change stream, is a separate write mode
and a separate guide.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_customers  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_customers  1 file
   ✓ generate (0.41s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves the tokens and checks the ``cdc_config`` — that ``keys``
is present, that ``scd_type`` is ``1`` or ``2``, that the column lists are not
in conflict — before you commit to generating.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/silver_customers/customer_dim.py``. This is the entire
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_st_cdc/generated/dev/silver_customers/customer_dim.py
   :language: python
   :caption: generated/dev/silver_customers/customer_dim.py
   :emphasize-lines: 35-44

There is no ``MERGE`` and no ``foreachBatch``. LHP wrote two things: a
``dp.create_streaming_table`` that declares the target dimension, and one
``dp.create_auto_cdc_flow`` that applies the changes into it. Every field of
your ``cdc_config`` came through as an argument — ``keys=["customer_id"]``,
``sequence_by="sequence_num"``, ``stored_as_scd_type=1``,
``apply_as_deletes="operation = 'DELETE'"``, and
``except_column_list=["operation", "sequence_num"]`` — and the ``${...}`` tokens
resolved into the three-part name ``dev_catalog.silver.customer_dim``. You
described the change semantics; LHP wrote the declaration that enforces them.

.. note::

   ``dp.create_auto_cdc_flow`` is Databricks' AUTO CDC — the declarative form of
   apply-changes. Lakehouse Plumber decides *how the flow is wired and
   declared*; Databricks applies the changes at run time. The source must be a
   streaming read (``readMode: stream``), which is what feeds each new batch of
   changes through the flow.

Switch to SCD Type 2
====================

SCD Type 1 keeps only the current row per key. To keep full history instead — a
new versioned row on every change, with the validity windows maintained for you
— change one field: set ``scd_type: 2``. LHP emits ``stored_as_scd_type=2`` in
the same ``create_auto_cdc_flow``, and the dimension starts tracking history. To
exclude audit columns from what counts as a tracked change, add
``track_history_except_column_list``. One constraint carries over from
Databricks: ``apply_as_truncates`` is not supported with SCD Type 2 — it is a
Type 1 option only.

What you just did
=================

Twenty-seven lines of YAML compiled to forty-four lines of Lakeflow Python.
**Zero lines of merge logic came from you** — not the ``create_auto_cdc_flow``
call, not the key matching, not the sequencing that resolves out-of-order
changes, not the delete branch. You declared the change semantics in a
``cdc_config`` block, and LHP owns the apply-changes boilerplate.

Flip ``scd_type`` and the *kind* of history changes with it; the wiring stays
LHP's job. That is the payoff: the CDC contract is a short declaration, and the
generated Python is code you own — version it, diff it, open it in the
Databricks editor like anything else.

What's next
===========

- **Handle periodic snapshots instead of a change stream.** When the source is
  full-table snapshots rather than row-level changes, a separate write mode
  (snapshot CDC) computes the diff for you — covered in its own guide.
- **See every CDC option.** ``track_history_column_list``,
  ``ignore_null_updates``, ``apply_as_truncates``, ``column_list``, and
  multi-column ``sequence_by`` all live on the same ``cdc_config``. The write
  action reference lists them with their constraints.
- **Fan several change sources into one dimension.** Multiple CDC write actions
  that target the same table combine into a single ``create_streaming_table``
  with one apply-changes flow per source.
