=============================================
Migrate a DLT pipeline to Lakehouse Plumber
=============================================

.. meta::
   :description: Move an existing Databricks DLT / Lakeflow Declarative Pipelines Python codebase to Lakehouse Plumber YAML flowgroups one at a time — map each DLT construct to an LHP action, keep the same generated Lakeflow, and stop maintaining the boilerplate by hand.

You already have Databricks pipelines that work: a folder of hand-written DLT
(Delta Live Tables, now Lakeflow Declarative Pipelines) Python, one ``@dlt.table``
and function per table, with operational metadata, column lists, and CDC keys
copy-pasted across files. Rewriting all of that from scratch is not the goal.
Reusing it is.

Lakehouse Plumber does not replace the Lakeflow engine your pipelines already run
on. It replaces the *hand-written boilerplate* with declarative YAML: you describe
each table once, and LHP regenerates the same kind of Lakeflow Python you were
maintaining by hand. Migrate one flowgroup at a time, keep the rest running
unchanged, and stop editing decorators. That is the idea on every page:
**declare your ETL, don't hand-write it.**

The payoff is maintenance, not runtime. DLT Python grows linearly — every table
is a decorator plus a function, and shared logic drifts as it is copied between
files. LHP collapses that into YAML, applies presets and templates across
flowgroups, and regenerates idempotently, so one change to a preset propagates
everywhere. What executes afterward is the same Lakeflow you already trust.

How LHP maps to DLT
===================

Most production DLT code is a handful of recurring shapes. Each one has a direct
Lakehouse Plumber equivalent — a load, a transform, or a write action:

.. list-table::
   :header-rows: 1
   :widths: 46 54

   * - Hand-written DLT construct
     - Lakehouse Plumber equivalent
   * - ``@dlt.table`` over a streaming read
     - A ``write`` action with a ``streaming_table`` target, fed by a ``load``
   * - ``@dlt.table`` over a complete DataFrame (an aggregation)
     - A ``write`` action with a ``materialized_view`` target
   * - ``@dlt.view`` / ``@dlt.create_view``
     - A ``transform`` action — LHP emits a ``@dp.temporary_view`` named ``v_<target>``
   * - ``spark.readStream`` / ``dlt.read_stream``
     - A ``load`` action with ``readMode: stream``
   * - ``spark.read`` / ``dlt.read``
     - A ``load`` action with ``readMode: batch``
   * - ``spark.readStream.format("cloudFiles")``
     - A ``load`` action with ``source.type: cloudfiles``
   * - ``@dlt.expect`` / ``@dlt.expect_all`` (and the ``_or_drop`` / ``_or_fail`` variants)
     - A ``data_quality`` transform with an ``expectations_file``
   * - ``dlt.apply_changes`` / ``dp.create_auto_cdc_flow``
     - A ``streaming_table`` write in ``mode: cdc`` with a ``cdc_config``
   * - ``dlt.apply_changes_from_snapshot``
     - A ``streaming_table`` write in ``mode: snapshot_cdc``

The common thread: DLT fuses the read, the checks, and the write into one
decorated function. LHP splits them into a ``load`` → ``transform`` → ``write``
chain wired by view name. That split is what lets you fan several sources into one
table, insert a quality gate, or add CDC later without restructuring the file.

Migrate one flowgroup at a time
===============================

You do not have to convert the whole pipeline in one pass. LHP-generated Python
lands alongside your existing DLT in the *same* Lakeflow pipeline, so you can run
a hybrid pipeline — some tables generated, some still hand-written — through the
whole transition:

1. Pick one table, or a small set of related tables in one bronze or silver
   layer, to move first.
2. Write the YAML flowgroup under ``pipelines/``.
3. Run ``lhp validate --env dev`` to catch schema errors, then
   ``lhp generate --env dev`` to produce the Python under ``generated/``.
4. Diff the generated Python against your hand-written DLT file. Adjust the YAML
   until the diff is acceptable — it will not be byte-identical, and it does not
   need to be.
5. Point your pipeline configuration at the generated file, retire the original
   ``.py``, and move to the next flowgroup.

Because each flowgroup is independent, nothing forces a big-bang cutover.

Before: a hand-written DLT table
================================

Here is a common shape — a silver ``orders`` table that reads a bronze stream and
carries three expectations: fail the update on a corrupt feed, drop impossible
rows, and flag suspicious ones without losing them.

.. code-block:: python
   :caption: silver_orders.py (hand-written DLT — the original)

   import dlt

   @dlt.table(
       name="main.silver.orders",
       comment="Validated orders",
   )
   @dlt.expect_all_or_fail({"valid_order_id": "order_id IS NOT NULL"})
   @dlt.expect_all_or_drop({"non_negative_amount": "amount >= 0"})
   @dlt.expect_all({"has_customer": "customer_id IS NOT NULL"})
   def orders():
       return spark.readStream.table("main.bronze.orders")

Three concerns are fused into one function: the read
(``spark.readStream.table``), the quality rules (three stacked decorators), and
the write (the ``@dlt.table`` name). To change a rule you edit Python; to add a
second source you rewrite the function.

After: an LHP flowgroup
=======================

The same table becomes a three-action flowgroup — a ``load`` that streams the
bronze table into a view, a ``data_quality`` transform that applies the rules, and
a ``write`` that appends the validated view into the silver streaming table. Create
``pipelines/silver_orders.yaml``:

.. code-block:: yaml
   :caption: pipelines/silver_orders.yaml (Lakehouse Plumber)

   pipeline: silver
   flowgroup: orders

   actions:
     - name: load_bronze_orders
       type: load
       readMode: stream
       source:
         type: delta
         catalog: main
         schema: bronze
         table: orders
       target: v_orders_raw

     - name: validate_orders
       type: transform
       transform_type: data_quality
       source: v_orders_raw
       target: v_orders_validated
       expectations_file: expectations/orders_quality.yaml
       mode: dqe

     - name: write_orders
       type: write
       source: v_orders_validated
       write_target:
         type: streaming_table
         catalog: main
         schema: silver
         table: orders
         comment: Validated orders

The rules move out of the decorators and into their own versioned file. Each entry
is a SQL boolean expression — the key — paired with an ``action`` and a ``name``.
Create ``expectations/orders_quality.yaml``:

.. code-block:: yaml
   :caption: expectations/orders_quality.yaml

   order_id IS NOT NULL:
     action: fail
     name: valid_order_id
   amount >= 0:
     action: drop
     name: non_negative_amount
   customer_id IS NOT NULL:
     action: warn
     name: has_customer

The ``action`` on each rule maps one-for-one to the DLT decorator it replaces:
``fail`` → ``@dp.expect_all_or_fail`` (the update halts), ``drop`` →
``@dp.expect_all_or_drop`` (the row is discarded), and ``warn`` →
``@dp.expect_all`` (the row is kept and logged). You described the same three
rules; LHP now owns which decorator each one lands on.

.. note::

   The ``data_quality`` transform reads its rules from ``expectations_file`` — a
   separate YAML file, not an inline list on the action. Point the action at the
   file and keep the rules there; ``lhp validate`` rejects a ``data_quality``
   transform that has no ``expectations_file``.

The catalog, schema, and table are written literally here so the before-and-after
mapping is exact (``main.silver.orders`` on both sides). Once the flowgroup
generates, replace them with ``${catalog}`` / ``${schema}`` tokens so the same
YAML targets dev, staging, and prod — the substitutions guide covers that step.

What Lakehouse Plumber writes
=============================

Generate the flowgroup and open the file under ``generated/``. It is ordinary
Lakeflow — nothing is hidden behind a runtime — and it is semantically equivalent
to your DLT, not byte-identical. Expect three differences:

- The import switches from ``import dlt`` to ``from pyspark import pipelines as dp``.
  ``@dlt.table`` and friends move to the supported ``pyspark.pipelines`` alias as
  a side effect of regeneration; the decorators are functionally equivalent.
- The inlined read becomes an explicit ``@dp.temporary_view`` named
  ``v_orders_raw``, even though your original opened the stream directly inside the
  table function. Every load and transform action produces one of these views —
  that is the seam the ``load`` → ``transform`` → ``write`` chain hangs on.
- A single decorated function becomes a ``dp.create_streaming_table(...)`` that
  declares the target, plus a ``@dp.append_flow(...)`` that streams the validated
  view into it. The three expectation decorators land above the transform view,
  sorted onto ``@dp.expect_all_or_fail``, ``@dp.expect_all_or_drop``, and
  ``@dp.expect_all`` by each rule's ``action``.

You are looking for that semantic equivalence when you diff, not a matching byte
count. The output is code you own: version it, diff it, and open it in the
Databricks editor like anything else.

Constructs that don't map cleanly
=================================

A handful of DLT patterns have no direct YAML equivalent. Handle them case by
case:

- **Named DLT views read by string.** ``@dlt.view`` and ``@dlt.create_view`` have
  no direct counterpart — LHP generates a ``@dp.temporary_view`` automatically for
  every load and transform. If another table read a named DLT view by string,
  point the consumer at the LHP view name (``v_<target>``) instead.
- **Imperative table generation.** Code that calls ``dlt.read`` /
  ``dlt.read_stream`` inside arbitrary control flow — a for-loop that emits tables
  dynamically — does not fit the declarative model. Flatten the loop into explicit
  flowgroups, or embed the logic in a ``python`` transform or ``custom_datasource``
  load action.
- **Runtime-only behaviors.** Pipeline event hooks, cross-pipeline reads, and
  ``%pip install`` magics are configured at the Lakeflow pipeline level (in your
  bundle or pipeline spec), not inside the generated Python. Move them there during
  the migration.
- **Hand-rolled SCD logic.** If you wrote SCD Type 2 by hand instead of using
  apply-changes, convert it to ``mode: cdc`` rather than reproduce the imperative
  merge. The result is supported and considerably shorter — see the CDC write
  guide.

.. important::

   ``dp.create_auto_cdc_flow`` and ``dp.create_auto_cdc_from_snapshot_flow`` need
   a recent Lakeflow Declarative Pipelines runtime. If your existing code still
   calls the legacy ``dlt.apply_changes`` / ``dlt.apply_changes_from_snapshot``,
   the regenerated LHP code is the supported replacement and runs on the same
   pipeline engine.

What you just did
=================

You moved one DLT table to Lakehouse Plumber without changing what runs. The
Lakeflow the pipeline executes is the same shape it always was — a streaming
table, an append flow, three expectation decorators. What changed is what you
maintain: a short YAML flowgroup and a rules file you can diff and reuse, instead
of a decorated function you hand-edit for every rule and every new source.

Repeat that per flowgroup and the boilerplate stops being yours. The read, the
checks, and the write are declarations; LHP owns the plumbing that turns them into
Lakeflow.

What's next
===========

- **Convert your CDC tables.** ``dlt.apply_changes`` maps to a ``streaming_table``
  write in ``mode: cdc`` with a ``cdc_config`` block — keys, ``sequence_by``, and
  ``scd_type``. See the CDC write guide.
- **Convert your aggregations.** A ``@dlt.table`` that returns a complete
  DataFrame becomes a ``materialized_view`` write, with the SQL inline or reading
  an upstream view. See the materialized-view guide.
- **Remove the duplication you just noticed.** Operational metadata and shared
  column lists that were copy-pasted across DLT files become a preset or a
  template applied across flowgroups. See the reuse-and-scale guides.
