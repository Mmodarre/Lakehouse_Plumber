==============================================
Stage an intermediate result in a temp table
==============================================

.. meta::
   :description: Materialise an intermediate result once with a Lakehouse Plumber temp_table transform, so several downstream actions share the staged rows instead of re-running the same query — a @dp.table(temporary=True) written from one declarative action.

Let's take a cleaned stream of orders and stage it **once**, so two downstream
branches — a silver append and a high-value flag — both read the same result
instead of each re-running the cast-and-filter query.

Chain a few ``transform_type: sql`` views together and every consumer re-runs
the whole chain: a view is a query definition, not stored rows. You could hand
that off to a ``@dp.table(temporary=True)`` you write yourself, remembering the
``temporary=True`` flag and the read. Or you declare ``transform_type:
temp_table``, and Lakehouse Plumber writes the materialised table for you. That
is the idea on every page: **declare your ETL, don't hand-write it.**

Before you start
================

A temp table sits in the middle of a flowgroup, between the actions that produce
its input and the actions that read its output. This guide assumes a ``load``
that streams raw CSV orders into ``v_orders_raw`` — the same ingest you built in
Get Started. Any flowgroup with a view to stage and more than one action that
reads it fits the same shape.

Declare the temp table
======================

Insert one ``transform`` action with ``transform_type: temp_table``. Point
``source`` at the view to materialise, name the ``target`` table it produces,
and — because the raw view is a stream — declare ``readMode: stream`` and wrap
the source in ``stream(...)`` inside the query. The ``sql`` does the heavy work
once: cast the raw string columns, and drop rows with no order id.

.. literalinclude:: ../../_fixtures/guide_transform_temptable/pipelines/orders_stage.yaml
   :language: yaml
   :caption: pipelines/orders_stage.yaml
   :emphasize-lines: 20-34

Actions chain by name. ``stage_orders`` produces ``orders_staged``, and **two**
actions read it: ``write_orders_silver`` appends it to the silver ``orders``
table, and ``flag_high_value`` runs a second ``SELECT`` over it to mark
high-value orders before its own write. That reuse is the reason to reach for a
temp table — more on it below. The ``sql`` is optional: drop it and the action
becomes a straight passthrough materialisation of ``source``.

.. note::

   The ``stream(...)`` rule from the SQL transform applies here too. Lakehouse
   Plumber hands your query straight to ``spark.sql(...)``, so wrap a streaming
   source in ``stream(<view>)`` to keep the staged table streaming. Reference a
   source plainly for a static snapshot read.

Generate the pipeline
=====================

Validate first, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_stage  0 files
   ✓ validate (0.31s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_stage  1 file
   ✓ generate (0.36s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Open the generated file. This is the entire output — nothing is hidden behind a
runtime:

.. literalinclude:: ../../_fixtures/guide_transform_temptable/generated/dev/silver_stage/orders_stage.py
   :language: python
   :caption: generated/dev/silver_stage/orders_stage.py
   :emphasize-lines: 34-49

Look at what the temp table became versus its neighbours. ``orders_staged`` is a
``@dp.table(temporary=True)`` — a table Lakeflow computes and **stores**. The
sibling SQL transform ``v_orders_flagged`` two functions down is a
``@dp.temporary_view()`` — a query definition with no stored rows. That is the
whole distinction: a temporary view is re-evaluated by every reader; a temp
table materialises the rows once. Both downstream branches then read the stored
table — the silver append flow calls ``spark.readStream.table("orders_staged")``
and the flag view reads ``stream(orders_staged)`` — so the cast-and-filter query
runs once, not once per branch.

When to reach for a temp table
==============================

A ``transform_type: sql`` view is the default middle-of-flowgroup transform, and
it is the right call when one action reads the result. Reach for ``temp_table``
when the trade-off tips:

- **Several downstream actions read the same intermediate.** Materialising it
  once keeps the upstream query from being recomputed for each consumer — the
  two-branch case in this guide.
- **A heavy DAG needs a checkpoint.** Staging a costly join or aggregation as a
  table breaks a long view chain into a materialised step the rest of the
  flowgroup builds on.

The temp table is dropped when the pipeline completes — it is scratch space for
the run, not a published table. When a single action consumes the result, stay
with a ``sql`` view and skip the materialisation.

What you just did
=================

You declared one action — a 15-line ``temp_table`` transform — and Lakehouse
Plumber generated the 16-line ``@dp.table(temporary=True)`` function, resolved
the environment tokens, and wired the staged table into both downstream
branches. Across the whole flowgroup: **49 lines of YAML became 102 lines of
Lakeflow Python, with zero lines of PySpark from you.** You never wrote
``@dp.table``, ``temporary=True``, or a ``readStream.table`` by hand.

The payoff is structural, not just line count. The cast-and-filter query is
written once, materialised once, and read by two actions — change it in one
place and both branches follow.

What's next
===========

- **Gate the staged rows with a data-quality check** — attach expectations to
  drop or quarantine bad records before they reach a table, covered in the
  data-quality guide.
- **Keep the transform a plain view** when only one action reads it — the SQL
  transform guide covers the default case and the ``stream(...)`` rule in full.
- For every ``temp_table`` field — ``sql``, ``readMode``, and the passthrough
  form — see the **Transform action reference**.
