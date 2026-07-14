======================================
Reshape data with a SQL transform
======================================

.. meta::
   :description: Reshape rows between a load and a write with a Lakehouse Plumber SQL transform — cast types, rename columns, filter, and enrich with a stream-static join, all from one declarative action.

Let's take a stream of raw order rows — string columns, cryptic names, no
context — and turn them into typed, enriched, table-ready records. One
``transform`` action, written as the SQL ``SELECT`` you'd write anyway.

You could hand-write this: a ``@dp.temporary_view`` function that wraps
``spark.sql(...)``, remembering to wrap the streaming source in ``stream(...)``,
then wire that view into the append flow that feeds the table. Or you declare
``transform_type: sql``, hand Lakehouse Plumber the query, and let it write the
wrapper and the wiring. That is the whole idea: **declare your ETL, don't
hand-write it.**

Before you start
================

A SQL transform sits in the middle of a flowgroup: it reads a view a ``load``
produced and hands a new view to a ``write``. This guide extends the orders
ingest from **Your first pipeline** — a ``load`` action that streams raw CSV
into ``v_orders_raw``. If you have any flowgroup with a load and a write, the
same shape applies.

Declare the transform
=====================

Insert one ``transform`` action between the load and the write. Give it a
``transform_type: sql``, point ``source`` at the view to read, name the
``target`` view it produces, and put the query in ``sql``:

.. literalinclude:: ../../_fixtures/guide_transform_sql/pipelines/orders_enrich.yaml
   :language: yaml
   :caption: pipelines/orders_enrich.yaml
   :emphasize-lines: 20-36

Actions chain by view name: ``load_orders_csv`` produces ``v_orders_raw``,
``enrich_orders`` reads it and produces ``v_orders_enriched``, and the write
reads that. The query does four things at once — it **casts** the raw string
columns to ``BIGINT`` / ``DOUBLE`` / ``TIMESTAMP``, **renames** ``cust_id`` to
``customer_id``, **filters** out rows with no ``order_id``, and **joins** each
order against a customer dimension to add ``customer_name`` and ``region``. The
``${catalog}``, ``${silver_schema}``, and ``${landing_path}`` tokens resolve per
environment from ``substitutions/dev.yaml``, so the same query ships unchanged
to prod.

The query is inline here. To keep a long query in its own file instead, drop
the ``sql`` key and point ``sql_path`` at a ``.sql`` file — the same ``${...}``
tokens work inside it. The Transform action reference covers ``sql_path``,
multiple source views, and operational-metadata columns; this guide stays on
the inline single-source case.

Choose streaming or static per source
=====================================

Look at the ``FROM`` clause. The orders view is wrapped in ``stream(...)``; the
customer dimension is referenced as a plain table name. That difference is
deliberate, and it is the one rule worth internalising for SQL transforms.

Lakehouse Plumber passes your query straight into ``spark.sql(...)`` — it does
not rewrite your ``FROM`` clauses. So each source's read semantics are decided
inside the SQL:

- Wrap a source in ``stream(<view>)`` for an incremental, streaming read. Here
  ``stream(v_orders_raw)`` keeps the pipeline processing only new order files as
  they land.
- Reference a source plainly — a view name, or a fully-qualified
  ``catalog.schema.table`` — for a static snapshot read. Here the
  ``dim_customers`` table is read as it stands at each update.

That is a **stream-static join**: an unbounded stream of orders enriched by a
slowly-changing dimension read as a snapshot. Forget the ``stream(...)`` wrapper
on a streaming source and the query silently runs in batch, reprocessing
everything each update.

.. note::

   ``stream(...)`` is a Databricks SQL construct, not Lakehouse Plumber syntax.
   You are writing ordinary Lakeflow SQL — Lakehouse Plumber only decides where
   it lands and how the view is wired in.

Generate and read the output
============================

Validate the flowgroup, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.34s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Open the generated file. The transform became a ``@dp.temporary_view`` that
wraps your query in ``spark.sql(...)`` — with the tokens resolved and the
``stream(...)`` / static split preserved exactly as you wrote it:

.. literalinclude:: ../../_fixtures/guide_transform_sql/generated/dev/silver_orders/orders_enrich.py
   :language: python
   :caption: generated/dev/silver_orders/orders_enrich.py
   :emphasize-lines: 34-49

The ``${catalog}.${silver_schema}.dim_customers`` reference resolved to
``dev_catalog.silver.dim_customers`` — a static read, no ``stream()`` — while
``stream(v_orders_raw)`` came through untouched. The view is wired into the
same append flow that feeds ``dev_catalog.silver.orders_enriched``.

What you just did
=================

You wrote one action — an 11-line SQL ``SELECT`` wrapped in six lines of YAML —
and Lakehouse Plumber generated the 16-line ``@dp.temporary_view`` function,
wrapped your query in ``spark.sql(...)``, resolved the environment tokens, and
wired ``v_orders_enriched`` into the append flow. Across the whole flowgroup:
**35 lines of YAML became 74 lines of Lakeflow Python, with zero lines of
PySpark from you.** You never wrote ``@dp.temporary_view``, ``spark.sql``, or a
``readStream.table`` by hand.

The SQL is yours to own — the casts, the join, the filter are exactly what you
typed. Lakehouse Plumber wrote only the boilerplate that turns a query into a
wired-up Lakeflow view.

What's next
===========

- **Gate the rows with a data-quality check** — attach expectations to drop or
  quarantine bad records after the transform, covered in the data-quality guide.
- **Reach for a Python transform** when the logic outgrows a ``SELECT`` — UDFs,
  external calls, or multi-source DataFrame code, covered in the Python
  transform guide.
- For the full option list — ``sql_path``, multiple source views, and
  operational metadata — see the **Transform action reference**.
