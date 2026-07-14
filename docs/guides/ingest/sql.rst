========================================
Ingest with a SQL query
========================================

.. meta::
   :description: Ingest data by declaring a SQL query as the source in Lakehouse Plumber — read and join existing tables into one view with a type: sql load, no hand-written spark.sql temporary-view boilerplate.

A Delta load reads one table. But the data you want is often not one table — it
is a join or an aggregate across several tables that already live in Unity
Catalog. A ``type: sql`` load lets you express that as the ``SELECT`` you would
write anyway, and lands the result as a view your pipeline can write.

You could hand-write it: a ``@dp.temporary_view`` function that wraps
``spark.sql(...)``, then the wiring that lands the view downstream. Or you
declare a ``type: sql`` load, hand Lakehouse Plumber the query, and let it write
the wrapper and the wiring. That is the idea on every page: **declare your ETL,
don't hand-write it.**

Let's ingest a silver ``orders_enriched`` table by reading two existing bronze
tables — ``orders`` and ``customers`` — and joining them in one query. No
upstream load, no PySpark: the query itself is the source.

Before you start
====================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide reads two existing bronze tables — ``orders`` and
``customers`` — that already live in Unity Catalog. A SQL load reads existing
tables; it does not create them.

Declare the SQL load
========================

A flowgroup is a short YAML file describing a sequence of **actions**. This one
has two — a ``load`` whose source is a SQL query, and a ``write`` that lands the
query's result.

Create ``pipelines/orders_enriched.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_sql/pipelines/orders_enriched.yaml
   :language: yaml
   :caption: pipelines/orders_enriched.yaml
   :emphasize-lines: 15

The ``load`` action's ``source`` is typed ``sql`` and carries the query inline
under ``sql``. Lakehouse Plumber runs that query with ``spark.sql(...)`` and
exposes the result as the ``v_orders_enriched`` view. The query reads two
fully-qualified tables — ``${catalog}.${bronze_schema}.orders`` and
``${catalog}.${bronze_schema}.customers`` — joins them on ``customer_id``, and
keeps the rows that have an ``order_id``.

The ``write`` lands that view in a silver ``orders_enriched`` materialized view.
The result is a recomputed join, not an incremental append, so a materialized
view is the right target: the engine keeps it current by re-running the query.

The query is inline here. To keep a long query in its own file, drop the ``sql``
key and point ``sql_path`` at a ``.sql`` file — the same ``${...}`` tokens
resolve inside it. The load action reference documents ``sql`` versus
``sql_path`` and the rule that exactly one of them is set.

A load, not a transform
============================

A SQL load and a SQL transform both run a query through ``spark.sql(...)``, so it
is worth being clear on which is which. A SQL **transform** sits in the middle of
a flowgroup: it reads a view an upstream ``load`` produced and reshapes it. A SQL
**load** is the entry point: its query reads tables directly, and there is no
upstream action to read from.

Here ``load_orders_enriched`` names ``${catalog}.${bronze_schema}.orders`` and
``.customers`` in its ``FROM`` clause, so it belongs at the front of the
flowgroup as the source. Reach for a SQL load when your source *is* a query over
existing tables; reach for a SQL transform when you are reshaping a view a load
already produced.

Point the tokens at your environment
==========================================

The ``${...}`` tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_sql/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. To read the same tables
from a production catalog later, you write a ``prod.yaml`` with production
values — the query itself never changes.

Generate the pipeline
==========================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.32s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.33s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
========================================

Open ``generated/dev/silver_orders/orders_enriched.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_sql/generated/dev/silver_orders/orders_enriched.py
   :language: python
   :caption: generated/dev/silver_orders/orders_enriched.py
   :emphasize-lines: 21,49

Your query became a ``@dp.temporary_view`` named ``v_orders_enriched`` that wraps
``spark.sql(...)`` — with ``${catalog}`` and ``${bronze_schema}`` resolved to
``dev_catalog.bronze``. The write became a ``@dp.materialized_view`` that reads
that view with a batch ``spark.read.table(...)``. You wrote a query and named a
target; LHP wrote the view function and the declaration that connect them.

.. note::

   This query is a batch read of the tables it names. To read a source as a
   stream instead, wrap it in ``stream(...)`` inside the query — the same
   Databricks SQL construct the SQL transform guide covers in depth. A plain
   table name is read as a batch snapshot.

What you just did
======================

Twenty-seven lines of YAML — ten of them the ``SELECT`` you would write anyway —
compiled to fifty-one lines of Lakeflow Python. **Zero lines of PySpark from
you**: you never wrote ``@dp.temporary_view``, ``spark.sql``, or the
``@dp.materialized_view`` declaration. And the output is code you own — version
it, diff it, and open it in the Databricks editor like anything else.

Add a second query, join a third table, or point the same query at a production
catalog, and each is a few more lines of YAML — not another view function
written by hand every time.

What's next
================

- **See every SQL-load option.** The load action reference documents inline
  ``sql`` versus an external ``sql_path`` file, substitution inside ``.sql``
  files, and the constraint that exactly one of the two is set.
- **Reshape a view a load produced.** When the query reads a view already in the
  flowgroup rather than tables, that is a SQL transform — covered in the SQL
  transform guide.
- **Pull from an external database.** When the query has to run against an RDBMS
  rather than Unity Catalog tables, a JDBC load carries the connection and the
  query. See the load action reference.
