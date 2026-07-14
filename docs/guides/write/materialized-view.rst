==========================
Write a materialized view
==========================

.. meta::
   :description: Declare a Databricks materialized view in Lakehouse Plumber — a query-defined, engine-maintained gold table — from one write action, without hand-writing the @dp.materialized_view decorator.

Let's produce a gold **materialized view** — a per-customer order summary — as a
query-defined, engine-maintained table. And let's do it without hand-writing the
``@dp.materialized_view`` decorator, the three-part table name, or the batch read
that wires the query to the table.

The streaming tables you built in Get Started append rows as data arrives: LHP
generates ``dp.create_streaming_table`` plus an ``@dp.append_flow`` that reads a
stream. A materialized view is different in kind. Its contents *are* a query —
you give Databricks a ``SELECT``, and the engine keeps the table current by
recomputing it from that query. Reach for a materialized view when the table is
batch-computed analytics — a gold aggregation or dashboard source — rather than
an incremental append. That "streaming table vs materialized view" decision has
more to it; this page covers one task: writing the materialized view.

The change is one line of intent. Instead of ``write_target.type:
streaming_table``, you declare ``write_target.type: materialized_view`` and hand
it the query. LHP writes the rest.

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a source table for the view to read — here, a silver
``orders`` table your gold summary aggregates. The materialized view is defined
entirely by its query, so this flowgroup needs no ``load`` and no upstream
view.

Declare the materialized view
=============================

A materialized view is a single ``write`` action whose ``write_target`` is typed
``materialized_view`` and carries the defining query inline as ``sql``. Create
``pipelines/gold_analytics.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_matview/pipelines/gold_analytics.yaml
   :language: yaml
   :caption: pipelines/gold_analytics.yaml
   :emphasize-lines: 15

The ``sql`` block is the view: a ``GROUP BY`` over ``${catalog}.${silver_schema}.orders``
that rolls each customer's rows into an order count, a total, and a last-order
timestamp. The ``${...}`` tokens resolve per environment, so the same flowgroup
targets ``dev``, staging, and prod unchanged. Because the query defines the
view, no action-level ``source`` is needed — the query *is* the source. The
write action reference covers the other ways to supply the query (a ``source``
view or an external ``sql_path`` file) and the tuning options (``refresh_policy``,
clustering, ``table_properties``, Unity Catalog ``tags``).

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ gold_analytics  0 files
   ✓ validate (0.34s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ gold_analytics  1 file
   ✓ generate (0.33s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/gold_analytics/customer_summary.py``. This is the entire
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_matview/generated/dev/gold_analytics/customer_summary.py
   :language: python
   :caption: generated/dev/gold_analytics/customer_summary.py
   :emphasize-lines: 18-22

Compare it to a streaming table. There's no ``dp.create_streaming_table`` and no
``@dp.append_flow`` — a materialized view needs neither. Instead LHP wrote one
``@dp.materialized_view`` decorator, resolved your catalog and schema tokens into
the three-part name ``dev_catalog.gold.customer_summary``, and generated a batch
``spark.sql(...)`` read of your query. You described the *table's definition*;
LHP wrote the *declaration*.

What you just did
=================

Twenty lines of YAML — seven of them your ``SELECT`` — compiled to thirty-four
lines of Lakeflow Python. **Zero lines of the ``@dp.materialized_view`` wiring
came from you**: not the decorator, not the three-part name, not the batch read.
Change the query and regenerate, and LHP rewrites the declaration to match — you
never touch the Python.

Point the same write action at ``streaming_table`` instead and you get the
append-flow shape from Get Started. That's the payoff: the *kind* of table is a
one-line declaration, and LHP owns the boilerplate for each kind.

What's next
===========

- **Tune the refresh and layout** — set ``refresh_policy`` or automatic liquid
  clustering on the same write target. See the write action reference for the
  full option set.
- **Factor out the shared shape** — when several gold views follow this same
  pattern, a template or preset stamps them out from short per-view configs.
