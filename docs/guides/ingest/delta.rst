=========================
Ingest from a Delta table
=========================

.. meta::
   :description: Ingest an existing Unity Catalog Delta table into a downstream streaming table with a declarative Lakehouse Plumber load action — no spark.readStream.table boilerplate by hand.

Not every source is a pile of raw files. Often the data you need already lives
in a Delta table another team publishes in Unity Catalog — a bronze ``orders``
table — and your job is to read it and land it downstream in your own silver
layer.

You could hand-write ``spark.readStream.table("catalog.schema.orders")``, then a
``create_streaming_table``, then an ``append_flow`` to wire them together. Or you
declare a ``type: delta`` load and let Lakehouse Plumber write that Lakeflow for
you. That is the idea on every page: **declare your ETL, don't hand-write it.**

Let's ingest an existing bronze ``orders`` Delta table into a silver streaming
table, streaming, without touching PySpark.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide assumes the bronze ``orders`` Delta table already exists in
Unity Catalog — a Delta load reads an existing table, it does not create the
source.

Declare the Delta load
======================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that reads the Delta
table into a view, and a ``write`` that appends the view into a streaming table.

Create ``pipelines/silver_orders.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_delta/pipelines/silver_orders.yaml
   :language: yaml
   :caption: pipelines/silver_orders.yaml

The ``load`` action names the source with three parts — ``catalog``, ``schema``,
and ``table`` — which resolve to the fully qualified ``${catalog}.${bronze_schema}.orders``.
``readMode: stream`` is the field that decides how the table is read: ``stream``
generates ``spark.readStream.table(...)`` for continuous ingestion, and ``batch``
generates ``spark.read.table(...)`` for a one-time load. The ``write`` action
appends the resulting view into a silver streaming table named ``orders``.

The ``${...}`` tokens are placeholders you resolve per environment, so the same
flowgroup runs unchanged in dev, staging, and prod.

Point the tokens at your environment
====================================

Those tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_delta/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. To read the same bronze
table from a production catalog later, you write a ``prod.yaml`` with production
values — the flowgroup itself never changes.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

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
   ✓ generate (0.40s)
   ✓ format (0.06s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.5s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/silver_orders/orders_from_delta.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_delta/generated/dev/silver_orders/orders_from_delta.py
   :language: python
   :caption: generated/dev/silver_orders/orders_from_delta.py

It's ordinary Lakeflow. Because you set ``readMode: stream``, the source view is a
``@dp.temporary_view`` wrapping ``spark.readStream.table("dev_catalog.bronze.orders")``.
The target is a ``dp.create_streaming_table``, and a ``@dp.append_flow`` streams
the view into it. You described a source table and a target table; LHP wrote the
three pieces of plumbing that connect them.

Switch to a batch read
=======================

Streaming is the default path for continuous ingestion, but the same load reads
a one-time snapshot when you change one field. Set ``readMode: batch`` (or drop
the field — ``batch`` is the default) and LHP generates ``spark.read.table(...)``
in place of ``spark.readStream.table(...)``. Everything else in the flowgroup
stays the same. That is the declarative payoff: you change *intent* in one line,
not the generated Python.

.. note::

   A Delta load reads a table that already exists in Unity Catalog. It does not
   create the source table — that is the job of whatever pipeline writes it. If
   the table is missing at run time, the pipeline fails when it runs, not when
   LHP generates the code.

What you just did
=================

Twenty lines of YAML. Forty-six lines of Lakeflow Python. **Zero lines of PySpark
from you** — you never wrote ``readStream.table``, ``create_streaming_table``, or
``append_flow``. And the output is code you own: version it, diff it, and open it
in the Databricks editor like anything else.

The payoff compounds. Reading a second Delta table, switching to a batch
snapshot, or adding a transform is a few more lines of YAML — not another
forty-six lines of Python each time.

What's next
===========

- **Read the Change Data Feed.** The same Delta source reads row-level changes
  (inserts, updates, deletes) instead of the current table state — set
  ``options: {readChangeFeed: "true"}`` on the source and pair it with a starting
  bound. See the CDC patterns guide.
- **See every Delta option.** ``options`` also covers time travel
  (``versionAsOf`` / ``timestampAsOf``), ``ignoreDeletes``, ``skipChangeCommits``,
  and more. The load action reference lists them all with their constraints.
- **Add a transform and a quality gate.** Reshape the rows and drop or quarantine
  bad records before they reach the silver table.
