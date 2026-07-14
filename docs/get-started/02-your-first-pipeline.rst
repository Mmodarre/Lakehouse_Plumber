====================
Your first pipeline
====================

.. meta::
   :description: Build your first Lakehouse Plumber pipeline — ingest CSV files into a bronze streaming table from a few lines of declarative YAML, no PySpark by hand.

Let's build a pipeline that ingests raw CSV order files into a bronze streaming
table in Unity Catalog — and let's do it without hand-writing a single line of
PySpark.

You describe *what* the pipeline is: a source to read, a table to write.
Lakehouse Plumber writes the *how* — the ``readStream``, the streaming-table
declaration, the append flow — as plain, readable Lakeflow code you own and can
open in the Databricks editor. That is the whole idea: **declare your ETL,
don't hand-write it.**

By the end of this page you'll have a real, generated pipeline on disk, built
from under two dozen lines of YAML.

Before you start
================

You need Lakehouse Plumber installed and an empty project to work in:

.. code-block:: bash

   pip install lakehouse-plumber

   mkdir first_pipeline
   cd first_pipeline
   lhp init first_pipeline --no-bundle

``lhp init`` scaffolds the project into the *current* directory, so create the
folder and ``cd`` into it first. ``--no-bundle`` keeps this first project
minimal — you'll add Databricks bundle packaging later, when you deploy.

Declare the ingest
==================

A pipeline in Lakehouse Plumber is a **flowgroup**: a small YAML file describing
a sequence of **actions**. Your first flowgroup has exactly two — one ``load``
and one ``write``.

Create ``pipelines/bronze_ingest.yaml``:

.. literalinclude:: ../_fixtures/first_pipeline/pipelines/bronze_ingest.yaml
   :language: yaml
   :caption: pipelines/bronze_ingest.yaml

Read it top to bottom and it says what it does. The ``load`` action streams CSV
files from a landing volume with Auto Loader into a view, ``v_orders_raw``. The
``write`` action appends that view into a streaming table named ``orders``. The
``${...}`` tokens — ``${catalog}``, ``${bronze_schema}``, ``${landing_path}`` —
are placeholders you resolve per environment, so the same flowgroup runs
unchanged in dev, staging, and prod.

Point the tokens at your environment
====================================

Those tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../_fixtures/first_pipeline/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. To ship to production
later, you write a ``prod.yaml`` with production values — the flowgroup itself
never changes.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  0 files
   ✓ validate (0.38s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.41s)
   ✓ format (0.07s)
   1 pipeline generated · 1 file · 0.5s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_ingest/orders_ingest.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../_fixtures/first_pipeline/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py

It's ordinary Lakeflow: a ``@dp.temporary_view`` for the source, a
``dp.create_streaming_table`` for the target, and a ``@dp.append_flow`` that
wires them together. You could have written this by hand — but you didn't, and
you won't for the next fifty tables either.

What you just did
=================

Eighteen lines of YAML. Fifty lines of Lakeflow Python. **Zero lines of PySpark
from you** — you never touched ``readStream``, ``create_streaming_table``, or
``append_flow``. And the output is code you own: version it, diff it, and open
it in the Databricks editor like anything else.

The payoff compounds. Adding a data-quality check, a second table, or a CDC
merge is a few more lines of YAML — not another fifty lines of Python each time.
That's ETL at scale: write the pattern once, and Lakehouse Plumber repeats the
plumbing.

What's next
===========

- **Add a transform and a data-quality check** — reshape the rows and quarantine
  bad records before they reach the table.
- **Reuse the pattern with a template** — turn this flowgroup into a template and
  stamp out twenty tables from twenty short configs.

.. note::

   These next pages are part of the rebuilt Get Started course, landing next.
   This page is an early preview of the new documentation voice.
