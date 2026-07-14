.. Lakehouse Plumber documentation master file

=================
Lakehouse Plumber
=================

.. meta::
   :description: Lakehouse Plumber turns concise YAML into readable Lakeflow Spark Declarative Pipelines — declare your ETL instead of hand-writing thousands of lines of PySpark.

**ETL at scale, the way it should be.**

Managing dozens of Lakeflow pipelines means writing — and rewriting — thousands
of lines of near-identical PySpark. The patterns repeat; only the table names
change. Lakehouse Plumber turns concise YAML **actions** into fully-featured
Lakeflow Spark Declarative Pipelines. You describe *what* each pipeline is —
a source to read, a table to write; LHP writes the *how* as plain, readable
Python you own and open in the Databricks editor. **Declare your ETL, don't
hand-write it.**

See it
======

The same ingest, three ways: the YAML you write, the Lakeflow code Lakehouse
Plumber generates from it, and the two commands that turn one into the other.

.. tab-set::

   .. tab-item:: You write — YAML

      Eighteen lines describe a ``load`` and a ``write``. The ``${...}`` tokens
      resolve per environment, so the same flowgroup runs unchanged in dev and
      prod.

      .. literalinclude:: _fixtures/first_pipeline/pipelines/bronze_ingest.yaml
         :language: yaml
         :caption: pipelines/bronze_ingest.yaml

   .. tab-item:: LHP generates — Lakeflow

      Ordinary Lakeflow Python — a temporary view for the source, a streaming
      table for the target, an append flow wiring them together. Nothing hidden
      behind a runtime; you could have written it by hand, but you didn't.

      .. literalinclude:: _fixtures/first_pipeline/generated/dev/bronze_ingest/orders_ingest.py
         :language: python
         :caption: generated/dev/bronze_ingest/orders_ingest.py

   .. tab-item:: You run — CLI

      Validate resolves every token and checks the actions; generate writes the
      Python.

      .. code-block:: console

         $ lhp validate --env dev
         ✓ discover (0.01s)
         ✓ bronze_ingest  0 files
         ✓ validate (0.38s)
         1 validated · 0.4s

         $ lhp generate --env dev
         ✓ discover (0.01s)
         ✓ bronze_ingest  1 file
         ✓ generate (0.41s)
         1 pipeline generated · 1 file · 0.5s

The payoff compounds. Adding a data-quality check, a second table, or a CDC
merge is a few more lines of YAML — not another fifty lines of Python each time.
The :doc:`Get Started course <get-started/index>` grows this one example into a
reusable, deployable project, one primitive at a time.

The model
=========

Every pipeline follows the same shape: load a source, apply zero or more
transforms, write a target.

.. mermaid::

   graph LR
       A[Load] --> B{0..N Transform}
       B --> C[Write]

Where to next
=============

.. grid:: 1 1 2 2
   :gutter: 3

   .. grid-item-card:: Get Started →
      :link: get-started/index
      :link-type: doc

      From an empty folder to a reusable, deployable pipeline — one primitive at
      a time. Every step is fixture-backed and generates the code it shows.

   .. grid-item-card:: Reference →
      :link: reference/index
      :link-type: doc

      CLI flags, the Python API, every action option, error codes, and
      configuration schemas. Pure lookup, generated from the code itself.

Task-shaped recipes live in the :doc:`Guides <guides/index>`; the reasoning
behind LHP's design lives in :doc:`Concepts <concepts/index>`.

.. toctree::
   :maxdepth: 1
   :caption: Documentation

   get-started/index
   guides/index
   concepts/index
   reference/index
