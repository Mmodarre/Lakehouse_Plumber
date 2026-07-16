====================
Deploy and run
====================

.. meta::
   :description: Deploy the generated Lakehouse Plumber pipelines to a Databricks workspace as a bundle, run the sample job, and re-run it to watch incremental ingestion and CDC advance.

You have generated Lakeflow code and bundle resources on disk. The last step is
shipping them to a Databricks workspace and running the job. Lakehouse Plumber's
work ends at ``generate``; the Databricks CLI takes it from here.

Before you deploy
=================

You need:

- A Databricks workspace with **Unity Catalog** and **serverless compute**
  enabled. The demo reads the read-only ``samples.tpch`` catalog present on every
  UC workspace.
- The **Databricks CLI**, authenticated to your workspace
  (``databricks auth login --host https://<your-workspace>``).
- The two edits from :doc:`02-configure-the-project`: your ``workspace.host`` and
  your ``catalog`` (set in both ``databricks.yml`` and ``substitutions/dev.yaml``).

Deploy
======

From the project root, validate the bundle and deploy it:

.. code-block:: console

   $ databricks bundle validate --target dev
   $ databricks bundle deploy   --target dev

``deploy`` uploads the generated Python (the ``sync.include`` glob in
``databricks.yml`` ships ``generated/dev/``) and registers the four pipelines and
the job in your workspace.

Run the job
===========

The sample ships a hand-authored job, ``sample_job``, that orchestrates
everything in order — a data-prep notebook first, then the pipelines:

.. literalinclude:: ../_fixtures/sample_project/resources/sample_job.yml
   :language: yaml
   :caption: resources/sample_job.yml
   :lines: 15-38

Run it:

.. code-block:: console

   $ databricks bundle run sample_job --target dev

The ``data_prep`` task seeds the demo — it creates the schemas and landing
volume, slices ``samples.tpch`` into files for Auto Loader, and builds the CDC
and snapshot source tables. Then ``ingest`` → ``silver`` → ``gold`` run in
sequence. When it finishes, query the results:

.. code-block:: sql

   -- Bronze: ingested raw tables
   SELECT count(*) FROM <your_catalog>.sample_bronze.orders_raw;

   -- Silver: cleansed orders and the SCD2 customer dimension
   SELECT count(*) FROM <your_catalog>.sample_silver.orders;
   SELECT count(*) FROM <your_catalog>.sample_silver.dim_customer;

   -- Gold: the aggregated materialized view
   SELECT * FROM <your_catalog>.sample_gold.sales_by_nation ORDER BY revenue DESC;

Run it again to see incremental behavior
=========================================

Re-run the same command — nothing else:

.. code-block:: console

   $ databricks bundle run sample_job --target dev

The data-prep notebook detects the previous run and advances the demo one round:
new files land (Auto Loader ingests **only the new ones**), the customer change
feed gets fresh updates and deletes (the SCD2 dimension grows history rows), and
a new supplier snapshot arrives. That's the incremental machinery you declared —
running end to end, on real Databricks compute.

What you just did
=================

You took a folder of YAML from an empty directory to a running, incrementally
updating medallion pipeline on Databricks — without hand-writing a line of
PySpark or bundle YAML. Everything you configured (the catalog, the workspace,
the compute) is in four files you now understand. That's the full lifecycle:
**declare, generate, deploy, run.**

.. tip::

   To tear the demo down: ``databricks bundle destroy --target dev``, then
   ``DROP SCHEMA <your_catalog>.sample_bronze CASCADE`` (and the silver/gold
   schemas). The serverless job is small but not free.

Next
====

You've run the whole thing from the terminal. Now see the same project in the
visual workbench — :doc:`06-explore-lhp-web`.
