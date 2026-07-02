.. meta::
   :description: Deploy and run the Lakehouse Plumber TPC-H sample project — scaffold it with lhp init --sample, generate the pipeline code, deploy the bundle, and watch SCD2 history grow across two job runs.

======================================
Tutorial: Run the TPC-H sample project
======================================

By the end of this tutorial you have deployed the Lakehouse Plumber (LHP)
sample project to your Databricks workspace and run its job twice: the first
run builds a bronze → silver → gold medallion architecture from the read-only
``samples.tpch`` dataset, and the second run feeds change data through the
silver dimensions so you can watch their history grow.

Requirements
============

- A Databricks workspace with Unity Catalog enabled. The sample reads the
  read-only ``samples.tpch`` catalog, which is available on every Unity
  Catalog workspace — no data upload needed.
- Serverless compute enabled. The notebook task and all three pipelines run
  serverless.
- A catalog you can ``CREATE`` in. The demo creates three schemas
  (``sample_bronze``, ``sample_silver``, ``sample_gold``) and one volume
  inside it.
- The Databricks CLI installed and authenticated
  (``databricks auth login --host https://<your-workspace>``).
- LHP installed (``pip install lakehouse-plumber``).

About the sample project
========================

The sample is a complete LHP project: three pipelines — ``sample_ingest``
(raw → bronze), ``sample_silver`` (cleansing and Type 2 slowly changing
dimensions, SCD2), and ``sample_gold`` (an aggregated materialized view) —
orchestrated by one job. A data-prep notebook (``notebooks/data_prep.py``)
runs as the first job task: it creates the schemas and the landing volume,
slices ``samples.tpch`` (the TPC-H decision-support benchmark dataset) into
per-round files, and seeds the Change Data Capture (CDC) source tables. LHP compiles the YAML in ``pipelines/`` into
Lakeflow Spark Declarative Pipelines (SDP) Python code, and the project
deploys as a Declarative Automation Bundle. Each job run executes one
notebook task and three pipeline updates on serverless compute — small, but
not free; the final step tears everything down.

Step 1: Scaffold the project
============================

#. Create an empty directory and change into it:

   .. code-block:: bash

      mkdir my_demo && cd my_demo

#. Scaffold the sample project — ``lhp init`` writes the files into the
   current directory:

   .. code-block:: bash

      lhp init my_demo --sample

#. List the directory:

   .. code-block:: bash

      ls

You see ``lhp.yaml``, ``databricks.yml``, ``pipelines/``, ``notebooks/``, and
a hand-authored job definition at ``resources/sample_job.yml``. That confirms
the scaffold is in place.

Step 2: Point the project at your workspace and catalog
=======================================================

#. In ``databricks.yml``, under ``targets.dev``, set ``workspace.host`` to
   your workspace URL. In the same file, set the ``catalog`` variable's
   ``default`` (it ships as ``main``) to a catalog you can ``CREATE`` in.

#. In ``substitutions/dev.yaml``, set ``catalog`` to the same value, and set
   ``landing_volume`` to ``/Volumes/<your-catalog>/sample_bronze/landing``.

.. important::

   The catalog lives in two places, and both edits are required. The
   ``catalog`` entry in ``substitutions/dev.yaml`` is a substitution token:
   ``lhp generate`` bakes it into the generated pipeline code (table names,
   volume paths). The ``catalog`` variable in ``databricks.yml`` is a
   Declarative Automation Bundles variable that the job passes to the
   data-prep notebook at run time. The two systems resolve their values
   independently — if they disagree, the notebook seeds one catalog while the
   pipelines read and write another, and the demo fails.

A third file, ``config/pipeline_config.yaml``, also names the catalog — but
through the ``${catalog}`` substitution token, so it follows your
``substitutions/dev.yaml`` edit automatically and needs no change.

Confirm both files name the same catalog:

.. code-block:: bash

   grep -n "catalog" databricks.yml substitutions/dev.yaml

The ``default:`` line from ``databricks.yml`` and the ``catalog:`` line from
``substitutions/dev.yaml`` show the same name. That is the success condition
for this step.

Step 3: Validate and generate the pipeline code
===============================================

#. Validate the configuration:

   .. code-block:: bash

      lhp validate --env dev -pc config/pipeline_config.yaml

#. Generate the SDP Python code:

   .. code-block:: bash

      lhp generate --env dev -pc config/pipeline_config.yaml

The ``-pc`` flag points at ``config/pipeline_config.yaml``, which supplies
the catalog, schema, and serverless compute settings for the generated
pipeline resources; it is required because the project is bundle-enabled. The
file ships ready to use — its values are ``${...}`` substitution tokens that
follow your Step 2 edits. For the full flag catalog, see the
:doc:`CLI reference </cli>`.

``lhp generate`` writes one Python file per FlowGroup under ``generated/dev/``
— seven files across the three pipeline directories, plus a
``custom_python_functions/`` subdirectory under ``sample_silver/`` holding
the helper modules its Python transforms import — and one
``<pipeline>.pipeline.yml`` per pipeline under ``resources/lhp/``. That
directory is LHP-managed and rewritten on every generate;
``resources/sample_job.yml`` is hand-authored and never touched. Open
``generated/dev/sample_silver/dim_customer.py``: it contains a
``dp.create_auto_cdc_flow(`` call with ``stored_as_scd_type=2``. That
confirms generation succeeded.

Step 4: Deploy the bundle
=========================

#. Deploy the project to your workspace:

   .. code-block:: bash

      databricks bundle deploy

When the command finishes, your workspace contains the sample job (deployed
name ``lhp_sample_quickstart``) and the three pipelines. The ``dev`` target
deploys in development mode, so resource names carry a
``[dev <your-user-name>]`` prefix.

Step 5: Run the job
===================

#. Run the deployed job:

   .. code-block:: bash

      databricks bundle run sample_job

The job runs four tasks in order: ``data_prep`` (notebook) → ``ingest`` →
``silver`` → ``gold``. On this first run the notebook creates the three
schemas and the landing volume, lands the round-1 files, and seeds the CDC
source tables (``customer_cdf`` and ``supplier_snapshots``); the three
pipelines then build the bronze, silver, and gold tables. The step is
complete when all four tasks succeed.

Step 6: Inspect the results
===========================

#. Query the tables the run created:

   .. code-block:: sql

      -- Bronze: ingested raw tables
      SELECT count(*) FROM <your-catalog>.sample_bronze.orders_raw;

      -- Silver: cleansed orders and the SCD2 customer dimension
      SELECT count(*) FROM <your-catalog>.sample_silver.orders;
      SELECT count(*) FROM <your-catalog>.sample_silver.dim_customer;

      -- Gold: the aggregated materialized view
      SELECT * FROM <your-catalog>.sample_gold.sales_by_nation
      ORDER BY revenue DESC;

The bronze and silver counts are non-zero, and ``sales_by_nation`` returns
rows with ``nation``, ``order_count``, and ``revenue`` columns.
``dim_customer`` carries the SCD2 history columns ``__START_AT`` and
``__END_AT`` — the next step creates the closed-out versions you can query
through them.

Step 7: Run the job again
=========================

#. Re-run the same command — nothing else changes:

   .. code-block:: bash

      databricks bundle run sample_job

   The notebook's ``round`` parameter defaults to ``auto``: it detects what
   the previous run produced and advances the demo one round. On round 2:

   - New files land under the landing volume's ``orders/round2/`` and
     ``lineitem/round2/`` directories, and Auto Loader (``cloudfiles``)
     ingests only the new files.
   - The notebook applies updates, inserts, and deletes to
     ``sample_bronze.customer_cdf``; the change feed drives the SCD2 write in
     ``dim_customer`` — updated customers get a new version row, and deleted
     customers have their current row closed out.
   - The notebook inserts supplier snapshot 2, and snapshot CDC advances the
     history of ``dim_supplier``.

#. Query the history that round 2 created:

   .. code-block:: sql

      -- Customers with closed-out versions (updated or deleted in round 2)
      SELECT c_custkey, c_name, __START_AT, __END_AT
      FROM <your-catalog>.sample_silver.dim_customer
      WHERE __END_AT IS NOT NULL
      ORDER BY c_custkey, __START_AT;

      -- Supplier history tracked from snapshot 1 to snapshot 2
      SELECT s_suppkey, s_acctbal, __START_AT, __END_AT
      FROM <your-catalog>.sample_silver.dim_supplier
      ORDER BY s_suppkey, __START_AT;

The first query now returns rows — customer versions closed out by round 2 —
and the supplier query shows superseded versions with ``__END_AT`` set next
to their current replacements. That is the SCD2 history growing.

Step 8: Clean up
================

#. Remove the deployed job and pipelines:

   .. code-block:: bash

      databricks bundle destroy

#. Drop the demo schemas:

   .. code-block:: sql

      DROP SCHEMA IF EXISTS <your-catalog>.sample_bronze CASCADE;
      DROP SCHEMA IF EXISTS <your-catalog>.sample_silver CASCADE;
      DROP SCHEMA IF EXISTS <your-catalog>.sample_gold CASCADE;

``databricks bundle destroy`` removes the job and the pipelines; the
``CASCADE`` drops the demo tables and the landing volume (it lives inside
``sample_bronze``). Your workspace is back where it started.

Learn more
==========

- :doc:`/cli` — the full flag catalog for ``lhp init``, ``lhp validate``, and
  ``lhp generate``
- :doc:`/quickstart` — build a pipeline from scratch
- :doc:`/configure_bundles` — Declarative Automation Bundles integration in
  depth
- :doc:`/substitutions` — how ``${token}`` substitution works
