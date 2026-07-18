=======================
Install and scaffold
=======================

.. meta::
   :description: Install Lakehouse Plumber and scaffold the ready-to-run TPC-H sample project in two commands — a complete medallion pipeline you can read, generate, and deploy.

Two commands and you have a complete, runnable project on disk.

Rather than build a pipeline line by line, this course starts from the **sample
project** Lakehouse Plumber ships — a compact medallion architecture
(bronze → silver → gold) over the ``samples.tpch`` dataset that exists in every
Unity Catalog workspace. You'll read it, generate its Lakeflow code, configure it
for your workspace, and deploy it. By the end you'll know every moving part of an
LHP project well enough to start your own.

Install
=======

Lakehouse Plumber is a Python package. Install it with pip:

.. code-block:: bash

   pip install lakehouse-plumber

That gives you the ``lhp`` command. Check it:

.. code-block:: bash

   lhp --version

Scaffold the sample project
===========================

``lhp init`` scaffolds into the **current directory** — it does not create a
folder for you. Make an empty one, ``cd`` into it, and initialize with
``--sample``:

.. code-block:: bash

   mkdir sample_lakehouse
   cd sample_lakehouse
   lhp init sample_lakehouse --sample

The ``--sample`` flag scaffolds the ready-to-run TPC-H demo (a plain
``lhp init sample_lakehouse`` gives you an empty project skeleton instead). The
name you pass — ``sample_lakehouse`` here — is baked into the bundle name and
``lhp.yaml``.

.. note::

   ``--sample`` builds a **bundle-enabled** project, because the demo deploys to
   Databricks. That is why it is incompatible with ``--no-bundle``. You'll wire
   the bundle up to your workspace in :doc:`02-configure-the-project`.

What LHP created
================

The scaffold is a complete, working project. Here is the shape of it:

.. code-block:: text

   sample_lakehouse/
   ├── lhp.yaml                     # project config: operational metadata, event log, monitoring
   ├── README.md                    # the three-step quickstart, in the project itself
   ├── databricks.yml               # bundle definition: workspace host + catalog variable
   ├── substitutions/
   │   ├── dev.yaml                 # per-environment values behind every ${...} token
   │   └── prod.yaml
   ├── config/
   │   └── pipeline_config.yaml     # catalog/schema + compute for the generated pipelines
   ├── presets/                     # reusable config fragments (bronze_layer, silver_quality)
   ├── templates/                   # one parametrized ingestion pattern (cloudfiles_ingest)
   ├── pipelines/
   │   ├── 01_ingest/               # raw → bronze  (cloudfiles + delta loads)
   │   ├── 02_silver/               # cleansing + SCD2 dimensions
   │   └── 03_gold/                 # aggregated materialized view
   ├── schemas/  schema_transforms/  expectations/  uc_tags/  sql/  transforms/  functions/
   ├── notebooks/data_prep.py       # seeds the demo data (runs first in the job)
   └── resources/sample_job.yml     # the job that orchestrates it all

Three folders do the day-to-day work:

- ``pipelines/`` holds your **flowgroups** — the YAML that describes each
  pipeline. Everything else exists to serve these.
- ``substitutions/`` holds per-environment values that fill the ``${...}`` tokens
  in your flowgroups, so one flowgroup runs unchanged in dev and prod.
- ``generated/`` is where ``lhp generate`` writes readable Lakeflow Python. You
  never hand-edit it; it doesn't exist yet, because you haven't generated.

The rest — ``presets/``, ``templates/``, ``config/``, ``databricks.yml`` — is
where reuse, compute, and deployment are configured. The next page opens each of
those files and shows you what it controls.

Next
====

You have a complete project, but it points at a placeholder catalog and
workspace. Before you generate anything, learn where those settings live —
:doc:`02-configure-the-project`.
