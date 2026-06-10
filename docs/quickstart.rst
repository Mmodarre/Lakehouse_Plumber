Quickstart
==========

.. meta::
   :description: Build your first Lakehouse Plumber pipeline in about ten minutes. Scaffold a project, declare a FlowGroup against a Databricks samples table, and generate Lakeflow Declarative Pipelines Python code.

Build your first Lakehouse Plumber (LHP) pipeline in about ten minutes. This
walk-through uses the ``samples.tpch.customer_sample`` table that ships with every
Databricks workspace, so you do not need to upload data first. By the end you have
a generated Lakeflow Declarative Pipelines Python file you can run in Databricks.

Prerequisites
-------------

* Python 3.11 or later (3.12 recommended).
* A Databricks workspace with Unity Catalog enabled.
* Read access to the ``samples`` catalog. Verify with:

  .. code-block:: sql

     SELECT 1 FROM samples.tpch.customer_sample LIMIT 1;

  If the ``samples`` catalog is unavailable in your tenant (some sovereign-cloud
  workspaces — for example GovCloud and China — do not provision it), follow
  :doc:`ingest_with_autoloader` instead for an Auto Loader walk-through against
  your own landing volume.
* A catalog and schema you can write to. Note the names — the substitutions file
  in Step 3 needs them.

Step 1 — Install LHP
--------------------

Create a virtual environment and install the CLI:

.. code-block:: bash

   python -m venv .venv
   source .venv/bin/activate
   pip install lakehouse-plumber

Confirm the install:

.. code-block:: bash

   lhp --version

Step 2 — Scaffold a project
---------------------------

Run ``lhp init`` from an empty directory:

.. code-block:: bash

   mkdir my_first_pipeline && cd my_first_pipeline
   lhp init my_first_pipeline

The default ``lhp init`` enables :term:`Databricks Asset Bundle <DAB>` (DAB) integration. To
opt out, pass ``--no-bundle``.

The command creates these directories:

* ``pipelines/`` — your FlowGroup YAML files.
* ``substitutions/`` — per-environment token files (``dev``, ``tst``, ``prod``).
* ``templates/`` — reusable FlowGroup templates (Jinja2).
* ``presets/`` — Action defaults applied by type.
* ``schemas/`` and ``expectations/`` — schema files and data-quality rules.
* ``generated/`` — destination for generated Python code.
* ``resources/lhp/`` — DAB resource definitions (bundle mode only).

It also creates ``lhp.yaml`` (the project file), ``databricks.yml`` (DAB
manifest, bundle mode), and a ``.vscode/`` directory wired for YAML
IntelliSense.

You see a success panel like this:

.. code-block:: text

   ╭─────────────────────────────────────────────────────────────────────╮
   │ ✓ Initialized Databricks Asset Bundle project: my_first_pipeline    │
   │ Created directories: presets, templates, pipelines, substitutions,  │
   │ schemas, expectations, generated, config, resources                 │
   │ Example files: presets/bronze_layer.yaml,                           │
   │ templates/standard_ingestion.yaml, databricks.yml                   │
   │ VS Code IntelliSense automatically configured for YAML files        │
   │                                                                     │
   │ Next steps:                                                         │
   │   # Create your first pipeline                                      │
   │   mkdir pipelines/my_pipeline                                       │
   │   # Add flowgroup configurations                                    │
   │   # Deploy bundle with: databricks bundle deploy                    │
   ╰─────────────────────────────────────────────────────────────────────╯

The example ``presets/`` and ``templates/`` files ship as ``.tmpl`` to prevent
them from running until you adopt them. Leave them in place for now.

Step 3 — Configure ``substitutions/dev.yaml``
---------------------------------------------

The init step creates ``substitutions/dev.yaml.tmpl`` with placeholders. Rename
it and replace the catalog and schema values:

.. code-block:: bash

   mv substitutions/dev.yaml.tmpl substitutions/dev.yaml

Open ``substitutions/dev.yaml`` and set ``catalog`` and ``bronze_schema`` to the
catalog and schema you can write to:

.. code-block:: yaml
   :caption: substitutions/dev.yaml

   dev:
     env: dev
     catalog: my_catalog
     bronze_schema: my_bronze_schema
     silver_schema: my_silver_schema
     gold_schema: my_gold_schema
     landing_path: /Volumes/my_catalog/my_bronze_schema/landing

LHP substitutes ``${catalog}`` and ``${bronze_schema}`` into your FlowGroup at
generate time. The substitution syntax in YAML and SQL is ``${token}``.

Step 4 — Write your first FlowGroup
-----------------------------------

Create ``pipelines/customer_sample.yaml``:

.. code-block:: yaml
   :caption: pipelines/customer_sample.yaml

   pipeline: tpch_sample_ingestion
   flowgroup: customer_ingestion

   actions:
     - name: customer_sample_load
       type: load
       readMode: stream
       source:
         type: delta
         database: "samples.tpch"
         table: customer_sample
       target: v_customer_sample_raw
       description: "Load customer sample table from the Databricks samples catalog"

     - name: write_customer_sample_bronze
       type: write
       source: v_customer_sample_raw
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: "tpch_sample_customer"
       description: "Write customer sample to bronze"

The file declares one :term:`Pipeline` (``tpch_sample_ingestion``) containing one
:term:`FlowGroup` (``customer_ingestion``) with two :term:`Actions <Action>`: a streaming ``load`` from
the samples catalog and a ``write`` into your bronze schema.

Step 5 — Validate
-----------------

Run validation before generating code:

.. code-block:: bash

   lhp validate --env dev

A passing run ends with a footer line like:

.. code-block:: text

   Validated 1 pipeline — all passed

If validation fails, LHP prints a structured error with the file, action name,
and a suggestion. Fix the reported issue and run ``lhp validate`` again.

Step 6 — Generate Python code
-----------------------------

Run code generation:

.. code-block:: bash

   lhp generate --env dev

LHP writes the generated file to
``generated/tpch_sample_ingestion/customer_ingestion.py`` and prints a footer
line like:

.. code-block:: text

   Generated 1 files in 0.4s — all 1 pipeline passed

Inspect the file. The body looks like this:

.. code-block:: python
   :caption: generated/tpch_sample_ingestion/customer_ingestion.py

   # Generated by LakehousePlumber
   # Pipeline: tpch_sample_ingestion
   # FlowGroup: customer_ingestion

   from pyspark import pipelines as dp

   PIPELINE_ID = "tpch_sample_ingestion"
   FLOWGROUP_ID = "customer_ingestion"

   @dp.temporary_view()
   def v_customer_sample_raw():
       """Load customer sample table from the Databricks samples catalog"""
       df = spark.readStream.table("samples.tpch.customer_sample")
       return df

   dp.create_streaming_table(
       name="my_catalog.my_bronze_schema.tpch_sample_customer",
       comment="Streaming table: tpch_sample_customer",
       table_properties={
           "delta.autoOptimize.optimizeWrite": "true",
           "delta.enableChangeDataFeed": "true",
       },
   )

   @dp.append_flow(
       target="my_catalog.my_bronze_schema.tpch_sample_customer",
       name="f_write_customer_sample_bronze",
       comment="Write customer sample to bronze",
   )
   def f_write_customer_sample_bronze():
       df = spark.readStream.table("v_customer_sample_raw")
       return df

The generated file is a standard Lakeflow Declarative Pipelines script. The
``${catalog}`` and ``${bronze_schema}`` tokens are resolved against
``substitutions/dev.yaml``.

Step 7 — Run it in Databricks
-----------------------------

You have two ways to run the generated code:

**Bundle deploy.** Because ``lhp init`` enabled DAB by default, your project
already has a ``databricks.yml`` and matching resource definitions under
``resources/lhp/``. From the project root, deploy with the Databricks CLI:

.. code-block:: bash

   databricks bundle deploy --target dev
   databricks bundle run tpch_sample_ingestion --target dev

**Manual.** Sync the ``generated/`` folder to your workspace (via Repos or the
Databricks CLI), create a Lakeflow Declarative Pipeline in the UI, and point
its source field at ``generated/tpch_sample_ingestion``.

.. tip::

   Re-run ``lhp generate --env dev`` whenever you change a FlowGroup to refresh
   the generated Python.

Next steps
----------

You now have a working first pipeline. To go further, see :doc:`how_to_index`
for task-shaped recipes — adding transforms, ingesting with Auto Loader,
attaching data-quality expectations, layering presets, and deploying bundles
across environments. To understand how the YAML maps to generated Python and
why LHP designs FlowGroups and Actions this way, read :doc:`architecture`.
