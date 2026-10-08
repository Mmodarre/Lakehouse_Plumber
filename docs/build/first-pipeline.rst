Build your own pipeline
=======================

Use this after the :doc:`sample course </get-started/index>` to build with
your own data. This example reads CSV files and writes a bronze streaming table.
You need LHP installed; generation works locally without a running Spark cluster.

Create an empty project
-----------------------

.. code-block:: bash

   mkdir my_project
   cd my_project
   lhp init my_project --no-bundle

This starts with local code generation. Add the bundle configuration in
:doc:`/develop/bundles` when you are ready to deploy.

Set the environment
-------------------

Edit ``substitutions/dev.yaml`` with your catalog, bronze schema and landing
volume. These must be accessible to the pipeline's run-as identity when deployed.

.. literalinclude:: ../_fixtures/first_pipeline/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

``${catalog}`` in an action uses the ``catalog`` value inside the selected
``dev`` mapping. See :doc:`configure` for the other configuration scopes.

Declare one flowgroup
---------------------

Create ``pipelines/bronze_ingest.yaml``:

.. literalinclude:: ../_fixtures/first_pipeline/pipelines/bronze_ingest.yaml
   :language: yaml
   :caption: pipelines/bronze_ingest.yaml

``pipeline`` names the generated pipeline. ``flowgroup`` names this group of
actions. The load creates ``v_orders_raw`` and the write consumes it. A pipeline
can contain several flowgroups and a file can contain several YAML documents;
one file per flowgroup is a useful starting convention.

For another input, use the :doc:`source chooser </guides/ingest/index>`.
For exact fields, use :doc:`/reference/actions/load/cloudfiles` and
:doc:`/reference/actions/write`.

Validate and inspect
--------------------

Run from the project root:

.. code-block:: bash

   lhp validate --env dev
   lhp generate --env dev

Open ``generated/dev/bronze_ingest/orders_ingest.py``. Check the resolved input
path, catalog and schema before deploying. Generation produces Python and does
not read your source data or create tables in Databricks.

Continue with :doc:`/develop/bundles`. As the project grows, add
:doc:`transforms </guides/transform/index>`,
:doc:`quality checks </guides/test/index>` and :doc:`compose`.
When configuration repeats, introduce :doc:`/guides/reuse-and-scale/presets`.
