Configure and deploy bundles
============================

Use this with a project that already passes ``lhp validate``. You need a
Databricks CLI installation and authentication to deploy; local generation
does not require workspace access.

Configure resource inclusion
----------------------------

An LHP project created with bundle support already contains ``databricks.yml``.
For a project created with ``--no-bundle``, add it at the project root:

.. code-block:: yaml

   bundle:
     name: my_project
   include:
     - resources/*.yml
     - resources/lhp/*.yml
   targets:
     dev:
       mode: development
       default: true
       workspace:
         host: https://your-workspace-host

Replace the workspace host and use the same target name as the LHP environment.
The top-level ``resources`` glob includes generated jobs; ``resources/lhp``
includes generated pipeline resources. LHP leaves this bundle file under your
control. See :doc:`/reference/config/bundle` for the integration boundary.

Set pipeline defaults
---------------------

Create ``config/pipeline_config.yaml``:

.. code-block:: yaml

   project_defaults:
     catalog: "${catalog}"
     schema: "${bronze_schema}"
     serverless: true

Define those tokens in ``substitutions/dev.yaml``. Add per-pipeline documents
when individual pipelines need different settings; see the complete
:doc:`pipeline configuration reference </reference/config/pipeline-config>`.

Generate, validate and deploy
-----------------------------

From the project root:

.. code-block:: bash

   lhp validate --env dev -pc config/pipeline_config.yaml
   lhp generate --env dev -pc config/pipeline_config.yaml
   databricks bundle validate -t dev
   databricks bundle deploy -t dev

Inspect ``resources/lhp/*.pipeline.yml`` before deployment. Confirm the catalog,
schema, libraries and compute settings. Deployment creates or updates workspace
resources; run a pipeline or the generated job to process data.
Continue with :doc:`jobs` to schedule dependent pipelines, or use the
:doc:`sample deployment walkthrough </get-started/05-deploy-and-run>`.
