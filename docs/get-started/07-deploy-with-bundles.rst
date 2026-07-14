=====================
Deploy with bundles
=====================

.. meta::
   :description: Package your generated Lakehouse Plumber pipelines as a Databricks bundle and deploy them to a workspace with the databricks CLI.

You have pipelines on disk. The last step is getting them into a Databricks
workspace as a versioned, deployable unit — not a pile of notebooks pasted in by
hand. Lakehouse Plumber generates the bundle wiring for you; the Databricks CLI
ships it.

A project becomes a bundle project the moment a ``databricks.yml`` sits at its
root. From then on, ``lhp generate`` writes not only the Lakeflow Python but the
bundle **resources** that tell Databricks how to run it.

Declare the bundle
==================

``databricks.yml`` is the bundle definition — its name, where the generated
resources live, and one **target** per environment. The target name ``dev``
matches your ``substitutions/dev.yaml``:

.. literalinclude:: ../_fixtures/deploy_bundle/databricks.yml
   :language: yaml
   :caption: databricks.yml

The ``include`` globs pull in two folders: ``resources/lhp/`` (what LHP writes)
and ``resources/`` (anything you write by hand). LHP owns the first and leaves
the second alone.

Point generate at a pipeline config
===================================

On a bundle project, ``lhp generate`` needs the catalog and schema to stamp into
each bundle resource. Those come from a **pipeline config** file:

.. literalinclude:: ../_fixtures/deploy_bundle/config/pipeline_config.yaml
   :language: yaml
   :caption: config/pipeline_config.yaml

Skip it and generate stops before writing anything:

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✗ preflight (0.00s)
   LHP-CFG-023  Configuration Error
   pipeline_config is required for bundle validation

That's the safety net — a bundle project won't emit half-configured resources.
Pass the config with ``-pc`` and it runs clean, with an extra ``bundle_sync``
step at the end:

.. code-block:: console

   $ lhp generate --env dev -pc config/pipeline_config.yaml
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.35s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   ✓ bundle_sync (0.01s)
   1 pipeline generated · 1 file · 0.4s

Read the generated resource
===========================

Alongside the usual Lakeflow Python, LHP wrote a bundle resource — one per
pipeline, under ``resources/lhp/``. Your ``catalog`` and ``schema`` are resolved
and embedded; the ``${workspace...}`` and ``${bundle...}`` variables are left for
the Databricks CLI to fill at deploy time:

.. literalinclude:: ../_fixtures/deploy_bundle/resources/lhp/bronze_ingest.pipeline.yml
   :language: yaml
   :caption: resources/lhp/bronze_ingest.pipeline.yml
   :lines: 1-22

Below these lines LHP appends a commented catalogue of every other pipeline
option, trimmed here. This folder is wiped and regenerated on every ``generate``,
so never hand-edit it — put custom resources in the top-level ``resources/``
instead.

Ship it
=======

Generation is where Lakehouse Plumber's job ends and the Databricks CLI takes
over. From the project root:

.. code-block:: console

   $ databricks bundle validate --target dev
   $ databricks bundle deploy   --target dev
   $ databricks bundle run bronze_ingest_pipeline --target dev

``deploy`` uploads the generated Python and registers the pipeline; ``run``
triggers it. The run target ``bronze_ingest_pipeline`` is the resource key LHP
wrote in the file above.

What you just did
=================

You turned a folder of generated Python into a deployable Databricks bundle
without hand-writing a line of bundle YAML. LHP wrote the resource, resolved your
catalog and schema into it, and left the workspace variables for deploy time. One
``lhp generate``, one ``databricks bundle deploy``, and the same pipelines you
built in dev ship to any target you define.

Where to go next
================

You've climbed the whole ladder: a first pipeline, then a transform and a quality
gate, then presets, templates, and blueprints for reuse, and now deployment. From
here the **Guides** go deep on each piece — every ingestion source, every write
mode, quarantine, monitoring, CI/CD — and the **Concepts** section explains the
model underneath. Reach for them as the work demands.
