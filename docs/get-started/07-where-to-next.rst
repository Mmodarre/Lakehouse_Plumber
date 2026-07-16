================
Where to next
================

.. meta::
   :description: After the Lakehouse Plumber Get Started course — start your own project, then explore the features the sample didn't cover: data tests and reporting, monitoring, quarantine, the sandbox, the web IDE, and coding agents.

You've run the sample end to end. You know the four config files, the action
model, the four write modes, and the deploy lifecycle. Two things are left: how
to start your **own** project, and where to go for the features the sample
didn't cover.

Start your own project
======================

Scaffold an empty project instead of the sample:

.. code-block:: bash

   mkdir my_project
   cd my_project
   lhp init my_project          # add --no-bundle to skip Databricks packaging

Then, using what the sample showed you:

- Set your environment values in ``substitutions/dev.yaml`` — the ``${...}``
  tokens your flowgroups reference.
- Add flowgroups under ``pipelines/``, one YAML file per pipeline.
- If it's a bundle project, keep ``config/pipeline_config.yaml`` and pass it with
  ``-pc`` on every ``generate``.
- Reach for :doc:`presets, templates, and blueprints </concepts/presets-templates-blueprints>`
  once a pattern starts repeating.

The :doc:`Guides </guides/index>` go deep on each task — every ingestion source,
every write mode, every transform type.

Features the sample didn't cover
=================================

The sample project is deliberately compact. Lakehouse Plumber does more — here
is what you skipped, and where to learn it when you need it.

Data tests and reporting
------------------------

Beyond the ``data_quality`` transform (which gates rows *within* a flow), LHP has
a fourth action kind — **test** actions — for asserting things about your data:
uniqueness, row counts, referential integrity, and more. Test results can be
**published** to an external system like Azure DevOps Test Plans or a Delta audit
table.

- :doc:`Write a data test </guides/test/data-tests>`
- :doc:`Report test results to an external system </guides/test/test-reporting>`

Monitoring and logging
----------------------

The sample enabled monitoring in ``lhp.yaml`` and LHP generated a monitoring
pipeline — but the course didn't dig in. LHP can consolidate every pipeline's
event log into one place so you can watch health, throughput, and data-quality
metrics across all your pipelines.

- :doc:`Monitor every pipeline from one place </guides/ops/monitoring>`
- :doc:`See how your pipelines connect </guides/ops/dependency-analysis>`

Keep bad data instead of dropping it
------------------------------------

The sample drops rows that fail expectations (``mode: dqe``). When a failed row
is something you can't silently lose, **quarantine** it instead — route it to a
dead-letter table you can inspect, fix, and recycle back into the pipeline.

- :doc:`Quarantine bad rows instead of dropping them </guides/transform/quarantine>`

Develop faster
--------------

You edited YAML in a text editor and ran the CLI. LHP has richer authoring
surfaces:

- :doc:`Develop in a sandbox </guides/develop/sandbox>` — an isolated, scoped
  copy of your pipelines for safe iteration.
- :doc:`Develop in the local web IDE </guides/develop/web-ide>` — a browser UI
  with a graph view, form editing, and live generation.
- :doc:`Get authoring help from the in-IDE assistant </guides/develop/ai-assistant>`.
- :doc:`Drive Lakehouse Plumber from a coding agent </guides/develop/coding-agents>`.

Scale and ship
--------------

- :doc:`Fan one blueprint across tenants and regions </guides/reuse-and-scale/blueprints>`
  — generate whole pipelines from a single pattern.
- :doc:`Validate and ship pipelines in CI </guides/ship/ci-cd>`.
- :doc:`Migrate an existing DLT pipeline to Lakehouse Plumber </guides/ship/migrate-from-dlt>`.

Understand the model
====================

If you want the reasoning underneath what you just did, the
:doc:`Concepts </concepts/index>` section explains how Lakehouse Plumber
compiles pipelines, why dependencies resolve the way they do, and how the reuse
ladder is meant to be climbed.
