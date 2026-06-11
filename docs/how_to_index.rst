Overview
========

.. meta::
   :description: Problem-indexed how-to pages for Lakehouse Plumber — decide between primitives, ingest data, operate pipelines, reuse patterns, and deploy.

If you have a specific data-engineering problem and want Lakehouse Plumber (LHP) to
solve it, start here. Pick a category below and jump to the task.

Decide
------

* :doc:`decisions` — Choose between Preset, Template, and Blueprint; pick a load
  source, write target, and write mode.

Ingest data
-----------

* :doc:`ingest_with_autoloader` — Stream files from cloud object storage with
  Auto Loader (CloudFiles).
* :doc:`pipeline_patterns` — Apply multi-source fan-in, path filtering, and other
  recipes from the patterns cookbook.

Operate and monitor
-------------------

* :doc:`enable_monitoring` — Set up centralized event-log monitoring across every
  pipeline in your project.
* :doc:`quarantine_records` — Route failed rows to a Dead Letter Queue (DLQ) and
  recycle corrected rows back into the pipeline.

Reusability and patterns
------------------------

* :doc:`multi_flowgroup_guide` — Combine multiple FlowGroups in a single YAML file
  with shared settings and inheritance.
* :doc:`dynamic_templates_guide` — Use Jinja2 conditionals, loops, and filters in
  Templates for advanced parameter patterns.

Develop in parallel
-------------------

* :doc:`develop_in_a_sandbox` — Generate a personal, namespaced copy of your
  pipelines so parallel developers never collide on shared tables.

Deploy
------

* :doc:`configure_bundles` — Enable Declarative Automation Bundle integration for
  environment-specific deployments.
* :doc:`package_pipelines_as_wheels` — Package a pipeline as a deterministic Python
  wheel to cut workspace sync and deploy cost.
* :doc:`cicd` — Apply CI/CD patterns and DataOps practices for GitHub Actions,
  Azure DevOps, and Bitbucket Pipelines.

See also
--------

* :doc:`architecture` — Explanation of the LHP execution model and why generation
  works the way it does.
* :doc:`actions/index` — Reference catalog of every Load, Transform, Write, and
  Test action.
