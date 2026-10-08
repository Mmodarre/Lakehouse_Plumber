Generate and schedule orchestration jobs
========================================

LHP can generate Databricks jobs from pipeline dependencies. First generate the
pipeline resources using :doc:`bundles`. Give related flowgroups a ``job_name``
when you want explicit job grouping; see :doc:`/reference/config/flowgroups`.

Create ``config/job_config.yaml`` with defaults and optional job overrides:

.. code-block:: yaml

   project_defaults:
     max_concurrent_runs: 1
     generate_master_job: true
   ---
   job_name: bronze_daily
   schedule:
     quartz_cron_expression: "0 0 6 * * ?"
     timezone_id: UTC
     pause_status: PAUSED

The override name must match a generated job's group. Keep a new schedule paused
until you have inspected and tested the deployed job.

.. code-block:: bash

   lhp dag --env dev --bundle-output --job-config config/job_config.yaml

Inspect the generated jobs and dependency order, then validate and deploy the
bundle. Enable the schedule when the deployed job is ready to run.

Use :doc:`/guides/ops/dependency-analysis` to inspect why an edge exists.
The complete file grammar, merge rules and master-job controls are in
:doc:`/reference/config/jobs`. Monitoring uses a
:doc:`separate job configuration contract </reference/config/monitoring-job>`.
