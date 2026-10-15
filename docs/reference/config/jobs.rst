Orchestration job configuration
===============================

**File:** the project-relative path supplied to ``lhp dag --job-config``.
Without that flag, LHP looks for ``templates/bundle/job_config.yaml`` and uses
built-in defaults when the file is absent. An explicitly named missing file
is an error. For setup, see :doc:`/develop/jobs`.

This is the orchestration job contract. The file referenced by
``monitoring.job_config_path`` has its own :doc:`monitoring-job` contract.

File shapes
-----------

A single document can contain flat defaults:

.. code-block:: yaml

   max_concurrent_runs: 1
   timeout_seconds: 3600

For per-job overrides, use multiple documents:

.. code-block:: yaml

   project_defaults:
     max_concurrent_runs: 1
     generate_master_job: true
   ---
   job_name: [bronze_daily, silver_daily]
   timeout_seconds: 3600
   ---
   job_name: gold_daily
   schedule:
     quartz_cron_expression: "0 0 7 * * ?"
     timezone_id: UTC
     pause_status: PAUSED

``job_name`` is a non-empty string or list of names in an override document.
Each name may appear only once across those documents. A lone document with
``project_defaults`` is also accepted. A lone ``job_name`` document is treated
as flat defaults, so always use the multi-document form for job overrides.

LHP controls and defaults
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 31 20 49

   * - Key
     - Default
     - Behaviour
   * - ``project_defaults``
     - empty mapping
     - Shared job settings in the multi-document form.
   * - ``job_name``
     - Required on each override document
     - Selects the job group; removed from the emitted settings.
   * - ``generate_master_job``
     - ``true``
     - Project-default boolean controlling master orchestration-job generation; consumed by LHP.
   * - ``master_job_name``
     - ``<project_name>_master``
     - Optional project-default string overriding the generated master-job name; consumed by LHP.
   * - ``max_concurrent_runs``
     - ``1``
     - Maximum simultaneous runs of a job.
   * - ``queue.enabled``
     - ``true``
     - Queue runs when concurrency is exhausted.
   * - ``performance_target``
     - ``STANDARD``
     - Databricks job performance target.

Merge order is built-in defaults, project defaults, then settings for the
specific job. Nested mappings merge recursively; lists and scalar values are
replaced. Environment tokens resolve for the selected environment.

Databricks job settings
-----------------------

``schedule``, ``timeout_seconds``, ``tags``, ``email_notifications``,
``webhook_notifications`` and ``permissions`` are supported job settings.
Other job-resource fields pass through to the generated YAML, including new
Databricks fields. LHP generates the pipeline tasks and dependency order;
inspect the resulting job before deployment.

Use the `Databricks bundle job resource reference
<https://docs.databricks.com/aws/en/dev-tools/bundles/resources#job>`_
for the full external field definitions and ``databricks bundle validate``
to check the generated resources.
