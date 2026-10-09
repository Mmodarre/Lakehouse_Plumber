Adjust monitoring settings
==========================

Use this after :doc:`enabling monitoring </guides/ops/monitoring>`.
Edit the source configuration, regenerate and deploy the resulting changes.

Choose the setting
------------------

.. list-table::
   :header-rows: 1
   :widths: 35 40 25

   * - Change
     - Setting
     - File
   * - Concurrent event-log collection streams
     - :ref:`monitoring.max_concurrent_streams <monitoring-concurrency>`
     - ``lhp.yaml``
   * - Summary queries and materialized views
     - :ref:`monitoring.materialized_views <monitoring-materialized-views>`
     - ``lhp.yaml``
   * - Where monitoring data is stored
     - ``monitoring.catalog``, ``monitoring.schema``, ``monitoring.streaming_table``
     - ``lhp.yaml``
   * - Checkpoint storage
     - :ref:`monitoring.checkpoint_path <monitoring-checkpoints>`
     - ``lhp.yaml``
   * - Schedule, notifications or notebook compute
     - :doc:`Monitoring job settings </reference/config/monitoring-job>`
     - File named by ``monitoring.job_config_path``

For example, add ``max_concurrent_streams: 5`` to your existing ``monitoring``
mapping. Preserve the required ``checkpoint_path`` and ``job_config_path``
fields. The complete defaults, ranges and inheritance rules are in
:doc:`/reference/config/monitoring`.

Review and apply the change
---------------------------

.. code-block:: bash

   lhp validate --env dev -pc config/pipeline_config.yaml
   lhp generate --env dev -pc config/pipeline_config.yaml
   databricks bundle validate -t dev

Review the generated monitoring notebook, pipeline and job resource. Deploy
the bundle and run its monitoring job, then check the job's tasks and resulting
monitoring tables. Generation alone does not apply changes to a running job.

Treat checkpoint and destination changes as state changes: verify the intended
history and replay behaviour before changing them. See
:ref:`checkpoint behaviour <monitoring-checkpoints>`.
To diagnose missing data or a failed job, use :doc:`troubleshooting`.
