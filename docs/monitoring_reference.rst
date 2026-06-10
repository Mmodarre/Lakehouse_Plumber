Monitoring Reference
=====================

.. meta::
   :description: Configuration reference for Lakehouse Plumber pipeline monitoring — event log fields, monitoring fields, materialized view rules, workflow job schema, reserved aliases, and validation error codes.

This page catalogs every ``event_log`` and ``monitoring`` key in ``lhp.yaml``, the
schema of the dedicated job-config file, the reserved ``__eventlog_monitoring``
alias, and the validation errors LHP raises. For the walk-through, see
:doc:`enable_monitoring`.

Prerequisites: Databricks Asset Bundles enabled (``databricks.yml`` exists),
Unity Catalog workspace, and a cloud storage path (typically a Unity Catalog
volume) for streaming checkpoints.

Event Log Configuration
-----------------------

The ``event_log`` block in ``lhp.yaml`` injects ``event_log:`` blocks into every
generated pipeline resource file during ``lhp generate``. String fields support
``${token}`` substitution from ``substitutions/<env>.yaml``.

.. list-table:: ``event_log`` fields
   :header-rows: 1
   :widths: 22 12 18 48

   * - Field
     - Type
     - Default
     - Description
   * - ``enabled``
     - boolean
     - ``true``
     - Set to ``false`` to keep the section but skip injection.
   * - ``catalog``
     - string
     - (required when ``enabled``)
     - Unity Catalog name for event log tables.
   * - ``schema``
     - string
     - (required when ``enabled``)
     - Schema name for event log tables.
   * - ``name_prefix``
     - string
     - ``""``
     - Prefix prepended to the generated event log table name.
   * - ``name_suffix``
     - string
     - ``""``
     - Suffix appended to the generated event log table name.

Per-pipeline table name formula: ``{name_prefix}{pipeline_name}{name_suffix}``.

Pipeline-Level Overrides
~~~~~~~~~~~~~~~~~~~~~~~~

In ``pipeline_config.yaml`` (loaded by ``lhp generate -pc``):

* An ``event_log:`` mapping for a pipeline **fully replaces** the project-level
  block (no merge).
* ``event_log: false`` opts that pipeline out of event logging. Opt-outs are
  excluded from the monitoring notebook's source list.

For the full ``pipeline_config.yaml`` schema, see :doc:`bundle_config_reference`.

Monitoring Configuration
------------------------

The ``monitoring`` block generates three artifacts: a union notebook, an
MVs-only Lakeflow Declarative Pipeline, and a Databricks Workflow job chaining
them. Monitoring requires ``event_log`` enabled (LHP raises ``LHP-CFG-008``
otherwise).

.. list-table:: ``monitoring`` fields
   :header-rows: 1
   :widths: 25 10 22 43

   * - Field
     - Type
     - Default
     - Description
   * - ``enabled``
     - boolean
     - ``true``
     - When ``false``, no monitoring artifacts are generated and previously
       generated artifacts are removed on the next ``lhp generate``.
   * - ``checkpoint_path``
     - string
     - required when ``enabled``
     - Base path for streaming checkpoints. Each monitored pipeline gets the
       subdirectory ``{checkpoint_path}/{pipeline_name}/``. Typically a Unity
       Catalog volume.
   * - ``job_config_path``
     - string
     - required when ``enabled``
     - Project-root-relative path to a flat single-document YAML file describing
       the monitoring Workflow job. See `Workflow Job Schema`_.
   * - ``max_concurrent_streams``
     - integer
     - ``10``
     - ``ThreadPoolExecutor`` ``max_workers`` for the union notebook. Must be ≥ 1.
   * - ``pipeline_name``
     - string
     - ``${project_name}_event_log_monitoring``
     - Used for the generated pipeline, job, and the directory under
       ``generated/<env>/``.
   * - ``catalog``
     - string
     - inherits ``event_log.catalog``
     - Override catalog for monitoring tables.
   * - ``schema``
     - string
     - inherits ``event_log.schema``
     - Override schema for monitoring tables.
   * - ``streaming_table``
     - string
     - ``all_pipelines_event_log``
     - Delta table the union notebook writes into (regular Delta table, created by
       Structured Streaming on first run).
   * - ``materialized_views``
     - list
     - one default view
     - Materialized view definitions. See `Materialized Views`_.
   * - ``enable_job_monitoring``
     - boolean
     - ``false``
     - Adds a ``jobs_stats`` view populated via the Databricks SDK. See
       `Job Monitoring`_.

Both required strings support ``${token}`` substitution. When ``job_config_path``
contains unresolved tokens, LHP defers the file-existence check until the
environment is selected. ``monitoring: {}`` parses but always fails validation,
because both required strings are empty.

Minimal Valid Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     job_config_path: "config/monitoring_job_config.yaml"

Override Resolution
~~~~~~~~~~~~~~~~~~~

Monitoring catalog and schema resolve as: ``monitoring.catalog`` /
``monitoring.schema`` (when set) wins; otherwise inherits from ``event_log``. The
Delta table fully qualified name is ``{catalog}.{schema}.{streaming_table}``.

Materialized Views
------------------

The ``materialized_views`` field has three modes:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Value
     - Behavior
   * - omitted or ``null``
     - LHP generates the default ``events_summary`` view (see `Default events_summary Schema`_).
   * - ``[]`` (empty list)
     - No materialized views and no MVs pipeline file. The Workflow job contains
       only the notebook task. Cleanup removes any previously generated
       ``monitoring.py``.
   * - explicit list
     - Only the listed views are generated, replacing the default.

View Definition Fields
~~~~~~~~~~~~~~~~~~~~~~

Each entry must declare ``name`` (required, unique within the list) and exactly
one of ``sql`` (inline SQL string) or ``sql_path`` (project-root-relative path to
a SQL file). Each view becomes a ``@dp.materialized_view`` function in
``generated/<env>/<pipeline_name>/monitoring.py``, fully qualified as
``{monitoring_catalog}.{monitoring_schema}.{view_name}``.

.. code-block:: yaml
   :caption: lhp.yaml (custom views)

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     job_config_path: "config/monitoring_job_config.yaml"
     materialized_views:
       - name: error_events
         sql: "SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"
       - name: custom_analysis
         sql_path: "sql/monitoring_custom_analysis.sql"

Default events_summary Schema
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The default view aggregates run status, timing, row metrics, and run config from
the union Delta table.

.. list-table::
   :header-rows: 1
   :widths: 32 14 54

   * - Column
     - Type
     - Description
   * - ``pipeline_name``, ``pipeline_id``, ``update_id``
     - STRING
     - Pipeline identifiers and run (update) ID.
   * - ``run_status``
     - STRING
     - Final status (``COMPLETED``, ``FAILED``, ``CANCELED``, …).
   * - ``trigger_cause``
     - STRING
     - ``USER_ACTION``, ``SCHEDULED``, ``API_CALL``, …
   * - ``is_full_refresh``
     - BOOLEAN
     - Full refresh vs. incremental.
   * - ``dbr_version``
     - STRING
     - Databricks Runtime version.
   * - ``compute_type``
     - STRING
     - ``Serverless`` or ``Classic``.
   * - ``run_start_time``, ``run_end_time``
     - TIMESTAMP
     - Run start and end.
   * - ``duration_minutes``
     - DOUBLE
     - Run duration in minutes (2 decimal places).
   * - ``tables_processed``
     - BIGINT
     - Distinct tables (flows) processed.
   * - ``total_upserted_rows``, ``total_deleted_rows``, ``total_rows_affected``
     - BIGINT
     - Row counts across all tables.
   * - ``total_dropped_records``
     - BIGINT
     - Records dropped by data quality expectations.

.. _monitoring-job-config:

Workflow Job Schema
-------------------

``monitoring.job_config_path`` must point to a flat single-document YAML mapping.
LHP deep-merges the file over its defaults (``max_concurrent_runs=1``,
``performance_target=STANDARD``, ``queue.enabled=true``) and then token-substitutes
via the active environment's ``substitutions/<env>.yaml``. The job name is fixed
at ``{pipeline_name}_job`` and the ``pipeline_task`` is generated automatically.

.. list-table:: Job-config fields (all optional)
   :header-rows: 1
   :widths: 32 68

   * - Field
     - Description
   * - ``notebook_cluster.new_cluster``
     - New-cluster spec for the notebook task. Mutually exclusive with
       ``existing_cluster_id``. Serverless when neither is set.
   * - ``notebook_cluster.existing_cluster_id``
     - Attach the notebook task to an existing cluster.
   * - ``queue.enabled``
     - Enable job-run queueing.
   * - ``performance_target``
     - ``STANDARD`` (default) or ``PERFORMANCE_OPTIMIZED``.
   * - ``timeout_seconds``
     - Job-level timeout in seconds.
   * - ``max_concurrent_runs``
     - Maximum concurrent runs. Default ``1``.
   * - ``schedule``
     - Quartz cron schedule: ``quartz_cron_expression``, ``timezone_id``,
       ``pause_status``.
   * - ``tags``
     - Free-form ``{key: value}`` mapping. Merged recursively over LHP defaults.
   * - ``email_notifications``, ``webhook_notifications``
     - ``on_start`` / ``on_success`` / ``on_failure`` recipient or webhook lists.
   * - ``permissions``
     - User/group permission entries.

Disallowed keys: a top-level ``project_defaults:`` wrapper, ``job_name:``, and
``pipeline_task`` entries (silently overridden).

.. warning::
   Legacy auto-pickup of ``templates/bundle/job_config.yaml`` for monitoring jobs
   is **removed**. Use ``monitoring.job_config_path`` to point at a dedicated
   file. The ``__eventlog_monitoring`` alias inside the generic
   ``config/job_config.yaml`` consumed by ``lhp deps`` for *orchestration* jobs is
   unaffected.

.. _monitoring-jobs:

Job Monitoring
--------------

When ``monitoring.enable_job_monitoring: true``, LHP adds a Python-load chain
that correlates Databricks Jobs with their pipeline runs via the Databricks SDK.

Generated files under ``generated/<env>/<pipeline_name>/``:

* ``monitoring.py`` — adds a ``v_jobs_stats`` view and a ``jobs_stats`` MV.
* ``jobs_stats_loader.py`` — calls the SDK to scan recent job runs, correlate
  each pipeline update with its triggering job, and enrich rows with pipeline
  tags (``spec.tags``) and job tags (``settings.tags``).

Default SDK lookback is 7 days, configurable via the ``lookback_hours`` pipeline
parameter. The ``jobs_stats`` view inherits the monitoring pipeline's catalog
and schema.

.. list-table:: ``jobs_stats`` schema
   :header-rows: 1
   :widths: 28 14 58

   * - Column
     - Type
     - Description
   * - ``pipeline_id``, ``pipeline_name``
     - STRING
     - Pipeline identifiers.
   * - ``update_id``
     - STRING
     - Pipeline update correlated with the job run.
   * - ``job_id``, ``job_run_id``, ``job_name``
     - STRING
     - Triggering job identifiers and name.
   * - ``job_run_start_time``, ``job_run_end_time``
     - TIMESTAMP
     - Job run start and end.
   * - ``job_run_status``
     - STRING
     - Final status (``SUCCESS``, ``FAILED``, ``UNKNOWN``, …).
   * - ``pipeline_tags``, ``job_tags``
     - STRING
     - JSON map of ``spec.tags`` and ``settings.tags`` respectively.

Reserved Aliases
----------------

The ``__eventlog_monitoring`` alias (double-underscore prefix) targets the
monitoring pipeline from ``pipeline_config.yaml`` without hardcoding its name. It
resolves to ``monitoring.pipeline_name`` (default
``${project_name}_event_log_monitoring``) at generation time.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Constraint
     - Violation result
   * - Monitoring must be enabled in ``lhp.yaml``.
     - Alias entry is silently dropped with a warning.
   * - Cannot coexist with the actual monitoring pipeline name in the same config.
     - LHP raises ``LHP-VAL-010``.
   * - Must appear as a standalone ``pipeline:`` value, not inside a list.
     - LHP raises ``LHP-VAL-011``.

The alias inside the generic orchestration ``config/job_config.yaml`` (consumed
by ``lhp deps``) is independent and still supported.

Automatic Cleanup
-----------------

LHP reconciles monitoring artifacts on every ``lhp generate``. Toggling
monitoring, renaming ``pipeline_name``, or switching ``materialized_views``
between populated and empty never leaves stale files.

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Artifact
     - Reconciliation
   * - Notebook directory
     - ``monitoring/<env>/`` is cleared before each write; an empty
       ``monitoring/`` is removed.
   * - Job resource
     - Any ``resources/lhp/*.job.yml`` whose header matches the monitoring job
       comment is removed before write. Renaming ``pipeline_name`` cleans up the
       prior file.
   * - Pipeline directory
     - When monitoring is disabled, LHP scans ``generated/<env>/*`` for
       ``monitoring.py`` files marked ``FLOWGROUP_ID = "monitoring"`` and removes
       the directory.

Validation Errors
-----------------

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Code
     - Trigger
   * - ``LHP-CFG-006``
     - ``event_log`` is not a YAML mapping, or fails to parse.
   * - ``LHP-CFG-007``
     - ``event_log.enabled`` is ``true`` but ``catalog`` or ``schema`` is missing.
   * - ``LHP-CFG-008``
     - ``monitoring`` is not a mapping; ``monitoring`` enabled without
       ``event_log``; ``checkpoint_path`` or ``job_config_path`` missing;
       ``job_config_path`` file not found (when path has no unresolved tokens);
       ``materialized_views`` is not a list or contains a non-mapping entry; a
       materialized view is missing ``name``; duplicate view names; both ``sql``
       and ``sql_path`` set on one view.
   * - ``LHP-VAL-010``
     - Both ``__eventlog_monitoring`` and the resolved monitoring pipeline name
       appear in ``pipeline_config.yaml``.
   * - ``LHP-VAL-011``
     - ``__eventlog_monitoring`` appears inside a pipeline list.
   * - ``LHP-IO-001``
     - At post-substitution time, the resolved ``job_config_path`` does not point
       to a readable file.

For the full error catalog, see :doc:`errors_reference`.

See also
--------

* :doc:`enable_monitoring` — how-to walk-through for turning monitoring on, with
  the workflow-job configuration steps and pre-V0.8.2 migration notes.
* :doc:`operational_metadata` — reference for project-level operational metadata
  columns that complement event log monitoring.
* :doc:`architecture` — explanation of the LHP generation model, including how
  the orchestrator finalizes monitoring artifacts.
* :doc:`bundle_config_reference` — bundle integration and ``pipeline_config.yaml``
  schema.
* :doc:`errors_reference` — full error code reference.
