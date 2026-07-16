Monitoring configuration
========================

.. meta::
   :description: Reference for LHP pipeline monitoring — the event_log and monitoring blocks in lhp.yaml, materialized-view definitions, the monitoring job-config file schema, their fields and defaults, and the validation error codes.

Monitoring is configured by two top-level blocks in ``lhp.yaml``: ``event_log``
(inject an event-log table into every generated pipeline) and ``monitoring``
(build a pipeline that consolidates and summarizes those event logs). String
fields support ``${token}`` substitution from ``substitutions/<env>.yaml``.

When ``monitoring`` is enabled, ``lhp generate`` emits three artifacts: a union
notebook (``monitoring/<env>/union_event_logs.py``), a materialized-views-only
pipeline (``generated/<env>/<pipeline_name>/monitoring.py``), and a Databricks
job (``resources/<pipeline_name>.job.yml`` — written directly under
``resources/``, not a ``resources/lhp/`` subdirectory).

.. seealso::

   How-to guide: :doc:`/guides/ops/monitoring`.

Event log fields
----------------

The ``event_log`` block. Fields live under ``event_log:`` in ``lhp.yaml``.

.. list-table::
   :header-rows: 1
   :widths: 20 12 22 46

   * - Field
     - Type
     - Default
     - Description
   * - ``enabled``
     - bool
     - ``true``
     - Set to ``false`` to keep the block but skip injection.
   * - ``catalog``
     - string
     - —
     - Unity Catalog for event-log tables. Required when ``enabled``.
   * - ``schema``
     - string
     - —
     - Schema for event-log tables. Required when ``enabled``.
   * - ``name_prefix``
     - string
     - ``""``
     - Prefix prepended to each generated event-log table name.
   * - ``name_suffix``
     - string
     - ``""``
     - Suffix appended to each generated event-log table name.

Per-pipeline table name is ``{name_prefix}{pipeline_name}{name_suffix}``.

.. code-block:: yaml

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

Monitoring fields
-----------------

The ``monitoring`` block. Fields live under ``monitoring:`` in ``lhp.yaml``.
Monitoring requires ``event_log`` enabled.

.. list-table::
   :header-rows: 1
   :widths: 22 10 26 42

   * - Field
     - Type
     - Default
     - Description
   * - ``enabled``
     - bool
     - ``true``
     - When ``false``, no artifacts are generated and previously generated ones are removed on the next ``lhp generate``.
   * - ``pipeline_name``
     - string
     - ``<project_name>_event_log_monitoring``
     - Name of the generated pipeline, job, and the ``generated/<env>/`` subdirectory.
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
     - Delta table the union notebook appends into (created on first run).
   * - ``checkpoint_path``
     - string
     - ``""``
     - Base path for streaming checkpoints; each pipeline gets ``{checkpoint_path}/{pipeline_name}/``. Required when ``enabled``.
   * - ``job_config_path``
     - string
     - —
     - Project-root-relative path to the job-config file (below). Required when ``enabled``. Supports ``${token}``.
   * - ``max_concurrent_streams``
     - integer
     - ``10``
     - ``ThreadPoolExecutor`` ``max_workers`` for the union notebook. Range 1–20.
   * - ``materialized_views``
     - list
     - one default view
     - Materialized-view definitions. See `Materialized views`_.
   * - ``enable_job_monitoring``
     - bool
     - ``false``
     - Add a ``jobs_stats`` view and materialized view populated via the Databricks SDK.

Catalog and schema resolve as: ``monitoring.catalog`` / ``monitoring.schema``
when set, otherwise inherited from ``event_log``. The Delta table's fully
qualified name is ``{catalog}.{schema}.{streaming_table}``.

.. code-block:: yaml

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     job_config_path: "config/monitoring_job_config.yaml"

Materialized views
------------------

``monitoring.materialized_views`` has three modes.

.. list-table::
   :header-rows: 1
   :widths: 26 74

   * - Value
     - Behavior
   * - omitted or ``null``
     - Generate the default ``events_summary`` view.
   * - ``[]`` (empty list)
     - No materialized views and no pipeline file; the job contains only the notebook task.
   * - explicit list
     - Generate only the listed views, replacing the default.

Each list entry is a mapping with these fields.

.. list-table::
   :header-rows: 1
   :widths: 18 14 14 54

   * - Field
     - Type
     - Required
     - Description
   * - ``name``
     - string
     - Yes
     - View name, unique within the list.
   * - ``sql``
     - string
     - One of
     - Inline SQL query. Setting both ``sql`` and ``sql_path`` raises ``LHP-CFG-008``.
   * - ``sql_path``
     - string
     - One of
     - Project-root-relative path to a ``.sql`` file.

The default ``events_summary`` view (generated when ``materialized_views`` is
omitted) aggregates per-run status, timing, and row-count columns from the
union Delta table; its full column list is documented in the monitoring guide,
not repeated here.

.. code-block:: yaml

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     job_config_path: "config/monitoring_job_config.yaml"
     materialized_views:
       - name: error_events
         sql: "SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"
       - name: custom_analysis
         sql_path: "sql/monitoring_custom_analysis.sql"

Job-config file
---------------

``monitoring.job_config_path`` points to a flat single-document YAML mapping.
LHP deep-merges it over its defaults (``max_concurrent_runs: 1``,
``queue.enabled: true``, ``performance_target: STANDARD``) and then
token-substitutes it per environment. The job name is fixed at
``<pipeline_name>_job`` and the ``pipeline_task`` is generated automatically.
All fields are optional.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Field
     - Description
   * - ``max_concurrent_runs``
     - Maximum concurrent job runs. Default ``1``.
   * - ``performance_target``
     - ``STANDARD`` (default) or ``PERFORMANCE_OPTIMIZED``.
   * - ``queue``
     - ``queue.enabled`` toggles job-run queueing.
   * - ``notebook_cluster.new_cluster``
     - New-cluster spec for the notebook task. Mutually exclusive with ``existing_cluster_id``; serverless when neither is set.
   * - ``notebook_cluster.existing_cluster_id``
     - Attach the notebook task to an existing cluster.
   * - ``timeout_seconds``
     - Job-level timeout in seconds.
   * - ``tags``
     - Free-form ``{key: value}`` mapping.
   * - ``email_notifications``
     - ``on_start`` / ``on_success`` / ``on_failure`` recipient lists.
   * - ``webhook_notifications``
     - ``on_start`` / ``on_success`` / ``on_failure`` webhook-id lists.
   * - ``permissions``
     - User/group permission entries (``level`` plus ``user_name`` or ``group_name``).
   * - ``schedule``
     - Quartz cron schedule: ``quartz_cron_expression``, ``timezone_id``, ``pause_status``.

Any key not listed above is passed through verbatim into the job resource, so
new Databricks Jobs fields work without an LHP change. A top-level
``project_defaults:`` wrapper, a ``job_name:`` key, and ``pipeline_task``
entries are rejected or overridden.

.. code-block:: yaml

   # config/monitoring_job_config.yaml
   max_concurrent_runs: 1
   performance_target: STANDARD
   queue:
     enabled: true
   tags:
     environment: "${bundle.target}"

Validation errors
-----------------

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Code
     - Trigger
   * - ``LHP-CFG-006``
     - ``event_log`` is not a mapping, or fails to parse.
   * - ``LHP-CFG-007``
     - ``event_log`` is enabled but ``catalog`` or ``schema`` is missing.
   * - ``LHP-CFG-008``
     - ``monitoring`` is not a mapping; monitoring enabled without ``event_log``; ``checkpoint_path`` or ``job_config_path`` missing; ``job_config_path`` file not found at validate time (path has no unresolved tokens); the job-config file is not a top-level mapping; ``materialized_views`` is not a list or has a non-mapping entry; a view is missing ``name``; duplicate view names; both ``sql`` and ``sql_path`` set on one view; ``max_concurrent_streams`` outside 1–20.
   * - ``LHP-IO-001``
     - At generate time, the resolved ``job_config_path`` does not point to a readable file.
   * - ``LHP-IO-002``
     - The monitoring job-config file is not valid YAML.
