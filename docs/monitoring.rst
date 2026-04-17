====================================
Pipeline Monitoring
====================================

.. meta::
   :description: Centralized event log monitoring and analysis across all Databricks Lakeflow Declarative Pipelines managed by Lakehouse Plumber.

This page covers Lakehouse Plumber's pipeline monitoring capabilities — declarative
event log aggregation and analysis across all your pipelines.


Overview
--------

Pipeline monitoring in LakehousePlumber provides centralized observability for all your
Databricks Lakeflow Declarative Pipelines without manual infrastructure setup. It combines
two related capabilities:

1. **Event Log Injection** — Automatically adds ``event_log`` blocks to every pipeline's
   Databricks Asset Bundle resource file, directing each pipeline's operational events to
   a Unity Catalog table.

2. **Monitoring Assets** — Three coordinated artifacts that aggregate all event logs
   and produce analytical views:

   * A **notebook** (``monitoring/{env}/union_event_logs.py``) that runs N independent
     Structured Streaming queries — one per pipeline event log — into a single Delta
     table. Each query has its own checkpoint directory.
   * A **Lakeflow Declarative Pipeline** (``generated/{env}/{pipeline_name}/monitoring.py``)
     containing only materialized views that read from the union Delta table.
   * A **Databricks Workflow job** (``resources/lhp/{pipeline_name}.job.yml``) that chains
     the notebook and the pipeline.

Together, these features give you a single pane of glass for pipeline health, performance,
and event analysis — configured entirely through ``lhp.yaml``.

.. note::
   Pipeline monitoring is entirely optional. Your existing pipelines work unchanged without
   it. You can enable event log injection on its own, or combine it with the monitoring
   assets for full centralized observability.

**Prerequisites:**

* Databricks Asset Bundles integration enabled (``databricks.yml`` exists in project root)
* Unity Catalog enabled workspace
* A Unity Catalog volume (or other cloud storage path) available for streaming checkpoints
* At least one pipeline generating code via ``lhp generate``

.. _monitoring-architecture:

**Architecture**

.. mermaid::

   flowchart LR
       P1["Pipeline A"] --> EL1["Event Log A"]
       P2["Pipeline B"] --> EL2["Event Log B"]
       P3["Pipeline N"] --> ELN["Event Log N"]

       subgraph notebook ["union_event_logs.py (notebook)"]
           EL1 --> S1["Stream A<br/>checkpoint_path/A"]
           EL2 --> S2["Stream B<br/>checkpoint_path/B"]
           ELN --> SN["Stream N<br/>checkpoint_path/N"]
       end

       S1 --> UT["all_pipelines_event_log<br/>(Delta table)"]
       S2 --> UT
       SN --> UT

       UT --> MV1["events_summary<br/>(Materialized View)"]
       UT --> MV2["Custom MVs<br/>(Optional)"]

       subgraph job ["Workflow Job"]
           NT["notebook_task"] --> PT["pipeline_task<br/>(MVs)"]
       end

       subgraph opt ["enable_job_monitoring: true"]
           PL["Python Load<br/>(Databricks SDK)"] --> JS["jobs_stats<br/>(Materialized View)"]
       end

       style P1 fill:#e1f5fe
       style P2 fill:#e1f5fe
       style P3 fill:#e1f5fe
       style EL1 fill:#fff3e0
       style EL2 fill:#fff3e0
       style ELN fill:#fff3e0
       style S1 fill:#e3f2fd
       style S2 fill:#e3f2fd
       style SN fill:#e3f2fd
       style UT fill:#e8f5e8
       style MV1 fill:#fce4ec
       style MV2 fill:#fce4ec
       style NT fill:#f3e5f5
       style PT fill:#f3e5f5
       style PL fill:#e0f2f1
       style JS fill:#fce4ec
       style opt fill:none,stroke:#999,stroke-dasharray: 5 5
       style job fill:none,stroke:#999,stroke-dasharray: 5 5
       style notebook fill:none,stroke:#999,stroke-dasharray: 5 5

.. note::
   Each pipeline's stream owns an independent checkpoint directory
   (``{checkpoint_path}/{pipeline_name}/``). Adding or removing a pipeline only creates or
   leaves a checkpoint directory — it does **not** invalidate any existing stream's
   checkpoint. Streams run in a ``ThreadPoolExecutor`` and use
   ``trigger(availableNow=True)`` so the notebook terminates once all data has been
   processed.


Quick Start
-----------

Get centralized pipeline monitoring in three steps:

**Step 1: Add event log and monitoring to lhp.yaml**

.. code-block:: yaml
   :caption: lhp.yaml
   :emphasize-lines: 4-7,9-10

   name: my_project
   version: "1.0"

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. tip::
   ``checkpoint_path`` is the only required field under ``monitoring``. Everything else
   has sensible defaults: the pipeline is named ``${project_name}_event_log_monitoring``,
   uses the same catalog/schema as ``event_log``, creates a Delta table called
   ``all_pipelines_event_log``, and exposes a default ``events_summary`` materialized view
   that summarizes pipeline run status, duration, and row metrics.

**Step 2: Generate code and resources**

.. code-block:: bash

   lhp generate -e dev

You will see output indicating the monitoring artifacts were generated:

.. code-block:: text

   ✅ Generated: my_project_event_log_monitoring/monitoring.py
   ✅ Generated monitoring notebook: monitoring/dev/union_event_logs.py
   ✅ Generated monitoring job resource: resources/lhp/my_project_event_log_monitoring.job.yml

**Step 3: Inspect the generated output**

.. code-block:: text

   generated/
   └── dev/
       ├── my_pipeline_a/
       │   └── ...
       ├── my_pipeline_b/
       │   └── ...
       └── my_project_event_log_monitoring/         ← MVs-only DLT pipeline
           └── monitoring.py

   monitoring/
   └── dev/
       └── union_event_logs.py                       ← Streaming union notebook

   resources/
   └── lhp/
       ├── my_pipeline_a.pipeline.yml               ← Now includes event_log block
       ├── my_pipeline_b.pipeline.yml               ← Now includes event_log block
       ├── my_project_event_log_monitoring.pipeline.yml   ← MVs pipeline resource
       └── my_project_event_log_monitoring.job.yml        ← Workflow job

Event Log Configuration
-----------------------

Event log configuration controls how Databricks pipeline event logs are stored. When
defined in ``lhp.yaml``, event log blocks are automatically injected into all pipeline
resource files during ``lhp generate`` — no ``-pc`` flag or ``pipeline_config.yaml`` required.

Configuration Reference
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 20 10 15 55

   * - Option
     - Type
     - Default
     - Description
   * - ``enabled``
     - boolean
     - ``true``
     - Enable/disable event log injection. Set to ``false`` to define the section without activating it.
   * - ``catalog``
     - string
     - (required)
     - Unity Catalog name for the event log table. Supports LHP token substitution.
   * - ``schema``
     - string
     - (required)
     - Schema name for the event log table. Supports LHP token substitution.
   * - ``name_prefix``
     - string
     - ``""``
     - Prefix prepended to the generated event log table name.
   * - ``name_suffix``
     - string
     - ``""``
     - Suffix appended to the generated event log table name.

.. note::
   All ``event_log`` fields support LHP token substitution. Tokens like ``${catalog}``
   are resolved from your ``substitutions/{env}.yaml`` files, just like all other
   configuration fields.

Event Log Table Naming
~~~~~~~~~~~~~~~~~~~~~~

The event log table name for each pipeline is generated using the formula:

``{name_prefix}{pipeline_name}{name_suffix}``

**Examples:**

.. list-table::
   :header-rows: 1
   :widths: 25 15 15 45

   * - Pipeline Name
     - name_prefix
     - name_suffix
     - Generated Event Log Table Name
   * - ``bronze_load``
     - ``""``
     - ``_event_log``
     - ``bronze_load_event_log``
   * - ``silver_transform``
     - ``el_``
     - ``""``
     - ``el_silver_transform``
   * - ``gold_analytics``
     - ``""``
     - ``_events``
     - ``gold_analytics_events``

Pipeline-Level Overrides
~~~~~~~~~~~~~~~~~~~~~~~~

Individual pipelines can override or opt out of project-level event logging through
``pipeline_config.yaml``.

**Full replace:** A pipeline-specific ``event_log`` in ``pipeline_config.yaml`` **completely
replaces** the project-level configuration for that pipeline:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   ---
   pipeline: silver_analytics
   event_log:
     name: custom_event_log
     catalog: analytics_catalog
     schema: monitoring

.. important::
   Override is a **full replace**, not a merge. When a pipeline defines its own ``event_log``
   dict in ``pipeline_config.yaml``, the entire project-level event_log is ignored for that
   pipeline.

**Pipeline-level opt-out:** Set ``event_log: false`` to disable event logging for a
specific pipeline, even when project-level event logging is enabled:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   ---
   pipeline: temp_debug_pipeline
   event_log: false

.. note::
   Project-level event logging does **not** require the ``-pc`` flag. It is applied
   automatically during ``lhp generate``. The ``-pc`` flag is only needed if you want
   to use ``pipeline_config.yaml`` for pipeline-specific overrides or other settings.

Generated Resource Output
~~~~~~~~~~~~~~~~~~~~~~~~~

Here is a concrete example showing how ``lhp.yaml`` event log configuration translates to
a generated pipeline resource file.

**Input:**

.. code-block:: yaml
   :caption: lhp.yaml (excerpt)

   event_log:
     catalog: acme_edw_dev
     schema: _meta
     name_suffix: "_event_log"

**Generated output** for a pipeline named ``event_log_basic``:

.. code-block:: yaml
   :caption: resources/lhp/event_log_basic.pipeline.yml (excerpt)
   :emphasize-lines: 4-6

   # ...pipeline configuration...
   channel: CURRENT
   event_log:
     name: event_log_basic_event_log
     schema: _meta
     catalog: acme_edw_dev

Monitoring Configuration
-------------------------

The monitoring assets are configured in ``lhp.yaml`` under the ``monitoring`` key. LHP
generates a notebook, an MVs-only Lakeflow Declarative Pipeline, and a Databricks
Workflow job that chains them.

.. warning::
   Monitoring requires ``event_log`` to be enabled. If ``monitoring`` is configured but
   ``event_log`` is missing or disabled, LHP raises error ``LHP-CFG-008``.

.. warning::
   ``checkpoint_path`` is **required** when ``monitoring.enabled`` is ``true``. LHP raises
   ``LHP-CFG-008`` if it is missing. Prior releases accepted ``monitoring: {}`` — that
   form is no longer valid.

Configuration Reference
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 25 40

   * - Option
     - Type
     - Default
     - Description
   * - ``enabled``
     - boolean
     - ``true``
     - Enable/disable monitoring. When ``false``, no notebook, pipeline, or job is generated
       and any previously generated monitoring artifacts are cleaned up on the next
       ``lhp generate``.
   * - ``checkpoint_path``
     - string
     - **required**
     - Base path for streaming checkpoints. Each monitored pipeline gets a subdirectory:
       ``{checkpoint_path}/{pipeline_name}/``. Typically a Unity Catalog volume path
       (e.g., ``/Volumes/my_catalog/_meta/checkpoints/event_logs``). Supports LHP token
       substitution.
   * - ``max_concurrent_streams``
     - integer
     - ``10``
     - Maximum number of concurrent streaming queries inside the notebook
       (``ThreadPoolExecutor`` ``max_workers``). Must be at least 1.
   * - ``pipeline_name``
     - string
     - ``${project_name}_event_log_monitoring``
     - Custom name for the MVs-only DLT pipeline and the Workflow job. Also used as the
       directory name under ``generated/{env}/``.
   * - ``catalog``
     - string
     - Inherits from ``event_log.catalog``
     - Unity Catalog for monitoring tables. Overrides the event_log default.
   * - ``schema``
     - string
     - Inherits from ``event_log.schema``
     - Schema for monitoring tables. Overrides the event_log default.
   * - ``streaming_table``
     - string
     - ``all_pipelines_event_log``
     - Name of the **Delta table** that the notebook writes into. The table is created
       implicitly by Structured Streaming on first run. It is *not* a DLT streaming table.
   * - ``materialized_views``
     - list
     - One default ``events_summary`` MV
     - List of materialized view definitions. Set to ``[]`` to suppress both the default MV
       and the entire DLT pipeline (notebook and job are still generated).
   * - ``enable_job_monitoring``
     - boolean
     - ``false``
     - When enabled, adds a Python load + ``jobs_stats`` materialized view to the DLT
       pipeline. See :ref:`monitoring-jobs`.

.. note::
   The ``streaming_table`` value no longer refers to a DLT streaming table. Under the
   current implementation, the notebook uses ``writeStream.toTable(...)`` to write the
   union into a regular Delta table. The catalog and schema must exist; the table itself
   is created on first write. Users only need write and checkpoint permissions on the
   target catalog/schema/volume for the identity that runs the notebook.

Minimal Configuration
~~~~~~~~~~~~~~~~~~~~~

The simplest valid monitoring configuration specifies just the checkpoint path:

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

This creates:

* Notebook named ``union_event_logs.py`` under ``monitoring/${env}/``
* DLT pipeline named ``${project_name}_event_log_monitoring``
* Delta table ``all_pipelines_event_log`` in the same catalog/schema as event_log
* Default ``events_summary`` materialized view (pipeline run summary with status,
  duration, and row metrics)
* Workflow job ``${project_name}_event_log_monitoring_job`` chaining the notebook and the
  pipeline

Custom Pipeline Name
~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     pipeline_name: "my_custom_monitor"
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

The pipeline name affects:

* The directory name under ``generated/`` (e.g., ``generated/dev/my_custom_monitor/``)
* The DLT pipeline resource file (e.g., ``resources/lhp/my_custom_monitor.pipeline.yml``)
* The Workflow job resource file (``resources/lhp/my_custom_monitor.job.yml``)
* The job's ``name`` field (``my_custom_monitor_job``)
* The pipeline identifier in Databricks

Custom Catalog and Schema
~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the monitoring assets write to the same catalog and schema as configured
in ``event_log``. You can override either or both:

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     catalog: "analytics_cat"
     schema: "_analytics"
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

**Override priority:**

1. Monitoring-level ``catalog``/``schema`` (highest — if specified)
2. Event log ``catalog``/``schema`` (default fallback)

Generated Artifacts
-------------------

Enabling monitoring produces up to three artifacts per environment. Each is described
below.

Union Notebook
~~~~~~~~~~~~~~

The notebook aggregates all eligible pipeline event logs into the Delta table named by
``streaming_table``. It runs N independent streaming queries — one per pipeline — each
with its own checkpoint directory.

.. code-block:: text
   :caption: Path

   monitoring/{env}/union_event_logs.py

.. code-block:: python
   :caption: monitoring/dev/union_event_logs.py (excerpt)

   TARGET_TABLE = "analytics_cat._analytics.all_pipelines_event_log"
   CHECKPOINT_BASE = "/Volumes/acme_edw_dev/_meta/checkpoints/event_logs"
   MAX_WORKERS = 10

   SOURCES = [
       ("bronze_load",     "acme_edw_dev._meta.bronze_load_event_log"),
       ("silver_transform","acme_edw_dev._meta.silver_transform_event_log"),
       ("gold_analytics",  "acme_edw_dev._meta.gold_analytics_event_log"),
   ]

   def process_source(pipeline_name: str, table_ref: str) -> str:
       checkpoint = f"{CHECKPOINT_BASE}/{pipeline_name}"
       query = (
           spark.readStream
           .format("delta")
           .table(table_ref)
           .withColumn("_source_pipeline", F.lit(pipeline_name))
           .writeStream.format("delta")
           .outputMode("append")
           .option("checkpointLocation", checkpoint)
           .option("mergeSchema", "true")
           .trigger(availableNow=True)
           .toTable(TARGET_TABLE)
       )
       query.awaitTermination()
       return pipeline_name

   with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
       futures = {executor.submit(process_source, n, r): n for n, r in SOURCES}
       ...

Key aspects:

* **Per-pipeline checkpoints.** Each source has its own directory at
  ``{CHECKPOINT_BASE}/{pipeline_name}/``. Changing the set of sources never invalidates
  existing checkpoints.
* **Append-only.** Streams use ``outputMode("append")`` with ``mergeSchema=true`` so the
  target Delta table can absorb schema evolution across event log sources.
* **Finite batches.** ``trigger(availableNow=True)`` processes all available data and
  then terminates — ideal for scheduled job execution rather than always-on streaming.
* **Parallel execution.** Sources run concurrently via ``ThreadPoolExecutor`` bounded by
  ``max_concurrent_streams``.
* **Error aggregation.** Per-source failures are collected and reported together. The
  notebook exits with a ``RuntimeError`` if any source failed.
* **Alphabetical source order.** Pipeline names are sorted for deterministic output.

MVs-Only Lakeflow Declarative Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The Lakeflow Declarative Pipeline generated by LHP for monitoring contains only
materialized views that read from the Delta table populated by the notebook. There is
**no** DLT streaming table, no source view, and no append flow — those were removed in
favor of the notebook-based union.

.. code-block:: text
   :caption: Path

   generated/{env}/{pipeline_name}/monitoring.py

.. code-block:: python
   :caption: monitoring.py (excerpt, default MV)

   PIPELINE_ID = "my_project_event_log_monitoring"
   FLOWGROUP_ID = "monitoring"

   @dp.materialized_view(
       name="acme_edw_dev._meta.events_summary",
       comment="Materialized view: events_summary",
       table_properties={},
   )
   def events_summary():
       df = spark.sql("""WITH run_info AS (
           SELECT origin.pipeline_name, origin.pipeline_id, origin.update_id,
                  MIN(`timestamp`) AS run_start_time,
                  MAX(`timestamp`) AS run_end_time,
                  ...
           FROM acme_edw_dev._meta.all_pipelines_event_log
           GROUP BY origin.pipeline_name, origin.pipeline_id, origin.update_id
       ),
       run_metrics AS (...),
       run_config AS (...)
       SELECT ri.pipeline_name, ri.pipeline_id, ri.update_id, ri.run_status, ...
       FROM run_info ri LEFT JOIN run_metrics rm ON ... LEFT JOIN run_config rc ON ...
       ORDER BY ri.run_start_time DESC""")
       return df

.. important::
   When ``materialized_views: []`` and ``enable_job_monitoring`` is ``false``, the DLT
   pipeline has no actions — LHP omits the pipeline entirely (no ``monitoring.py``, no
   pipeline resource, no ``pipeline_task`` in the job). Only the notebook and the
   notebook task of the job are generated.

Workflow Job
~~~~~~~~~~~~

LHP generates a Databricks Workflow job resource that orchestrates the notebook and the
DLT pipeline. Task ``union_event_logs`` runs the notebook, then the DLT pipeline task is
triggered via ``depends_on``.

.. code-block:: text
   :caption: Path

   resources/lhp/{pipeline_name}.job.yml

.. code-block:: yaml
   :caption: resources/lhp/my_project_event_log_monitoring.job.yml

   # Generated by LakehousePlumber - Monitoring Job for my_project_event_log_monitoring

   resources:
     jobs:
       my_project_event_log_monitoring_job:
         name: my_project_event_log_monitoring_job
         max_concurrent_runs: 1
         tasks:
           - task_key: union_event_logs
             notebook_task:
               notebook_path: ${workspace.file_path}/monitoring/${bundle.target}/union_event_logs
               source: WORKSPACE
           - task_key: my_project_event_log_monitoring_pipeline
             depends_on:
               - task_key: union_event_logs
             pipeline_task:
               pipeline_id: ${resources.pipelines.my_project_event_log_monitoring_pipeline.id}
               full_refresh: false
         queue:
           enabled: true
         performance_target: STANDARD

Customizing the job (cluster, schedule, permissions, notifications, tags, etc.) is
described in :ref:`monitoring-job-config`.

Materialized Views
-------------------

Default events_summary MV
~~~~~~~~~~~~~~~~~~~~~~~~~

When ``materialized_views`` is omitted from ``monitoring``, LHP creates a default
``events_summary`` materialized view — a pipeline run summary that extracts run status,
duration, row metrics, and configuration from the union Delta table:

.. code-block:: python
   :caption: monitoring.py (excerpt) — default events_summary MV

   @dp.materialized_view(
       name="acme_edw_dev._meta.events_summary",
       comment="Materialized view: events_summary",
       table_properties={},
   )
   def events_summary():
       df = spark.sql("""WITH run_info AS (
       SELECT
           origin.pipeline_name,
           origin.pipeline_id,
           origin.update_id,
           MIN(`timestamp`) AS run_start_time,
           MAX(`timestamp`) AS run_end_time,
           MAX_BY(
               CASE WHEN event_type = 'update_progress'
                   THEN details:update_progress:state::STRING END,
               CASE WHEN event_type = 'update_progress'
                   THEN `timestamp` END
           ) AS run_status
       FROM acme_edw_dev._meta.all_pipelines_event_log
       GROUP BY origin.pipeline_name, origin.pipeline_id, origin.update_id
   ),
   ...
   SELECT
       ri.pipeline_name, ri.pipeline_id, ri.update_id, ri.run_status,
       rc.trigger_cause, rc.is_full_refresh, rc.dbr_version, rc.compute_type,
       ri.run_start_time, ri.run_end_time,
       ROUND((...) / 60, 2) AS duration_minutes,
       COALESCE(rm.tables_processed, 0) AS tables_processed,
       COALESCE(rm.total_upserted_rows, 0) AS total_upserted_rows,
       ...
   FROM run_info ri
   LEFT JOIN run_metrics rm ON ...
   LEFT JOIN run_config rc ON ...
   ORDER BY ri.run_start_time DESC""")
       return df

The default SQL joins three CTEs from the event log:

* **run_info** — pipeline name, update ID, start/end time, final run status
* **run_metrics** — upserted rows, deleted rows, dropped records, tables processed
* **run_config** — DBR version, compute type (Serverless/Classic), trigger cause, full refresh flag

**``events_summary`` schema:**

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Column
     - Type
     - Description
   * - ``pipeline_name``
     - STRING
     - Name of the Lakeflow pipeline
   * - ``pipeline_id``
     - STRING
     - Unique pipeline identifier
   * - ``update_id``
     - STRING
     - Unique identifier for this pipeline run (update)
   * - ``run_status``
     - STRING
     - Final status of the run (e.g., ``COMPLETED``, ``FAILED``, ``CANCELED``)
   * - ``trigger_cause``
     - STRING
     - What triggered the run (e.g., ``USER_ACTION``, ``SCHEDULED``, ``API_CALL``)
   * - ``is_full_refresh``
     - BOOLEAN
     - Whether this was a full refresh or incremental update
   * - ``dbr_version``
     - STRING
     - Databricks Runtime version used for the run
   * - ``compute_type``
     - STRING
     - ``Serverless`` or ``Classic``
   * - ``run_start_time``
     - TIMESTAMP
     - When the pipeline run started
   * - ``run_end_time``
     - TIMESTAMP
     - When the pipeline run ended
   * - ``duration_minutes``
     - DOUBLE
     - Run duration in minutes (rounded to 2 decimal places)
   * - ``tables_processed``
     - BIGINT
     - Number of distinct tables (flows) processed in this run
   * - ``total_upserted_rows``
     - BIGINT
     - Total rows upserted across all tables
   * - ``total_deleted_rows``
     - BIGINT
     - Total rows deleted across all tables
   * - ``total_rows_affected``
     - BIGINT
     - Sum of upserted + deleted rows
   * - ``total_dropped_records``
     - BIGINT
     - Total records dropped by data quality expectations

At generation time, LHP substitutes the ``{streaming_table}`` placeholder in the default
SQL with the fully-qualified Delta table name (e.g.,
``acme_edw_dev._meta.all_pipelines_event_log``).

Custom Materialized Views
~~~~~~~~~~~~~~~~~~~~~~~~~

You can fully customize the materialized views created by the monitoring pipeline, from
inline SQL to external files, or disable them entirely.

Inline SQL
^^^^^^^^^^

Define materialized views with inline SQL using the ``sql`` property:

.. code-block:: yaml
   :caption: lhp.yaml
   :emphasize-lines: 3-8

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views:
       - name: "error_events"
         sql: "SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"
       - name: "pipeline_latency"
         sql: >-
           SELECT _source_pipeline, avg(duration_ms) as avg_duration
           FROM all_pipelines_event_log GROUP BY _source_pipeline

This generates two materialized view functions instead of the default ``events_summary``:

.. code-block:: python
   :caption: monitoring.py (excerpt) — custom inline MVs

   @dp.materialized_view(
       name="acme_edw_dev._meta.error_events",
       comment="Materialized view: error_events",
       table_properties={},
   )
   def error_events():
       df = spark.sql(
           """SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"""
       )
       return df

   @dp.materialized_view(
       name="acme_edw_dev._meta.pipeline_latency",
       comment="Materialized view: pipeline_latency",
       table_properties={},
   )
   def pipeline_latency():
       df = spark.sql(
           """SELECT _source_pipeline, avg(duration_ms) as avg_duration FROM all_pipelines_event_log GROUP BY _source_pipeline"""
       )
       return df

External SQL Files
^^^^^^^^^^^^^^^^^^

For complex queries, use ``sql_path`` to reference an external SQL file:

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views:
       - name: "custom_analysis"
         sql_path: "sql/monitoring_custom_analysis.sql"

.. code-block:: sql
   :caption: sql/monitoring_custom_analysis.sql

   SELECT
     _source_pipeline,
     event_type,
     date_trunc('DAY', timestamp) AS event_day,
     count(*) AS daily_event_count
   FROM all_pipelines_event_log
   WHERE event_type IN ('FLOW_PROGRESS', 'DATASET_CREATED')
   GROUP BY _source_pipeline, event_type, date_trunc('DAY', timestamp)

.. note::
   ``sql_path`` is resolved relative to the project root directory (where ``lhp.yaml`` lives).

Disabling Materialized Views
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

To generate only the notebook and the notebook task of the job, set
``materialized_views`` to an empty list:

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views: []

When omitted entirely (or set to ``null``), the default ``events_summary`` MV is created.
This means there are three behaviors:

======================= ================================================================
Setting                 Behavior
======================= ================================================================
Omitted / ``null``      Default ``events_summary`` MV is created
``[]`` (empty list)     No MVs and no DLT pipeline — only notebook + notebook-only job
Explicit list           Only the specified MVs are created
======================= ================================================================

.. note::
   When ``materialized_views: []`` and ``enable_job_monitoring`` is ``false``, LHP does
   **not** emit a DLT pipeline file, a pipeline resource file, or a ``pipeline_task`` in
   the Workflow job. The job contains only the ``union_event_logs`` notebook task.

Validation Rules
^^^^^^^^^^^^^^^^

LHP validates materialized view definitions at configuration load time:

* **Name required** — Each MV must have a ``name`` field
* **Unique names** — MV names must not repeat within the ``materialized_views`` list
* **Mutual exclusion** — Each MV must specify either ``sql`` or ``sql_path``, not both

Violations raise ``LHP-CFG-008`` with a descriptive error message.

.. _monitoring-job-config:

Customizing the Workflow Job
----------------------------

The generated Workflow job resource can be customized from ``config/job_config.yaml``.
LHP provides a reserved alias so you do not need to hardcode the monitoring job name.

Using the __eventlog_monitoring Alias in job_config.yaml
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``__eventlog_monitoring`` reserved keyword as a job name in
``config/job_config.yaml`` to target the monitoring job without hardcoding its generated
name.

.. code-block:: yaml
   :caption: config/job_config.yaml

   project_defaults:
     max_concurrent_runs: 1
     performance_target: STANDARD

   ---
   job_name: __eventlog_monitoring
   max_concurrent_runs: 1
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 3600
   notebook_cluster:
     new_cluster:
       spark_version: "15.4.x-scala2.12"
       node_type_id: "Standard_D4ds_v5"
       num_workers: 2
   schedule:
     quartz_cron_expression: "0 0 * * * ?"
     timezone_id: UTC
     pause_status: UNPAUSED
   tags:
     purpose: event_log_monitoring
     team: data-platform
   email_notifications:
     on_failure:
       - monitoring-alerts@company.com

At generation time, ``__eventlog_monitoring`` resolves to the actual monitoring job
name (``${pipeline_name}_job``). ``project_defaults`` still merges in as usual.

Available job fields (all optional):

* ``notebook_cluster.new_cluster`` or ``notebook_cluster.existing_cluster_id`` — cluster
  for the notebook task (Serverless used when neither is set)
* ``queue.enabled`` — job-run queueing
* ``performance_target``, ``timeout_seconds``, ``max_concurrent_runs``
* ``schedule`` — Quartz cron schedule
* ``tags`` — free-form tags
* ``email_notifications`` — ``on_start``/``on_success``/``on_failure``
* ``webhook_notifications`` — same events as email
* ``permissions`` — user/group permission entries

Rules
~~~~~

* If monitoring is **not configured or disabled** in ``lhp.yaml``, the alias entry is
  silently ignored with a warning.
* If **both** the alias and the resolved monitoring job name appear in the config, LHP
  raises an error.
* The ``pipeline_task`` in the job is always generated automatically — do not redefine
  it in ``job_config.yaml``.

Pipeline Configuration for Monitoring
--------------------------------------

The monitoring DLT pipeline (materialized views) can also be configured through
``pipeline_config.yaml``. Because the pipeline name is dynamic, LHP provides a reserved
alias to avoid hardcoding.

Using the __eventlog_monitoring Alias in pipeline_config.yaml
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use the ``__eventlog_monitoring`` reserved keyword in ``pipeline_config.yaml`` to target
the monitoring pipeline without knowing its exact name:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   ---
   pipeline: __eventlog_monitoring
   serverless: false
   edition: ADVANCED
   clusters:
     - label: default
       node_type_id: Standard_D4ds_v5
       autoscale:
         min_workers: 1
         max_workers: 4
   notifications:
     - email_recipients:
         - monitoring-alerts@company.com
       alerts:
         - on-update-failure
         - on-update-fatal-failure
   tags:
     purpose: event_log_monitoring

At generation time, ``__eventlog_monitoring`` automatically resolves to the actual monitoring
pipeline name defined in ``lhp.yaml``. The ``project_defaults`` section still applies and
merges as usual.

Behavior and Rules
~~~~~~~~~~~~~~~~~~

- If monitoring is **not configured or disabled** in ``lhp.yaml``, the alias entry is
  silently ignored with a warning
- If **both** the alias and the actual monitoring pipeline name appear in the config,
  an error is raised (``LHP-VAL-010``)
- The alias must be used as a **standalone** pipeline entry, not in a pipeline list
  (``LHP-VAL-011``)

.. code-block:: yaml
   :caption: Incorrect — alias in a list (triggers LHP-VAL-011)

   ---
   pipeline:
     - bronze_pipeline
     - __eventlog_monitoring

.. code-block:: yaml
   :caption: Correct — separate documents

   ---
   pipeline: bronze_pipeline
   serverless: false

   ---
   pipeline: __eventlog_monitoring
   serverless: false

.. _monitoring-jobs:

Job Monitoring
--------------

When ``enable_job_monitoring: true`` is set, LHP adds an additional Python load chain to
the monitoring DLT pipeline that correlates Databricks Jobs with their associated
pipeline runs using the Databricks SDK. The results are written to a separate
``jobs_stats`` materialized view alongside the user-specified MVs.

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     enable_job_monitoring: true

**What it generates:**

In addition to the notebook and the default/user MVs, the DLT pipeline adds:

1. **Python Load → ``v_jobs_stats``** — calls a ``get_jobs_stats`` function from a
   generated ``jobs_stats_loader.py`` module to fetch job run statistics via the
   Databricks SDK.
2. **Write → ``jobs_stats``** — a materialized view in the same catalog/schema as the
   monitoring pipeline, populated from the ``v_jobs_stats`` view. A materialized view
   is used (rather than a streaming table) because the Python SDK source returns batch
   data, not a streaming DataFrame.

**Generated files:**

.. code-block:: text

   generated/
   └── dev/
       └── my_project_event_log_monitoring/
           ├── monitoring.py              ← includes Python load + jobs_stats write
           └── jobs_stats_loader.py        ← shipped alongside the pipeline

The ``jobs_stats_loader.py`` module uses the Databricks SDK to scan recent job runs
(default lookback: 7 days), find pipeline tasks, and correlate each pipeline update to its
triggering job. It also enriches rows with pipeline tags (from ``spec.tags``) and job tags
(from ``settings.tags``). The lookback window is configurable via the ``lookback_hours``
pipeline parameter.

.. note::
   The ``jobs_stats`` materialized view inherits its catalog and schema from the monitoring
   pipeline configuration (which itself defaults to the ``event_log`` catalog/schema).

**``jobs_stats`` schema:**

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Column
     - Type
     - Description
   * - ``pipeline_id``
     - STRING
     - Unique pipeline identifier
   * - ``pipeline_name``
     - STRING
     - Name of the Lakeflow pipeline
   * - ``update_id``
     - STRING
     - Pipeline update (run) identifier correlated to the job run
   * - ``job_id``
     - STRING
     - Databricks Job ID that triggered this pipeline run
   * - ``job_run_id``
     - STRING
     - Specific job run identifier
   * - ``job_name``
     - STRING
     - Name of the triggering job
   * - ``job_run_start_time``
     - TIMESTAMP
     - When the job run started
   * - ``job_run_end_time``
     - TIMESTAMP
     - When the job run ended
   * - ``job_run_status``
     - STRING
     - Final job run status (e.g., ``SUCCESS``, ``FAILED``, ``UNKNOWN``)
   * - ``pipeline_tags``
     - STRING
     - JSON map of pipeline ``spec.tags`` (e.g., ``{"team": "data-platform"}``)
   * - ``job_tags``
     - STRING
     - JSON map of job ``settings.tags`` (e.g., ``{"environment": "production"}``)

Automatic Cleanup
-----------------

LHP automatically reconciles monitoring artifacts on every ``lhp generate``:

* **Notebook directory** — ``monitoring/{env}/`` is cleared before the new notebook is
  written. Empty ``monitoring/`` is removed.
* **Job resources** — any ``resources/*.job.yml`` whose header matches the monitoring
  job comment is removed before a new job is written (so renaming ``pipeline_name``
  cleans up the old file).
* **DLT pipeline directory** — when monitoring is *disabled or removed*, LHP scans
  ``generated/{env}/*`` for ``monitoring.py`` files with ``FLOWGROUP_ID = "monitoring"``
  and removes the directory.

This means toggling monitoring on/off, renaming the pipeline, or switching
``materialized_views`` between populated and empty forms never leaves stale files behind.

Common Patterns
---------------

Minimal Setup
~~~~~~~~~~~~~

The simplest possible monitoring configuration:

.. code-block:: yaml
   :caption: lhp.yaml

   name: my_project
   version: "1.0"

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

This gives you:

* Event log injection on all pipelines
* A notebook at ``monitoring/${env}/union_event_logs.py``
* A DLT pipeline named ``my_project_event_log_monitoring`` with a default
  ``events_summary`` materialized view
* A Workflow job ``my_project_event_log_monitoring_job`` chaining the two

Full Customization
~~~~~~~~~~~~~~~~~~

A fully customized monitoring setup:

.. code-block:: yaml
   :caption: lhp.yaml

   name: acme_edw
   version: "1.0"

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_prefix: ""
     name_suffix: "_event_log"

   monitoring:
     pipeline_name: "central_observability"
     catalog: "analytics_catalog"
     schema: "_monitoring"
     streaming_table: "unified_event_stream"
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     max_concurrent_streams: 20
     materialized_views:
       - name: "error_events"
         sql: "SELECT * FROM unified_event_stream WHERE event_type = 'error'"
       - name: "hourly_summary"
         sql: >-
           SELECT _source_pipeline, date_trunc('HOUR', timestamp) AS hour,
           count(*) AS cnt FROM unified_event_stream
           GROUP BY _source_pipeline, date_trunc('HOUR', timestamp)
       - name: "daily_analysis"
         sql_path: "sql/monitoring_custom_analysis.sql"

Selective Pipeline Monitoring
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To exclude specific pipelines from event log monitoring, use ``pipeline_config.yaml``
to opt individual pipelines out:

.. code-block:: yaml
   :caption: lhp.yaml — event log enabled for all by default

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. code-block:: yaml
   :caption: config/pipeline_config.yaml — opt out specific pipelines

   ---
   pipeline: temp_debug_pipeline
   event_log: false

   ---
   pipeline: experimental_pipeline
   event_log: false

Pipelines that opt out with ``event_log: false`` are excluded from the notebook's
``SOURCES`` list and therefore do not contribute rows to the union Delta table.

Environment-Specific Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use LHP substitution tokens for environment-aware monitoring:

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: "${monitoring_schema}"
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/${monitoring_schema}/checkpoints/event_logs"

.. code-block:: yaml
   :caption: substitutions/dev.yaml

   dev:
     catalog: acme_edw_dev
     monitoring_schema: _meta

.. code-block:: yaml
   :caption: substitutions/prod.yaml

   prod:
     catalog: acme_edw_prod
     monitoring_schema: _monitoring

This produces environment-specific event log table and checkpoint references at
generation time:

* **Dev:** ``acme_edw_dev._meta.bronze_load_event_log`` →
  checkpoint ``/Volumes/acme_edw_dev/_meta/checkpoints/event_logs/bronze_load/``
* **Prod:** ``acme_edw_prod._monitoring.bronze_load_event_log`` →
  checkpoint ``/Volumes/acme_edw_prod/_monitoring/checkpoints/event_logs/bronze_load/``

Migrating From the Pre-V0.8.2 Monitoring Pipeline
--------------------------------------------------

Before V0.8.2, LHP generated a single Lakeflow Declarative Pipeline containing a
``UNION ALL`` SQL source view, a DLT streaming table, and the materialized views. That
architecture had a structural limitation: adding or removing a monitored pipeline
changed the UNION schema and could invalidate the checkpoint, forcing a full reload.

V0.8.2 replaces that with the notebook + MVs-only pipeline + job design described above.
Migration steps:

1. **Add ``checkpoint_path``** to your ``monitoring`` block (now required — typically a
   Unity Catalog volume path).
2. **Re-run ``lhp generate``.** LHP will clean up the old single-pipeline artifacts and
   write the new notebook, MVs-only pipeline, and job resource.
3. **Redeploy via Databricks Asset Bundles.** The old pipeline is removed by the bundle
   deploy; the new job and (MVs-only) pipeline are deployed in its place.
4. **Backfill the union Delta table** if needed. Historical event log rows written before
   the new notebook's first run are not replayed into the union table unless you manually
   run a one-off backfill from each event log table.

Troubleshooting
---------------

Monitoring-related errors use codes ``LHP-CFG-006`` through ``LHP-CFG-008`` (event log
and monitoring configuration) and ``LHP-VAL-010``/``LHP-VAL-011`` (pipeline config alias
issues).

Common issues:

* **``LHP-CFG-008`` — "Monitoring checkpoint_path is required"** — add
  ``checkpoint_path`` under ``monitoring`` or set ``monitoring.enabled: false``.
* **No rows in ``all_pipelines_event_log``** — check that the Workflow job has run
  successfully. The notebook writes via Structured Streaming; the Delta table is created
  on first successful write.
* **``mergeSchema`` errors** — the notebook enables ``mergeSchema`` automatically on
  write. If you receive schema mismatch errors, ensure your checkpoint directories are
  fresh (each per-pipeline directory under ``checkpoint_path`` can be deleted
  independently without affecting the others).
* **Missing pipelines in the union** — pipelines that set ``event_log: false`` in
  ``pipeline_config.yaml`` are excluded by design.

.. seealso::
   :doc:`errors_reference` for detailed before/after examples and resolution steps for
   each error code.

Related Documentation
---------------------

* :doc:`databricks_bundles` — Bundle integration, pipeline configuration, and resource generation
* :doc:`concepts` — Understanding pipelines, flowgroups, and project configuration
* :doc:`actions/test_reporting` — publish DQ test results to external systems
* :doc:`errors_reference` — Complete error code reference
* :doc:`cli` — Command-line reference
