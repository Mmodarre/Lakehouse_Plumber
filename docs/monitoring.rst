====================================
Pipeline Monitoring
====================================

.. meta::
   :description: Centralized event log monitoring and analysis across all Databricks DLT pipelines managed by Lakehouse Plumber.

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

2. **Monitoring Pipeline** — A synthetic pipeline that UNIONs all event log tables into
   a single streaming table, with optional materialized views for analysis and dashboards.

Together, these features give you a single pane of glass for pipeline health, performance,
and event analysis — configured entirely through ``lhp.yaml``.

.. note::
   Pipeline monitoring is entirely optional. Your existing pipelines work unchanged without
   it. You can enable event log injection on its own, or combine it with the monitoring
   pipeline for full centralized observability.

**Prerequisites:**

* Databricks Asset Bundles integration enabled (``databricks.yml`` exists in project root)
* Unity Catalog enabled workspace
* At least one pipeline generating code via ``lhp generate``

**Architecture**

.. mermaid::

   flowchart LR
       P1["Pipeline A"] --> EL1["Event Log Table A"]
       P2["Pipeline B"] --> EL2["Event Log Table B"]
       P3["Pipeline N"] --> EL3["Event Log Table N"]

       EL1 --> UV["v_all_event_logs<br/>(UNION ALL)"]
       EL2 --> UV
       EL3 --> UV

       UV --> ST["all_pipelines_event_log<br/>(Streaming Table)"]
       ST --> MV1["events_summary<br/>(Materialized View)"]
       ST --> MV2["Custom MVs<br/>(Optional)"]

       subgraph opt ["enable_job_monitoring: true"]
           PL["Python Load<br/>(Databricks SDK)"] --> JS["jobs_stats<br/>(Materialized View)"]
       end

       style P1 fill:#e1f5fe
       style P2 fill:#e1f5fe
       style P3 fill:#e1f5fe
       style EL1 fill:#fff3e0
       style EL2 fill:#fff3e0
       style EL3 fill:#fff3e0
       style UV fill:#f3e5f5
       style ST fill:#e8f5e8
       style MV1 fill:#fce4ec
       style MV2 fill:#fce4ec
       style PL fill:#e0f2f1
       style JS fill:#fce4ec
       style opt fill:none,stroke:#999,stroke-dasharray: 5 5

Quick Start
-----------

Get centralized pipeline monitoring in three steps:

**Step 1: Add event log and monitoring to lhp.yaml**

.. code-block:: yaml
   :caption: lhp.yaml
   :emphasize-lines: 4-7,9

   name: my_project
   version: "1.0"

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring: {}

.. tip::
   ``monitoring: {}`` enables the monitoring pipeline with sensible defaults: the pipeline
   is named ``${project_name}_event_log_monitoring``, uses the same catalog/schema as
   ``event_log``, and creates a default ``events_summary`` materialized view that
   summarizes pipeline run status, duration, and row metrics.

**Step 2: Generate code and resources**

.. code-block:: bash

   lhp generate -e dev

You will see output indicating the monitoring pipeline was generated:

.. code-block:: text

   ✅ Generated: my_project_event_log_monitoring/monitoring.py
   🔄 Syncing bundle resources with generated files...
   ✅ Updated 4 bundle resource file(s)

**Step 3: Inspect the generated output**

.. code-block:: text

   generated/
   └── dev/
       ├── my_pipeline_a/
       │   └── ...
       ├── my_pipeline_b/
       │   └── ...
       └── my_project_event_log_monitoring/   ← New!
           └── monitoring.py

   resources/
   └── lhp/
       ├── my_pipeline_a.pipeline.yml         ← Now includes event_log block
       ├── my_pipeline_b.pipeline.yml         ← Now includes event_log block
       └── my_project_event_log_monitoring.pipeline.yml   ← New!

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

Monitoring Pipeline Configuration
---------------------------------

The monitoring pipeline is configured in ``lhp.yaml`` under the ``monitoring`` key. It
creates a synthetic pipeline that aggregates all event log tables into a single streaming
table with optional materialized views.

.. warning::
   Monitoring requires ``event_log`` to be enabled. If ``monitoring`` is configured but
   ``event_log`` is missing or disabled, LHP raises error ``LHP-CFG-008``.

Configuration Reference
~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 10 30 35

   * - Option
     - Type
     - Default
     - Description
   * - ``enabled``
     - boolean
     - ``true``
     - Enable/disable the monitoring pipeline.
   * - ``pipeline_name``
     - string
     - ``${project_name}_event_log_monitoring``
     - Custom name for the monitoring pipeline.
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
     - Name of the centralized streaming table.
   * - ``materialized_views``
     - list
     - One default ``events_summary`` MV (pipeline run summary)
     - List of materialized view definitions. Set to ``[]`` to disable MVs.
   * - ``enable_job_monitoring``
     - boolean
     - ``false``
     - When enabled, generates a Python load action that correlates Databricks Jobs with pipeline runs using the Databricks SDK, populating a separate ``jobs_stats`` materialized view.

Minimal Configuration
~~~~~~~~~~~~~~~~~~~~~

The simplest monitoring configuration uses an empty mapping, which enables all defaults:

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring: {}

This creates:

* Pipeline named ``${project_name}_event_log_monitoring``
* Streaming table ``all_pipelines_event_log`` in the same catalog/schema as event_log
* Default ``events_summary`` materialized view (pipeline run summary with status, duration, and row metrics)

Custom Pipeline Name
~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     pipeline_name: "my_custom_monitor"

The pipeline name affects:

* The directory name under ``generated/`` (e.g., ``generated/dev/my_custom_monitor/``)
* The resource file name (e.g., ``resources/lhp/my_custom_monitor.pipeline.yml``)
* The pipeline identifier in Databricks

Custom Catalog and Schema
~~~~~~~~~~~~~~~~~~~~~~~~~

By default, the monitoring pipeline writes to the same catalog and schema as configured
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

**Override priority:**

1. Monitoring-level ``catalog``/``schema`` (highest — if specified)
2. Event log ``catalog``/``schema`` (default fallback)

Generated Pipeline Structure
-----------------------------

The monitoring pipeline generates a Python file with three types of artifacts: a source
view, a streaming table, and materialized views. Here is what each component does and
what the generated code looks like.

SQL Source View
~~~~~~~~~~~~~~~

A temporary view named ``v_all_event_logs`` is created that UNIONs all pipeline event log
tables. Each row is tagged with a ``_source_pipeline`` column identifying its origin:

.. code-block:: python
   :caption: generated/dev/my_project_event_log_monitoring/monitoring.py (excerpt)

   @dp.temporary_view()
   def v_all_event_logs():
       """SQL source: load_all_event_logs"""
       df = spark.sql("""SELECT *, 'bronze_load' as _source_pipeline
   FROM stream(acme_edw_dev._meta.bronze_load_event_log)
   UNION ALL
   SELECT *, 'silver_transform' as _source_pipeline
   FROM stream(acme_edw_dev._meta.silver_transform_event_log)""")

       return df

Key aspects:

* Uses ``stream()`` wrappers for streaming reads from each event log table
* Adds ``_source_pipeline`` literal column for traceability
* Pipeline names are sorted alphabetically for deterministic output
* Substitution tokens in catalog/schema (e.g., ``${catalog}``) are resolved at generation time

Streaming Table
~~~~~~~~~~~~~~~

An append-flow streaming table named ``all_pipelines_event_log`` (by default) continuously
ingests from the source view:

.. code-block:: python
   :caption: monitoring.py (excerpt)

   # Create the streaming table
   dp.create_streaming_table(
       name="acme_edw_dev._meta.all_pipelines_event_log",
       comment="Streaming table: all_pipelines_event_log",
   )

   # Define append flow(s)
   @dp.append_flow(
       target="acme_edw_dev._meta.all_pipelines_event_log",
       name="f_all_event_logs",
       comment="Append flow to acme_edw_dev._meta.all_pipelines_event_log",
   )
   def f_all_event_logs():
       """Append flow to acme_edw_dev._meta.all_pipelines_event_log"""
       # Streaming flow
       df = spark.readStream.table("v_all_event_logs")
       return df

Materialized Views
~~~~~~~~~~~~~~~~~~

By default, LHP creates an ``events_summary`` materialized view — a pipeline run summary
that extracts run status, duration, row metrics, and configuration from the event log:

.. code-block:: python
   :caption: monitoring.py (excerpt) — default events_summary MV

   @dp.materialized_view(
       name="acme_edw_dev._meta.events_summary",
       comment="Materialized view: events_summary",
       table_properties={},
   )
   def events_summary():
       """Write to acme_edw_dev._meta.events_summary from multiple sources"""
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

The ``${streaming_table}`` placeholder in the SQL template is replaced with the
fully-qualified streaming table name (e.g.,
``acme_edw_dev._meta.all_pipelines_event_log``) at generation time.

Bundle Resource
~~~~~~~~~~~~~~~

The monitoring pipeline also generates a Databricks Asset Bundle resource file:

.. code-block:: yaml
   :caption: resources/lhp/acme_edw_event_log_monitoring.pipeline.yml (excerpt)

   # Generated by LakehousePlumber - Bundle Resource for acme_edw_event_log_monitoring
   resources:
     pipelines:
       acme_edw_event_log_monitoring_pipeline:
         name: acme_edw_event_log_monitoring_pipeline
         catalog: ${var.default_pipeline_catalog}
         schema: ${var.default_pipeline_schema}
         serverless: true
         libraries:
           - glob:
               include: ${workspace.file_path}/generated/${bundle.target}/acme_edw_event_log_monitoring/**
         root_path: ${workspace.file_path}/generated/${bundle.target}/acme_edw_event_log_monitoring
         configuration:
           bundle.sourcePath: ${workspace.file_path}/generated/${bundle.target}
         channel: CURRENT

Job Monitoring
--------------

When ``enable_job_monitoring: true`` is set, the monitoring pipeline generates an additional
Python load chain that correlates Databricks Jobs with their associated pipeline runs using
the Databricks SDK. The results are written to a separate ``jobs_stats`` materialized view
alongside the main event log streaming table.

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     enable_job_monitoring: true

**What it generates:**

In addition to the standard event log pipeline (SQL load → streaming table → MVs), the
monitoring pipeline adds:

1. **Python Load → ``v_jobs_stats``** — calls a ``get_jobs_stats`` function from a
   generated ``jobs_stats_loader.py`` module to fetch job run statistics via the
   Databricks SDK.
2. **Write → ``jobs_stats``** — a materialized view in the same catalog/schema as the
   event log streaming table, populated from the ``v_jobs_stats`` view. A materialized
   view is used (rather than a streaming table) because the Python SDK source returns
   batch data, not a streaming DataFrame.

**Generated files:**

.. code-block:: text

   generated/
   └── dev/
       └── my_project_event_log_monitoring/
           ├── monitoring.py              ← includes Python load + jobs_stats write
           └── jobs_stats_loader.py        ← placeholder module (see below)

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

Custom Materialized Views
-------------------------

You can fully customize the materialized views created by the monitoring pipeline, from
inline SQL to external files, or disable them entirely.

Inline SQL
~~~~~~~~~~

Define materialized views with inline SQL using the ``sql`` property:

.. code-block:: yaml
   :caption: lhp.yaml
   :emphasize-lines: 5-8

   monitoring:
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
~~~~~~~~~~~~~~~~~~

For complex queries, use ``sql_path`` to reference an external SQL file:

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To create only the streaming table without any materialized views, set
``materialized_views`` to an empty list:

.. code-block:: yaml
   :caption: lhp.yaml

   monitoring:
     materialized_views: []

When omitted entirely (or set to ``null``), the default ``events_summary`` MV is created.
This means there are three behaviors:

======================= ============================================
Setting                 Behavior
======================= ============================================
Omitted / ``null``      Default ``events_summary`` MV is created
``[]`` (empty list)     No materialized views — streaming table only
Explicit list           Only the specified MVs are created
======================= ============================================

Validation Rules
~~~~~~~~~~~~~~~~

LHP validates materialized view definitions at configuration load time:

* **Name required** — Each MV must have a ``name`` field
* **Unique names** — MV names must not repeat within the ``materialized_views`` list
* **Mutual exclusion** — Each MV must specify either ``sql`` or ``sql_path``, not both

Violations raise ``LHP-CFG-008`` with a descriptive error message.

Pipeline Configuration for Monitoring
--------------------------------------

The monitoring pipeline can be configured like any other pipeline through
``pipeline_config.yaml``. Since the monitoring pipeline name is dynamic, LHP provides
a reserved alias to avoid hardcoding.

Using the __eventlog_monitoring Alias
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   monitoring: {}

This gives you:

* Event log injection on all pipelines
* A monitoring pipeline named ``my_project_event_log_monitoring``
* Streaming table ``all_pipelines_event_log`` in ``${catalog}._meta``
* Default ``events_summary`` materialized view

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

   monitoring: {}

.. code-block:: yaml
   :caption: config/pipeline_config.yaml — opt out specific pipelines

   ---
   pipeline: temp_debug_pipeline
   event_log: false

   ---
   pipeline: experimental_pipeline
   event_log: false

Pipelines that opt out with ``event_log: false`` are excluded from the monitoring
pipeline's UNION ALL query.

Environment-Specific Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use LHP substitution tokens for environment-aware monitoring:

.. code-block:: yaml
   :caption: lhp.yaml

   event_log:
     catalog: "${catalog}"
     schema: "${monitoring_schema}"
     name_suffix: "_event_log"

   monitoring: {}

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

This produces environment-specific event log table references at generation time:

* **Dev:** ``acme_edw_dev._meta.bronze_load_event_log``
* **Prod:** ``acme_edw_prod._monitoring.bronze_load_event_log``

Troubleshooting
---------------

Monitoring-related errors use codes ``LHP-CFG-006`` through ``LHP-CFG-008`` (event log
and monitoring configuration) and ``LHP-VAL-010``/``LHP-VAL-011`` (pipeline config alias
issues).

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
