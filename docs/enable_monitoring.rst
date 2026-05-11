Enable Monitoring
=================

.. meta::
   :description: How to turn on Lakehouse Plumber centralized event log monitoring: the lhp.yaml setting, the generated notebook, materialized views, and Workflow job.

This how-to walks you through enabling Lakehouse Plumber (LHP) centralized :term:`event log
monitoring <Event log monitoring>`. After adding two blocks to ``lhp.yaml`` you get an event-log union notebook,
a materialized-views pipeline, and a Workflow job that chains them — across every LHP
pipeline in your project.

For the full configuration schema (every option, every default, every error code), see
:doc:`monitoring_reference`.

How it works
------------

Each LHP pipeline writes its own event log. The monitoring pipeline wires every event
log into a single Delta table with a streaming notebook, then publishes Materialized
Views (MVs) on top of that table. A Databricks Workflow Job chains the notebook task
and the MVs pipeline task.

.. mermaid::

   flowchart LR
       P1["Pipeline A"] --> EL1["Event Log A"]
       P2["Pipeline B"] --> EL2["Event Log B"]
       P3["Pipeline N"] --> ELN["Event Log N"]

       subgraph notebook ["union_event_logs.py (notebook)"]
           EL1 --> S1["Stream A<br/>checkpoint/A"]
           EL2 --> S2["Stream B<br/>checkpoint/B"]
           ELN --> SN["Stream N<br/>checkpoint/N"]
       end

       S1 --> UT["all_pipelines_event_log<br/>(Delta table)"]
       S2 --> UT
       SN --> UT

       UT --> MV1["events_summary<br/>(default MV)"]
       UT --> MV2["Custom MVs<br/>(optional)"]

       subgraph job ["Workflow Job"]
           NT["notebook_task"] --> PT["pipeline_task (MVs)"]
       end

       style P1 fill:#e1f5fe
       style P2 fill:#e1f5fe
       style P3 fill:#e1f5fe
       style EL1 fill:#fff3e0
       style EL2 fill:#fff3e0
       style ELN fill:#fff3e0
       style UT fill:#e8f5e8
       style MV1 fill:#fce4ec
       style MV2 fill:#fce4ec
       style NT fill:#f3e5f5
       style PT fill:#f3e5f5
       style job fill:none,stroke:#999,stroke-dasharray: 5 5
       style notebook fill:none,stroke:#999,stroke-dasharray: 5 5

Each stream owns an independent checkpoint at ``{checkpoint_path}/{pipeline_name}/``,
so adding or removing a pipeline never invalidates an existing checkpoint. Streams run
in a ``ThreadPoolExecutor`` and use ``trigger(availableNow=True)`` so the notebook
terminates once all available data has been processed.

Turn it on
----------

Add ``event_log`` and ``monitoring`` blocks to ``lhp.yaml``, then run ``lhp generate``.

Step 1: Configure ``lhp.yaml``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
     job_config_path: "config/monitoring_job_config.yaml"

The presence of the ``monitoring`` block — with ``event_log`` enabled — is what turns
monitoring on. The ``enabled: true`` default is implicit; set ``monitoring.enabled:
false`` to keep the configuration but skip generation.

``checkpoint_path`` and ``job_config_path`` are the only required keys under
``monitoring``. Every other field has a default:

* Pipeline name: ``${project_name}_event_log_monitoring``.
* Catalog and schema: inherited from ``event_log``.
* Streaming Delta table: ``all_pipelines_event_log``.
* Materialized views: a single ``events_summary`` MV that rolls up run status,
  duration, and row counts per pipeline update.
* ``max_concurrent_streams``: ``10``.

Step 2: Generate
~~~~~~~~~~~~~~~~

.. code-block:: bash

   lhp generate -e dev

You should see output similar to:

.. code-block:: text

   Generated: my_project_event_log_monitoring/monitoring.py
   Generated monitoring notebook: monitoring/dev/union_event_logs.py
   Generated monitoring job resource: resources/lhp/my_project_event_log_monitoring.job.yml

Step 3: Inspect the generated layout
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: text

   generated/
   └── dev/
       ├── my_pipeline_a/
       ├── my_pipeline_b/
       └── my_project_event_log_monitoring/         # MVs-only pipeline
           └── monitoring.py

   monitoring/
   └── dev/
       └── union_event_logs.py                       # Streaming union notebook

   resources/
   └── lhp/
       ├── my_pipeline_a.pipeline.yml                # event_log block injected
       ├── my_pipeline_b.pipeline.yml                # event_log block injected
       ├── my_project_event_log_monitoring.pipeline.yml
       └── my_project_event_log_monitoring.job.yml

Generated artifacts
-------------------

Enabling monitoring produces up to three artifacts per environment.

Union notebook
~~~~~~~~~~~~~~

The notebook aggregates every eligible pipeline event log into the Delta table named by
``streaming_table`` (default ``all_pipelines_event_log``). It runs one streaming query
per pipeline, each with its own checkpoint.

Key behaviors:

* **Pre-created target.** The notebook pre-creates the target Delta table from the
  first readable source schema before launching the executor pool, so parallel streams
  do not race to create the table on a cold run.
* **Per-pipeline checkpoints.** Each source has its own directory under
  ``checkpoint_path``. Changing the set of sources never invalidates existing
  checkpoints.
* **Append-only with schema merge.** Streams use ``outputMode("append")`` with
  ``mergeSchema=true`` to absorb event-log schema evolution.
* **Finite batches.** ``trigger(availableNow=True)`` processes available data and then
  exits — suited to scheduled job runs, not always-on streaming.
* **Parallel execution.** Sources run concurrently via ``ThreadPoolExecutor`` bounded
  by ``max_concurrent_streams``.

MVs-only Lakeflow Declarative Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The pipeline at ``generated/{env}/{pipeline_name}/monitoring.py`` contains only
materialized views that read from the Delta table the notebook writes. There is no
DLT streaming table — that pattern was replaced in V0.8.2 by the notebook-based union.

The default ``events_summary`` MV summarizes each pipeline update:

.. code-block:: python
   :caption: monitoring.py (excerpt)

   @dp.materialized_view(
       name="acme_edw_dev._meta.events_summary",
       comment="Materialized view: events_summary",
   )
   def events_summary():
       return spark.sql("""
           WITH run_info AS (
               SELECT origin.pipeline_name, origin.pipeline_id, origin.update_id,
                      MIN(`timestamp`) AS run_start_time,
                      MAX(`timestamp`) AS run_end_time, ...
               FROM acme_edw_dev._meta.all_pipelines_event_log
               GROUP BY origin.pipeline_name, origin.pipeline_id, origin.update_id
           ), ...
           SELECT ri.pipeline_name, ri.update_id, ri.run_status, ...
       """)

When ``materialized_views: []`` and ``enable_job_monitoring`` is ``false``, the
pipeline has no actions and LHP omits it entirely — no ``monitoring.py``, no pipeline
resource, no pipeline task in the job. Only the notebook (and its notebook task) are
generated.

Workflow job
~~~~~~~~~~~~

The job resource at ``resources/lhp/{pipeline_name}.job.yml`` runs the notebook, then
the pipeline task via ``depends_on``:

.. code-block:: yaml
   :caption: resources/lhp/my_project_event_log_monitoring.job.yml

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
         queue:
           enabled: true
         performance_target: STANDARD

Override the defaults
---------------------

Use ``job_config_path``
~~~~~~~~~~~~~~~~~~~~~~~

Customize the job (cluster, schedule, permissions, notifications, tags) in the YAML
file referenced by ``monitoring.job_config_path``:

.. code-block:: yaml
   :caption: config/monitoring_job_config.yaml

   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 3600
   schedule:
     quartz_cron_expression: "0 0 * * * ?"
     timezone_id: UTC
     pause_status: UNPAUSED
   tags:
     purpose: event_log_monitoring
     environment: ${bundle_target}
   email_notifications:
     on_failure:
       - monitoring-alerts@company.com

LHP reads this file, applies ``${...}`` substitution from the active environment, and
deep-merges the result over its own defaults (``max_concurrent_runs=1``,
``performance_target=STANDARD``, ``queue.enabled=true``). Nested dicts merge
recursively; lists replace wholesale. The job name is always ``${pipeline_name}_job``
— do not add a ``job_name`` key.

Replace or extend the default MV
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Set ``materialized_views`` under ``monitoring`` to define your own list. The default
``events_summary`` is replaced — include it in your list to keep it.

.. code-block:: yaml
   :caption: lhp.yaml — custom MVs

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     job_config_path: "config/monitoring_job_config.yaml"
     materialized_views:
       - name: error_events
         sql: "SELECT * FROM all_pipelines_event_log WHERE event_type = 'error'"
       - name: daily_analysis
         sql_path: "sql/monitoring_custom_analysis.sql"

Each MV requires a ``name`` and exactly one of ``sql`` or ``sql_path``. Setting
``materialized_views: []`` produces a notebook-only setup (no DLT pipeline, no
pipeline task).

Target the monitoring pipeline with ``__eventlog_monitoring``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

To attach pipeline-level settings (compute, channel, edition) to the monitoring
pipeline without hard-coding its resolved name, use the reserved alias
``__eventlog_monitoring`` (note the double-underscore prefix) inside
``pipeline_config.yaml``:

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

LHP resolves the alias to the actual monitoring pipeline name at generation time
(``${project_name}_event_log_monitoring`` by default, or the value of
``monitoring.pipeline_name`` if you set one). The alias must appear as a standalone
``pipeline:`` entry — using it inside a list raises ``LHP-VAL-011``. Configuring both
the alias and the resolved name raises ``LHP-VAL-010``.

Opt pipelines out
~~~~~~~~~~~~~~~~~

To exclude a pipeline from the union, set ``event_log: false`` in
``pipeline_config.yaml``. Opted-out pipelines are removed from the notebook's
``SOURCES`` list and contribute no rows to the union Delta table.

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   ---
   pipeline: temp_debug_pipeline
   event_log: false

Troubleshooting
---------------

Monitoring errors use codes ``LHP-CFG-006`` through ``LHP-CFG-008`` (configuration)
and ``LHP-VAL-010`` / ``LHP-VAL-011`` (alias rules). Common cases:

* ``LHP-CFG-008`` *"Monitoring checkpoint_path is required"* — add ``checkpoint_path``
  under ``monitoring`` or set ``monitoring.enabled: false``.
* ``LHP-CFG-008`` *"Monitoring job_config_path is required"* — add ``job_config_path``
  or disable monitoring.
* ``LHP-CFG-008`` / ``LHP-IO-001`` *"Monitoring job_config file not found"* — the
  file referenced by ``job_config_path`` does not exist. The error message includes
  the resolved absolute path. ``lhp init`` scaffolds
  ``config/monitoring_job_config_env.yaml.tmpl`` as a starter.
* **No rows in ``all_pipelines_event_log``** — confirm the Workflow job has run
  successfully. The notebook creates the Delta table on its first successful write.
* **Missing pipelines in the union** — pipelines with ``event_log: false`` in
  ``pipeline_config.yaml`` are excluded by design.

For detailed before/after examples for each code, see :doc:`errors_reference`.

See also
--------

* :doc:`architecture` — why monitoring uses a notebook plus an MVs-only pipeline, and
  how the state file tracks the generated artifacts.
* :doc:`monitoring_reference` — exhaustive schema: every option, every default, the
  ``jobs_stats`` MV reference, and the reserved-alias rules.
* :doc:`operational_metadata` — related: inject row-level lineage columns into your
  bronze and silver tables.
