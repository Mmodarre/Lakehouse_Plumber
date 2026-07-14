=====================================
Monitor every pipeline from one place
=====================================

.. meta::
   :description: Turn on centralized event-log monitoring with two blocks in lhp.yaml — Lakehouse Plumber generates the union notebook, the run-summary materialized view, and the Workflow job that chains them across every pipeline in your project.

A project is rarely one pipeline. It is a bronze ingest, a handful of silver
transforms, a gold rollup — each writing its own Databricks event log. When you
want to answer one operational question across all of them — did every pipeline
run last night, how long did each take, how many rows moved, how many were
dropped by a quality gate — that answer is scattered across N separate event
logs.

You could hand-write the machinery that pulls them together: a notebook with one
streaming query per pipeline event log, each with its own checkpoint so adding a
pipeline never invalidates the others; a pre-create step so parallel writers do
not race to create the target table on a cold run; a ``ThreadPoolExecutor`` to
run the streams concurrently; a run-summary query that parses the event-log JSON
into status, duration, and row counts; and a Databricks Workflow job that runs
the notebook and then the summary. Or you add two blocks to ``lhp.yaml`` and let
Lakehouse Plumber write all of it. That is the idea on every page: **declare your
ETL, don't hand-write it** — and here, don't hand-wire your observability either.

Let's turn on monitoring for a project with a single bronze ``orders`` pipeline
and watch Lakehouse Plumber generate the union notebook, the run-summary view,
and the Workflow job that chains them.

Before you start
================

You need a Lakehouse Plumber project with at least one pipeline. Monitoring is a
**project-level** setting, not a per-flowgroup one — it observes the event logs
your pipelines emit, and it does not change any flowgroup. This guide uses one
bronze pipeline that streams CSV orders into a streaming table; any pipeline
works.

Two values you supply at configuration time are consumed at run time, not
generation time: ``checkpoint_path`` (a cloud storage path, typically a Unity
Catalog volume, where the union streams keep their checkpoints) and the event
log catalog and schema (where each pipeline publishes its event log). You do not
need either to *exist* to generate the code — but the pipeline will need them
when it runs.

Turn on monitoring
==================

Monitoring builds on the event log: each pipeline writes one, and monitoring
aggregates them. So you enable both with an ``event_log`` block and a
``monitoring`` block in ``lhp.yaml``.

Add them to ``lhp.yaml``:

.. literalinclude:: ../../_fixtures/guide_qo_monitoring/lhp.yaml
   :language: yaml
   :caption: lhp.yaml
   :emphasize-lines: 8-11, 16-18

The presence of the ``monitoring`` block, with ``event_log`` enabled, is what
turns monitoring on — ``enabled: true`` is the implicit default on both, and you
set ``monitoring.enabled: false`` to keep the configuration but skip generation.

- ``event_log`` names where each pipeline's event log lives. The per-pipeline
  table name is ``{name_prefix}{pipeline_name}{name_suffix}`` in the given
  catalog and schema — here ``${catalog}._meta.bronze_orders_event_log``. The
  ``${...}`` tokens resolve per environment from ``substitutions/<env>.yaml``.
- ``monitoring`` has exactly two required keys: ``checkpoint_path`` (the base
  path for the union streams) and ``job_config_path`` (a project-root-relative
  path to the Workflow-job config, covered next). Every other monitoring field —
  the pipeline name, the target Delta table name, the catalog and schema
  override, the materialized-view list — has a default. The full list of fields
  and defaults is in the monitoring reference.

Describe the Workflow job
=========================

``job_config_path`` points at a flat, single-document YAML file that describes
the monitoring Workflow job. Create ``config/monitoring_job_config.yaml``:

.. literalinclude:: ../../_fixtures/guide_qo_monitoring/config/monitoring_job_config.yaml
   :language: yaml
   :caption: config/monitoring_job_config.yaml

Lakehouse Plumber deep-merges this over its own defaults
(``max_concurrent_runs: 1``, ``performance_target: STANDARD``,
``queue.enabled: true``) and then resolves ``${...}`` tokens from the active
environment. Nested maps merge recursively; lists replace wholesale. The
``${bundle.target}`` token is a Databricks bundle-runtime variable, so it passes
through untouched into the generated resource.

.. note::

   The file must be a flat mapping. Do not wrap it in a ``project_defaults:``
   key and do not add a ``job_name:`` key — the job name is always derived as
   ``{pipeline_name}_job``, and the ``pipeline_task`` is generated for you. The
   full job schema (schedule, clusters, notifications, permissions) is in the
   monitoring reference.

Generate the pipelines
======================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  0 files
   ✓ retail_edw_event_log_monitoring  0 files
   ✓ validate (0.41s)
   2 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  1 file
   ✓ retail_edw_event_log_monitoring  1 file
   ✓ generate (0.37s)
   ✓ format (0.05s)
   ✓ monitoring (0.01s)
   2 pipelines generated · 2 files · 0.4s

Two things changed the moment monitoring turned on. A second pipeline,
``retail_edw_event_log_monitoring``, now appears alongside ``bronze_orders`` —
Lakehouse Plumber synthesized it from your configuration; there is no flowgroup
for it on disk. And the ``✓ monitoring`` step, a no-op in a project without
monitoring, now does real work: it writes the union notebook and the Workflow
job.

Read what Lakehouse Plumber wrote
=================================

Enabling monitoring produced three artifacts, none of which you wrote by hand:

- ``monitoring/dev/union_event_logs.py`` — the streaming union notebook.
- ``generated/dev/retail_edw_event_log_monitoring/monitoring.py`` — a
  materialized-view pipeline that reads the unioned table.
- ``resources/retail_edw_event_log_monitoring.job.yml`` — the Workflow job that
  chains them.

The union notebook is the heart of it. Open
``monitoring/dev/union_event_logs.py`` — this is the entire file, nothing hidden
behind a runtime:

.. literalinclude:: ../../_fixtures/guide_qo_monitoring/monitoring/dev/union_event_logs.py
   :language: python
   :caption: monitoring/dev/union_event_logs.py
   :emphasize-lines: 15-21, 82-88

Every design decision from the antithesis is here, generated:

- **The sources.** ``SOURCES`` is the resolved list of ``(pipeline_name,
  event_log_table)`` pairs — one entry per pipeline, ``bronze_orders`` mapped to
  ``retail_dev._meta.bronze_orders_event_log``. Add a pipeline and it appears
  here on the next generate.
- **Per-pipeline checkpoints.** Each stream writes to
  ``{CHECKPOINT_BASE}/{pipeline_name}``, so the set of pipelines can change
  without invalidating any existing checkpoint.
- **No cold-start race.** ``_ensure_target_exists`` pre-creates the target Delta
  table from the first readable source before the streams launch, so parallel
  writers do not collide creating it.
- **Finite, parallel runs.** The streams run in a ``ThreadPoolExecutor`` bounded
  by ``MAX_WORKERS`` (the ``max_concurrent_streams`` default of ``10``), each
  with ``trigger(availableNow=True)`` and ``mergeSchema=true`` — so the notebook
  processes what is available and exits, suited to a scheduled job.

The materialized-view pipeline at
``generated/dev/retail_edw_event_log_monitoring/monitoring.py`` is a single
``@dp.materialized_view`` named ``events_summary``. Its SQL rolls the unioned
event log up into one row per pipeline run — run status, trigger cause, duration
in minutes, tables processed, and rows upserted, deleted, and dropped. The exact
column list is in the monitoring reference; the point here is that you get a
queryable run-summary table without writing the query.

The Workflow job resource ties the two together:

.. literalinclude:: ../../_fixtures/guide_qo_monitoring/resources/retail_edw_event_log_monitoring.job.yml
   :language: yaml
   :caption: resources/retail_edw_event_log_monitoring.job.yml
   :emphasize-lines: 9-18

The ``union_event_logs`` notebook task runs first; the ``pipeline_task`` that
refreshes the ``events_summary`` view runs after it via ``depends_on``. Your
``max_concurrent_runs``, ``queue``, ``performance_target``, and ``tags`` from the
job-config file are merged in.

What you just did
=================

You wrote two blocks in ``lhp.yaml`` and a seven-line job-config file — around
fourteen lines of configuration. From that, Lakehouse Plumber generated **257
lines of monitoring plumbing: a 136-line union notebook, a 97-line
materialized-view pipeline, and a 24-line Workflow job.** Not one streaming
query, checkpoint path, ``ThreadPoolExecutor``, or event-log-parsing SQL
statement came from you.

And the ``bronze_orders`` flowgroup did not change at all. Monitoring is purely
additive and project-level: it reads the event logs your pipelines already emit,
so turning it on — or off — never touches a flowgroup. The generated notebook,
pipeline, and job are code you own: version them, diff them, and open them in the
Databricks editor like anything else.

What's next
===========

- **Replace the default view.** Set ``materialized_views`` under ``monitoring``
  to your own list — each entry needs a ``name`` and exactly one of ``sql`` or
  ``sql_path``. Your list replaces the default ``events_summary``; include it if
  you still want it. Setting ``materialized_views: []`` generates the notebook
  and job with no view pipeline at all.
- **Opt a pipeline out.** Set ``event_log: false`` for a pipeline in
  ``pipeline_config.yaml`` and it drops out of the notebook's ``SOURCES`` list,
  contributing no rows to the union.
- **Correlate runs with their jobs.** Set ``enable_job_monitoring: true`` to add
  a ``jobs_stats`` view that matches each pipeline update to the Databricks job
  that triggered it, via the Databricks SDK.
- **See every field.** The full ``event_log`` and ``monitoring`` schema, the
  complete job-config options, the ``events_summary`` column list, and the
  validation error codes are in the monitoring reference.
