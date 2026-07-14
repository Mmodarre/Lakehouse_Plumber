==============================
See how your pipelines connect
==============================

.. meta::
   :description: Run lhp dag to compute the dependency graph across your Lakehouse Plumber pipelines and flowgroups, pick an output format (text, json, dot, or a Databricks orchestration job), and read the execution order — instead of tracing the edges by hand.

A real project is many pipelines, and they feed each other. A ``bronze_ingest``
pipeline lands an ``orders`` table; a ``silver`` pipeline reads it. Multiply that
across a dozen pipelines and the wiring — who reads whom, what has to run first —
is real work, and it lives in no single YAML file.

You could reconstruct that graph by hand: open every flowgroup, read every load
action and every SQL ``FROM`` clause, and draw the arrows yourself. Or you let
Lakehouse Plumber read the connections back out of the YAML you already wrote.
That is the idea here: **you declared the connections; let the tool compute the
graph.**

Let's analyze a two-pipeline project — a ``bronze_ingest`` pipeline that lands
``orders`` and ``customers``, and a ``silver`` pipeline that joins them — and let
``lhp dag`` show how they connect.

Before you start
================

You need Lakehouse Plumber installed and a project with at least two flowgroups
where one reads a table another writes. The guide on composing a pipeline from
many flowgroups builds exactly this shape; this page picks it up and analyzes it.
``lhp dag`` reads your YAML statically — you do not need to have deployed or run
anything.

See the connection Lakehouse Plumber finds
===========================================

``lhp dag`` records a dependency wherever one action reads a table another action
writes. In this project the ``bronze_ingest`` pipeline writes a bronze ``orders``
streaming table:

.. literalinclude:: ../../_fixtures/guide_qo_dependency/pipelines/bronze_ingest.yaml
   :language: yaml
   :start-at: - name: write_orders_bronze
   :end-at: table: orders
   :caption: pipelines/bronze_ingest.yaml (the producer)

and the ``silver`` pipeline reads that same table:

.. literalinclude:: ../../_fixtures/guide_qo_dependency/pipelines/silver_orders.yaml
   :language: yaml
   :start-at: - name: load_orders_bronze
   :end-at: target: v_orders_bronze
   :caption: pipelines/silver_orders.yaml (the consumer)

Both name ``${catalog}.${bronze_schema}.orders``. That single shared reference is
the edge: ``silver`` depends on ``bronze_ingest``. The ``customers`` table forms a
second edge the same way.

.. note::

   Matching is byte-for-byte, after lowercasing and stripping backticks. Lakehouse
   Plumber does **not** resolve the ``${...}`` tokens during analysis — it compares
   the reference the producer writes against the reference the consumer reads as the
   literal strings you authored. They match here because both sides spell the
   reference with the identical token bytes. A producer written with concrete
   values (``dev_catalog.bronze.orders``) would not match a consumer written with
   tokens.

Run the analysis
================

Run ``lhp dag`` from the project root:

.. code-block:: console

   $ lhp dag
   Dependency analysis
     Pipelines analyzed: 2
     Execution stages: 2
     External sources: 0
   Execution order
   Stage  Pipeline       Notes
   1      bronze_ingest
   2      silver
   Dependencies
   Pipeline       Depends on
   bronze_ingest  -
   silver         bronze_ingest
   Generated files
   Format  Label  Path
   DOT     -      .lhp/dependencies/pipeline_dependencies.dot
   JSON    -      .lhp/dependencies/pipeline_dependencies.json
   TEXT    -      .lhp/dependencies/pipeline_dependencies.txt
   JOB     -      .lhp/dependencies/guide_qo_dependency_orchestration.job.yml

Read the summary top-down: two pipelines, two execution stages, zero external
sources. ``bronze_ingest`` is Stage 1 — it depends on nothing inside the project.
``silver`` is Stage 2 — it depends on ``bronze_ingest``, so it cannot start until
Stage 1 finishes. Zero external sources means every table read resolved to a
producer inside the project; nothing points outside it.

With no ``--format`` given, ``lhp dag`` writes one file per format into
``.lhp/dependencies/``. Use ``--output`` to write them elsewhere.

.. note::

   The command was named ``lhp deps`` in earlier releases. That name still works as
   a hidden alias but prints a deprecation notice — use ``lhp dag``.

Read the text report
====================

The ``text`` format is the human-readable report. Open
``.lhp/dependencies/pipeline_dependencies.txt``:

.. code-block:: text

   ================================================================================
   LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS
   ================================================================================
   Generated at: 2026-07-14 23:14:24

   SUMMARY
   ----------------------------------------
   Total Pipelines: 2
   Total Execution Stages: 2
   External Sources: 0
   Circular Dependencies: 0

   EXECUTION ORDER
   ----------------------------------------
   Stage 1: bronze_ingest
   Stage 2: silver

   PIPELINE DETAILS
   ----------------------------------------
   Pipeline: bronze_ingest
     Flowgroups: 2
     Actions: 4
     Depends on: None
     Stage: 0
     Can run parallel: False

   Pipeline: silver
     Flowgroups: 1
     Actions: 4
     Depends on: bronze_ingest
     Stage: 1
     Can run parallel: False

   DEPENDENCY TREE
   ----------------------------------------
   └── bronze_ingest (2 flowgroups, 4 actions)
       └── silver (1 flowgroups, 4 actions)

``DEPENDENCY TREE`` is the same graph drawn as an indented tree, and
``PIPELINE DETAILS`` adds per-pipeline flowgroup and action counts. Two pipelines
in the same stage would list ``Can run parallel: True`` — here each stage holds one
pipeline. The per-pipeline ``Stage:`` field counts from 0 (``bronze_ingest`` at
stage 0), while the ``EXECUTION ORDER`` list numbers the stages from 1; both name
the same order.

Choose an output format
=======================

``lhp dag`` writes four formats. ``--format`` picks which — a single value, a
comma-separated list, or ``all`` (the default):

.. list-table::
   :header-rows: 1
   :widths: 12 88

   * - Format
     - What you get
   * - ``text``
     - The human-readable report above: summary, execution order, per-pipeline
       detail, dependency tree.
   * - ``json``
     - Structured data for a CI gate or a custom visualizer.
   * - ``dot``
     - GraphViz source for a picture — one file at pipeline granularity, one at
       flowgroup granularity.
   * - ``job``
     - A Databricks orchestration job that runs the pipelines in dependency order.

For programmatic use, the ``json`` format is the structured form of the same
analysis:

.. code-block:: json

   {
     "metadata": {
       "total_pipelines": 2,
       "total_external_sources": 0,
       "total_stages": 2,
       "has_circular_dependencies": false,
       "total_warnings": 0,
       "total_warning_occurrences": 0
     },
     "pipelines": {
       "bronze_ingest": {
         "depends_on": [],
         "flowgroup_count": 2,
         "action_count": 4,
         "external_sources": [],
         "can_run_parallel": false,
         "stage": 0
       },
       "silver": {
         "depends_on": [
           "bronze_ingest"
         ],
         "flowgroup_count": 1,
         "action_count": 4,
         "external_sources": [],
         "can_run_parallel": false,
         "stage": 1
       }
     },
     "execution_stages": [
       ["bronze_ingest"],
       ["silver"]
     ],
     "external_sources": [],
     "circular_dependencies": [],
     "warnings": []
   }

The ``dot`` format is GraphViz source for a diagram. The pipeline-level graph is
small enough to show whole:

.. code-block:: text

   digraph pipeline_dependencies {
     rankdir=LR;
     node [shape=box];
     "bronze_ingest" [label="bronze_ingest\n(2 flowgroups)"];
     "silver" [label="silver\n(1 flowgroups)"];
     "bronze_ingest" -> "silver";
   }

``lhp dag`` also writes ``flowgroup_dependencies.dot`` — the same graph one level
down, so you can see which flowgroup reads which:

.. code-block:: text

   digraph flowgroup_dependencies {
     rankdir=LR;
     node [shape=box];
     "bronze_ingest.orders_ingest" [label="bronze_ingest.orders_ingest\n(2 actions)"];
     "bronze_ingest.customers_ingest" [label="bronze_ingest.customers_ingest\n(2 actions)"];
     "silver.orders_silver" [label="silver.orders_silver\n(4 actions)"];
     "bronze_ingest.orders_ingest" -> "silver.orders_silver";
     "bronze_ingest.customers_ingest" -> "silver.orders_silver";
   }

Render either file with any GraphViz tool to get boxes and arrows.

The ``job`` format turns the same graph into a Databricks orchestration job — one
task per pipeline, wired so Stage 1 runs before Stage 2:

.. code-block:: yaml
   :caption: .lhp/dependencies/guide_qo_dependency_orchestration.job.yml (task block)

   resources:
     jobs:
       guide_qo_dependency_orchestration:
         name: guide_qo_dependency_orchestration
         max_concurrent_runs: 1
         tasks:
           - task_key: bronze_ingest_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.bronze_ingest_pipeline.id}
               full_refresh: false
           - task_key: silver_pipeline
             depends_on:
               - task_key: bronze_ingest_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.silver_pipeline.id}
               full_refresh: false

The generated file also emits a block of commented-out job options — schedule,
notifications, tags, permissions — for you to uncomment. Deploying this job, naming
it with ``--job-name``, and writing it straight into ``resources/`` with
``--bundle-output`` are orchestration concerns; the deploy-and-orchestrate guide
covers them, and the CLI reference lists every ``lhp dag`` flag.

Focus on one pipeline
=====================

On a larger project you often want one pipeline's view alone. ``--pipeline``
restricts the analysis to it:

.. code-block:: console

   $ lhp dag --format text --pipeline silver
   Dependency analysis
     Pipelines analyzed: 1
     Execution stages: 1
     External sources: 2
   Execution order
   Stage  Pipeline  Notes
   1      silver
   Dependencies
   Pipeline  Depends on
   silver    -
   External sources
     ${catalog}.${bronze_schema}.customers
     ${catalog}.${bronze_schema}.orders
   Generated files
   Format  Label  Path
   TEXT    -      .lhp/dependencies/pipeline_dependencies.txt

With ``bronze_ingest`` out of scope, ``silver``'s two reads no longer match any
producer, so they surface as **external sources** — printed with the tokens
verbatim, because analysis never resolves them. The ``job`` format is skipped under
any filter: an orchestration job is a whole-project artifact.

What you just did
=================

One command turned a two-pipeline project into its dependency graph — execution
order, per-pipeline detail, machine-readable JSON, a GraphViz picture, and a
ready-to-deploy orchestration job — without you tracing a single edge by hand. You
never wrote down "silver runs after bronze"; ``lhp dag`` derived it from the
``orders`` table one pipeline writes and the other reads.

The graph stays derived, so it stays correct. Add a ``gold`` pipeline that reads
``silver``'s ``orders_fct`` table and it slots in as Stage 3 on the next run — you
never edit the graph, because there is no graph to edit, only reads you declared.

What's next
===========

- **Run the pipelines in order.** ``lhp dag --format job --bundle-output`` writes
  the orchestration job into ``resources/`` for a Databricks Asset Bundle to
  deploy. The deploy-and-orchestrate guide covers the job flags end to end.
- **Declare an edge the parser cannot see.** When a table is read through a value
  known only at run time, static analysis cannot find it. Add the missing upstream
  with the ``depends_on`` field on the action, covered in the dependency-analysis
  reference.
- **Understand how matching works.** Which reads become edges, how two-part and
  three-part references reconcile, and what counts as an external source are
  explained in the dependency-analysis concept page.
