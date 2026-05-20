Decisions
=========

.. meta::
   :description: Decision matrices for common Lakehouse Plumber design choices: which reuse primitive to pick, streaming vs batch, load source, write target, write mode, and single vs multi-job orchestration.

Most LHP design choices reduce to a handful of decisions. Each section gives
a one-screen matrix and a pointer to the relevant reference page. For
definitions of FlowGroups, Actions, Presets, Templates, and Blueprints, see
:doc:`architecture`.

Preset vs Template vs Blueprint
-------------------------------

LHP has five reusability primitives. They layer rather than compete. Pick
the one that factors out the axis of repetition you want to eliminate.

.. list-table::
   :header-rows: 1
   :widths: 20 60 20

   * - Primitive
     - Use when you want to factor out…
     - Where it lives
   * - **Action**
     - Nothing — this is the atomic unit.
     - Inside a FlowGroup
   * - **Preset**
     - Default values (table properties, ``cloudFiles`` options, Spark
       config) repeated across actions of one type.
     - ``presets/*.yaml``
   * - **Template**
     - A parametrised group of actions repeated inside a single FlowGroup.
     - ``templates/*.yaml``
   * - **FlowGroup**
     - Nothing — this is the unit of generation.
     - ``pipelines/**/*.yaml``
   * - **Blueprint**
     - A parametrised list of FlowGroups repeated across many similar
       deployments.
     - ``blueprints/*.yaml``
   * - **Instance**
     - Parameter values supplied to a Blueprint.
     - ``pipelines/**/*.yaml`` (co-located)

.. tip::
   Factor by the **smallest axis that repeats**. Three actions repeating
   with a table name is a Template; twenty FlowGroups repeating across
   regional sites is a Blueprint.

Template vs Blueprint — the most common confusion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both reduce repetition, but at different granularities.

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Use a Template when…
     - Use a Blueprint when…
   * - The same three actions ingest a CSV table; you want to parametrise
       them by table name.
     - The same bronze/silver shape repeats across ten regional sites;
       you want to parametrise it by ``site_name``.
   * - One Template + one FlowGroup yields one FlowGroup's actions.
     - One Blueprint + N instances yield N × M synthetic FlowGroups.
   * - Used via ``use_template:`` in a FlowGroup file.
     - Used via ``use_blueprint:`` in an instance file.
   * - Parameters as Jinja2 ``{{ var }}``.
     - Parameters as ``%{var}`` local variables.

The two compose: a Blueprint FlowGroup spec can declare ``use_template:``
like a disk-sourced FlowGroup.

Preset layering — how many to apply
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Presets deep-merge in order; explicit action config always wins. Pick
depth by how much variation the workload tolerates:

- **One global preset** (for example ``bronze_layer``) when every Bronze
  table shares the same options.
- **Layered presets** (``bronze_layer`` + ``cdc_overrides``) when a
  subset of FlowGroups overrides a few keys. Reach for ``extends:`` only
  when the second preset is reusable as a named building block.
- **No preset** when the FlowGroup is one-of-a-kind — inlining a single
  option is clearer than naming a preset for it.

Streaming vs batch
------------------

:term:`Lakeflow Declarative Pipeline` decides execution order at runtime, but you
pick streaming or batch when you choose how a Load reads and which Write
target receives. The two ends must agree.

.. list-table::
   :header-rows: 1
   :widths: 30 35 35

   * - Workload shape
     - Pick at Load
     - Pick at Write
   * - Files trickling into object storage; exactly-once and schema
       evolution.
     - ``cloudfiles``
     - ``streaming_table``
   * - Delta source appended by another job; you want the latest rows.
     - ``delta`` with ``readMode: stream``
     - ``streaming_table``
   * - Delta source you re-aggregate on a schedule.
     - ``delta`` with ``readMode: batch``
     - ``materialized_view``
   * - Change Data Capture (:term:`CDC`) events; :term:`SCD` Type 1 or Type 2.
     - ``delta`` (CDF) or ``cloudfiles``
     - ``streaming_table`` ``mode: cdc``
   * - Full snapshots that LHP diffs.
     - ``delta`` or ``python``
     - ``streaming_table`` ``mode: snapshot_cdc``
   * - Gold dashboards; freshness measured in hours.
     - ``sql`` against Silver tables
     - ``materialized_view``

If you mix the two — a batch Load feeding a ``streaming_table`` — the
pipeline parses, but Lakeflow re-reads the source on every refresh.
Usually not what you want.

Choosing a load source
----------------------

Pick the Load sub-type by what the data looks like at rest and how it is
delivered:

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Sub-type
     - Use when…
   * - ``cloudfiles``
     - Files arrive in object storage (S3, ADLS, GCS, Unity Catalog
       volumes); incremental ingestion with checkpoints and schema
       evolution. Streaming only.
   * - ``delta``
     - Reading an existing Delta table or its Change Data Feed (CDF).
       Batch or streaming.
   * - ``sql``
     - An arbitrary SQL query materialised as a temporary view (often
       joins or windowed aggregates across already-loaded sources).
   * - ``jdbc``
     - Pulling from an external RDBMS (Oracle, SQL Server, Postgres,
       MySQL); credentials via Databricks secrets.
   * - ``python``
     - The format is not covered by a built-in sub-type, or you need
       custom pre-processing in Python before the flow sees the data.
   * - ``custom_datasource``
     - You have or want a PySpark DataSourceV2 implementation and want
       LHP to register and invoke it.

For the full sub-type reference see :doc:`actions/load_actions`; for a
walk-through of ``cloudfiles`` see :doc:`ingest_with_autoloader`.

Choosing a write target
-----------------------

Pick the Write sub-type by the kind of object you want to produce:

.. list-table::
   :header-rows: 1
   :widths: 25 75

   * - Sub-type
     - Use when…
   * - ``streaming_table``
     - Persisting incrementally arriving data. Bronze and Silver layers,
       CDC, fan-in from multiple sources. Supports ``standard``, ``cdc``,
       and ``snapshot_cdc`` modes.
   * - ``materialized_view``
     - Batch-computed analytics — Gold dashboards and aggregations that
       refresh on a schedule.
   * - ``sink``
     - Pushing data **out** of the lakehouse — Kafka, Delta to an
       external catalog, Azure Event Hubs, or a custom REST endpoint.

For the full sub-type reference see :doc:`actions/write_actions`.

Choosing a write mode (streaming_table)
---------------------------------------

Within ``streaming_table``, the ``mode`` field decides how rows are applied
to the target:

.. list-table::
   :header-rows: 1
   :widths: 20 40 40

   * - Mode
     - When to use
     - Notes
   * - ``standard`` *(default)*
     - Append-only or fan-in workloads where each row is new.
     - Multiple write actions targeting the same table fan in via
       ``@dp.append_flow``; only the first action sets
       ``create_table: true``.
   * - ``cdc``
     - Source emits change events (insert / update / delete) with a
       sequence column. SCD Type 1 or Type 2 targets.
     - Requires ``cdc_config.keys`` and ``sequence_by``. Supports
       ``scd_type: 1`` or ``scd_type: 2`` with optional
       ``track_history_column_list``. Multi-CDC fan-in works when
       contributors agree on the shared CDC fields.
   * - ``snapshot_cdc``
     - Source delivers full snapshots rather than change events; LHP
       diffs successive snapshots.
     - Backed by ``create_auto_cdc_from_snapshot_flow()``. Source is a
       Delta table or a Python function returning ``(df, version)``.

Single-job vs multi-job orchestration
-------------------------------------

By default, LHP generates one orchestration job that runs every Lakeflow
pipeline in dependency order. Set ``job_name`` on a FlowGroup to split
work across multiple jobs.

.. list-table::
   :header-rows: 1
   :widths: 35 65

   * - Pick…
     - When…
   * - **Single job** (omit ``job_name``)
     - All pipelines share one cadence; you want one place to monitor
       success; the dependency graph is small enough to run end-to-end on
       every trigger.
   * - **Multi-job** (set ``job_name`` on every FlowGroup)
     - Pipelines have different schedules (hourly POS, nightly ERP);
       source systems need isolation (a failing SAP feed must not block
       NCR ingestion); different teams own different jobs and want
       separate alerting and permissions.

If you set ``job_name`` on any FlowGroup, you must set it on every
FlowGroup — the validator rejects mixed configurations. LHP emits a
master orchestration job that triggers the per-system jobs in dependency
order. For the full reference see :doc:`bundle_config_reference`.

See also
--------

- :doc:`architecture` — definitions for FlowGroups, Actions, Presets,
  Templates, and Blueprints.
- :doc:`presets_reference` — preset schema and deep-merge semantics.
- :doc:`templates_reference` — Jinja2 parameter syntax and template
  composition.
- :doc:`blueprints` — Blueprint and instance schema, parameter validation,
  and expansion.
