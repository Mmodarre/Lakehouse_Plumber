Performance
===========

.. meta::
   :description: Reasoning behind LHP performance choices — streaming versus materialized view trade-offs, dependency resolution, and runtime tuning levers.

For the decision frameworks behind individual generator choices, see
:doc:`../decisions`. This page explains the performance properties LHP
inherits or constructs, and the trade-offs the framework intentionally
exposes.

Two performance domains, not one
--------------------------------

LHP has two performance stories that get conflated. *Generation
performance* is how long ``lhp generate`` takes. *Runtime performance*
is how long the Databricks pipeline takes once deployed. They are
governed by different mechanisms and tuned with different levers.

Generation is bounded by file I/O and YAML parsing; a project with one
thousand :term:`FlowGroups <FlowGroup>` generates in seconds.
Runtime is bounded by data volume, shuffle costs, and the cost of
joins; LHP does not control these directly, but the choice of write
target (:term:`streaming table <Streaming table>` versus :term:`materialized view <Materialized view>`) and the use of
``cluster_columns`` versus ``partition_columns`` affect them
significantly.

Streaming tables versus materialized views
------------------------------------------

The most consequential write-target decision in any LHP project is
``streaming_table`` versus ``materialized_view``. The wrong choice
shows up at the worst time — when a dimension changes and downstream
metrics quietly stop updating, or when a backfill takes ten times
longer than expected.

The mental model that holds up: streaming tables are append-optimal
and incremental, materialized views are recompute-correct and may
fully recompute on source changes.

**Streaming tables** generate ``dp.create_streaming_table()`` plus
``@dp.append_flow()`` functions. They process new records as they
arrive. They do not recompute when old data or referenced dimensions
change. The right uses are bronze ingestion (each record is processed
once), change-data-capture targets (``mode: cdc`` with explicit
``cdc_config``), and any append-only flow where you want lower latency
and lower cost.

**Materialized views** generate ``@dp.materialized_view()``. They
reprocess when source data changes — including when joined dimensions
change. They are the correct choice for any logic that depends on
multiple tables maintaining a consistent view. For silver enrichment
that joins a fact stream against a slowly-changing dimension, the
materialized view is the only target that gives correct results
without you writing your own change-tracking logic.

The trap that catches most teams is using streaming tables for
joins. The streaming table picks up new fact rows. It does not see
dimension updates, so a customer's enriched record retains whatever
``customer_name`` was current at the moment the fact arrived. For
analytics, this is usually wrong. The fix is the materialized view.

CDC patterns and snapshot CDC
-----------------------------

LHP supports two CDC modes on streaming tables. The ``cdc`` mode
generates ``dp.create_auto_cdc_flow()`` and consumes a change feed
with ``keys``, ``sequence_by``, and ``scd_type`` (1 or 2). The
``snapshot_cdc`` mode generates ``dp.create_auto_cdc_from_snapshot_flow()``
and consumes full snapshots, computing the differences itself.

The choice depends on what the source provides. If you have a
change feed — Debezium output, a Delta change-data feed, a Kafka
topic with insert/update/delete markers — use ``cdc``. If you have
periodic full dumps with no change indicators, use ``snapshot_cdc``
and let Databricks detect changes by comparing snapshots.

The ``snapshot_cdc`` mode is more expensive at runtime (it has to
diff snapshots) but tolerates messier sources. The trade-off is
worth it when the source genuinely does not emit change events.

Rate limiting and Auto Loader
-----------------------------

CloudFiles loads can flood downstream tables if the landing zone has a
large backlog. The Auto Loader options ``cloudFiles.maxFilesPerTrigger``
and ``cloudFiles.maxBytesPerTrigger`` cap per-microbatch ingestion, so
bronze ingestion runs at a sustainable rate and downstream pipelines
keep up.

Set these in a ``brz_standard`` preset, not on individual FlowGroups —
the values are organisational policy, not per-FlowGroup tuning.
Pairing them with ``cloudFiles.schemaEvolutionMode: rescue`` and
``cloudFiles.rescuedDataColumn: _rescued_data`` (also in the preset)
makes the bronze layer robust to schema drift: unknown fields land in
``_rescued_data`` rather than failing the load, and the rate limiter
keeps the ingestion pace within capacity.

Clustering and partitioning
---------------------------

LHP supports both ``cluster_columns`` (liquid clustering) and
``partition_columns`` on write targets. The modern recommendation is
``cluster_columns`` for almost every case. Liquid clustering is
incremental, allows the keys to be redefined without rewriting the
table, and works well with high-cardinality columns where
partitioning would create too many small files.

Partition columns still have a place for very predictable
date-partitioned access patterns, but the default should be liquid
clustering. The cost of getting it wrong with clustering is lower —
you change the keys and continue — while the cost of getting it
wrong with partitioning can mean rewriting the table.

Dependency resolution as a non-decision
---------------------------------------

LHP's dependency resolver topologically sorts actions within a
FlowGroup. The Load comes first, then Transforms in dependency order,
then Write, then Tests. You can list actions in any order in the YAML;
LHP works out the right one.

This is intentional. Asking the YAML author to maintain action order
manually creates a class of bug (an action referencing a target
defined later in the file) that the resolver eliminates entirely.
The recommendation to list actions in Load/Transform/Write/Test order
is for readability, not correctness.

The same principle applies across FlowGroups within a pipeline.
``lhp deps`` analyses cross-FlowGroup dependencies and produces a
dependency graph at pipeline level. The orchestration job that LHP
generates (``--format job``) respects this graph, so a downstream
pipeline only runs after its upstream completes.

Anti-patterns
-------------

**Streaming tables for join-based enrichment.** Stale dimension data
is the predictable outcome. Materialized views are correct here.

**Partition columns by reflex.** Liquid clustering is the modern
default, and it tolerates being wrong much better.

See also
--------

- :doc:`../decisions` for write-target and action-type decision
  frameworks.
- :doc:`../dependency_analysis` for how ``lhp deps`` works under the
  hood.
- :doc:`../actions/write_actions` for the full streaming-table and
  materialized-view configuration surface.
