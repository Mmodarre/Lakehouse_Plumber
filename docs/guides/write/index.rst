=====
Write
=====

.. meta::
   :description: Choose the right Lakehouse Plumber write target — a standard streaming table, CDC or snapshot CDC for dimensions, a materialized view for aggregates, or an external sink.

A ``write`` action persists a flow into a target. The write **mode** you choose
depends on how the data changes and what shape you need downstream.

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - You need to…
     - Reach for
     - Guide
   * - Append or upsert rows into a table as they arrive
     - A **standard streaming table**
     - :doc:`streaming-table-standard`
   * - Track row changes over time (a slowly changing dimension) from a change feed
     - **CDC** into a streaming table (``mode: cdc``)
     - :doc:`streaming-table-cdc`
   * - Build a dimension from periodic full **snapshots** instead of a change feed
     - **Snapshot CDC** (``mode: snapshot_cdc``)
     - :doc:`streaming-table-snapshot-cdc`
   * - Serve a query result that Databricks keeps fresh (an aggregate, a join)
     - A **materialized view**
     - :doc:`materialized-view`
   * - Send rows out to Kafka, Delta, or another external system
     - An **external sink**
     - :doc:`sinks`

.. tip::

   Streaming table vs materialized view, in one line: a **streaming table** is
   *incremental* — it processes new rows as they arrive. A **materialized view**
   is *declarative* — it re-derives its full result when its inputs change. Use a
   streaming table for ingestion and CDC; a materialized view for aggregates and
   joins over existing tables.

.. toctree::
   :maxdepth: 1
   :hidden:

   Streaming table <streaming-table-standard>
   CDC (change feed) <streaming-table-cdc>
   CDC (snapshots) <streaming-table-snapshot-cdc>
   Materialized view <materialized-view>
   External sinks <sinks>
