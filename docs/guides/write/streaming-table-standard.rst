==================================
Configure a streaming table write
==================================

.. meta::
   :description: Shape a Databricks streaming table in Lakehouse Plumber — set its comment, Delta table_properties, and clustering, and fan two sources into one table — from declarative write_target fields, without hand-writing create_streaming_table or an append flow per source.

The streaming table you built in Get Started was bare: a name, a source, an
append flow. A real silver table carries more. It wants a **comment** so the
next engineer knows what it holds, **Delta properties** like Change Data Feed
and auto-optimize, a **clustering key** so queries prune, and it is often fed by
**more than one source** at once. Let's build that table — one silver ``orders``
table, clustered and change-data-feed enabled, fed by two bronze channels.

Hand-written, each of those is a keyword argument you look up and type onto
``dp.create_streaming_table(...)``, and every extra source is another
``spark.readStream`` plus another ``@dp.append_flow`` you wire yourself. Declared,
they are fields on ``write_target`` and one write action per source. Lakehouse
Plumber writes the call. That is the whole idea: **declare your ETL, don't
hand-write it.**

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and two upstream bronze tables to read — here an ``orders_online``
and an ``orders_instore`` Delta table your two channels publish. This guide
assumes you already know the basic single-source append from **Your first
pipeline**; it goes deeper on the write target itself.

Declare the write target's options
==================================

The write target is where you describe the physical table. Create
``pipelines/orders_union.yaml``. The first write action, ``write_orders_online``,
owns the table — it carries every write-target option:

.. literalinclude:: ../../_fixtures/guide_write_st_standard/pipelines/orders_union.yaml
   :language: yaml
   :caption: pipelines/orders_union.yaml
   :emphasize-lines: 43-49, 62

Each highlighted field maps to one argument on the generated
``create_streaming_table`` call:

- ``comment`` documents the table in Unity Catalog.
- ``table_properties`` is a map of Delta properties — here it turns on Change
  Data Feed and auto-optimize and stamps a ``quality`` marker.
- ``cluster_columns`` sets the liquid-clustering key. For automatic clustering
  instead, set ``cluster_by_auto: true`` (the two are mutually exclusive). To
  partition rather than cluster, use ``partition_columns``.

The ``${catalog}`` and ``${silver_schema}`` tokens resolve per environment from
``substitutions/dev.yaml``, so the same flowgroup targets dev, staging, and prod
unchanged. This guide stays on the standard append (``mode: standard``); the
write action reference covers the full option set — ``table_schema``, ``tags``,
``row_filter``, ``spark_conf``, ``path`` — and the ``cdc`` and ``snapshot_cdc``
modes.

Fan in a second source
======================

One silver table, two channels. The second write action,
``write_orders_instore``, targets the **same** ``catalog.schema.table`` and sets
``create_table: false``. That is the rule that lets two actions share a table:
exactly one owns creation, the rest only append.

.. important::

   Across everything targeting a given streaming table, exactly one action may
   set ``create_table: true`` (the default). Every other action feeding the same
   table must set ``create_table: false``, or generation fails — two actions
   cannot both create the table. The creating action is where the write-target
   options live.

Lakehouse Plumber combines the two actions into a single table declaration plus
one append flow each. The channels fan in without a ``UNION`` in your SQL and
without you writing a second ``readStream``.

Generate the pipeline
=====================

Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.34s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.34s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/silver_orders/orders_union.py``. This is the entire
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_st_standard/generated/dev/silver_orders/orders_union.py
   :language: python
   :caption: generated/dev/silver_orders/orders_union.py
   :emphasize-lines: 40-46

Every option you declared landed on one ``dp.create_streaming_table`` call:
``comment`` resolved into the argument, ``table_properties`` into a
``table_properties={...}`` dict, ``cluster_columns`` into ``cluster_by=["customer_id"]``,
and your tokens into the three-part name ``dev_catalog.silver.orders``. Below it,
LHP wrote **two** ``@dp.append_flow`` functions — ``f_orders_online`` and
``f_orders_instore`` — each reading its own view and appending into the one
table. The second write action added no second ``create_streaming_table``; it
contributed only its flow.

What you just did
=================

You wrote two write actions — 26 lines of YAML — and Lakehouse Plumber generated
one ``create_streaming_table`` call carrying every option and two append flows,
one per channel. Across the whole flowgroup, **47 lines of YAML became 74 lines
of Lakeflow Python, with zero lines of streaming-table plumbing from you**: not
the ``create_streaming_table`` call, not its keyword arguments, not the two
``@dp.append_flow`` functions, not a single ``readStream``.

The physical shape of the table is now a set of declarative fields. Add a
clustering key, flip a Delta property, or fan in a third channel, and you edit
YAML and regenerate — you never touch the Python.

What's next
===========

- **Tag the table for governance** — add a ``tags`` map to ``write_target`` and
  Lakehouse Plumber generates a per-pipeline UC tagging hook (over 350 lines that
  drive the Unity Catalog REST API) so the tags apply during the run. See the
  write action reference.
- **Capture changes instead of appending** — switch the write target to
  ``mode: cdc`` for SCD Type 1/2 upserts, covered in the CDC patterns guide.
- **Compute a table from a query** — when the table is a batch aggregation rather
  than an incremental append, reach for a materialized view, covered in the
  materialized view guide.
- For the full option set — ``table_schema``, ``row_filter``, ``tags``,
  ``cluster_by_auto``, and the ``cdc`` / ``snapshot_cdc`` modes — see the
  **Write action reference**.
