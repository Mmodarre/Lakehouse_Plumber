.. meta::
   :description: Migrate an existing Databricks Lakeflow Declarative Pipelines (formerly Delta Live Tables / DLT) Python codebase to Lakehouse Plumber (LHP) YAML flowgroups, one pattern at a time.

Migrate from DLT to LHP
=======================

This how-to walks you through migrating an existing Databricks :term:`Lakeflow
Declarative Pipeline` codebase (still widely known by its former name, :term:`Delta Live
Tables <DLT>`, or DLT) to Lakehouse Plumber (LHP) YAML. The four patterns below
(:term:`streaming table <Streaming table>` from cloud files, :term:`materialized view <Materialized view>`, :term:`Change Data Capture <CDC>`, and
:term:`expectations <Expectation>`) cover the vast majority of production DLT code.

LHP-generated Python imports ``from pyspark import pipelines as dp`` and emits
``@dp.table``, ``@dp.materialized_view``, ``dp.create_streaming_table``,
``dp.create_auto_cdc_flow``, and friends. If your existing code uses
``import dlt`` and ``@dlt.table``, the decorators are functionally equivalent —
LHP simply moves you to the supported ``pyspark.pipelines`` alias as a
side-effect of regeneration.

Why migrate?
------------

DLT Python code grows linearly with the number of tables: each table is a
hand-written decorator plus a function, and shared logic (operational metadata,
column lists, CDC keys) tends to copy-paste across files. LHP collapses the
boilerplate into declarative YAML, applies presets and templates across
flowgroups, and regenerates idempotently — so a single change to a preset
propagates everywhere.

Migration approach
------------------

Migrate **one flowgroup at a time**. The LHP-generated Python lives alongside
your existing DLT code in the same Lakeflow pipeline until you cut over:

1. Pick a single table (or a small set of related tables in one bronze/silver
   layer) to migrate first.
2. Write the YAML flowgroup under ``pipelines/<pipeline_name>/``.
3. Run ``lhp generate --env dev`` to produce the Python file under
   ``generated/``.
4. Diff the generated Python against your hand-written DLT file. Adjust the
   YAML until the diff is acceptable (it will not be byte-identical — LHP uses
   ``@dp.append_flow`` and ``dp.create_streaming_table`` patterns that may be
   structured differently from your original).
5. Swap the file references in your pipeline configuration to point at the
   generated file, retire the original ``.py``, and move to the next flowgroup.

Because each flowgroup is independent, you can run a hybrid pipeline where some
tables are LHP-generated and others are still hand-written DLT during the
transition.

Pattern 1: Streaming table from cloud files
-------------------------------------------

The most common ingestion pattern in DLT: an Auto Loader streaming table fed
from a cloud storage path. In LHP this becomes a ``cloudfiles`` load action
plus a ``streaming_table`` write action.

**Existing DLT code:**

.. code-block:: python
   :caption: bronze_events.py (existing DLT)

   import dlt
   from pyspark.sql.functions import col

   @dlt.table(
       name="main.bronze.events",
       comment="Raw events from object storage",
   )
   def events():
       return (
           spark.readStream.format("cloudFiles")
               .option("cloudFiles.format", "json")
               .option("cloudFiles.schemaLocation", "/Volumes/main/bronze/schemas/events")
               .option("cloudFiles.inferColumnTypes", "true")
               .load("/Volumes/main/landing/events/")
       )

**LHP YAML:**

.. code-block:: yaml
   :caption: pipelines/bronze/events.yaml

   pipeline: bronze
   flowgroup: events
   actions:
     - name: load_events
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: /Volumes/main/landing/events/
         format: json
         options:
           cloudFiles.schemaLocation: /Volumes/main/bronze/schemas/events
           cloudFiles.inferColumnTypes: "true"
       target: v_events_raw

     - name: write_events
       type: write
       source: v_events_raw
       write_target:
         type: streaming_table
         catalog: main
         schema: bronze
         table: events
         comment: Raw events from object storage

The load action generates a ``@dp.temporary_view()``-decorated function named
``v_events_raw`` that opens the ``cloudFiles`` reader. The write action
generates ``dp.create_streaming_table(name="main.bronze.events", ...)`` followed
by a ``@dp.append_flow(target="main.bronze.events", ...)`` reading from
``v_events_raw``. The two-step view-then-append pattern is what enables LHP's
later patterns (multiple sources writing into one table, CDC, etc.) without
restructuring the YAML.

Pattern 2: Materialized view
----------------------------

A DLT table that returns a complete DataFrame (not a streaming read) becomes an
LHP ``materialized_view`` write. The most common form is a SQL-driven
aggregation against another table.

**Existing DLT code:**

.. code-block:: python
   :caption: gold_daily_revenue.py (existing DLT)

   import dlt

   @dlt.table(name="main.gold.daily_revenue")
   def daily_revenue():
       return spark.sql("""
           SELECT order_date, SUM(amount) AS revenue
           FROM main.silver.orders
           GROUP BY order_date
       """)

**LHP YAML:**

.. code-block:: yaml
   :caption: pipelines/gold/daily_revenue.yaml

   pipeline: gold
   flowgroup: daily_revenue
   actions:
     - name: write_daily_revenue
       type: write
       write_target:
         type: materialized_view
         catalog: main
         schema: gold
         table: daily_revenue
         sql: |
           SELECT order_date, SUM(amount) AS revenue
           FROM main.silver.orders
           GROUP BY order_date

This generates ``@dp.materialized_view(name="main.gold.daily_revenue", ...)``
over a function whose body is ``df = spark.sql("""...""")``. If your source is
another LHP view rather than an inline SQL string, drop the ``sql:`` key and
use a ``source:`` key referencing the upstream view name.

Pattern 3: CDC pipeline
-----------------------

DLT's ``dlt.apply_changes`` (the legacy API) and the newer
``dp.create_auto_cdc_flow`` both map to LHP's ``streaming_table`` write with
``mode: cdc``. LHP emits ``dp.create_streaming_table(...)`` followed by
``dp.create_auto_cdc_flow(target=..., source=..., keys=..., sequence_by=...,
stored_as_scd_type=...)``.

**Existing DLT code:**

.. code-block:: python
   :caption: silver_customers_scd2.py (existing DLT)

   import dlt

   dlt.create_streaming_table(name="main.silver.dim_customer")

   dlt.apply_changes(
       target="main.silver.dim_customer",
       source="v_customer_changes",
       keys=["customer_id"],
       sequence_by="_commit_timestamp",
       stored_as_scd_type=2,
   )

**LHP YAML:**

.. code-block:: yaml
   :caption: pipelines/silver/dim_customer.yaml

   pipeline: silver
   flowgroup: dim_customer
   actions:
     - name: load_customer_cdc
       type: load
       readMode: stream
       source:
         type: delta
         catalog: main
         schema: bronze
         table: customer
         options:
           readChangeFeed: "true"
       target: v_customer_changes

     - name: write_dim_customer
       type: write
       source: v_customer_changes
       write_target:
         type: streaming_table
         catalog: main
         schema: silver
         table: dim_customer
         mode: cdc
         cdc_config:
           keys: [customer_id]
           sequence_by: _commit_timestamp
           scd_type: 2

The exact YAML keys are load-bearing:

- ``mode: cdc`` selects the ``dp.create_auto_cdc_flow`` code path.
- ``keys`` is required and must be a non-empty list of strings.
- ``sequence_by`` accepts a string (single column) or a list of strings
  (which LHP renders as ``sequence_by=struct(...)`` and adds the required
  ``from pyspark.sql.functions import struct`` import).
- ``scd_type`` is ``1`` (default) or ``2``. For SCD 2, you may add
  ``track_history_column_list`` or ``track_history_except_column_list``
  (mutually exclusive).
- ``apply_as_truncates`` is rejected when ``scd_type: 2``.

For snapshot-based CDC, set ``mode: snapshot_cdc`` and provide a
``snapshot_cdc_config`` with either a ``source`` table reference or a
``source_function`` block. LHP emits ``dp.create_auto_cdc_from_snapshot_flow``.

.. warning::

   ``dp.create_auto_cdc_flow`` and ``dp.create_auto_cdc_from_snapshot_flow``
   require a sufficiently recent Databricks Runtime — Lakeflow Declarative
   Pipelines builds. If your existing code still uses ``dlt.apply_changes`` and
   ``dlt.apply_changes_from_snapshot``, regenerated LHP code is the supported
   replacement and runs on the same pipeline engine.

Pattern 4: Expectations
-----------------------

DLT decorator-based expectations (``@dlt.expect``, ``@dlt.expect_or_drop``,
``@dlt.expect_or_fail``, and their ``_all`` variants) map to LHP's
``data_quality`` transform action. The generated code uses ``@dp.expect_all``,
``@dp.expect_all_or_drop``, and ``@dp.expect_all_or_fail``.

**Existing DLT code:**

.. code-block:: python
   :caption: silver_orders.py (existing DLT)

   import dlt

   @dlt.table(name="main.silver.orders")
   @dlt.expect_all({"valid_amount": "amount > 0"})
   @dlt.expect_all_or_drop({"has_customer": "customer_id IS NOT NULL"})
   @dlt.expect_all_or_fail({"valid_date": "order_date IS NOT NULL"})
   def orders():
       return spark.readStream.table("main.bronze.orders")

**LHP YAML:**

.. code-block:: yaml
   :caption: pipelines/silver/orders.yaml

   pipeline: silver
   flowgroup: orders
   actions:
     - name: load_bronze_orders
       type: load
       readMode: stream
       source:
         type: delta
         catalog: main
         schema: bronze
         table: orders
       target: v_orders_raw

     - name: validate_orders
       type: transform
       transform_type: data_quality
       source: v_orders_raw
       target: v_orders_validated
       expectations:
         - name: valid_amount
           expression: amount > 0
           action: warn
         - name: has_customer
           expression: customer_id IS NOT NULL
           action: drop
         - name: valid_date
           expression: order_date IS NOT NULL
           action: fail

     - name: write_orders
       type: write
       source: v_orders_validated
       write_target:
         type: streaming_table
         catalog: main
         schema: silver
         table: orders

LHP splits each expectation into one of three buckets by its ``action``:
``warn`` → ``@dp.expect_all``, ``drop`` → ``@dp.expect_all_or_drop``, ``fail``
→ ``@dp.expect_all_or_fail``. The generated transform is a
``@dp.temporary_view()`` function with the matching expectation decorators
stacked above it.

Verifying the migration
-----------------------

After writing each flowgroup, regenerate and compare:

.. code-block:: bash

   lhp generate --env dev
   diff -u original/silver_orders.py generated/silver/orders.py

You are looking for **semantic equivalence**, not byte-identical output. The
generated file will:

- Import ``from pyspark import pipelines as dp`` instead of ``import dlt``.
- Use ``@dp.temporary_view()`` intermediate views (named ``v_<something>``)
  even when your original code inlined the read directly inside the table
  function.
- Split a single hand-written ``@dlt.table``-plus-CDC pattern into a
  ``dp.create_streaming_table(...)`` call followed by
  ``dp.create_auto_cdc_flow(...)``.

Run ``lhp validate --env dev`` first to catch YAML schema errors before
generation. ``lhp generate --env dev --dry-run`` previews the output without
writing files.

.. tip::

   When migrating a pipeline that already runs in production, generate into a
   separate output directory first (configurable per project), point a staging
   Lakeflow pipeline at the generated files, and validate that the staging
   pipeline produces an identical result to production before swapping over.

Things that don't map cleanly
-----------------------------

A handful of DLT constructs do not have a direct LHP equivalent. Handle them
case by case:

- **Legacy decorator names.** ``@dlt.view`` and ``@dlt.create_view`` have no
  direct counterpart. LHP's ``@dp.temporary_view()`` is generated automatically
  for every load and transform action; if you depended on a named DLT view
  that other tables read by string, refactor the consumer to read from the LHP
  view name (``v_<target>``) instead.
- **Imperative-only DLT code.** Code that calls ``dlt.read``, ``dlt.read_stream``
  inside arbitrary control flow (for-loops generating tables dynamically) does
  not fit the declarative YAML model. Either flatten the loop into explicit
  flowgroups, or use LHP's ``custom_datasource`` or ``python`` transform action
  to embed a hand-written function.
- **Runtime-only behaviors.** Pipeline event hooks, ``dlt.read_stream`` against
  another pipeline's table, and any ``%pip install`` magics in your DLT
  notebook are configured at the Lakeflow pipeline level (in
  ``databricks.yml`` or the pipeline spec), not inside the generated Python.
  Move them there during the migration.
- **Hand-rolled SCD logic.** If you wrote SCD 2 logic by hand instead of using
  ``dlt.apply_changes``, the cleanest migration is to convert it to
  ``mode: cdc`` in LHP rather than reproduce the imperative code. The result
  is supported and considerably shorter.

See also
--------

- :doc:`architecture` — how Pipelines, FlowGroups, and Actions relate, and why
  the load/transform/write split makes incremental migration practical.
- :doc:`actions/index` — the full reference for every load, transform, write,
  and test action, including options not covered in the four patterns above.
- :doc:`quickstart` — if you are new to LHP and want to build a working
  pipeline from scratch before tackling a migration.
