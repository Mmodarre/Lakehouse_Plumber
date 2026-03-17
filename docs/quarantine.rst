=======================================
Quarantine (Dead Letter Queue)
=======================================

.. meta::
   :description: Configure quarantine mode for data quality transforms in Lakehouse Plumber — route failed rows to a Dead Letter Queue (DLQ) and automatically recycle fixed records.

.. versionadded:: 0.7.7

Overview
--------

Quarantine mode extends the standard ``data_quality`` transform with a **Dead Letter Queue (DLQ)**
recycling pattern using an **inbox/outbox** design. Instead of simply dropping rows that fail
expectations, quarantine mode:

1. Applies all expectations as ``drop`` to produce a clean stream.
2. Routes failed rows through an **inverse filter** into an external DLQ table (inbox) via ``MERGE``.
3. Reads fixed rows from the DLQ inbox via CDF and deduplicates them into an **outbox** table
   using a ``foreachBatch`` sink with ``MERGE`` (keyed on ``_dlq_sk``).
4. Validates recycled rows from the outbox through a ``_recycled_*`` view with
   ``@dp.expect_all_or_drop`` (excluding ``_rescued_data`` expectations).
5. ``UNION``\s the clean stream with the validated recycled stream to produce the final output view.

The outbox table acts as a permanent "already processed" ledger, preventing duplicate ingestion
when a DLQ row is updated multiple times between pipeline runs.

.. image:: _static/quarantine_data_flow.svg
   :alt: Quarantine data flow — source table splits into clean records (pass) and quarantine (fail), with DLQ recycling via inbox/outbox pattern back into the output UNION view.
   :align: center

**When to use quarantine:**

- You need to **preserve** failed rows for investigation rather than silently dropping them.
- Operators must be able to **fix and recycle** bad records without reprocessing the entire source.
- Compliance or audit requirements mandate a record of all rejected data.
- You want a single shared DLQ table across multiple flowgroups.

.. note::
   In quarantine mode, **all** expectations are coerced to ``drop`` regardless of their original
   ``failureAction`` / ``action`` setting. Expectations with ``fail`` or ``warn`` actions produce
   a validation-time warning but are treated as ``drop`` at runtime.


Prerequisites
-------------

Before configuring quarantine mode, ensure the following:

- **Databricks Runtime 15.4+** with Unity Catalog enabled.
- An **expectations file** with at least one expectation rule.
- The upstream load action uses ``readMode: stream`` (quarantine generates streaming code).
- A **pre-created DLQ table** (inbox) with the schema below.
- A **pre-created DLQ outbox table** (auto-derived name: ``<dlq_table>_outbox``).

**DLQ Table DDL (Inbox)**

.. code-block:: sql
   :caption: Create the DLQ table (run once per catalog/schema)
   :linenos:

   CREATE TABLE IF NOT EXISTS catalog.schema.universal_dlq (
       _dlq_sk            STRING    NOT NULL,
       _dlq_source_table  STRING    NOT NULL,
       _dlq_status        STRING    NOT NULL,  -- 'quarantined' | 'fixed'
       _dlq_timestamp     TIMESTAMP NOT NULL,
       _dlq_failed_rules  ARRAY<STRUCT<name: STRING, rule: STRING>>,
       _dlq_rescued_data  STRING,
       _row_data          VARIANT   NOT NULL
   )
   TBLPROPERTIES (
       'delta.enableChangeDataFeed' = 'true',
       'delta.enableRowTracking' = 'true',
       'delta.deletedFileRetentionDuration' = 'interval 90 days'
   );

**Column reference:**

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Column
     - Type
     - Description
   * - ``_dlq_sk``
     - STRING
     - Deterministic surrogate key (``xxhash64`` of source table + row JSON). Used as MERGE key to prevent duplicates.
   * - ``_dlq_source_table``
     - STRING
     - Fully qualified name of the source table (from ``quarantine.source_table``).
   * - ``_dlq_status``
     - STRING
     - Row lifecycle status: ``'quarantined'`` on insert, manually updated to ``'fixed'`` for recycling.
   * - ``_dlq_timestamp``
     - TIMESTAMP
     - Timestamp when the row was written to the DLQ.
   * - ``_dlq_failed_rules``
     - ARRAY<STRUCT>
     - Array of ``{name, rule}`` structs identifying which expectations the row violated.
   * - ``_dlq_rescued_data``
     - STRING
     - JSON string of rescued data from CloudFiles (``NULL`` when source lacks ``_rescued_data`` column).
   * - ``_row_data``
     - VARIANT
     - Full row data stored as a Databricks VARIANT for schema-agnostic querying.

**DLQ Outbox Table DDL**

.. code-block:: sql
   :caption: Create the DLQ outbox table (run once per catalog/schema)
   :linenos:

   CREATE TABLE IF NOT EXISTS catalog.schema.universal_dlq_outbox (
       _dlq_sk            STRING    NOT NULL,
       _dlq_source_table  STRING    NOT NULL,
       _row_data          VARIANT   NOT NULL,
       _dlq_recycled_at   TIMESTAMP NOT NULL
   )
   TBLPROPERTIES (
       'delta.enableRowTracking' = 'true'
   );

.. note::
   The outbox table name is **auto-derived** by appending ``_outbox`` to the ``dlq_table`` value.
   For example, if ``dlq_table`` is ``catalog.schema.universal_dlq``, the outbox table will be
   ``catalog.schema.universal_dlq_outbox``. No YAML configuration is needed.

.. danger::
   The DLQ table **must** have Change Data Feed (CDF) and row tracking enabled. Without CDF,
   the recycled-rows stream (``readChangeFeed``) will fail at runtime. Without row tracking,
   CDF cannot capture the ``update_postimage`` events needed for recycling.


Quick Start
-----------

**1. FlowGroup YAML**

.. code-block:: yaml
   :caption: pipelines/02_bronze/quarantine_flow.yaml
   :linenos:
   :emphasize-lines: 24-28

   pipeline: acmi_edw_bronze
   flowgroup: quarantine_flow

   actions:
     - name: orders_raw_load
       type: load
       readMode: stream
       source:
         type: delta
         database: "{catalog}.{raw_schema}"
         table: orders_raw
       target: v_orders_raw
       description: "Load orders from raw schema"

     - name: orders_quarantine_dq
       type: transform
       transform_type: data_quality
       source: v_orders_raw
       target: v_orders_validated
       readMode: stream
       expectations_file: "expectations/quarantine_quality.yaml"
       description: "Apply quarantine data quality checks to orders"
       mode: quarantine
       quarantine:
         dlq_table: "{catalog}.{bronze_schema}.universal_dlq"
         source_table: "{catalog}.{bronze_schema}.orders"

     - name: write_orders_bronze
       type: write
       source: v_orders_validated
       write_target:
         create_table: true
         type: streaming_table
         database: "{catalog}.{bronze_schema}"
         table: "orders_quarantined"

**2. Expectations file**

.. code-block:: yaml
   :caption: expectations/quarantine_quality.yaml
   :linenos:

   order_id IS NOT NULL:
     action: drop
     name: valid_order_id
   order_amount > 0:
     action: warn
     name: positive_amount
   customer_id IS NOT NULL:
     action: fail
     name: valid_customer_id

Note the mix of ``drop``, ``warn``, and ``fail`` actions. In quarantine mode, all three are
coerced to ``drop`` — a warning is emitted during validation for ``warn`` and ``fail`` entries.

**3. Validate and generate**

.. code-block:: bash

   lhp validate --env dev
   lhp generate --env dev

**What happens:** LHP generates a single Python file containing six components — shared constants,
a clean view, a DLQ sink with quarantine flow, a recycle sink/flow (inbox → outbox dedup),
a recycled validation view, and a final UNION output view — plus the standard
load and write actions. See :ref:`quarantine-generated-code` for a full walkthrough.


Configuration Reference
-----------------------

**Fields on a data quality transform action:**

``mode``
   Set to ``quarantine`` to enable quarantine mode. Defaults to ``dqe`` (standard expectations
   decorators) when omitted.

``quarantine.dlq_table``
   **(required)** Fully qualified name of the pre-created DLQ table
   (e.g. ``{catalog}.{schema}.universal_dlq``).

``quarantine.source_table``
   **(required)** Fully qualified name of the logical source table. Used to tag rows in the DLQ
   (``_dlq_source_table`` column) and to filter the CDF stream during recycling. Typically this
   is the target of the downstream write action (e.g. ``{catalog}.{bronze_schema}.orders``).

**Validation rules:**

- ``mode: quarantine`` requires a ``quarantine`` block with both ``dlq_table`` and ``source_table``.
- A ``quarantine`` block is invalid when ``mode`` is ``dqe`` or omitted.
- The expectations file must contain at least one expectation (an empty file produces an
  invalid inverse filter).
- ``warn`` and ``fail`` expectations produce a validation warning (coerced to ``drop``).

.. important::
   Substitution tokens (``{catalog}``, ``${secret:scope/key}``, etc.) are fully supported in
   ``dlq_table`` and ``source_table`` values. They are resolved during code generation, so the
   generated Python contains the environment-specific values.

.. warning::
   Do not add a ``quarantine`` block without setting ``mode: quarantine``, or set the mode without
   providing the quarantine block. Both sides must be present — the validator enforces this symmetry.


.. _quarantine-generated-code:

Generated Code Walkthrough
--------------------------

The following is the quarantine-specific code generated from the Quick Start example above.
The load and write sections are standard and omitted for brevity.

.. code-block:: python
   :linenos:

   # --- Rules & constants ---

   _EXPECTATIONS_v_orders_raw = {
       "valid_order_id": "order_id IS NOT NULL",
       "positive_amount": "order_amount > 0",
       "valid_customer_id": "customer_id IS NOT NULL",
   }

   _INVERSE_FILTER_v_orders_raw = (
       "NOT ((order_id IS NOT NULL) AND (order_amount > 0) AND (customer_id IS NOT NULL))"
   )

   _FAILED_RULE_EXPRS_v_orders_raw = [...]  # omitted for brevity

   DLQ_TABLE_v_orders_raw = "acme_edw_dev.edw_bronze.universal_dlq"
   DLQ_OUTBOX_TABLE_v_orders_raw = "acme_edw_dev.edw_bronze.universal_dlq_outbox"
   SOURCE_TABLE_v_orders_raw = "acme_edw_dev.edw_bronze.orders"

   _EXPECTATIONS_RECYCLED_v_orders_raw = {
       "valid_order_id": "order_id IS NOT NULL",
       "positive_amount": "order_amount > 0",
       "valid_customer_id": "customer_id IS NOT NULL",
   }


   # --- Clean path (provides DQ metrics in event log) ---
   @dp.temporary_view()
   @dp.expect_all_or_drop(_EXPECTATIONS_v_orders_raw)
   def _clean_v_orders_raw():
       """Apply quarantine data quality checks to orders — clean records"""
       return spark.readStream.table("v_orders_raw")


   # --- Quarantine path (DLQ sink + routing) ---
   @dp.foreach_batch_sink(name="dlq_sink_v_orders_raw")
   def dlq_sink_v_orders_raw(batch_df, batch_id):
       """Write quarantined rows to DLQ table"""
       ...  # MERGE into DLQ inbox (omitted for brevity)


   @dp.append_flow(target="dlq_sink_v_orders_raw", name="quarantine_flow_v_orders_raw")
   def quarantine_flow_v_orders_raw():
       """Route failed rows to DLQ"""
       return (
           spark.readStream.table("v_orders_raw")
           .filter(_INVERSE_FILTER_v_orders_raw)
           .withColumn("_dlq_failed_rules",
               F.array_compact(F.array(*_FAILED_RULE_EXPRS_v_orders_raw)))
       )


   # --- Recycle path (dedup inbox → outbox) ---
   @dp.foreach_batch_sink(name="recycle_sink_v_orders_raw")
   def recycle_sink_v_orders_raw(batch_df, batch_id):
       """Deduplicate fixed DLQ rows and write to outbox"""
       if batch_df.isEmpty():
           return

       spark = batch_df.sparkSession
       w = Window.partitionBy("_dlq_sk").orderBy(F.desc("_commit_version"))
       deduped = (
           batch_df.withColumn("_rn", F.row_number().over(w))
           .filter("_rn = 1")
           .drop("_rn")
           .select(
               "_dlq_sk", "_dlq_source_table", "_row_data",
               F.current_timestamp().alias("_dlq_recycled_at"),
           )
       )

       outbox = DeltaTable.forName(spark, DLQ_OUTBOX_TABLE_v_orders_raw)
       (
           outbox.alias("out")
           .merge(deduped.alias("new"), "out._dlq_sk = new._dlq_sk")
           .whenNotMatchedInsertAll()
           .execute()
       )


   @dp.append_flow(
       target="recycle_sink_v_orders_raw", name="recycle_flow_v_orders_raw"
   )
   def recycle_flow_v_orders_raw():
       """Read fixed rows from DLQ inbox via CDF"""
       return (
           spark.readStream.option("readChangeFeed", "true")
           .table(DLQ_TABLE_v_orders_raw)
           .filter(
               "_dlq_status = 'fixed' "
               "AND _change_type IN ('insert', 'update_postimage') "
               f"AND _dlq_source_table = '{SOURCE_TABLE_v_orders_raw}'"
           )
       )


   # --- Recycled path (outbox → validated recycled view) ---
   @dp.temporary_view()
   @dp.expect_all_or_drop(_EXPECTATIONS_RECYCLED_v_orders_raw)
   def _recycled_v_orders_raw():
       """Validate recycled rows from DLQ outbox"""
       clean = spark.readStream.table("_clean_v_orders_raw")
       return (
           spark.readStream.option("skipChangeCommits", "true")
           .table(DLQ_OUTBOX_TABLE_v_orders_raw)
           .filter(f"_dlq_source_table = '{SOURCE_TABLE_v_orders_raw}'")
           .select([
               F.try_variant_get(
                   F.col("_row_data"), f"$.{field.name}", field.dataType.simpleString()
               ).alias(field.name)
               for field in clean.schema.fields
           ])
       )


   # --- Validated output (clean + recycled) ---
   @dp.temporary_view()
   def v_orders_validated():
       """Apply quarantine data quality checks to orders — clean + recycled records"""
       clean = spark.readStream.table("_clean_v_orders_raw")
       recycled = spark.readStream.table("_recycled_v_orders_raw")

       df = clean.union(recycled)

       return df


Component 1: Shared Constants
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Module-level constants derived from the expectations file and quarantine configuration:

- ``_EXPECTATIONS_*`` — the expectations dict (all coerced to drop). Passed to
  ``@dp.expect_all_or_drop``.
- ``_INVERSE_FILTER_*`` — a SQL expression that matches rows failing **any** expectation.
  Built as ``NOT ((rule1) AND (rule2) AND ...)``.
- ``_FAILED_RULE_EXPRS_*`` — a list of ``F.when(...)`` expressions that produce
  ``{name, rule}`` structs for each failed expectation. Used to populate ``_dlq_failed_rules``.
- ``DLQ_TABLE_*`` / ``DLQ_OUTBOX_TABLE_*`` / ``SOURCE_TABLE_*`` — the resolved table names from
  the quarantine config. The outbox name is auto-derived as ``<dlq_table>_outbox``.
- ``_EXPECTATIONS_RECYCLED_*`` — expectations with ``_rescued_data`` rules filtered out, used
  for validating recycled rows from the outbox.

.. note::
   Variable names use a ``safe_source_view`` suffix (e.g. ``v_orders_raw``) derived from the
   source view name with non-alphanumeric characters replaced by underscores. This ensures valid
   Python identifiers when source views contain special characters.

Component 2: Clean View
~~~~~~~~~~~~~~~~~~~~~~~~

The ``_clean_*`` view applies ``@dp.expect_all_or_drop`` to the source view. Rows passing all
expectations flow through to this view. Failed rows are silently dropped here but captured
separately by the quarantine flow (Component 3).

The clean view is also visible in the Databricks DLT event log, providing data quality metrics
(pass/fail counts) without additional configuration.

Component 3: DLQ Sink + Quarantine Flow
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two functions work together:

- **``dlq_sink_*``** — a ``@dp.foreach_batch_sink`` that receives micro-batches of failed rows
  and ``MERGE``\s them into the DLQ table. The MERGE uses ``_dlq_sk`` (a deterministic
  ``xxhash64`` hash) to prevent duplicate inserts on retries.
- **``quarantine_flow_*``** — an ``@dp.append_flow`` that reads the same source view, applies the
  inverse filter to select only **failed** rows, and annotates each row with ``_dlq_failed_rules``.

Component 4: Recycle Sink + Flow (Inbox → Outbox)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Two functions handle deduplication of fixed DLQ rows:

- **``recycle_sink_*``** — a ``@dp.foreach_batch_sink`` that deduplicates fixed rows by
  ``_dlq_sk`` using a ``Window`` with ``row_number()``, keeping only the latest
  ``_commit_version``. The deduplicated rows are ``MERGE``\d into the outbox table
  (``whenNotMatchedInsertAll``), ensuring each row is processed exactly once.
- **``recycle_flow_*``** — an ``@dp.append_flow`` that reads the DLQ inbox via CDF,
  filtering for ``_dlq_status = 'fixed'`` rows.

This design prevents duplicate ingestion when a DLQ row is updated multiple times (e.g.
multiple edits before a pipeline run, or re-edits after recycling).

Component 5: Recycled Validation View
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The ``_recycled_*`` view reads from the outbox table with ``skipChangeCommits`` (to avoid
reprocessing MERGE commits) and reconstructs the original schema using ``try_variant_get``.
It applies ``@dp.expect_all_or_drop(_EXPECTATIONS_RECYCLED_*)`` to validate recycled rows,
using a filtered expectations dict that excludes ``_rescued_data`` rules (since recycled rows
are stored as VARIANT and don't have the original ``_rescued_data`` column).

Component 6: Final Output View (UNION)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The target view (e.g. ``v_orders_validated``) UNIONs two streams:

1. **Clean stream** — from ``_clean_*`` (rows that passed all expectations).
2. **Recycled stream** — from ``_recycled_*`` (validated rows from the outbox).

If operational metadata columns are configured, they are added to the UNION output.


Limitations
-----------

- **Triggered pipelines only:** The inbox/outbox pattern relies on CDF and ``foreachBatch``,
  which require a triggered (non-continuous) pipeline mode.
- **1-run lag:** Fixed rows written to the outbox during pipeline run *N* are picked up by the
  recycled view in run *N+1*. This is inherent to the streaming checkpoint model.
- **No re-fix via DLQ:** Once a row has been written to the outbox and ingested into the target
  table, updating the DLQ row again will not trigger a second ingestion (the outbox MERGE uses
  ``whenNotMatchedInsertAll``). To re-fix, delete the row from the outbox first.
- **Outbox table is user-managed DDL:** The outbox table must be created before the pipeline runs.
  See the DDL in the `Prerequisites`_ section.


CloudFiles Support
------------------

When the upstream load action uses **CloudFiles** with a rescue column (``_rescued_data``),
the quarantine DLQ sink **automatically detects** the column at runtime and handles it correctly.
No additional configuration is required.

**How it works:**

The generated ``dlq_sink_*`` function checks ``if "_rescued_data" in batch_df.columns:`` at
runtime (once per micro-batch, zero performance impact):

- **If ``_rescued_data`` exists:** A UDF merges the main row JSON with the rescued JSON, ensuring
  ``_row_data`` contains the complete row including rescued columns. The ``_rescued_data`` column
  is renamed to ``_dlq_rescued_data`` in the DLQ.
- **If ``_rescued_data`` does not exist:** The row data is stored directly and
  ``_dlq_rescued_data`` is set to ``NULL``.

This means the same generated code works correctly regardless of whether the upstream source
produces a ``_rescued_data`` column or not.


DLQ Operations Guide
---------------------

This section covers querying, inspecting, and recycling quarantined rows. These operations are
performed directly against the DLQ table using Databricks SQL.

Querying Quarantined Rows
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sql
   :caption: View all quarantined rows for a specific source table

   SELECT
       _dlq_sk,
       _dlq_source_table,
       _dlq_status,
       _dlq_timestamp,
       _dlq_failed_rules,
       _row_data
   FROM catalog.schema.universal_dlq
   WHERE _dlq_source_table = 'catalog.schema.orders'
     AND _dlq_status = 'quarantined'
   ORDER BY _dlq_timestamp DESC;

Inspecting Failed Rules
~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: sql
   :caption: Explode failed rules to see which expectations each row violated

   SELECT
       _dlq_sk,
       rule.name AS rule_name,
       rule.rule AS rule_expression,
       _dlq_timestamp
   FROM catalog.schema.universal_dlq
   LATERAL VIEW EXPLODE(_dlq_failed_rules) AS rule
   WHERE _dlq_source_table = 'catalog.schema.orders'
     AND _dlq_status = 'quarantined';

Extracting Row Data
~~~~~~~~~~~~~~~~~~~

.. code-block:: sql
   :caption: Extract specific fields from the VARIANT _row_data column

   SELECT
       _dlq_sk,
       _row_data:order_id::INT AS order_id,
       _row_data:customer_id::STRING AS customer_id,
       _row_data:order_amount::DOUBLE AS order_amount,
       _dlq_failed_rules
   FROM catalog.schema.universal_dlq
   WHERE _dlq_source_table = 'catalog.schema.orders'
     AND _dlq_status = 'quarantined';

Fixing Rows (Recycling Workflow)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The recycling workflow lets operators mark corrected rows as ``fixed``. The pipeline's CDF reader
automatically picks up these changes and includes them in the next micro-batch of the output view.

**Step-by-step:**

1. Query the DLQ to identify rows that need fixing.
2. Verify or correct the underlying data issue (e.g. backfill a missing ``customer_id``).
3. Update the row status to ``'fixed'``:

.. code-block:: sql
   :caption: Mark a quarantined row as fixed for recycling

   UPDATE catalog.schema.universal_dlq
   SET _dlq_status = 'fixed'
   WHERE _dlq_sk = '<surrogate_key_value>'
     AND _dlq_status = 'quarantined';

4. On the next pipeline refresh, the CDF reader detects the ``update_postimage`` event and the
   recycled row flows through the UNION into the target view.

.. warning::
   Recycling relies on **Change Data Feed (CDF)**. If CDF is disabled on the DLQ table, status
   updates will not be detected by the pipeline. Ensure ``delta.enableChangeDataFeed = 'true'``
   is set as a table property.

.. note::
   The ``_dlq_sk`` is a **deterministic hash** (``xxhash64``) of the source table name and the
   row's JSON representation. The same row will always produce the same key, which prevents
   duplicate inserts on retries and allows targeted updates.


Integration with Other Features
-------------------------------

**Operational Metadata**

If your project defines operational metadata columns in ``lhp.yaml``, they are automatically
added to the UNION output view (after the ``clean.union(recycled)`` call). Recycled rows
receive fresh metadata values.

**Substitution Tokens**

The ``dlq_table`` and ``source_table`` fields support all substitution syntaxes:

- Environment tokens: ``{catalog}.{schema}.universal_dlq``
- Dollar-brace tokens: ``${catalog}.${schema}.universal_dlq``
- Secret references: ``${secret:scope/key}`` (if needed)

**Presets**

You can define quarantine configuration in a preset and override per-flowgroup:

.. code-block:: yaml
   :caption: Quarantine settings in a preset

   # presets/bronze_quarantine.yaml
   type: transform
   transform_type: data_quality
   mode: quarantine
   quarantine:
     dlq_table: "{catalog}.{bronze_schema}.universal_dlq"

Individual flowgroups then only need to set ``source_table`` (which is typically unique per
flowgroup):

.. code-block:: yaml

   quarantine:
     source_table: "{catalog}.{bronze_schema}.orders"

.. seealso::
   - :doc:`operational_metadata` — configuring audit columns.
   - :doc:`substitutions` — environment tokens, local variables, and secret management.
   - :doc:`presets_reference` — reusable default configurations and deep merge behavior.


Troubleshooting
---------------

**"Quarantine mode requires at least one expectation"**
   The expectations file referenced by ``expectations_file`` is empty or contains no parseable
   rules. Add at least one expectation, or remove ``mode: quarantine`` to use standard DQE mode.

**"'quarantine' configuration block is only valid when mode='quarantine'"**
   You added a ``quarantine:`` block but did not set ``mode: quarantine`` on the action. Either
   add ``mode: quarantine`` or remove the ``quarantine`` block.

**"requires a 'quarantine' configuration block"**
   You set ``mode: quarantine`` but omitted the ``quarantine:`` block. Add the block with at
   least ``dlq_table`` and ``source_table``.

**Runtime: DLQ table not found**
   The table specified in ``dlq_table`` does not exist. Create it using the DDL in the
   `Prerequisites`_ section before running the pipeline.

**Runtime: CDF not enabled**
   The DLQ table exists but does not have ``delta.enableChangeDataFeed = 'true'``. Alter the
   table to add this property, or recreate it with the full DDL.

**Expectations with fail/warn actions produce warnings**
   This is expected behavior. In quarantine mode, all expectations are coerced to ``drop``. The
   warnings during ``lhp validate`` inform you that the original ``fail`` or ``warn`` action will
   be ignored.

.. seealso::
   :doc:`errors_reference` — full error code catalog with resolution steps.
