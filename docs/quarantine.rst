Quarantine (Dead Letter Queue)
=======================================

.. meta::
   :description: Reference for quarantine mode on data_quality transforms — overview, prerequisites/DDL, configuration schema, limitations, CloudFiles handling, integration points, and troubleshooting.

.. versionadded:: 0.7.7

This is the reference page for LHP's quarantine (Dead Letter Queue) feature. It documents
the configuration schema, the required DLQ table DDL, limitations, and troubleshooting.
For the step-by-step "how do I turn this on?" walk-through — Quick Start, generated code
walk-through, and DLQ operations recipes — see :doc:`quarantine_records`.

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

**Behavior:** In quarantine mode, **all** expectations are coerced to ``drop`` regardless
of their original ``failureAction`` / ``action`` setting. Expectations with ``fail`` or
``warn`` actions produce a validation-time warning but are treated as ``drop`` at runtime.


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

The outbox table name is **auto-derived** by appending ``_outbox`` to the ``dlq_table``
value. For example, if ``dlq_table`` is ``catalog.schema.universal_dlq``, the outbox
table will be ``catalog.schema.universal_dlq_outbox``. No YAML configuration is needed.

.. danger::
   The DLQ table **must** have Change Data Feed (CDF) and row tracking enabled. Without CDF,
   the recycled-rows stream (``readChangeFeed``) will fail at runtime. Without row tracking,
   CDF cannot capture the ``update_postimage`` events needed for recycling.


Configuration Reference
-----------------------

**Fields on a data quality transform action:**

``mode``
   Set to ``quarantine`` to enable quarantine mode. Defaults to ``dqe`` (standard expectations
   decorators) when omitted.

``quarantine.dlq_table``
   **(required)** Fully qualified name of the pre-created DLQ table
   (e.g. ``${catalog}.${schema}.universal_dlq``).

``quarantine.source_table``
   **(required)** Fully qualified name of the logical source table. Used to tag rows in the DLQ
   (``_dlq_source_table`` column) and to filter the CDF stream during recycling. Typically this
   is the target of the downstream write action (e.g. ``${catalog}.${bronze_schema}.orders``).

**Validation rules:**

- ``mode: quarantine`` requires a ``quarantine`` block with both ``dlq_table`` and ``source_table``.
- A ``quarantine`` block is invalid when ``mode`` is ``dqe`` or omitted.
- The expectations file must contain at least one expectation (an empty file produces an
  invalid inverse filter).
- ``warn`` and ``fail`` expectations produce a validation warning (coerced to ``drop``).

Substitution tokens (``${catalog}``, ``${secret:scope/key}``, etc.) are fully supported in
``dlq_table`` and ``source_table`` values. They are resolved during code generation, so the
generated Python contains the environment-specific values.

.. warning::
   Do not add a ``quarantine`` block without setting ``mode: quarantine``, or set the mode without
   providing the quarantine block. Both sides must be present — the validator enforces this symmetry.

.. seealso::
   For the Quick Start, the generated-code walk-through, and the DLQ operations recipes, see
   :doc:`quarantine_records`.


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


Integration with Other Features
-------------------------------

**Operational Metadata**

If your project defines operational metadata columns in ``lhp.yaml``, they are automatically
added to the UNION output view (after the ``clean.union(recycled)`` call). Recycled rows
receive fresh metadata values.

**Substitution Tokens**

The ``dlq_table`` and ``source_table`` fields support all substitution syntaxes:

- Environment tokens: ``${catalog}.${schema}.universal_dlq``
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
     dlq_table: "${catalog}.${bronze_schema}.universal_dlq"

Individual flowgroups then only need to set ``source_table`` (which is typically unique per
flowgroup):

.. code-block:: yaml

   quarantine:
     source_table: "${catalog}.${bronze_schema}.orders"

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
