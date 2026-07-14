Quarantine (DLQ) configuration
==============================

.. meta::
   :description: Schema reference for quarantine mode on a data_quality transform — the mode and quarantine fields, the derived outbox name, the DLQ inbox and outbox table DDL and column schema, the CDF requirement, and limits.

Quarantine mode on a ``data_quality`` transform routes rows that fail
expectations into an external Dead Letter Queue (DLQ) table instead of dropping
them. It is enabled per action, and both DLQ tables are user-managed DDL::

   - name: <action_name>
     type: transform
     transform_type: data_quality
     source: <view>
     target: <view>
     expectations_file: <path>
     mode: quarantine
     readMode: stream
     quarantine:
       dlq_table: <fully.qualified.name>
       source_table: <fully.qualified.name>

Action fields
-------------

Fields on the ``data_quality`` transform that select and configure quarantine.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``mode``
     - string
     - No
     - ``dqe``
     - ``dqe`` (inline expectation decorators) or ``quarantine`` (DLQ subsystem).
   * - ``quarantine``
     - mapping
     - When ``quarantine``
     - —
     - DLQ configuration block (below). Required when ``mode: quarantine``; rejected when ``mode`` is ``dqe`` or omitted.

The transform must also set ``readMode: stream`` (the default; other values are
rejected) and reference an ``expectations_file`` containing at least one rule.

quarantine block
----------------

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 14 38

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``dlq_table``
     - string
     - Yes
     - —
     - Fully qualified name of the pre-created DLQ inbox table.
   * - ``source_table``
     - string
     - Yes
     - —
     - Fully qualified name of the logical source table. Tags DLQ rows (``_dlq_source_table``) and filters the recycle stream.

Both values accept substitution tokens (``${catalog}``, ``${secret:scope/key}``)
and are resolved at generation time. The outbox table name is derived by
appending ``_outbox`` to ``dlq_table`` (for example
``main.ops.orders_dlq`` → ``main.ops.orders_dlq_outbox``); it is not configured
separately.

DLQ inbox table schema
----------------------

The inbox table receives quarantined rows. Create it before the pipeline runs.

.. list-table::
   :header-rows: 1
   :widths: 24 34 42

   * - Column
     - Type
     - Description
   * - ``_dlq_sk``
     - STRING
     - Surrogate key: ``xxhash64`` of ``source_table`` + row JSON, cast to string. MERGE key.
   * - ``_dlq_source_table``
     - STRING
     - Fully qualified source table name (from ``quarantine.source_table``).
   * - ``_dlq_status``
     - STRING
     - Row status: ``quarantined`` on insert; set to ``fixed`` by an operator to recycle.
   * - ``_dlq_timestamp``
     - TIMESTAMP
     - Time the row was written to the DLQ (``current_timestamp()``).
   * - ``_dlq_failed_rules``
     - ARRAY<STRUCT<name: STRING, rule: STRING>>
     - The expectations the row violated, each as a ``{name, rule}`` struct.
   * - ``_dlq_rescued_data``
     - STRING
     - Rescued-data JSON from the source ``_rescued_data`` column; ``NULL`` when absent.
   * - ``_row_data``
     - VARIANT
     - Full row payload stored as a Databricks ``VARIANT``.

.. code-block:: sql
   :caption: DLQ inbox table DDL

   CREATE TABLE IF NOT EXISTS <dlq_table> (
       _dlq_sk            STRING    NOT NULL,
       _dlq_source_table  STRING    NOT NULL,
       _dlq_status        STRING    NOT NULL,
       _dlq_timestamp     TIMESTAMP NOT NULL,
       _dlq_failed_rules  ARRAY<STRUCT<name: STRING, rule: STRING>>,
       _dlq_rescued_data  STRING,
       _row_data          VARIANT   NOT NULL
   )
   TBLPROPERTIES (
       'delta.enableChangeDataFeed' = 'true',
       'delta.enableRowTracking' = 'true'
   );

The inbox table must have Change Data Feed and row tracking enabled: the recycle
stream reads it with ``readChangeFeed`` and consumes ``update_postimage`` events,
and fails at runtime when either property is off.

DLQ outbox table schema
-----------------------

The outbox table is the deduplicated "already recycled" ledger. Its name is
``<dlq_table>_outbox``. Create it before the pipeline runs.

.. list-table::
   :header-rows: 1
   :widths: 24 34 42

   * - Column
     - Type
     - Description
   * - ``_dlq_sk``
     - STRING
     - Surrogate key, carried over from the inbox. MERGE key.
   * - ``_dlq_source_table``
     - STRING
     - Fully qualified source table name.
   * - ``_row_data``
     - VARIANT
     - Full row payload.
   * - ``_dlq_recycled_at``
     - TIMESTAMP
     - Time the row was written to the outbox (``current_timestamp()``).

.. code-block:: sql
   :caption: DLQ outbox table DDL

   CREATE TABLE IF NOT EXISTS <dlq_table>_outbox (
       _dlq_sk            STRING    NOT NULL,
       _dlq_source_table  STRING    NOT NULL,
       _row_data          VARIANT   NOT NULL,
       _dlq_recycled_at   TIMESTAMP NOT NULL
   )
   TBLPROPERTIES (
       'delta.enableRowTracking' = 'true'
   );

Limits
------

- Every expectation is coerced to ``drop`` at runtime regardless of its configured ``action``; ``warn`` / ``fail`` rules emit a validation warning.
- The expectations file must contain at least one rule.
- The inbox and outbox tables are user-managed DDL and must exist before the pipeline runs.
- Recycling uses Change Data Feed and a ``foreachBatch`` sink, so it requires a triggered (non-continuous) pipeline.

Minimal example
----------------

.. code-block:: yaml

   - name: dq_orders
     type: transform
     transform_type: data_quality
     source: v_orders
     target: v_orders_validated
     expectations_file: "expectations/orders.yaml"
     mode: quarantine
     readMode: stream
     quarantine:
       dlq_table: "${catalog}.${ops_schema}.orders_dlq"
       source_table: "${catalog}.${bronze_schema}.orders"
