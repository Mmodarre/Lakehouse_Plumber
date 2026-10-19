Quarantine (DLQ) configuration
==============================

.. meta::
   :description: Schema reference for quarantine mode on a data_quality transform — the mode and quarantine fields, the derived outbox name, the DLQ inbox and outbox table DDL and column schema, the CDF requirement, the shared-DLQ concurrency rules, operator guidance for marking rows fixed, and limits.

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

.. seealso::

   How-to guide and recovery-path diagram: :doc:`/guides/transform/quarantine`.

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
     - Fully qualified name of the logical source table. Tags DLQ rows (``_dlq_source_table``) and filters the recycle stream. It also scopes every generated MERGE, so it must be unique per action sharing a DLQ.

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
     - Fully qualified source table name (from ``quarantine.source_table``). Clustering column; every generated MERGE filters on it.
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
   CLUSTER BY (_dlq_source_table)
   TBLPROPERTIES (
       'delta.enableChangeDataFeed' = 'true',
       'delta.enableRowTracking' = 'true',
       'delta.enableDeletionVectors' = 'true'
   );

The inbox table must have Change Data Feed and row tracking enabled: the recycle
stream reads it with ``readChangeFeed`` and consumes ``update_postimage`` events,
and fails at runtime when either property is off. Both the inbox and the outbox
must also have deletion vectors enabled: without them, the inbox MERGEs of
quarantine actions that share the inbox can fail with
``ConcurrentAppendException`` when their commits overlap (see `Concurrent
writers and shared DLQ tables`_).

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
     - Fully qualified source table name. Clustering column; every generated MERGE filters on it.
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
   CLUSTER BY (_dlq_source_table)
   TBLPROPERTIES (
       'delta.enableRowTracking' = 'true',
       'delta.enableDeletionVectors' = 'true'
   );

Concurrent writers and shared DLQ tables
----------------------------------------

Each quarantine action runs its own two ``foreachBatch`` sinks, which MERGE
quarantined rows into the DLQ inbox and recycled rows into the outbox. Quarantine
actions in one pipeline update, or in pipelines running at the same time, can
commit to a shared DLQ and outbox at the same time.

Each generated MERGE filters the target on its own source. The inbox MERGE
condition is:

.. code-block:: python

   (F.col("dlq._dlq_source_table") == F.lit(SOURCE_TABLE_<view>))
   & (F.col("dlq._dlq_sk") == F.col("new._dlq_sk"))

- On the clustered tables above, concurrent sinks for different ``source_table``
  values commit without conflicting while deletion vectors are enabled. Without
  them, overlapping commits can fail with ``ConcurrentAppendException``
  (``DELTA_CONCURRENT_APPEND``), even though each MERGE filters on its source.
- ``source_table`` must be unique per quarantine action that shares a DLQ. Two
  actions with the same ``source_table`` filter the same slice of the DLQ and
  outbox, so their MERGEs can conflict, and each action's recycle stream also
  picks up the other's fixed rows. LHP does not check this.
- The outbox MERGE uses the same condition with the alias ``out`` in place of
  ``dlq``; create the outbox (``<dlq_table>_outbox``) with the same clustering
  and deletion vectors as the inbox.

Operating the queue safely
--------------------------

To recycle a corrected row, an operator rewrites its payload and marks it
``fixed`` in the DLQ inbox:

.. code-block:: sql

   UPDATE <dlq_table>
   SET _dlq_status = 'fixed',
       _row_data   = <corrected_row_data>
   WHERE _dlq_source_table = '<source_table>'
     AND _dlq_sk IN (<dlq_sk_values>);

``<corrected_row_data>`` is a VARIANT value, for example ``parse_json('{...}')``,
whose field names match the source columns: recycled rows are read back by
column name.

- Always include the ``_dlq_source_table`` predicate. Without it, the UPDATE is
  not scoped to one source and can conflict with other sources' sinks.
- Mark rows ``fixed`` while the pipeline is idle (quarantine requires a triggered
  pipeline; see `Limits`_). An UPDATE that races the same source's sink can fail
  either the UPDATE or the pipeline's MERGE.

Limits
------

- Every expectation is coerced to ``drop`` at runtime regardless of its configured ``action``; ``warn`` / ``fail`` rules emit a validation warning.
- The expectations file must contain at least one rule.
- The inbox and outbox tables are user-managed DDL and must exist before the pipeline runs.
- Recycling uses Change Data Feed and a ``foreachBatch`` sink, so it requires a triggered (non-continuous) pipeline.
- Deletion vectors must stay enabled on a DLQ (and its outbox) shared by concurrently running quarantine actions.

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
