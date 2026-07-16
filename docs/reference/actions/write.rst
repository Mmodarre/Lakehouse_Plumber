.. meta::
   :description: Reference for LHP write actions — streaming table (standard, CDC, snapshot CDC), materialized view, and sink (delta, kafka, Event Hubs, foreachbatch, custom) write_target fields and emitted Lakeflow constructs.

=============
Write actions
=============

A write action has ``type: write`` and a ``write_target`` block. The
``write_target.type`` field selects the target: ``streaming_table``,
``materialized_view``, or ``sink``. This page lists every ``write_target``
field, the mode-specific config blocks, and the Lakeflow construct each target
emits.

.. seealso::

   How-to guides: :doc:`/guides/write/streaming-table-standard`, :doc:`/guides/write/streaming-table-cdc`, :doc:`/guides/write/streaming-table-snapshot-cdc`, :doc:`/guides/write/materialized-view`, :doc:`/guides/write/sinks`.

Shared fields
=============

These ``write_target`` fields apply to ``streaming_table`` and
``materialized_view``. Sinks use a separate field set (see Sink below).

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``type``
     - string
     - required
     - ``streaming_table``, ``materialized_view``, or ``sink``.
   * - ``catalog``
     - string
     - —
     - UC catalog.
   * - ``schema``
     - string
     - —
     - UC namespace schema (not DDL — use ``table_schema`` for DDL).
   * - ``table``
     - string
     - —
     - Target table/view name.
   * - ``create_table``
     - bool
     - ``true``
     - Streaming table only; ``snapshot_cdc`` forces ``true``.
   * - ``temporary``
     - bool
     - ``false``
     - Emitted as ``temporary=``.
   * - ``comment``
     - string
     - derived
     - Defaults to a description derived from the target table name.
   * - ``table_properties``
     - dict
     - —
     - Delta table properties.
   * - ``tags``
     - dict
     - —
     - UC tags ``{key: value}``; applied by a generated tagging hook (REST API), not table DDL.
   * - ``partition_columns``
     - list
     - —
     - Emitted as ``partition_cols=``.
   * - ``cluster_columns``
     - list
     - —
     - Liquid clustering; mutually exclusive with ``cluster_by_auto``.
   * - ``cluster_by_auto``
     - bool
     - —
     - Auto liquid clustering; mutually exclusive with ``cluster_columns``.
   * - ``spark_conf``
     - dict
     - —
     - Spark configuration.
   * - ``table_schema``
     - string
     - —
     - Inline DDL, or a ``.ddl``/``.sql``/``.yaml``/``.json`` file path (auto-detected).
   * - ``row_filter``
     - string
     - —
     - Row-filter clause, emitted as ``row_filter=``.
   * - ``path``
     - string
     - —
     - Storage location, emitted as ``path=``.
   * - ``database``
     - string
     - —
     - Deprecated (removed at 1.0.0); use ``catalog`` + ``schema``.

Action-level ``once: true`` emits ``once=True`` on the flow. Action-level
``readMode: batch`` switches a standard append flow from
``spark.readStream.table(...)`` to ``spark.read.table(...)`` (default
``stream``).

Streaming table
===============

``write_target.type: streaming_table``. Handler:
``StreamingTableWriteGenerator``. The ``mode`` field selects the flow shape.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``mode``
     - string
     - ``standard``
     - One of ``standard``, ``cdc``, ``snapshot_cdc``.

``source`` may be a single view or a list of views (multi-source append flow
into one table).

.. code-block:: yaml

   - name: write_customer_silver
     type: write
     source: v_customer_bronze
     write_target:
       type: streaming_table
       mode: standard
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim

Emitted constructs
------------------

- ``standard``: ``dp.create_streaming_table(...)`` (when ``create_table`` is
  true) plus one ``@dp.append_flow(target=, name=, comment=)`` decorator per
  source view.
- ``cdc``: ``dp.create_streaming_table(...)`` (when ``create_table`` is true)
  plus ``dp.create_auto_cdc_flow(...)``.
- ``snapshot_cdc``: ``dp.create_streaming_table(...)`` (always) plus
  ``dp.create_auto_cdc_from_snapshot_flow(...)``.

mode: cdc
---------

``mode: cdc`` requires a ``cdc_config`` block. Fields under
``write_target.cdc_config``:

.. list-table::
   :header-rows: 1
   :widths: 28 18 10 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``keys``
     - list[string]
     - required
     - Non-empty business keys.
   * - ``sequence_by``
     - string / list[string]
     - —
     - Ordering column(s); a list emits ``sequence_by=struct(...)``.
   * - ``scd_type``
     - int
     - ``1``
     - ``1`` or ``2``; emitted as ``stored_as_scd_type=``.
   * - ``ignore_null_updates``
     - bool
     - —
     - —
   * - ``apply_as_deletes``
     - string
     - —
     - SQL expression.
   * - ``apply_as_truncates``
     - string
     - —
     - SQL expression; not allowed with ``scd_type: 2``.
   * - ``track_history_column_list``
     - list[string]
     - —
     - ``scd_type: 2``; mutually exclusive with ``track_history_except_column_list``.
   * - ``track_history_except_column_list``
     - list[string]
     - —
     - ``scd_type: 2``; mutually exclusive with ``track_history_column_list``.
   * - ``column_list``
     - list[string]
     - —
     - Mutually exclusive with ``except_column_list``.
   * - ``except_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``column_list``.

.. code-block:: yaml

   - name: write_customer_silver
     type: write
     source: v_customer_bronze
     write_target:
       type: streaming_table
       mode: cdc
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim
       cdc_config:
         keys: ["customer_id"]
         sequence_by: "last_modified_dt"
         scd_type: 2

mode: snapshot_cdc
------------------

``mode: snapshot_cdc`` requires a ``snapshot_cdc_config`` block and forces
``create_table: true``. Fields under ``write_target.snapshot_cdc_config``:

.. list-table::
   :header-rows: 1
   :widths: 28 18 10 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``source``
     - string
     - one-of
     - External table/path; exactly one of ``source`` / ``source_function``.
   * - ``source_function``
     - dict
     - one-of
     - ``{file, function, parameters?}``; ``file`` and ``function`` required.
   * - ``keys``
     - list[string]
     - required
     - Non-empty.
   * - ``stored_as_scd_type``
     - int
     - ``1``
     - ``1`` or ``2``.
   * - ``track_history_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``track_history_except_column_list``.
   * - ``track_history_except_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``track_history_column_list``.

With ``source_function``, each ``parameters`` entry is bound as a keyword
argument via ``functools.partial`` (the function must declare them as
keyword-only args after ``*``); ``source_function.file`` is copied alongside
the generated pipeline and resolved relative to project root.

.. code-block:: yaml

   - name: write_customer_snapshot
     type: write
     write_target:
       type: streaming_table
       mode: snapshot_cdc
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim
       snapshot_cdc_config:
         source: "${catalog}.${bronze_schema}.customer_snapshot"
         keys: ["customer_id"]
         stored_as_scd_type: 2

Materialized view
=================

``write_target.type: materialized_view``. Handler:
``MaterializedViewWriteGenerator``. Emits a ``@dp.materialized_view(...)``
decorated function. Uses the shared fields above plus the fields below.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``sql``
     - string
     - —
     - Inline query; one of ``sql`` / ``sql_path`` / action ``source``.
   * - ``sql_path``
     - string
     - —
     - External ``.sql`` query file.
   * - ``refresh_schedule``
     - string
     - —
     - Cron/schedule; emitted as ``refresh_schedule=``.
   * - ``refresh_policy``
     - string
     - —
     - One of ``auto``, ``incremental``, ``incremental_strict``, ``full``.

Define the view by exactly one of: an action-level ``source`` view, inline
``sql``, or ``sql_path``. When ``sql``/``sql_path`` is provided, no
action-level ``source`` is needed.

.. code-block:: yaml

   - name: write_customer_summary
     type: write
     write_target:
       type: materialized_view
       catalog: "${catalog}"
       schema: "${gold_schema}"
       table: customer_summary
       sql: "SELECT customer_id, COUNT(*) AS orders FROM v_orders GROUP BY customer_id"

Sink
====

``write_target.type: sink``. Handler: ``SinkWriteGenerator`` dispatches on
``sink_type``. Every sink emits
``dp.create_sink(name=, format=, options=)`` plus one
``@dp.append_flow(target=<sink_name>, name="f_<sink_name>_<index>",
comment=)`` per source view, reading the source with
``spark.readStream.table(...)``.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``sink_type``
     - string
     - required
     - ``delta``, ``kafka``, ``foreachbatch``, or ``custom``.
   * - ``sink_name``
     - string
     - —
     - Unique sink identifier; used in the emitted flow names.
   * - ``options``
     - dict
     - ``{}``
     - Sink options.
   * - ``comment``
     - string
     - derived
     - —

sink_type-specific fields
-------------------------

.. list-table::
   :header-rows: 1
   :widths: 20 36 44

   * - sink_type
     - Additional fields
     - Notes
   * - ``delta``
     - ``options.tableName`` or ``options.path``
     - Format fixed ``delta``; ``tableName`` and ``path`` mutually exclusive.
   * - ``kafka``
     - ``bootstrap_servers``, ``topic``, ``options``
     - Format fixed ``kafka``; source must carry a ``value`` column.
   * - ``kafka`` (Event Hubs)
     - ``options.kafka.sasl.mechanism: OAUTHBEARER``
     - No separate ``sink_type``; ``OAUTHBEARER`` flips the Kafka handler into Event Hubs mode. Endpoint ``<namespace>.servicebus.windows.net:9093``, Event Hub name as ``topic``.
   * - ``foreachbatch``
     - ``sink_name`` (required), ``module_path`` or ``batch_handler``
     - Exactly one of ``module_path`` / ``batch_handler``; ``source`` must be a single view string.
   * - ``custom``
     - ``module_path`` (required), ``custom_sink_class`` (required), ``options``
     - Writes through a user-supplied PySpark ``DataSink``.

.. code-block:: yaml

   - name: write_orders_to_delta_sink
     type: write
     source: v_orders
     write_target:
       type: sink
       sink_type: delta
       sink_name: orders_delta_sink
       options:
         tableName: "${catalog}.${gold_schema}.orders_export"

.. code-block:: yaml

   - name: write_orders_to_kafka_sink
     type: write
     source: v_orders_for_kafka
     write_target:
       type: sink
       sink_type: kafka
       sink_name: order_events_kafka
       bootstrap_servers: "${kafka_bootstrap_cluster}"
       topic: "acme.orders.fulfillment"
       options:
         kafka.security.protocol: "SASL_SSL"
         kafka.sasl.mechanism: "PLAIN"

.. code-block:: yaml

   - name: write_orders_to_eventhubs
     type: write
     source: v_orders_for_eventhubs
     write_target:
       type: sink
       sink_type: kafka
       sink_name: order_events_eventhubs
       bootstrap_servers: "${eh_namespace}.servicebus.windows.net:9093"
       topic: "acme-orders"
       options:
         kafka.security.protocol: "SASL_SSL"
         kafka.sasl.mechanism: "OAUTHBEARER"

.. code-block:: yaml

   - name: merge_customer_updates
     type: write
     source: v_customer_changes
     write_target:
       type: sink
       sink_type: foreachbatch
       sink_name: customer_merge_sink
       batch_handler: |
         df.createOrReplaceTempView("batch_view")
         df.sparkSession.sql("MERGE INTO ${catalog}.${gold_schema}.dim_customer ...")

.. code-block:: yaml

   - name: write_to_custom_sink
     type: write
     source: v_seed_rows
     write_target:
       type: sink
       sink_type: custom
       sink_name: backed_sink
       module_path: "py_functions/custom_sink.py"
       custom_sink_class: "MyCustomSink"
       options:
         output_path: "/tmp/custom_sink_output"
