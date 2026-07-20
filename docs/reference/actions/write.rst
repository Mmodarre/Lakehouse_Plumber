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
     - UC tags ``{key: value}``; applied by a generated tagging hook (REST API), not table DDL. See Unity Catalog tags below.
   * - ``tags_file``
     - string
     - —
     - Path to a unified schema/tags file (convention ``schemas/<table>.yaml``) whose table-level ``tags`` and per-column ``tags`` supply UC tags. May be the same file as ``table_schema``. Mutually exclusive with an inline ``tags`` mapping. See :ref:`uc-tags-file` below.
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
     - Inline DDL, or a ``.ddl``/``.sql``/``.yaml``/``.json`` file path (auto-detected). A ``.yaml``/``.json`` file is the unified schema/tags file; its per-column ``type``/``nullable``/``comment`` are read here. Any UC ``tags`` it carries are ignored unless the same file is also set as ``tags_file`` — otherwise ``lhp generate`` warns ``LHP-CFG-069``.
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

Unity Catalog tags
==================

.. versionadded:: 0.9.1
   The ``tags_file`` field and column-level UC tags, read from a unified
   schema/tags file that ``table_schema`` and ``tags_file`` can share.

A ``streaming_table`` or ``materialized_view`` write target can carry Unity
Catalog (UC) tags at the table level (a ``tags`` mapping on ``write_target``, or the
``tags:`` block of a ``tags_file``) and at the column level (the per-column
``tags:`` inside a ``tags_file`` — see :ref:`uc-tags-file` below). Because
Lakeflow Spark Declarative Pipelines (SDP) cannot set UC tags as part of table
creation, Lakehouse Plumber (LHP) collects every declared tag and emits one
per-pipeline ``_uc_tagging_hook.py`` that applies them through the Unity Catalog
*Entity Tag Assignments* REST API rather than table DDL.

.. code-block:: yaml

   - name: write_orders_silver
     type: write
     source: v_orders_bronze
     write_target:
       type: streaming_table
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: orders
       tags:
         team: platform
         cost_center: "1234"
         pii: ""            # key-only tag: "", ~, or an omitted value

The feature is on by default; declaring ``tags`` (or ``tags_file``) opts a table
in. Set ``uc_tagging.enabled: false`` in ``lhp.yaml`` to disable it. Only the
table-creating action is tagged (``create_table: true``); temporary tables and
sinks are excluded.

uc_tagging config block
-----------------------

The optional ``uc_tagging`` block in ``lhp.yaml`` tunes the hook:

.. list-table::
   :header-rows: 1
   :widths: 30 14 12 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``enabled``
     - bool
     - ``true``
     - Set ``false`` to disable tag generation entirely.
   * - ``remove_undeclared_tags``
     - bool
     - ``false``
     - ``false`` is additive (create/update declared tags only); ``true``
       reconciles to the declared set, deleting existing tags whose key is not
       declared for a managed entity. An explicit ``tags: {}`` then means
       "managed with an empty set".
   * - ``tag_update_concurrency``
     - int
     - ``16``
     - Max concurrent tag operations (range 1–20).

How the hook applies tags
-------------------------

The generated hook runs as a ``@dp.on_event_hook`` during the pipeline update.
It fires on ``update_progress`` ``RUNNING`` — when streaming tables already
exist — and on the terminal states, which catch materialized views that
materialize later; each entity is tagged at most once. Tagging is best-effort
and non-blocking: tag-write failures surface as pipeline event-log warnings and
never fail the update (event hooks cannot). Key-only tags use ``""``, ``~``, or
an omitted value.

Existing tag state is read once at module import with a single
``system.information_schema`` query (``table_tags`` ``UNION ALL``
``column_tags``); a read failure is caught at import, re-raised as a warning on
the first ``RUNNING`` event, and tagging then proceeds create-only.

.. important::

   Unity Catalog requires the pipeline's run-as identity to hold ``APPLY TAG``
   on the table and ``ASSIGN`` on any required governed tags to write tags via
   the REST API, plus ``USE CATALOG``, ``USE SCHEMA``, and ``SELECT`` on
   ``system.information_schema`` to read existing tag state. LHP does not verify
   these grants; a missing grant surfaces as an event-log warning at run time.

.. _uc-tags-file:

Schema & tags file
------------------

``tags_file`` and ``table_schema`` both point at a **unified schema/tags file**
(project convention ``schemas/<table>.yaml``). One file can serve both fields —
``table_schema`` reads the column types, ``tags_file`` reads the UC tags — or
they can point at different files. ``tags_file`` is mutually exclusive with an
inline ``tags`` mapping; ``table_schema`` is orthogonal and combines with
either.

The file is a mapping whose recognised keys are an optional identifier
(``table``, or its alias ``name``), a table-level ``tags`` mapping, and a
``columns`` list. The legacy schema keys ``version``, ``description``, and
``primary_key`` are tolerated and ignored. Each ``columns`` entry has a required
``name`` plus optional ``type``, ``nullable``, and ``comment`` (read by
``table_schema``) and ``tags`` (read by ``tags_file``). ``type`` is required
when the file is used as ``table_schema``; it is optional in a tags-only file. A
tag value of ``""``, ``~``, or an omitted value is a key-only tag, at either
level.

.. code-block:: yaml
   :caption: schemas/orders.yaml — point BOTH table_schema and tags_file here

   table: orders               # optional identifier; 'name' is an accepted alias
   tags:                       # table-level UC tags (read by tags_file)
     team: platform
     cost_center: "1234"
   columns:
     - name: email
       type: STRING            # required for table_schema use; optional tags-only
       nullable: false         # schema use
       comment: "PII"          # schema use
       tags:                   # column-level UC tags (read by tags_file)
         pii: high
     - name: region
       type: STRING
       tags:
         classification: public

.. code-block:: yaml

   - name: write_orders_silver
     type: write
     source: v_orders_bronze
     write_target:
       type: streaming_table
       catalog: main
       schema: silver
       table: orders
       table_schema: schemas/orders.yaml   # column types (+ nullable/comment)
       tags_file: schemas/orders.yaml       # same file: UC table + column tags

``lhp generate`` raises ``LHP-CFG-067`` when the file is not a mapping, carries
an unknown top-level key (the retired ``column_tags`` key is now rejected as
unknown), has a ``columns`` that is not a list, or a ``columns`` entry that is
not a mapping or carries an unknown key. Read as a ``tags_file`` it also rejects
a wrong-typed ``table``/``name``/``tags``, a column ``name`` that is missing,
empty, or duplicated, and a per-column ``tags`` that is not a mapping.

The identifier is optional. When present in a ``tags_file`` it should equal the
write target's table name; a mismatch logs a warning (``LHP-CFG-068``) and
generation proceeds using the write target's table. A file that declares both
``table`` and ``name`` with differing values also warns ``LHP-CFG-068`` (with
``table`` winning). A missing ``tags_file`` raises ``LHP-IO-001`` with the
searched locations. Under ``--sandbox`` the identifier cross-check is skipped
(sandbox renames the write target's table), and the file's tags are applied to
the renamed table.

Because a preset's ``tags`` default deep-merges into the write target before
validation, pairing a preset ``tags`` default with a flowgroup ``tags_file`` is
rejected as both-set (``cannot specify both 'tags' and 'tags_file'``).

.. note::

   A file set as ``table_schema`` but **not** also wired as ``tags_file`` has
   its UC ``tags`` silently dropped — the schema reader consumes only the column
   types. ``lhp generate`` emits an ``LHP-CFG-069`` warning in that case (from
   the streaming-table and materialized-view writes only, never the cloudfiles
   load path); point ``tags_file`` at the same file to apply the tags.
   (``lhp validate`` runs no code generation, so the warning surfaces at
   generate time.)

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
