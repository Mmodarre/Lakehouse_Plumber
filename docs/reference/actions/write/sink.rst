Sink
====

``write_target.type: sink``. ``sink_type`` selects the destination. Every sink emits
``dp.create_sink(name=, format=, options=)`` plus one
``@dp.append_flow(target=<sink_name>, name="f_<sink_name>_<index>",
comment=)`` per source view, reading the source with
``spark.readStream.table(...)``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

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

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

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

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/sinks`
- :doc:`All write actions </reference/actions/write>`
