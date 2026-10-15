Kafka stream (kafka)
====================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: kafka``.

Streams from Apache Kafka (or Kafka-protocol-compatible endpoints) into a
temporary view. Fields live under ``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_events
     type: load
     source:
       type: kafka
       bootstrap_servers: "broker1:9092,broker2:9092"
       subscribe: orders
     target: v_events

Source fields
-------------

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``kafka``.
   * - ``bootstrap_servers``
     - string
     - Yes
     - —
     - Maps to ``kafka.bootstrap.servers``.
   * - ``subscribe``
     - string
     - One of
     - —
     - Topic(s). Provide exactly one of ``subscribe`` / ``subscribePattern`` / ``assign``.
   * - ``subscribePattern``
     - string
     - One of
     - —
     - Topic regex. One of the three subscription methods.
   * - ``assign``
     - string (JSON)
     - One of
     - —
     - Partition assignment. One of the three subscription methods.
   * - ``options``
     - mapping
     - No
     - —
     - Extra ``kafka.*`` options.

``readMode`` must be ``stream`` (``batch`` is rejected). Kafka returns binary
``key``/``value`` columns — deserialize them in a downstream transform.

Read mode placement
-------------------

Prefer action-level ``readMode`` alongside ``source`` and ``target``.
The generator also accepts ``source.readMode`` as a fallback. An action-level
value takes precedence when both are supplied.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/kafka>` or
:doc:`choose another load source </reference/actions/load>`.

For the upstream option definitions, see the
`Spark Kafka integration reference <https://spark.apache.org/docs/latest/streaming/structured-streaming-kafka-integration.html>`_.
