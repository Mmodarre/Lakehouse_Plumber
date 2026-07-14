==============================
Ingest a streaming Kafka topic
==============================

.. meta::
   :description: Stream an Apache Kafka topic into a bronze streaming table with a declarative Lakehouse Plumber kafka load action — no spark.readStream.format("kafka") boilerplate and no hand-wired secret lookups.

Your order events don't wait for a nightly batch — they land on a Kafka topic
the moment each one happens. To get them into the lakehouse you need a
continuous reader: connect to the brokers, subscribe to the topic, authenticate
to a secured cluster, and keep streaming.

You could hand-write that reader — ``spark.readStream.format("kafka")`` with a
stack of ``.option(...)`` calls, a ``dbutils.secrets.get(...)`` for the
truststore password, then a ``create_streaming_table`` and an ``append_flow`` to
land the rows. Or you declare a ``type: kafka`` load and let Lakehouse Plumber
write that Lakeflow for you. That is the idea on every page: **declare your ETL,
don't hand-write it.**

Let's stream an ``orders`` topic off a TLS-secured Kafka cluster into a bronze
streaming table, without touching PySpark.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide assumes the ``orders`` topic already exists on your Kafka
cluster, and that the truststore password is stored in a Databricks secret scope
named ``kafka`` — a Kafka load reads a live topic, it does not create it.

Declare the Kafka load
======================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that streams the topic
into a view, and a ``write`` that appends the view into a streaming table.

Create ``pipelines/orders_stream.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_kafka/pipelines/orders_stream.yaml
   :language: yaml
   :caption: pipelines/orders_stream.yaml

The ``load`` action names the cluster with ``bootstrap_servers`` and picks the
topic with ``subscribe``. Kafka is always a streaming source, so there is no
batch alternative: LHP generates ``spark.readStream.format("kafka")`` and
``readMode`` defaults to ``stream``. Everything under ``options`` is passed
straight to the Kafka reader — ``startingOffsets: earliest`` reads the topic from
the beginning on the first run, and the three ``kafka.*`` keys turn on TLS and
point at the truststore.

The truststore password is a secret, not a literal. ``${secret:kafka/truststore-password}``
references the ``truststore-password`` key in the ``kafka`` secret scope; LHP
turns that into a ``dbutils.secrets.get(...)`` call at generation time, so the
password never appears in your YAML or in the generated Python. The other
``${...}`` tokens are per-environment placeholders you resolve next.

``subscribe`` is one of three subscription methods — the load action reference
covers ``subscribePattern`` (a topic regex) and ``assign`` (specific
partitions), the extra ``kafka.*`` authentication options for AWS MSK and Azure
Event Hubs, and offset controls like ``failOnDataLoss``. This guide stays on the
single-topic case.

Point the tokens at your environment
====================================

The ``${...}`` tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_ingest_kafka/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

This is the only place environment-specific values live. To stream the same
topic from a production cluster later, you write a ``prod.yaml`` with the
production brokers, truststore, and catalog — the flowgroup itself never
changes.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  0 files
   ✓ validate (0.32s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.36s)
   ✓ format (0.10s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.5s

``validate`` resolves every token and secret reference and checks the actions
before you commit to generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_ingest/orders_stream.py``. This is the *entire*
output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_ingest_kafka/generated/dev/bronze_ingest/orders_stream.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_stream.py

It's ordinary Lakeflow. The source view is a ``@dp.temporary_view`` wrapping
``spark.readStream.format("kafka")``, with every ``option(...)`` you declared
resolved in order and the token values substituted in. The secret became a bare
``dbutils.secrets.get(scope="kafka", key="truststore-password")`` call — a live
lookup at run time, not a string. The target is a ``dp.create_streaming_table``,
and a ``@dp.append_flow`` streams the view into it. You described a topic and a
target table; LHP wrote the reader, the secret lookup, and the two pieces of
plumbing that connect them.

.. note::

   A Kafka read returns a fixed schema: ``key`` and ``value`` as raw **binary**
   columns, plus ``topic``, ``partition``, ``offset``, ``timestamp``, and
   ``timestampType``. This flowgroup lands that envelope as-is in the bronze
   ``orders_raw`` table — the durable, replayable record of what arrived. To turn
   the binary ``value`` into typed columns, deserialize it downstream with a SQL
   transform (``CAST(value AS STRING)`` then ``from_json(...)``), covered in the
   SQL transform guide.

What you just did
=================

Twenty-three lines of YAML. Sixty lines of Lakeflow Python. **Zero lines of
PySpark from you** — you never wrote ``readStream.format("kafka")``, the six
``.option(...)`` calls, the ``dbutils.secrets.get`` lookup, ``create_streaming_table``,
or ``append_flow``. And the output is code you own: version it, diff it, and open
it in the Databricks editor like anything else.

The payoff compounds. Subscribing to a second topic, switching brokers per
environment, or authenticating with MSK IAM is a few more lines of YAML — not
another sixty lines of Python each time.

What's next
===========

- **Deserialize the payload.** The binary ``value`` column becomes typed rows
  with a SQL transform — ``CAST(value AS STRING)`` and ``from_json(...)`` — reading
  ``stream(v_orders_kafka_raw)``. See the SQL transform guide.
- **Authenticate to a managed broker.** AWS MSK IAM and Azure Event Hubs OAuth
  are extra ``kafka.sasl.*`` options on the same load, with credentials supplied
  as secrets. The load action reference documents both, end to end.
- **See every Kafka option.** ``subscribePattern`` and ``assign``,
  ``startingOffsets``, ``failOnDataLoss``, and the full ``kafka.*`` set are in the
  load action reference, with their constraints.
