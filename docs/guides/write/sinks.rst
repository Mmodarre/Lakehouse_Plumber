=========================
Write to an external sink
=========================

.. meta::
   :description: Route a Lakehouse Plumber pipeline's output to an external Delta table with a declarative sink write action — no hand-written dp.create_sink call or append-flow plumbing.

Your pipeline builds a gold ``order_summary`` table, but the team that consumes
it works out of a different Unity Catalog — one your pipeline doesn't manage.
You need to push a copy of the rows out to that external catalog as they land,
without making the external table part of your pipeline's own managed output.

You could hand-write that push: register the destination with
``dp.create_sink(...)``, then wire an ``@dp.append_flow`` that streams your view
into it. Or you declare a ``type: sink`` write and let Lakehouse Plumber write
that Lakeflow for you. That is the idea on every page: **declare your ETL, don't
hand-write it.**

Let's route a gold ``order_summary`` table out to an external analytics catalog
through a Delta sink, without touching PySpark.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide assumes the gold ``order_summary`` table already exists in
your pipeline's catalog, and that the external ``analytics_shared`` catalog
you're exporting to already exists — a sink writes *to* a destination, it does
not create the catalog or grant you access to it.

Declare the sink write
======================

A pipeline in Lakehouse Plumber is a **flowgroup**: a short YAML file describing
a sequence of **actions**. This one has two — a ``load`` that streams the gold
table into a view, and a ``write`` whose ``write_target`` is a **sink**.

Create ``pipelines/order_summary_export.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_sinks/pipelines/order_summary_export.yaml
   :language: yaml
   :caption: pipelines/order_summary_export.yaml
   :emphasize-lines: 24-29

The ``write`` action's ``write_target`` is typed ``sink`` with ``sink_type:
delta``. ``sink_name`` is the identifier LHP registers the sink under.
Everything under ``options`` is passed straight through to the Delta writer —
here ``tableName`` names the external three-part table the rows land in. A Delta
sink takes either ``tableName`` (a Unity Catalog table) or ``path`` (a
file-based Delta table), never both.

``delta`` is one of four sink kinds. A ``kafka`` sink streams rows to an Apache
Kafka topic or Azure Event Hubs; a ``custom`` sink writes through a Python
``DataSink``; and a ``foreachbatch`` sink runs custom Python per micro-batch for
merges and multi-destination writes. This guide stays on the Delta sink — the
write action reference documents the other three, end to end.

Point the tokens at your environment
====================================

The ``${...}`` tokens resolve from a per-environment substitutions file. Create
``substitutions/dev.yaml``:

.. literalinclude:: ../../_fixtures/guide_write_sinks/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml

``catalog`` and ``gold_schema`` locate the source table; ``analytics_catalog``
and ``analytics_schema`` name the external destination. This is the only place
environment-specific values live, so exporting from a production catalog later
is a ``prod.yaml`` with production values — the flowgroup itself never changes.

Generate the pipeline
=====================

Now compile the YAML into Lakeflow code. Validate first, then generate:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ export_analytics  0 files
   ✓ validate (0.34s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ export_analytics  1 file
   ✓ generate (0.34s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

``validate`` resolves every token and checks the actions before you commit to
generating; ``generate`` writes the Python.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/export_analytics/order_summary_export.py``. This is the
*entire* output — nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_write_sinks/generated/dev/export_analytics/order_summary_export.py
   :language: python
   :caption: generated/dev/export_analytics/order_summary_export.py
   :emphasize-lines: 33-49

It's ordinary Lakeflow. The source view is a ``@dp.temporary_view`` wrapping
``spark.readStream.table("dev_catalog.gold.order_summary")``. The sink itself is
a ``dp.create_sink(name="order_summary_export", format="delta", options={...})``
— the destination registered under your ``sink_name``, with your ``options``
resolved to a plain dict. Then a generated ``@dp.append_flow`` streams the view
into that sink. You described a source view and a destination table; LHP wrote
the sink registration and the append flow that connect them.

.. note::

   A sink is the opposite direction from a streaming-table write. The
   streaming-table write from Get Started emits ``dp.create_streaming_table``
   for a table your pipeline *owns and manages*. A sink emits ``dp.create_sink``
   plus an append flow to push rows *out* to a destination the pipeline does not
   manage. A Delta sink with ``tableName`` writes a real Unity Catalog table
   that downstream actions can read back; a ``path``-based Delta sink or a Kafka
   sink is purely terminal — nothing in the pipeline reads it again.

What you just did
=================

Twenty-one lines of YAML compiled to forty-nine lines of Lakeflow Python.
**Zero lines of the sink plumbing came from you** — not the
``dp.create_sink`` registration, not the ``@dp.append_flow``, not the
``readStream`` that feeds it. Change the destination table and regenerate, and
LHP rewrites the registration to match — you never touch the Python.

The payoff compounds. Adding a second export, switching the destination catalog
per environment, or swapping the Delta sink for a Kafka sink is a few more lines
of YAML — not another forty-nine lines of Python each time.

What's next
===========

- **Stream to a message bus.** A ``sink_type: kafka`` write pushes rows to an
  Apache Kafka topic or Azure Event Hubs instead of a Delta table. The source
  view must carry ``key`` and ``value`` columns; the write action reference
  shows how.
- **Run custom logic per batch.** A ``foreachbatch`` sink runs your own Python
  on each micro-batch (merges, upserts, multi-destination writes), and a
  ``custom`` sink writes through a Python ``DataSink`` for any destination
  Databricks does not support directly.
- **See every sink option.** The full option surface for each sink kind — and
  the ``tableName`` / ``path`` rule for Delta — is in the write action
  reference, with the constraints on each.
