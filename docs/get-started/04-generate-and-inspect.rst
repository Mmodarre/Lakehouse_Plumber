========================
Generate and inspect
========================

.. meta::
   :description: Run lhp validate and lhp generate on the sample project, then read the Lakeflow Python it produced — ordinary streaming tables, CDC flows, and materialized views you own.

Now compile the YAML into Lakeflow code and read what comes out. This is where
Lakehouse Plumber earns its keep: you wrote declarative YAML; it writes the
PySpark.

Validate first
==============

``validate`` resolves every ``${...}`` token and checks the actions without
writing anything. On a bundle project you pass the pipeline config with ``-pc``:

.. code-block:: console

   $ lhp validate --env dev -pc config/pipeline_config.yaml
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ sample_gold  0 files
   ✓ sample_ingest  0 files
   ✓ sample_lakehouse_event_log_monitoring  0 files
   ✓ sample_silver  0 files
   ✓ validate (0.38s)
   4 validated · 0.7s

Four pipelines: the three you explored, plus a monitoring pipeline Lakehouse
Plumber adds automatically because ``lhp.yaml`` enables monitoring. ``0 files``
means nothing is written — ``validate`` only checks.

.. note::

   ``-pc`` is required on a bundle project. Without it, ``generate`` stops at
   preflight with ``LHP-CFG-023`` rather than emit half-configured resources.
   Non-bundle projects don't need it.

Generate
========

.. code-block:: console

   $ lhp generate --env dev -pc config/pipeline_config.yaml
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ sample_gold  1 file
   ✓ sample_ingest  3 files
   ✓ sample_lakehouse_event_log_monitoring  1 file
   ✓ sample_silver  3 files
   ✓ generate (0.41s)
   ✓ format (0.06s)
   ✓ monitoring (0.00s)
   ✓ bundle_sync (0.02s)
   4 pipelines generated · 8 files · 0.8s

Eight Python files under ``generated/dev/``, one per flowgroup, plus the bundle
resources under ``resources/lhp/``. The ``format`` step runs the output through a
formatter; ``bundle_sync`` writes the bundle resources.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/sample_ingest/orders_ingest.py`` — the whole output of that
eight-line flowgroup:

.. literalinclude:: ../_fixtures/sample_project/generated/dev/sample_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/sample_ingest/orders_ingest.py

It's ordinary Lakeflow: a ``@dp.temporary_view`` for the Auto Loader source, a
``dp.create_streaming_table`` for the target, and a ``@dp.append_flow`` wiring
them together. Your schema hints became a typed DDL string; your
``operational_metadata`` list became three ``withColumn`` calls; your
``bronze_layer`` preset became the ``table_properties``. Nothing is hidden behind
a runtime — you could have written this by hand.

The CDC dimension is just as plain. Open
``generated/dev/sample_silver/dim_customer.py``:

.. literalinclude:: ../_fixtures/sample_project/generated/dev/sample_silver/dim_customer.py
   :language: python
   :caption: generated/dev/sample_silver/dim_customer.py
   :lines: 44-63

Your three-key CDC config became a ``dp.create_auto_cdc_flow`` call with the keys,
the sequencing column, and SCD type 2 — the exact API you'd otherwise have to
remember and hand-wire.

What you just did
=================

You turned eight flowgroups into eight readable Lakeflow files — **zero lines of
PySpark from you**. And it's code you own: version it, diff it, open it in the
Databricks editor. Change a flowgroup and regenerate, and only the affected files
change. That's the compounding payoff — the pattern is written once, and
Lakehouse Plumber repeats the plumbing for every table.

Next
====

The code is on disk. Ship it to a Databricks workspace —
:doc:`05-deploy-and-run`.
