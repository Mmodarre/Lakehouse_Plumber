==========================================
Gate rows with data-quality expectations
==========================================

.. meta::
   :description: Gate rows with a Lakehouse Plumber data_quality transform — declare drop, fail, and warn expectations in one file and let LHP write the @dp.expect_* decorators, instead of hand-stacking them on a view.

Let's take a stream of raw order rows and stop the junk before it reaches the
table: halt the pipeline on a corrupt feed, drop the impossible rows, and flag
the merely suspicious without losing them. Three different reactions to three
kinds of bad data — declared as rules, not hand-written enforcement.

You could hand-write this: three stacked decorators —
``@dp.expect_all_or_fail``, ``@dp.expect_all_or_drop``, and ``@dp.expect_all`` —
each wrapping a dict of named constraints on a ``@dp.temporary_view`` that
re-reads the upstream view, and you'd have to remember which decorator halts the
update, which drops the row, and which only logs. Or you declare the rules in one
file, tag each with an ``action``, and hand Lakehouse Plumber a
``data_quality`` transform. That is the idea on every page: **declare your ETL,
don't hand-write it.**

Before you start
================

A data-quality check sits just before a write: it reads the view an upstream
action produced and hands a *validated* view to the write. This guide extends the
orders ingest from **Your first pipeline** — a ``load`` action that streams raw
CSV into ``v_orders_raw``.

Get started step **Add a transform and data quality** already used a
``data_quality`` gate to *drop* bad rows. This guide goes deeper: the full
expectations-file format, all three rule actions, what each one generates, and
what ``mode: dqe`` means.

Write the expectations
======================

Put the rules in their own file. Each entry is a **SQL boolean expression** — the
key — paired with an ``action`` and a ``name``. Create
``expectations/orders_quality.yaml``:

.. literalinclude:: ../../_fixtures/guide_transform_dq/expectations/orders_quality.yaml
   :language: yaml
   :caption: expectations/orders_quality.yaml

The ``action`` on each rule is the whole point — it decides what happens to a row
that fails the expression:

- ``fail`` — a violation **halts the pipeline update**. Use it for invariants that
  should never break: here a null ``order_id`` means the source feed is corrupt,
  so stop rather than ingest garbage.
- ``drop`` — a violating row is **discarded** and the update continues. Use it to
  cleanse: a negative ``amount`` is a bad record, so drop it before it skews any
  total.
- ``warn`` — a violating row is **kept**, and the violation is logged rather than
  acted on. Use it for monitoring: an order with no ``customer_id`` is still a
  real order, so keep it and flag it.

Rules live in a file so they are a versioned artifact in their own right: diff
them, review them, and reuse the same file across flowgroups that share the shape.
The column names are whatever the source view exposes — gate a typed view and the
rules read as clean typed columns.

Declare the transform
=====================

Insert one ``transform`` action between the load and the write. Give it
``transform_type: data_quality``, point ``source`` at the view to check, name the
``target`` view it produces, point ``expectations_file`` at the rules, and set
``mode: dqe``:

.. literalinclude:: ../../_fixtures/guide_transform_dq/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml
   :emphasize-lines: 16-24

Actions chain by view name: ``load_orders_csv`` produces ``v_orders_raw``,
``gate_orders`` reads it and produces ``v_orders_validated``, and the write reads
that. ``mode: dqe`` — data-quality expectations — attaches the rules directly to
the view as Lakeflow expectation decorators. It is the default, and it is what
this guide covers.

.. note::

   The other mode is ``mode: quarantine``, which routes failing rows to a
   dead-letter table for later inspection and recycling instead of dropping them
   inline. That is a separate subsystem with its own guide — the quarantine guide
   under quality-and-ops. Everything below is ``mode: dqe``.

Generate the pipeline
=====================

Validate the flowgroup, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  0 files
   ✓ validate (0.35s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  1 file
   ✓ generate (0.34s)
   ✓ format (0.04s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Open the generated file. This is the entire output — nothing is hidden behind a
runtime:

.. literalinclude:: ../../_fixtures/guide_transform_dq/generated/dev/bronze_orders/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_orders/orders_ingest.py
   :emphasize-lines: 40-45

The gate became a ``@dp.temporary_view`` with your rules sorted onto three
decorators by their ``action``, one dict per severity:

- the ``fail`` rule went to ``@dp.expect_all_or_fail`` — the update stops if any
  row violates it;
- the ``drop`` rule went to ``@dp.expect_all_or_drop`` — violating rows are
  removed from the view;
- the ``warn`` rule went to plain ``@dp.expect_all`` — violating rows pass
  through, logged but kept.

Each rule became exactly one entry in exactly one decorator, keyed by the ``name``
you gave it. You described the *rules*; Lakehouse Plumber picked the *enforcement*.
Change a rule's ``action`` from ``warn`` to ``drop`` and the same name moves from
``@dp.expect_all`` to ``@dp.expect_all_or_drop`` on the next generate — you never
touch the Python.

What you just did
=================

You wrote one seven-line action pointing at a three-rule expectations file, and
Lakehouse Plumber generated the three Lakeflow expectation decorators — one per
rule, correctly split by action — the enforced view, and the wiring into the
append flow that feeds ``dev_catalog.bronze.orders``. Across the whole flowgroup:
**47 lines of YAML became 74 lines of Lakeflow Python, with zero lines of PySpark
from you.** You never wrote ``@dp.expect_all_or_fail``, ``@dp.expect_all_or_drop``,
or a ``@dp.temporary_view`` by hand.

The rules are yours to own — the SQL expressions and the action on each are
exactly what you typed. Lakehouse Plumber wrote only the boilerplate that turns a
rule into a wired-up, enforcement-tagged Lakeflow view, and it keeps the
fail/drop/warn mapping correct so a one-word edit re-enforces the whole gate.

What's next
===========

- **Keep the bad rows instead of dropping them** — route failures to a
  dead-letter table, fix them, and recycle them back into the pipeline. That is
  ``mode: quarantine``, covered in the quarantine guide under quality-and-ops.
- **Reshape before you gate** — cast and rename raw columns first so the rules
  read as typed columns, covered in the SQL transform and schema transform guides.
- For every field — ``mode``, ``quarantine``, ``readMode``, the old list-format
  expectations file — see the **Transform action reference**.
