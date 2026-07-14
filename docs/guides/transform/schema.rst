==============================
Enforce a schema on a view
==============================

.. meta::
   :description: Lock a view to an explicit column contract with a Lakehouse Plumber schema transform — rename, cast, and drop unmapped columns under strict enforcement, instead of hand-writing casts and a SELECT list.

Let's take a raw orders view — untyped string columns, cryptic names, and a
stray ``src_system`` column no one downstream wants — and lock it to an explicit
contract: exact column names, exact types, exact column list, in a fixed order.

You could hand-write that contract as a SQL transform: a ``SELECT`` with a
``CAST(... AS ...) AS ...`` for every column, re-typed inline and re-derived each
time the raw shape drifts. Or you declare ``transform_type: schema``, keep the
contract in one schema file, and let Lakehouse Plumber write the renames, the
casts, and the select. That is the idea on every page: **declare your ETL, don't
hand-write it.**

Before you start
================

A schema transform sits in the middle of a flowgroup: it reads a view a ``load``
produced and hands a typed view to a ``write``. This guide extends the orders
ingest from **Your first pipeline** — a ``load`` action that streams raw CSV into
``v_orders_raw``. Any flowgroup with a load and a write fits the same shape.

Write the schema contract
=========================

Put the column contract in its own file. Each line is an arrow mapping —
``source_column -> target_name: TYPE`` — so the schema reads as a table of what
goes in and what comes out. Create ``schemas/orders.yaml``:

.. literalinclude:: ../../_fixtures/guide_transform_schema/schemas/orders.yaml
   :language: yaml
   :caption: schemas/orders.yaml

Each entry renames a cryptic raw column to a clear name and casts it to a target
type in one line. The arrow format has three other forms — ``old -> new`` renames
without a cast, ``col: TYPE`` casts without a rename, and a bare ``col`` keeps a
column as-is — but a clean ``source -> target: TYPE`` per column is all this
contract needs. The file is a versioned artifact in its own right: diff it, review
it, and reuse it across flowgroups that share the shape.

Declare the transform
=====================

Insert one ``transform`` action between the load and the write. Give it
``transform_type: schema``, point ``source`` at the view to reshape, point
``schema_file`` at the contract, and set ``enforcement: strict``:

.. literalinclude:: ../../_fixtures/guide_transform_schema/pipelines/orders_typed.yaml
   :language: yaml
   :caption: pipelines/orders_typed.yaml
   :emphasize-lines: 22-28

``enforcement: strict`` is the field that makes this a *contract* rather than a
set of edits: only the columns named in the schema survive, every unlisted column
(the ``src_system`` cruft) is dropped, and the output columns come out in the
file's order. The one-sentence contrast with a ``sql`` transform: there you would
retype the ``CAST(... AS ...)`` list and the surviving-column set inside the query
every time; here the contract lives in one file the transform reads.

.. note::

   Under ``strict`` enforcement the schema columns **must exist** in the source —
   a missing declared column fails the pipeline at run time, so a silent shape
   change surfaces as an error rather than a wrong table. Omit ``enforcement`` (or
   set ``permissive``, the default) to cast and rename the listed columns while
   letting every other column pass through unchanged.

Generate the pipeline
=====================

Validate the flowgroup, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.38s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.38s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Open the generated file. This is the entire output — nothing is hidden behind a
runtime:

.. literalinclude:: ../../_fixtures/guide_transform_schema/generated/dev/silver_orders/orders_typed.py
   :language: python
   :caption: generated/dev/silver_orders/orders_typed.py
   :emphasize-lines: 36-59

The schema transform became a ``@dp.temporary_view`` that turns each contract line
into concrete PySpark: four ``withColumnRenamed`` calls map the cryptic names,
four ``withColumn(...).cast(...)`` calls set the types, and — because enforcement
is ``strict`` — a final ``df.select(...)`` keeps only the four declared columns,
in the declared order. ``src_system`` is never selected, so it never reaches the
silver table. The typed view is then wired into the append flow that feeds
``dev_catalog.silver.orders``.

What you just did
=================

You wrote one action — 7 lines of YAML pointing at a 5-line schema file — and
Lakehouse Plumber generated the 24-line ``@dp.temporary_view``: the four renames,
the four casts, and the select that enforces the column list and order. Across the
whole flowgroup: **30 lines of YAML became 83 lines of Lakeflow Python, with zero
lines of PySpark from you.** You never wrote ``withColumnRenamed``, a ``.cast()``,
or a ``.select()`` by hand.

The contract is the payoff, not just the line count. Names, types, column set,
and order all live in one reviewable file — change the shape there and the
generated view follows, and a source that drifts out of contract fails loudly
instead of leaking the wrong columns downstream.

What's next
===========

- **Reach for a SQL transform** when the reshape needs real logic — a join, a
  filter, a computed column — beyond renaming and casting a fixed column list.
  The SQL transform guide covers that case and the ``stream(...)`` rule.
- **Gate the typed rows with a data-quality check** — attach expectations to drop
  or quarantine bad records after the schema is enforced, covered in the
  data-quality guide.
- For inline schemas (``schema_inline``), the ``permissive`` mode, the full arrow
  syntax, and ``readMode``, see the **Transform action reference**.
