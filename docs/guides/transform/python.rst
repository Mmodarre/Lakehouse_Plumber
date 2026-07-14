========================================
Reshape data with a Python transform
========================================

.. meta::
   :description: Reshape rows with a Lakehouse Plumber Python transform when a SQL SELECT runs out — DataFrame code, UDFs, and multi-source joins from a function you own, instead of hand-writing a @dp.temporary_view.

Let's take that same stream of raw order rows and reshape it with logic a SQL
``SELECT`` can't reach: a Python UDF that collapses a dozen free-text channel
spellings — ``web``, ``Website``, ``WWW``, ``online`` — into one controlled
value. One ``transform`` action, written as the DataFrame function you'd write
anyway.

A SQL transform hands Lakehouse Plumber a ``SELECT``; a Python transform hands
it a function — reach for it when the logic outgrows a ``SELECT``: a UDF, a
multi-source DataFrame join, or a call to an external library.

You could hand-write this: a ``@dp.temporary_view`` that reads the source view
with ``spark.readStream.table(...)``, calls your function, and returns the
DataFrame — then remember to copy that module into the pipeline package and fix
up its import so it resolves at run time. Or you declare ``transform_type:
python``, point at the module and the function, and let Lakehouse Plumber write
the wrapper, copy the module, and wire the import. That is the whole idea:
**declare your ETL, don't hand-write it.**

Before you start
================

A Python transform sits in the middle of a flowgroup: it reads a view a
``load`` produced and hands a new view to a ``write``. This guide continues the
orders ingest from **Your first pipeline** — a ``load`` action that streams raw
CSV into ``v_orders_raw``. Any flowgroup with a load and a write has the same
shape.

Declare the transform
=====================

Insert one ``transform`` action between the load and the write. A Python
transform carries no query. Instead ``module_path`` points at a ``.py`` file
(relative to the project root), ``function_name`` names the function to call,
and ``parameters`` is a dict passed straight through to it:

.. literalinclude:: ../../_fixtures/guide_transform_python/pipelines/orders_normalize.yaml
   :language: yaml
   :caption: pipelines/orders_normalize.yaml
   :emphasize-lines: 22-31

Actions chain by view name, exactly as a SQL transform does:
``load_orders_csv`` produces ``v_orders_raw``, ``normalize_orders`` reads it and
produces ``v_orders_normalized``, and the write reads that. ``readMode: stream``
reads the source as a stream, so only new order files are processed each update.
The ``${catalog}``, ``${silver_schema}``, and ``${landing_path}`` tokens resolve
per environment from ``substitutions/dev.yaml``, so the same flowgroup ships
unchanged to prod.

Write the transform function
============================

The action pointed at ``transformations/order_transforms.py`` and
``normalize_orders``. Here is that module — the code you own:

.. literalinclude:: ../../_fixtures/guide_transform_python/transformations/order_transforms.py
   :language: python
   :caption: transformations/order_transforms.py
   :emphasize-lines: 41-53

The signature is fixed by the source shape. With a single source view the
function takes ``(df, spark, parameters)``; with a list of sources it takes
``(dataframes, spark, parameters)``; with no source — a data generator — it
takes ``(spark, parameters)``. The ``parameters`` argument arrives as the dict
from the action's ``parameters:`` block, so ``default_channel`` is read with
``parameters.get(...)``.

This is the logic a ``SELECT`` can't carry: the channel canonicalization is a
Python dict lookup wrapped in a UDF, with a nested helper. Lakehouse Plumber
copies the **whole file**, not just ``normalize_orders`` — the module-level
``_CHANNEL_ALIASES`` table and the inner ``canonical_channel`` helper come
along — so factor logic into helpers and constants freely.

.. note::

   Lakehouse Plumber calls your function; it does not run it at generate time.
   It copies the module into the pipeline and wires the import. Edit the
   original in ``transformations/`` — never the copy under
   ``generated/.../custom_python_functions/``, which carries a DO-NOT-EDIT
   header and is overwritten on every generate.

Generate and read the output
============================

Validate the flowgroup, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ silver_orders  0 files
   ✓ validate (0.36s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.00s)
   ✓ preflight (0.00s)
   ✓ silver_orders  1 file
   ✓ generate (0.34s)
   ✓ format (0.06s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Open the generated file. The transform became a ``@dp.temporary_view`` that
reads the source with ``spark.readStream.table`` — because you set ``readMode:
stream`` — rebuilds your ``parameters`` dict, and calls ``normalize_orders``.
The import at the top is wired to the copied module:

.. literalinclude:: ../../_fixtures/guide_transform_python/generated/dev/silver_orders/orders_normalize.py
   :language: python
   :caption: generated/dev/silver_orders/orders_normalize.py
   :emphasize-lines: 6,35-45

Lakehouse Plumber copied ``transformations/order_transforms.py`` to
``generated/dev/silver_orders/custom_python_functions/order_transforms.py`` and
emitted ``from custom_python_functions.order_transforms import
normalize_orders`` so the wrapper resolves at run time. The
``v_orders_normalized`` view is wired into the same append flow that feeds
``dev_catalog.silver.orders``.

What you just did
=================

You wrote one transform action — ten lines of YAML naming the module, the
function, and one parameter — and a 54-line Python module that is yours: the
alias table, the UDF, and the casts are exactly what you typed. Lakehouse
Plumber generated the 11-line ``@dp.temporary_view`` that reads
``v_orders_raw``, rebuilds your ``parameters`` dict, and calls
``normalize_orders``; copied your module into the pipeline package with a
DO-NOT-EDIT header; emitted the ``from custom_python_functions...`` import; and
wired ``v_orders_normalized`` into the append flow. Across the flowgroup: **41
lines of YAML became 69 lines of wired Lakeflow Python, with zero lines of
``@dp`` glue from you.**

The function is yours to own — Lakehouse Plumber wrote only the boilerplate that
turns it into a wired-up view, plus the file copying and import rewriting a
hand-written ``@dp.temporary_view`` would have left you to do by hand.

What's next
===========

- **Stay in SQL when the logic fits a SELECT** — casts, renames, filters, and
  stream-static joins don't need Python, covered in the SQL transform guide.
- **Gate the rows with a data-quality check** — attach expectations to drop or
  quarantine bad records after the transform, covered in the data-quality guide.
- For the full option list — multiple source views, ``operational_metadata``,
  and the rules for importing local helper modules — see the **Transform action
  reference**.
