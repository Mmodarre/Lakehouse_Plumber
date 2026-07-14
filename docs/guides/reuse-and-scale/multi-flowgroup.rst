===========================================
Compose a pipeline from many flowgroups
===========================================

.. meta::
   :description: Build one Lakehouse Plumber pipeline from several flowgroups — share a pipeline name, pack related ingests into one file with inheritance, and feed a silver flowgroup from the bronze tables an earlier flowgroup wrote.

A real pipeline is rarely one flowgroup. You ingest orders, you ingest
customers, then you build a silver fact on top of both. That is three units of
work, and cramming them into one giant flowgroup buries the boundaries between
them.

You could hand-write one sprawling Lakeflow module and hope the reader keeps the
layers straight. Or you write three focused flowgroups, give them the same
``pipeline`` name, and let Lakehouse Plumber generate one module per flowgroup
into a single pipeline. That is the idea on every page: **declare your ETL,
don't hand-write it** — and let the tool handle composition too.

Let's build a ``sales`` pipeline from three flowgroups: two bronze ingests that
share one file, and a silver flowgroup that reads the bronze tables they write.

Before you start
================

You need Lakehouse Plumber installed and a project to work in. If you followed
the Get Started course you already have one; otherwise scaffold an empty project
first. This guide assumes CSV files land in a volume for the bronze ingests to
read.

Give every flowgroup the same pipeline name
===========================================

A **flowgroup** is a YAML file describing a sequence of actions. A **pipeline**
is what you deploy. The bridge between them is one field: ``pipeline``. Every
flowgroup that names the same ``pipeline`` value composes into that one
pipeline — Lakehouse Plumber generates one Python module per flowgroup and
writes them all into ``generated/<env>/<pipeline>/``.

That is the whole composition rule. Flowgroups can live in separate files or be
packed together; what puts them in the same pipeline is the shared ``pipeline``
name, nothing else.

Pack related ingests into one file
==================================

When several flowgroups share configuration, you don't need a file each. The
``flowgroups:`` array syntax defines a list of flowgroups in one file, and any
field set at the document level is **inherited** by every flowgroup in the list.

Create ``pipelines/bronze_ingests.yaml`` with both source ingests:

.. literalinclude:: ../../_fixtures/guide_reuse_multiflowgroup/pipelines/bronze_ingests.yaml
   :language: yaml
   :caption: pipelines/bronze_ingests.yaml

``pipeline: sales`` is declared once, at the top, and both ``orders_ingest`` and
``customers_ingest`` inherit it — so you write the pipeline name one time
instead of repeating it per flowgroup. Five fields inherit this way:
``pipeline``, ``use_template``, ``presets``, ``operational_metadata``, and
``job_name``. A field set on an individual flowgroup overrides the inherited
value, and an explicit empty list (``presets: []``) opts out of inheritance
entirely.

.. note::

   If your flowgroups share no configuration, use YAML's multi-document syntax
   instead: separate complete flowgroups with ``---`` in one file. You cannot
   mix ``---`` documents and a ``flowgroups:`` array in the same file — Lakehouse
   Plumber rejects that with a "Mixed syntax detected" error.

Feed the next layer from the tables you wrote
=============================================

The silver flowgroup reads what the bronze ingests produced. Create
``pipelines/silver_orders.yaml`` as a third flowgroup in the same ``sales``
pipeline:

.. literalinclude:: ../../_fixtures/guide_reuse_multiflowgroup/pipelines/silver_orders.yaml
   :language: yaml
   :caption: pipelines/silver_orders.yaml

Two ``load`` actions read the bronze ``orders`` and ``customers`` **tables** the
ingests wrote, and a SQL transform joins them into an enriched fact. This works
because of one rule worth internalising:

- **Tables are global.** A streaming table or materialized view written by one
  flowgroup is a real Unity Catalog object, visible to any other flowgroup —
  in the same pipeline or a different one — that names it. That is how the
  silver flowgroup reaches the bronze ``orders`` table.
- **Views are pipeline-scoped.** A ``target`` view (a ``v_...`` name) produced
  by an action is visible only to flowgroups in the **same** pipeline, never
  across pipelines.

Because the silver flowgroup depends on the bronze tables, keep both in one
pipeline so Lakeflow orders the work correctly at run time. The
``stream(...)`` / plain-name split in the join is a stream-static read; the SQL
transform guide covers it in full — this page stays on how the flowgroups
connect.

Generate the pipeline
=====================

Validate every flowgroup, then generate. Both commands operate on the whole
project at once:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ sales  0 files
   ✓ validate (0.53s)
   1 validated · 0.5s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ sales  3 files
   ✓ generate (0.43s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 3 files · 0.5s

One pipeline, ``sales``, three files. All three flowgroups — across two YAML
files — generated into ``generated/dev/sales/``: ``orders_ingest.py``,
``customers_ingest.py``, and ``orders_silver.py``.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/sales/orders_silver.py``. This is the *entire* silver
module — and it shows the cross-flowgroup link resolved to concrete table names:

.. literalinclude:: ../../_fixtures/guide_reuse_multiflowgroup/generated/dev/sales/orders_silver.py
   :language: python
   :caption: generated/dev/sales/orders_silver.py

The two ``load`` actions became ``spark.readStream.table("dev_catalog.bronze.orders")``
and ``spark.read.table("dev_catalog.bronze.customers")`` — reading exactly the
tables ``orders_ingest`` and ``customers_ingest`` create in the other two
modules. Lakehouse Plumber never invents a runtime registry: the flowgroups are
wired together by the plain table names you declared.

What you just did
=================

Three flowgroups, one ``pipeline`` name. One ``lhp generate`` turned 104 lines
of YAML across two files into 174 lines of Lakeflow Python across three modules
in a single pipeline directory — **zero lines of PySpark from you**. You never
wrote ``create_streaming_table``, ``append_flow``, or a ``readStream.table``
call to join the layers.

The composition scales the same way. A fourth ingest is a fourth entry in the
array; a gold flowgroup reading the silver ``orders_fct`` table is a fourth
file. Each is a few lines of YAML — not another module of hand-written
plumbing.

What's next
===========

- **Cut repetition with presets.** When flowgroups repeat the same load or
  write settings, factor them into a preset and attach it once, covered in the
  presets guide.
- **Fan one pattern across many pipelines.** When you need the *same* pipeline
  per region or tenant, a blueprint expands one definition into many pipelines,
  covered in the blueprints guide.
- **Trace the wiring.** ``lhp dag`` renders the dependency graph across
  flowgroups so you can see the bronze-to-silver edges before you deploy.
