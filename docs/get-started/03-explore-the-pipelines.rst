========================
Explore the pipelines
========================

.. meta::
   :description: Read the sample project's flowgroups — cloudfiles and delta ingestion, a four-transform silver chain, CDC and snapshot-CDC dimensions, and a gold materialized view — to see the action model in practice.

The sample project is three pipelines built from eight flowgroups. This page
reads the interesting ones so you can see how **actions** — the load / transform
/ write / test steps — compose into real pipelines. You don't run anything here;
you learn to read the YAML.

Every flowgroup follows the same shape: a ``pipeline`` and ``flowgroup`` name,
then a list of ``actions`` (or a ``use_template`` that expands into them). Each
action has a ``type`` and names a ``target`` view or table the next action reads.

Ingest: cloudfiles and delta
============================

``01_ingest/`` reads raw data into bronze streaming tables two ways.

The **cloudfiles** (Auto Loader) ingests are driven by a *template* — one
pattern instantiated twice, for JSON orders and CSV lineitem:

.. literalinclude:: ../_fixtures/sample_project/pipelines/01_ingest/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/01_ingest/orders_ingest.yaml

Eight lines. ``use_template`` pulls in the ingestion pattern; the
``template_parameters`` fill its ``{{ ... }}`` blanks. The
``operational_metadata`` list opts this flowgroup into the columns defined in
``lhp.yaml``. :doc:`Templates </concepts/presets-templates-blueprints>` are how
one pattern serves many tables.

The **delta** ingest reads the read-only ``samples.tpch`` catalog directly — no
copy step, a cross-catalog load in a single flowgroup:

.. literalinclude:: ../_fixtures/sample_project/pipelines/01_ingest/nation_region.yaml
   :language: yaml
   :caption: pipelines/01_ingest/nation_region.yaml
   :lines: 4-20

Note the ``presets: [bronze_layer]`` line — a **preset** deep-merges shared
config (here, enabling Change Data Feed and a ``quality`` tag) into every
matching write. :doc:`Presets, templates, and blueprints </concepts/presets-templates-blueprints>`
are the three reuse tools, each removing a different kind of duplication.

Transform: four types in one chain
==================================

``02_silver/orders_clean.yaml`` is the richest flowgroup — it chains **all four
transform types** between a load and a write:

.. literalinclude:: ../_fixtures/sample_project/pipelines/02_silver/orders_clean.yaml
   :language: yaml
   :caption: pipelines/02_silver/orders_clean.yaml
   :lines: 15-40

Read the chain top to bottom:

- a **sql** transform filters bad rows,
- a **schema** transform renames and casts columns (``o_totalprice`` →
  ``total_price``),
- a **data_quality** transform applies expectations from a file, and
- a **python** transform localizes every timestamp column at runtime.

Each action's ``target`` is the next action's ``source``. The ``%{entity}``
tokens are **local variables** — a within-flowgroup substitution that names every
view consistently. Because the silver pipeline is separate from ingest, the load
reads the fully qualified bronze table (Lakeflow views are pipeline-scoped, so a
bare view name from another pipeline wouldn't resolve).

Write: CDC, snapshot CDC, and a materialized view
=================================================

The silver dimensions show the two change-data-capture write modes.
``dim_customer`` reads a Change Data Feed and applies it as **SCD2** history:

.. literalinclude:: ../_fixtures/sample_project/pipelines/02_silver/dim_customer.yaml
   :language: yaml
   :caption: pipelines/02_silver/dim_customer.yaml
   :lines: 30-44

``dim_supplier`` applies periodic full **snapshots** instead, via a source
function — a write action with no upstream source:

.. literalinclude:: ../_fixtures/sample_project/pipelines/02_silver/dim_supplier.yaml
   :language: yaml
   :caption: pipelines/02_silver/dim_supplier.yaml
   :lines: 10-28

And ``03_gold/sales_by_nation.yaml`` writes a **materialized view** from an
external SQL file:

.. literalinclude:: ../_fixtures/sample_project/pipelines/03_gold/sales_by_nation.yaml
   :language: yaml
   :caption: pipelines/03_gold/sales_by_nation.yaml
   :lines: 4-14

Four write modes across three flowgroups — streaming table, CDC, snapshot CDC,
materialized view. :doc:`The Write guides </guides/write/index>` explain when to
reach for each.

What you just read
==================

Eight flowgroups, every action kind, four write modes, two ingestion sources,
and all three reuse tools — and not one line of PySpark. The YAML says *what*
each pipeline is; Lakehouse Plumber writes the *how*. Next, watch it do exactly
that.

Next
====

Turn this YAML into Lakeflow Python — :doc:`04-generate-and-inspect`.
