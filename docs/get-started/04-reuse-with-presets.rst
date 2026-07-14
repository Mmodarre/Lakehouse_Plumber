=====================
Reuse with a preset
=====================

.. meta::
   :description: Stop repeating ingestion config across flowgroups — factor the shared bronze standard into a Lakehouse Plumber preset and apply it to every table with one line.

Your :doc:`orders pipeline <03-transform-and-quality>` works. Now the business
wants a ``customers`` table too — ingested the same way, into the same bronze
layer, with the same Auto Loader options and the same change-data-feed setting.
Then a third table, and a fourth.

You could copy the orders flowgroup and change three fields. But copy-paste is
how a data platform drifts: one table ends up with a different rescued-data
column, another forgets the quality tag, and nobody notices until it matters.
Let's factor the shared configuration into a **preset** so every table inherits
the same bronze standard — and adding a table stays a three-field job.

Name the repetition
===================

Both tables ingest CSV with Auto Loader and write a streaming table. What's
genuinely *shared* isn't the paths or the table names — it's the **bronze
standard**: the reader options, change data feed on, a ``quality`` tag. That's
what you don't want to retype, and keep in sync, across forty tables.

Declare the standard once
=========================

A **preset** is a named bundle of defaults. Create ``presets/bronze_defaults.yaml``:

.. literalinclude:: ../_fixtures/reuse_presets/presets/bronze_defaults.yaml
   :language: yaml
   :caption: presets/bronze_defaults.yaml

The ``defaults`` are keyed by action type. ``load_actions.cloudfiles`` merges
into any load whose ``source.type`` is ``cloudfiles``, and
``write_actions.streaming_table`` merges into any streaming-table write. This is
**implicit type matching** — you never wire a preset to a named action; it
applies wherever the types line up. A preset is for the configuration you want
identical across tables, so each flowgroup declares only what is distinct to it
and inherits the shared standard.

Apply it to each table
======================

Now each flowgroup opts in with a single ``presets:`` line and declares only what
is specific to that table — its source path, its view, its table name:

.. literalinclude:: ../_fixtures/reuse_presets/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml
   :emphasize-lines: 6-7

The ``customers`` flowgroup is the same shape, pointed at a different path:

.. literalinclude:: ../_fixtures/reuse_presets/pipelines/customers_ingest.yaml
   :language: yaml
   :caption: pipelines/customers_ingest.yaml
   :emphasize-lines: 6-7

Neither flowgroup mentions reader options, change data feed, or the quality tag.
Those live in the preset, once.

Generate both tables
====================

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  2 files
   ✓ generate (0.34s)
   ✓ format (0.06s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 2 files · 0.4s

Open ``generated/dev/bronze_ingest/orders_ingest.py`` and find the configuration
you never wrote in the flowgroup — the reader options and the table properties
came from the preset:

.. literalinclude:: ../_fixtures/reuse_presets/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py
   :emphasize-lines: 22-23,39

The ``customers`` file is identical in structure, with the same preset-supplied
options and properties. One preset, applied to both tables.

What you just did
=================

Two tables, 108 lines of Lakeflow between them — and the bronze standard written
**once**, in an 18-line preset, instead of copied into every flowgroup. Adding a
third table is a new flowgroup with three distinctive fields and one ``presets:``
line; it inherits the standard automatically, and it can't drift from it.

That's the first rung of Lakehouse Plumber's reuse ladder: a preset removes
*duplicated configuration*. Next you'll remove *duplicated structure*.

What's next
===========

- **Parameterize with a template** — the orders and customers flowgroups still
  repeat the same action *shape*. A template captures that shape once and stamps
  out tables from a few parameters.
