========================================
Parameterize a template for many tables
========================================

.. meta::
   :description: Go past a one-parameter template — declare multiple Lakehouse Plumber template parameters with defaults, override a different one per table, and layer a flowgroup preset over the template's own preset.

The Get Started templates step stamped bronze tables from a template with a
single parameter — the entity name. Real tables rarely differ by name alone.
One lands as CSV and the next as JSON; one is high-volume and wants a bigger
Auto Loader trigger batch; one wants Liquid clustering and the next does not.

You could copy the template's whole load-and-write shape into a fresh flowgroup
for each table and hand-edit the format, the reader options, the clustering. Or
you declare those differences as **parameters** once, give the common cases
**defaults**, and let each table override only what it needs. That is the reuse
thesis stacked on the base one: declare your ETL, don't hand-write it — and
describe a shape once instead of copying it per table.

Let's take one bronze-ingest template and stamp two tables from it — ``orders``
as clustered CSV, ``customers`` as plain JSON — where each flowgroup overrides a
*different* parameter and one layers on an extra preset.

Before you start
================

This guide assumes you have met templates in the Get Started course — a
``templates/`` file with ``{{ parameter }}`` holes and a flowgroup that names it
with ``use_template:``. Here you go deeper: several parameters, defaults,
per-table overrides, and preset composition. You need a project with
``templates/`` and ``presets/`` directories; the Get Started project already has
both.

Declare the parameters
======================

A template's ``parameters`` block declares what varies per table. Create
``templates/bronze_ingest.yaml`` with three of them:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/templates/bronze_ingest.yaml
   :language: yaml
   :caption: templates/bronze_ingest.yaml
   :emphasize-lines: 12-13,15-26

Each parameter is a name plus a few rules. ``entity`` is marked ``required:
true`` — every flowgroup must supply it, and omitting it stops generation with a
clear error before any Python is written. ``file_format`` and ``cluster_columns``
each carry a ``default``, which makes them optional: a flowgroup that says
nothing about them gets ``csv`` and no clustering. A parameter is optional
exactly when it has a default (or you mark it ``required: false``).

You do not declare a parameter's type. Lakehouse Plumber infers the kind from
the value you pass — a list stays a list, a number stays a number,
``true``/``false`` becomes a boolean. That is why ``cluster_columns`` can default
to an empty list ``[]`` and render into a real ``cluster_by`` list downstream,
not the string ``"[]"``.

The template also carries ``presets: [bronze_defaults]``, so every table it
stamps inherits the same Auto Loader reader options and streaming-table
properties. You set the bronze standard once, in the template, for all of them.

.. note::

   Templating fills ``{{ }}`` first; the ``${...}`` tokens are left untouched
   until later. So ``{{ entity }}`` and ``{{ file_format }}`` resolve during
   template expansion, and ``${catalog}`` / ``${bronze_schema}`` /
   ``${landing_path}`` resolve afterwards, per environment — the same template
   ships unchanged to dev, staging, and prod.

Stamp tables that differ
========================

Each table is a flowgroup that names the template and supplies its parameters.
``orders`` overrides ``cluster_columns`` — a native YAML list — and relies on the
``file_format`` default:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml
   :emphasize-lines: 7-13

``customers`` overrides a *different* parameter — ``file_format`` — and relies on
the ``cluster_columns`` default, so it needs no list at all:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/pipelines/customers_ingest.yaml
   :language: yaml
   :caption: pipelines/customers_ingest.yaml
   :emphasize-lines: 8-9

Note the two different keys. The template *declares* parameters under
``parameters``; a flowgroup *supplies* them under ``template_parameters``. Each
flowgroup names only what makes its table different — one word for the entity,
plus the one or two parameters it wants to change from the defaults.

Layer a preset on one table
===========================

Look again at ``orders_ingest.yaml``: alongside ``use_template`` it lists its own
``presets: [high_throughput]``. Template-level presets apply first; flowgroup-level
presets apply second and win on conflicts. So ``high_throughput`` can raise a
value the template's ``bronze_defaults`` already set — for ``orders`` only:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/presets/high_throughput.yaml
   :language: yaml
   :caption: presets/high_throughput.yaml
   :emphasize-lines: 11

``bronze_defaults`` sets ``cloudFiles.maxFilesPerTrigger`` to 200 for every table
the template stamps. ``orders`` layers ``high_throughput`` on top, which sets it
to 1000. ``customers`` layers nothing, so it keeps 200. The template holds the
shared standard; a flowgroup tunes one table without editing the template or
touching the other tables.

Generate and compare the output
===============================

Validate first, then generate the Lakeflow code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze  0 files
   ✓ validate (0.36s)
   1 validated · 0.4s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze  2 files
   ✓ generate (0.40s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 2 files · 0.5s

Two flowgroups in, two complete Lakeflow modules out. Open ``orders`` — it is the
full pipeline, with the layered preset and the clustering baked in, and not a
``{{ }}`` in sight:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/generated/dev/bronze/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze/orders_ingest.py
   :emphasize-lines: 24-25,41

``customers`` came from the same template, yet the module differs everywhere its
parameters did:

.. literalinclude:: ../../_fixtures/guide_reuse_templates/generated/dev/bronze/customers_ingest.py
   :language: python
   :caption: generated/dev/bronze/customers_ingest.py
   :emphasize-lines: 24-25

Read the three lines that moved. ``orders`` has
``cloudFiles.maxFilesPerTrigger`` at 1000 — the layered ``high_throughput``
preset overriding the template's 200 — while ``customers`` keeps 200. ``orders``
reads ``cloudFiles.format`` ``"csv"`` (the default) and ``customers`` reads
``"json"`` (its override). And ``orders`` carries
``cluster_by=["order_id", "order_date"]`` while ``customers``, on the empty-list
default, has no ``cluster_by`` at all. One template produced two genuinely
different modules; the only lines that changed are the ones each flowgroup chose
to change.

What you just did
=================

Two flowgroups — sixteen lines of parameters between them — generated **111 lines
of Lakeflow Python across two modules** from one template. The load-and-write
shape is written once. Defaults keep the common case terse — ``customers`` is six
lines of parameters — while per-flowgroup overrides and a layered preset handle
the exceptions without forking the template.

A third table is another short flowgroup: override the one or two parameters that
make it different, not another fifty-odd lines of Python you would keep in sync by
hand. That is how one template scales across a schema's worth of tables.

What's next
===========

- **Make the actions themselves vary.** When tables need not just different
  values but different *structure* — an optional surrogate-key column, a
  ``GROUP BY`` built from a list parameter — the template body can use Jinja2
  ``{% if %}`` conditionals, ``{% for %}`` loops, and filters. The dynamic
  templates guide covers those patterns.
- **See every field.** The templates reference lists the full parameter and
  template file schema, and how to organize templates in subdirectories
  (``use_template: ingestion/bronze_ingest``).
- **Scale past one pipeline.** A template stamps tables into one pipeline; a
  blueprint stamps whole pipelines — across regions, tenants, or environments —
  from one definition.
