=======================
Scale with blueprints
=======================

.. meta::
   :description: Generate many Lakehouse Plumber pipelines from one definition — fan a single bronze-ingest blueprint across regions, tenants, or environments.

A template stamps many tables into *one* pipeline. But real platforms repeat at a
larger grain: the same bronze layer for every region, the same ingest for every
tenant, the same pipeline in every environment. Copy a whole pipeline per region
and the drift you fought with presets comes back — at ten times the scale.

Let's write the pattern **once** as a **blueprint** and let a single definition
fan out into a pipeline per region. This is the thesis made literal: *ETL at
scale.*

One pattern, many regions
=========================

A **blueprint** is a parameterized set of flowgroups. You write the flowgroups
once, put ``%{parameter}`` placeholders in their names, and declare the parameters
the blueprint takes. Create ``blueprints/regional_bronze.yaml``:

.. literalinclude:: ../_fixtures/blueprints_scale/blueprints/regional_bronze.yaml
   :language: yaml
   :caption: blueprints/regional_bronze.yaml
   :emphasize-lines: 12-15,19-20

Two flowgroup specs — orders and customers — and one parameter, ``region``. The
``%{region}`` placeholder appears wherever the region must surface: the pipeline
name, the flowgroup names, the table names, and the landing path.

One syntax detail matters here. Identity fields like ``pipeline:`` and
``flowgroup:`` accept only the ``%{...}`` local-variable syntax, resolved as the
blueprint expands. The ``${...}`` environment tokens — ``${catalog}``,
``${landing_path}`` — aren't allowed in those names; they resolve later, per
environment, so they live only in the paths and table targets.

Bind it per region
==================

Each region is an **instance** — a tiny file that names the blueprint and binds
the parameter:

.. literalinclude:: ../_fixtures/blueprints_scale/pipelines/regions/us.yaml
   :language: yaml
   :caption: pipelines/regions/us.yaml
   :emphasize-lines: 2-4

``eu`` and ``apac`` are the same file with a different value. Three instances,
four lines each — that's the entire cost of a new region.

Generate every pipeline
=======================

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.02s)
   ✓ preflight (0.00s)
   ✓ apac_bronze  2 files
   ✓ eu_bronze  2 files
   ✓ us_bronze  2 files
   ✓ generate (0.50s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   3 pipelines generated · 6 files · 0.6s

One blueprint expanded into three pipelines. ``lhp list blueprints --instances``
shows the expansion explicitly:

.. code-block:: console

   $ lhp list blueprints --instances
   ...
   Instances
     regional_bronze
       - apac.yaml: 2 flowgroup(s) -> pipeline(s) ['apac_bronze']
       - eu.yaml:   2 flowgroup(s) -> pipeline(s) ['eu_bronze']
       - us.yaml:   2 flowgroup(s) -> pipeline(s) ['us_bronze']

Open the US orders module. It's an ordinary, complete pipeline file — the region
baked into the names, and not a placeholder left:

.. literalinclude:: ../_fixtures/blueprints_scale/generated/dev/us_bronze/us_orders_ingest.py
   :language: python
   :caption: generated/dev/us_bronze/us_orders_ingest.py

The other five modules are the same shape, one per region and table.

What you just did
=================

One 58-line blueprint and three four-line instances — **6 flowgroups across 3
pipelines, 300 lines of Lakeflow**. A new region is four lines and inherits the
whole bronze layer. A new table in every region is one more spec in the
blueprint, and all three regions get it at once.

That's the top of the reuse ladder: a preset removes duplicated configuration, a
template removes duplicated structure, and a blueprint removes duplicated
*pipelines*. You describe the pattern once; Lakehouse Plumber writes it
everywhere.

What's next
===========

- **Deploy with bundles** — you have generated pipelines on disk. The last step
  packages them as a Databricks bundle and ships them to a workspace.
