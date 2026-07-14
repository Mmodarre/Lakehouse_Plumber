============================================
Fan one blueprint across tenants and regions
============================================

.. meta::
   :description: Drive many Lakehouse Plumber pipelines from one multi-parameter blueprint — bind a tenant, a region, and optional config per instance, and let a single definition expand into a pipeline per tenant.

The Get Started course introduces a blueprint with a single parameter: one
``region`` fanned across three region files. Real platforms vary on more than
one axis at once — a **tenant** and the **region** it runs in, a schema and the
partitioning that schema needs. Bake those into copies of a pipeline and you
maintain the same plumbing in triplicate: every schema tweak is a find-and-replace
across tenants, and the region frozen into each copy is one more thing that drifts.

Let's write the per-tenant bronze layer **once** as a blueprint that takes
several parameters, then bind a tenant, its region, and its partition column per
instance. One definition expands into a pipeline per tenant. This guide assumes
the single-parameter intro from the Get Started course and goes deeper: multiple
parameters per instance, required versus optional-with-default, and how specs
times instances expand.

Before you start
================

You need a Lakehouse Plumber project and the blueprint basics from the Get
Started course — what a blueprint is, and that instance files bind its
parameters. This guide reuses the course's shape (CSV ``cloudfiles`` loads into
bronze streaming tables) and adds a second identity parameter and an optional
config parameter on top.

Parameterize the pattern
========================

A blueprint declares its **parameters** once, then uses ``%{parameter}``
placeholders wherever a value must vary per instance. Create
``blueprints/tenant_bronze.yaml``:

.. literalinclude:: ../../_fixtures/guide_reuse_blueprints/blueprints/tenant_bronze.yaml
   :language: yaml
   :caption: blueprints/tenant_bronze.yaml
   :emphasize-lines: 12-24,46-48

Three parameters, of two kinds. ``tenant`` and ``region`` are ``required: true``
— every instance must bind both, or expansion fails. ``partition_col`` is
``required: false`` with a ``default:`` of ``ingest_date`` — an instance may
supply it or leave it out. The two flowgroup specs, orders and customers, then
reference the parameters:

- ``%{tenant}`` drives the pipeline name (``%{tenant}_bronze``), both flowgroup
  names, and the leading segment of every table name.
- ``%{region}`` drives a path segment and is stamped into every table name, so
  ``acme`` in ``us`` and ``acme`` in ``eu`` land in distinct tables.
- ``%{partition_col}`` reaches a *config* value — ``partition_columns`` on the
  write target — not just a name. Parameters bind data-shaping config, not only
  identifiers.

One syntax rule carries the whole design, and it goes deeper than the course's
single-parameter case. Identity fields — ``pipeline:`` and ``flowgroup:`` —
accept **only** the ``%{...}`` local-variable syntax, resolved as the blueprint
expands. The ``${...}`` environment tokens (``${catalog}``, ``${bronze_schema}``,
``${landing_path}``) are the opposite: they are allowed everywhere **except**
those two fields, because they resolve later, per environment. So ``%{tenant}``
and ``%{region}`` name the pipelines and tables, while ``${catalog}`` and
``${landing_path}`` fill in the environment. Put a ``${...}`` token in a
``pipeline:`` or ``flowgroup:`` string and expansion rejects it with
``LHP-VAL-044`` — those names are the index the expander builds before
environment substitution runs, so they must be stable across environments.

Bind each tenant
================

Each tenant is an **instance** — a small file that names the blueprint with
``use_blueprint:`` and binds the parameters under ``parameters:``. Create
``pipelines/tenants/acme.yaml``:

.. literalinclude:: ../../_fixtures/guide_reuse_blueprints/pipelines/tenants/acme.yaml
   :language: yaml
   :caption: pipelines/tenants/acme.yaml

``acme`` binds all three parameters, overriding ``partition_col`` to
``order_date``. The other two tenants are the same file with different values —
and ``globex`` omits ``partition_col`` entirely:

.. literalinclude:: ../../_fixtures/guide_reuse_blueprints/pipelines/tenants/globex.yaml
   :language: yaml
   :caption: pipelines/tenants/globex.yaml

Because ``globex`` supplies no ``partition_col``, the blueprint default
``ingest_date`` applies. That is the effective-parameter rule: blueprint
defaults first, then the instance's ``parameters`` override them. The third
tenant, ``initech`` in ``apac``, is four lines with the same shape.

The instance format is strict, and the strictness is the payoff — it turns a
typo into a build error instead of a silently missing pipeline:

- **A required parameter is enforced.** Drop ``tenant`` or ``region`` from an
  instance and validation fails with ``LHP-VAL-042``, naming the missing
  parameter.
- **An unknown parameter is rejected.** Write ``tennant:`` and you get
  ``LHP-VAL-043`` with a "did you mean" suggestion against the declared names.
- **An unknown blueprint is rejected.** A ``use_blueprint:`` that names no
  blueprint fails with ``LHP-VAL-041``, again with a suggestion.

Instance files live under the ``instance_include`` glob, which defaults to
``pipelines/**/*.yaml`` — the same tree as hand-written flowgroups. That overlap
is intentional: Lakehouse Plumber routes by content, treating a file with a
``use_blueprint:`` key as an instance and everything else as a flowgroup, so
instances and ordinary flowgroups coexist in ``pipelines/``. Blueprint
definitions default to ``blueprints/**/*.yaml``. A flowgroup spec accepts more
than the fields shown here — ``presets``, ``use_template``, ``variables``,
``operational_metadata`` — and the blueprint reference catalogs every field and
error code in full.

Expand into pipelines
=====================

Validate first, then generate. Validation resolves every parameter, checks the
required ones, and expands the blueprint before it commits to writing code:

.. code-block:: console

   $ lhp validate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ acme_bronze  0 files
   ✓ globex_bronze  0 files
   ✓ initech_bronze  0 files
   ✓ validate (0.55s)
   3 validated · 0.6s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ acme_bronze  2 files
   ✓ globex_bronze  2 files
   ✓ initech_bronze  2 files
   ✓ generate (0.52s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   3 pipelines generated · 6 files · 0.6s

Two specs across three instances expanded into six flowgroups. That is the whole
arithmetic of a blueprint: **N specs times M instances equals N times M
flowgroups**, here 2 times 3 equals 6, grouped into one pipeline per tenant.
``lhp list blueprints --instances`` shows the expansion explicitly:

.. code-block:: console

   $ lhp list blueprints --instances
   Blueprints
   Name           Version  Params  Specs  Instances
   tenant_bronze  1.0      3       2      3
   Total blueprints: 1
   Instances
     tenant_bronze (blueprints/tenant_bronze.yaml):
       - acme.yaml:    2 flowgroup(s) -> pipeline(s) ['acme_bronze']
       - globex.yaml:  2 flowgroup(s) -> pipeline(s) ['globex_bronze']
       - initech.yaml: 2 flowgroup(s) -> pipeline(s) ['initech_bronze']

Each instance produced two flowgroups and its own named pipeline. ``--instances``
is the current form of the command; the older ``lhp list-blueprints`` no longer
exists.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/acme_bronze/acme_orders_ingest.py``. It is an ordinary,
complete pipeline module — the tenant and region baked into the names, and not a
placeholder left:

.. literalinclude:: ../../_fixtures/guide_reuse_blueprints/generated/dev/acme_bronze/acme_orders_ingest.py
   :language: python
   :caption: generated/dev/acme_bronze/acme_orders_ingest.py
   :emphasize-lines: 34-38

Every parameter came through. ``%{tenant}`` and ``%{region}`` resolved into the
table name ``dev_catalog.bronze.acme_us_orders`` and the landing path
``/Volumes/dev_catalog/landing/files/acme/us/orders/*.csv``; the ``${...}``
tokens resolved from ``substitutions/dev.yaml``. And ``partition_col`` reached
the config: ``partition_cols=["order_date"]``, because ``acme`` overrode the
default. The other five modules are the same shape, one per tenant and table —
and ``globex`` and ``initech``, which omitted ``partition_col``, carry
``partition_cols=["ingest_date"]`` from the blueprint default.

.. note::

   An instance file and a hand-written flowgroup share the ``pipelines/`` tree.
   Lakehouse Plumber tells them apart by content: a file with a
   ``use_blueprint:`` key is expanded as an instance, and every other file under
   the flowgroup glob is read as an ordinary flowgroup. You do not separate them
   by directory.

What you just did
=================

One 71-line blueprint and three instance files — 16 lines in total — expanded
into **6 flowgroups across 3 pipelines, 312 lines of Lakeflow Python** in six
modules. The pattern now scales on two axes at once. A new tenant is one
four-line instance and inherits the whole bronze layer, region and partitioning
included. A new table for *every* tenant is one more spec in the blueprint, and
all three tenants get it in the same generate run — 2 specs became 3 the moment
you add one, and 6 modules become 9.

That is the deep end of reuse: a preset removes duplicated configuration, a
template removes duplicated structure, and a multi-parameter blueprint removes
duplicated *pipelines* across every axis your platform repeats on. You describe
the pattern once; Lakehouse Plumber writes it everywhere.

What's next
===========

- **See every blueprint field.** The blueprint reference catalogs the full
  parameter schema, every flowgroup spec field (``variables``, ``presets``,
  ``use_template``, ``operational_metadata``), the parameter-resolution phases,
  and the complete error-code list.
- **Trace the fan-out as a graph.** ``lhp dag`` fully expands blueprint
  instances — one node per instance — so the dependency graph and job
  orchestration reflect every generated pipeline, not just one.
- **Deploy with bundles.** You have generated pipelines on disk. The bundle step
  packages them for a Databricks workspace, tenants and all.
