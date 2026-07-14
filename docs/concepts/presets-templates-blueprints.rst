======================
Choosing a reuse tool
======================

.. meta::
   :description: Which Lakehouse Plumber reuse tool to reach for — preset, template, or blueprint — and why. A decision framework that escalates by grain, from duplicated config to duplicated pipelines.

Copy-paste is how a data platform drifts. Duplicate a flowgroup and edit three
fields, and sooner or later one table ends up with a different rescued-data
column, another forgets the quality tag, and nobody notices until it matters.

Lakehouse Plumber gives you three tools so you never have to. They all serve one
idea — the reuse thesis stacked on the base one, **declare your ETL, don't
hand-write it**: describe the pattern once, and let Lakehouse Plumber repeat it.
The question is never *whether* to factor out repetition. It's *at what grain*.

The reuse ladder
================

Presets, templates, and blueprints are the same move at three sizes. Each
removes a strictly bigger unit of duplication than the one below it — config,
then structure, then whole pipelines. That is the whole mental model: **escalate
by grain**, and reach for the smallest rung that covers the repetition in front
of you.

**A preset removes duplicated configuration.** It is a named bundle of default
values — reader options, table properties, tags, Spark config — keyed by action
type and sub-type. Name a preset from a flowgroup and Lakehouse Plumber
deep-merges those defaults into every matching action. A preset changes
*settings*, never *shape*: the
flowgroup keeps its own actions and its own structure, and stops retyping
the values they share. Reach for a preset when the same options repeat
across actions of one kind but the flowgroups themselves are all different.

**A template removes duplicated structure.** It is a parameterized flowgroup
shape — a sequence of load, transform, and write actions with ``{{ parameter }}``
holes where tables differ. A flowgroup names it with ``use_template:`` and fills
the holes, and Lakehouse Plumber stamps the whole shape into that flowgroup.
Stamp it once per table and twenty near-identical tables collapse to twenty
short parameter blocks against one shared skeleton — in a single pipeline. Reach
for a template when the *shape* repeats and only leaf values (the table name, the
format, a cluster key) change from one table to the next.

**A blueprint removes duplicated pipelines.** It is a parameterized *set* of
flowgroups, carrying ``%{parameter}`` placeholders in the names that identify
each pipeline and table. An instance file names it with ``use_blueprint:`` and
supplies one set of parameter values; one blueprint and *N* instances expand
into *N* pipelines' worth of flowgroups. Reach for a blueprint when an entire
pipeline repeats across an outer axis — a region, a tenant, a source system, an
environment — and copying the whole thing per axis is what you're trying to
avoid.

Reach for the smallest grain that repeats
=========================================

The rule is one line: **factor by the smallest axis that repeats.** Match the
tool to what is actually duplicated, and no larger.

.. list-table::
   :header-rows: 1
   :widths: 18 44 22 16

   * - Reach for
     - When the thing that repeats is…
     - Grain it removes
     - Named with
   * - **Preset**
     - Configuration — reader options, table properties, tags — shared across
       actions of one type, while the flowgroups differ.
     - One action's config
     - ``presets:``
   * - **Template**
     - The structure — a load/transform/write skeleton — repeated across many
       tables that differ only in a few leaf values.
     - One flowgroup's shape
     - ``use_template:``
   * - **Blueprint**
     - Whole flowgroups — a complete pipeline — repeated across regions,
       tenants, source systems, or environments.
     - A set of flowgroups
     - ``use_blueprint:``

Read it as an escalator. Only the settings repeat, but every flowgroup is
one-of-a-kind: a preset. The settings *and* the shape repeat, table after table
inside one pipeline: a template. The shape repeats and so does the pipeline
around it, site after site: a blueprint. Picking the smallest rung that fits
keeps the abstraction honest — a blueprint used to dodge two repeated config
keys is a whole pipeline generator standing in for a preset.

The rungs compose
=================

The tools nest, because each rung's grain contains the one below it. A template
can apply presets, so the shape you stamp carries the shared defaults with it. A
blueprint's flowgroups can each declare ``use_template:``, so a pipeline you fan
across regions is itself built from stamped, preset-backed shapes. You are not
choosing one tool *instead* of the others — you are choosing how deep the
repetition goes, and stacking the rungs to match.

.. note::

   Escalating by grain is the reverse of premature abstraction. Start at the
   rung the current duplication demands. When a second axis of repetition
   appears — the same template now wanted in every region — climb one rung and
   wrap what you have, rather than rebuilding it.

Where to go next
================

This page is about *which* tool and *why*. The mechanics live elsewhere:

- The Get Started course teaches how to use each, in order: the **Reuse with a
  preset**, **Templates**, and **Scale with blueprints** steps build the same
  bronze ingest up the ladder.
- The reuse-and-scale guides go deeper — **Parameterize a template for many
  tables** and the **blueprints** guide cover multiple parameters, defaults,
  per-instance overrides, and preset composition.
- The configuration reference carries the exhaustive schema for each tool: every
  field, the preset deep-merge rules, and blueprint parameter validation.
