=============================
Parameterize with a template
=============================

.. meta::
   :description: Capture a load-and-write flowgroup shape once as a Lakehouse Plumber template, then stamp out any number of bronze tables from a few parameter lines each.

A preset removed your duplicated *configuration*. But look at the orders and
customers flowgroups from :doc:`the last step <04-reuse-with-presets>`: they
still repeat the same *structure* — a ``cloudfiles`` load into a view, then a
write to a streaming table. Add a ``products`` table and you copy that whole
shape a third time.

Let's capture the shape **once** as a **template** and stamp each table from a
few parameters. You describe the pattern one time; Lakehouse Plumber stamps out
as many tables as you have names for.

Capture the shape once
======================

A **template** is a flowgroup with holes in it — ``{{ parameter }}`` placeholders
filled in per table. Create ``templates/csv_ingest.yaml``:

.. literalinclude:: ../_fixtures/templates_ingest/templates/csv_ingest.yaml
   :language: yaml
   :caption: templates/csv_ingest.yaml
   :emphasize-lines: 15-16,18-21

Two things to notice. The ``parameters`` block declares what varies — here, a
single ``entity`` — and the actions use ``{{ entity }}`` wherever a table name
would go: the landing folder, the view, the target table. The template also
lists ``presets: [bronze_defaults]``, so every table it stamps still inherits the
bronze standard from the last step. You set it once, in the template, for all of
them.

The ``${...}`` tokens are left alone by templating. Templating fills
``{{ entity }}`` first; the ``${catalog}`` / ``${bronze_schema}`` /
``${landing_path}`` tokens resolve later, per environment, exactly as before.

Stamp out the tables
====================

Now each table is a flowgroup that names the template and supplies its
parameters — nothing else:

.. literalinclude:: ../_fixtures/templates_ingest/pipelines/orders_ingest.yaml
   :language: yaml
   :caption: pipelines/orders_ingest.yaml
   :emphasize-lines: 5-7

``customers`` is the same, one word different:

.. literalinclude:: ../_fixtures/templates_ingest/pipelines/customers_ingest.yaml
   :language: yaml
   :caption: pipelines/customers_ingest.yaml
   :emphasize-lines: 4-6

Note the two different keys: the template *declares* parameters under
``parameters``; a flowgroup *supplies* them under ``template_parameters``.

Generate every table
====================

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_ingest  3 files
   ✓ generate (0.36s)
   ✓ format (0.04s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 3 files · 0.4s

Three flowgroups in, three complete Lakeflow modules out. Open the orders one —
it's the full pipeline, with the preset's reader options and table properties
baked in, and not a ``{{ }}`` in sight:

.. literalinclude:: ../_fixtures/templates_ingest/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py
   :emphasize-lines: 22-23,39

The ``customers`` and ``products`` modules are identical in structure — same
options, same properties — with their own names throughout.

What you just did
=================

One 41-line template and three flowgroups of six or seven lines each — nineteen
lines of parameters in total — generated **162 lines of Lakeflow across three
tables**. The load-and-write shape is written once. A fourth table is one more
short flowgroup, and the only line that changes is ``entity:``.

That's the reuse ladder's second rung: a preset removes duplicated
configuration; a template removes duplicated structure. Both still describe
*one* pipeline at a time, though. Next you'll generate *many* pipelines from a
single definition.

What's next
===========

- **Scale with blueprints** — a template stamps tables into one pipeline; a
  blueprint stamps whole pipelines — across regions, tenants, or environments —
  from one definition. That's ETL at scale, made literal.
