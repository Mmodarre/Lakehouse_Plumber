====================
Develop in a sandbox
====================

.. meta::
   :description: Generate a personal, namespaced copy of your Lakehouse Plumber pipelines with lhp generate --sandbox, so you can iterate on a shared dev environment without colliding on teammates' tables.

You share a dev catalog with the rest of your team. You edit a flowgroup, run
it, and it writes ``dev_catalog.bronze.orders`` — the same table a teammate is
mid-run against. Now your two pipelines are fighting over one Delta table, and
whoever ran last wins.

You could give everyone their own catalog, or hand-rename every table in your
YAML before each private run and remember to change it all back. Or you turn on
sandbox mode: ``lhp generate --sandbox`` builds only your slice of the project
and renames the tables you produce into your own namespace, so you iterate in
isolation while every shared table your pipelines read stays exactly where it
is. Declare a namespace once; don't hand-edit table names.

Let's take the ``bronze_orders`` pipeline and generate a private copy for a
developer named ``alice``, leaving the team's shared tables untouched.

Before you begin
================

You need a Lakehouse Plumber project with at least one pipeline and a
``substitutions/dev.yaml`` for the ``dev`` environment. This guide sandboxes
against ``dev``. Deploying a sandbox uses the same Databricks bundle workflow as
any other build; see the deploy guide when you get there.

Sandbox mode follows one rule — **read-shared, write-own**. Only the tables your
in-scope pipelines *produce* are renamed: the write itself and every in-scope
read of it. Reads of tables produced outside your scope keep pointing at the
shared tables, because those are inputs the whole team depends on.

Set the team policy
===================

The rename policy is a one-time, team-level decision that lives in ``lhp.yaml``
under a ``sandbox:`` block. The whole block is optional — when it is absent, the
defaults below apply — but stating it makes the policy explicit and lets you
fence sandbox mode to the environments where it is safe:

.. literalinclude:: ../../_fixtures/guide_dev_sandbox/lhp.yaml
   :language: yaml
   :caption: lhp.yaml
   :lines: 6-12

``strategy: table`` renames the table *leaf* and passes catalog and schema
through unchanged. ``table_pattern`` is the ``str.format`` template applied to
that leaf — ``{namespace}`` and ``{table}`` are pattern placeholders, not
``${...}`` substitution tokens, and both must appear. ``allowed_envs`` lists the
environments where ``--sandbox`` is permitted; running it against any other
environment fails with ``LHP-CFG-065``. Omit the key to allow every environment.

Declare your sandbox profile
============================

Your namespace and the pipelines you own live in a personal profile at
``.lhp/profile.yaml``. The ``.lhp/`` directory is gitignored by the standard
project template, so this file never reaches version control — every developer
keeps their own. Sandbox is explicit opt-in: nothing is auto-detected.

.. literalinclude:: ../../_fixtures/guide_dev_sandbox/.lhp/profile.yaml
   :language: yaml
   :caption: .lhp/profile.yaml
   :lines: 4-7

``namespace`` must match ``^[a-z][a-z0-9_]{0,63}$`` — a lowercase letter, then
lowercase letters, digits, or underscores, 64 characters at most. ``pipelines``
is a non-empty list of exact pipeline names or case-sensitive globs; an entry
containing ``*``, ``?``, or ``[`` is treated as a glob. Here ``bronze_*`` scopes
``alice`` to the ``bronze_orders`` pipeline and any other ``bronze_`` pipeline,
while the team's downstream ``silver_orders`` pipeline stays out of scope.

The flowgroup you wrote uses plain, shared names
================================================

Nothing in your YAML mentions sandboxes. The ``orders_enriched`` flowgroup reads
the bronze ``orders`` table your project produces, joins it against a
``customers`` dimension a different team publishes in the ``raw`` schema, and
writes an enriched bronze table:

.. literalinclude:: ../../_fixtures/guide_dev_sandbox/pipelines/orders_enriched.yaml
   :language: yaml
   :caption: pipelines/orders_enriched.yaml
   :lines: 3-42

You write ``orders``, ``orders_enriched``, and ``raw.customers`` once, with their
real shared names. Sandbox mode decides which of them to rewrite at generate
time — you never fork the YAML.

Generate your slice
===================

A shared run builds every pipeline in the project. Validate and generate without
``--sandbox`` and all three flowgroups compile, plus the monitoring phase:

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  2 files
   ✓ silver_orders  1 file
   ✓ generate (0.40s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   2 pipelines generated · 3 files · 0.5s

Now add ``--sandbox``. The run scopes to your profile: only ``bronze_orders``
compiles, ``silver_orders`` is skipped, and the monitoring phase is skipped so a
sandbox build never overwrites the team's shared monitoring artifacts.

.. code-block:: console

   $ lhp validate --env dev --sandbox
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  0 files
   ✓ validate (0.36s)
   1 validated · 0.4s

   $ lhp generate --env dev --sandbox
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_orders  2 files
   ✓ generate (0.38s)
   ✓ format (0.05s)
   1 pipeline generated · 2 files · 0.4s

``validate --sandbox`` applies the renames and checks the scoped pipelines
before you commit to generating; ``generate --sandbox`` writes the Python.

.. note::

   Sandbox scope comes from your profile, so ``--sandbox`` cannot be combined
   with ``-p``/``--pipeline`` — passing both is a usage error (exit code 2). If
   there is no ``.lhp/profile.yaml``, the run fails with ``LHP-IO-025``.

Read what changed
=================

Open ``generated/dev/bronze_orders/orders_enriched.py``. A shared run and a
sandbox run both write to ``generated/dev/`` — this is the sandbox run:

.. literalinclude:: ../../_fixtures/guide_dev_sandbox/generated_sandbox/dev/bronze_orders/orders_enriched.py
   :language: python
   :caption: generated/dev/bronze_orders/orders_enriched.py (sandbox run)
   :emphasize-lines: 20, 50

Three things happened, and one deliberately did not:

* The read of the bronze ``orders`` table (line 20) is now
  ``spark.readStream.table("dev_catalog.bronze.alice_orders")``. Your scope
  includes the ``orders_ingest`` flowgroup that *produces* ``orders``, so that
  table is yours — and every in-scope read of it is renamed too.
* The write target (line 50) became ``dev_catalog.bronze.alice_orders_enriched``
  — the ``create_streaming_table`` name and the ``append_flow`` target both
  carry your namespace.
* The join against ``dev_catalog.raw.customers`` (line 38) is **unchanged**.
  Your scope does not produce that table, so the sandbox leaves the read pointed
  at the shared dimension the other team owns.

Only the leaf name changes — ``dev_catalog.bronze`` is intact; ``orders`` became
``alice_orders`` through the ``{namespace}_{table}`` pattern.

What you just did
=================

A shared run wrote three flowgroups across two pipelines and refreshed
monitoring. Your sandbox run wrote exactly the two flowgroups you own, and every
table those flowgroups produce — ``orders`` and ``orders_enriched`` — is now
namespaced ``alice_orders`` and ``alice_orders_enriched`` at every read and write
site. The one shared table you read, ``raw.customers``, was left untouched, and
the team's ``silver_orders`` pipeline was never touched at all.

You changed no YAML to get here. You declared a namespace and a scope once, and
LHP rewrote the table identities for this run — so you can deploy and iterate on
the shared ``dev`` environment without ever writing over a table a teammate
depends on.

Deploy, iterate, clean up
=========================

Deploy a sandbox build with the regular Databricks bundle workflow, targeting
your own development target:

.. code-block:: console

   $ databricks bundle deploy --target dev

Iterate by editing your YAML and re-running ``lhp generate --env dev --sandbox``.
Every generate is a full regenerate, so to return to shared names you
regenerate without ``--sandbox`` — no sandbox names survive. When you are done,
tear the sandbox resources down:

.. code-block:: console

   $ databricks bundle destroy --target dev

.. important::

   ``bundle destroy`` removes the sandbox streaming tables and materialized
   views, but Delta-sink tables created by sandbox runs are not removed. Drop
   those manually.

Where to go next
================

- **See every rename rule.** Snapshot-CDC sources, delta sinks, test-action
  references, table names inside ``spark.sql`` bodies, and table references
  passed into custom Python all follow the read-shared, write-own rule with
  their own edge cases. The sandbox reference catalogs each site, the full
  configuration schema, and every sandbox error and warning code.
- **Promote warnings to failures.** Pass ``--strict`` so a sandbox build with an
  in-scope read that could not be rewritten (``LHP-VAL-066``,
  ``LHP-VAL-067``) fails instead of slipping through.
- **Deploy the build.** The deploy guide walks through packaging generated
  pipelines as a Databricks bundle and shipping them to a workspace.
