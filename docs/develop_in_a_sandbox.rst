Develop in a sandbox
====================

.. meta::
   :description: How to use Lakehouse Plumber developer sandbox mode (--sandbox) to generate a personal, namespaced copy of your pipelines so parallel developers on a shared environment never collide on tables.

This how-to turns on Lakehouse Plumber (LHP) developer sandbox mode: ``lhp
generate --sandbox`` builds the Lakeflow Spark Declarative Pipelines (SDP) code
for your own slice of the project, with every table it produces renamed into
your personal namespace, so several developers can build and run against the
same shared environment without colliding on tables.

Sandbox mode applies one rule — **read-shared / write-own**: only tables
*produced* by your in-scope pipelines are renamed (the write targets and every
in-scope read of them), while reads of tables produced outside your scope keep
pointing at the shared tables. For the full rename semantics and configuration
schema, see :doc:`sandbox_reference`.

.. versionadded:: 0.9.1

Requirements
------------

* An LHP project — optionally with Declarative Automation Bundles enabled (a
  ``databricks.yml`` at the project root), which the deploy step below uses.
  See :doc:`configure_bundles`.
* Access to the target environment you sandbox against (for example ``dev``):
  the workspace, catalog, and schemas your pipelines write to.

Set the team policy (optional)
------------------------------

If you are the team lead, set the sandbox policy once in ``lhp.yaml``. This
step is optional: when the ``sandbox:`` block is absent, the defaults shown
below apply.

.. code-block:: yaml
   :caption: lhp.yaml

   sandbox:
     strategy: table                       # default
     table_pattern: "{namespace}_{table}"  # default
     allowed_envs:
       - dev

``strategy``
   How sandbox names are realized. This release supports only ``table``: the
   table *leaf* is renamed; catalog and schema pass through unchanged.

``table_pattern``
   The pattern applied to each produced table's leaf name. ``{namespace}`` and
   ``{table}`` are pattern placeholders — not ``${...}`` substitution tokens —
   and both must appear; literal text is limited to letters, digits, and
   underscores.

``allowed_envs``
   The environments in which sandbox runs are allowed. Omit the key to allow
   every environment.

Create your personal profile
----------------------------

Each developer declares a namespace and scope in a personal profile:

1. Create ``.lhp/profile.yaml`` in the project root. The ``.lhp/`` directory
   is gitignored by the standard project template, so the profile never
   reaches version control — every developer keeps their own.

2. Under the top-level ``sandbox:`` key, set your ``namespace`` and the
   ``pipelines`` you own:

   .. code-block:: yaml
      :caption: .lhp/profile.yaml

      sandbox:
        namespace: alice
        pipelines:
          - bronze_*          # glob: every pipeline whose name starts with bronze_
          - silver_orders     # exact pipeline name

``namespace`` must match ``^[a-z][a-z0-9_]{0,63}$`` — a lowercase letter
first, then lowercase letters, digits, or underscores, 64 characters at most.
``pipelines`` is a non-empty list of exact pipeline names or case-sensitive
globs (an entry containing ``*``, ``?``, or ``[`` is treated as a glob).

Validate and generate in sandbox mode
-------------------------------------

1. Validate your scoped pipelines with the renames applied:

   .. code-block:: bash

      lhp validate -e dev --sandbox

2. Generate:

   .. code-block:: bash

      lhp generate -e dev --sandbox

A sandbox run differs from a shared run in five ways:

* Only the profile-matched pipelines are generated — ``generated/dev/`` and
  ``resources/lhp/`` contain exactly the scoped pipelines.
* Every table the scoped pipelines produce is renamed through the pattern:
  write targets, delta-sink ``tableName`` options, and every in-scope read of
  those tables. With namespace ``alice``, a produced table
  ``edw_bronze.customer`` becomes ``edw_bronze.alice_customer`` — the leaf
  only; catalog and schema are unchanged.
* Reads of tables produced outside your scope are untouched and keep reading
  the shared tables.
* The project-level event-log table name is namespaced through the same
  pattern.
* The monitoring phase is skipped.

``--sandbox`` cannot be combined with ``-p``/``--pipeline``: the scope comes
from your profile, so combining them is a usage error (exit code 2).

Deploy and iterate
------------------

Deploy with the regular Declarative Automation Bundles development workflow,
targeting your own development target and workspace paths:

.. code-block:: bash

   databricks bundle deploy --target dev

Iterate by editing your YAML and re-running ``lhp generate -e dev --sandbox``.
To return to shared names, regenerate without ``--sandbox`` — every generate
is a full regenerate, so no sandbox names survive in the generated output.

Clean up
--------

When you are done with a sandbox, destroy its deployed resources:

.. code-block:: bash

   databricks bundle destroy --target dev

This removes the sandbox streaming tables and materialized views.

.. important::
   Delta-sink tables created by sandbox runs are **not** removed by
   ``databricks bundle destroy``. Drop them manually — for example with
   ``DROP TABLE`` on each namespaced sink table.

Troubleshooting
---------------

Sandbox failures map to dedicated error codes; see :doc:`errors_reference` for
each code in context.

* ``LHP-IO-025`` — no ``.lhp/profile.yaml`` found. Create the profile as shown
  above.
* ``LHP-CFG-065`` — the target environment is not in the team's
  ``allowed_envs``.
* ``LHP-VAL-064`` — a profile ``pipelines`` entry matched no pipeline, or an
  exact entry names the monitoring pipeline.
* ``LHP-VAL-065`` / ``LHP-VAL-066`` — warnings with category ``sandbox``; the
  run still succeeds. ``LHP-VAL-065``: a renamed table is also produced by an
  out-of-scope pipeline. ``LHP-VAL-066``: an in-scope read inside a Python
  file could not be rewritten (an indirect reference), so that file still
  references the shared name; ``lhp generate`` reports it, ``lhp validate``
  does not.

.. tip::
   Pass ``--strict`` to promote warnings — including ``LHP-VAL-065`` and
   ``LHP-VAL-066`` — to failures, so a sandbox build with an unrewritten read
   does not slip through.

Related articles
----------------

* :doc:`sandbox_reference` — full sandbox semantics: scope resolution, rename
  rules, and the complete configuration schema.
* :doc:`errors_reference` — the sandbox error and warning codes in context.
* :doc:`configure_bundles` — enable Declarative Automation Bundles, the deploy
  path for sandbox builds.
* :doc:`substitutions` — per-environment token resolution; sandbox renames
  apply to the fully resolved configuration.
