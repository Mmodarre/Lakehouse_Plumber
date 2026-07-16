========================
Configure the project
========================

.. meta::
   :description: Tour the four configuration surfaces of a Lakehouse Plumber project — lhp.yaml, substitutions, the pipeline config, and databricks.yml — and set the two values the sample needs to run.

Before you generate a line of code, learn where a Lakehouse Plumber project keeps
its settings. Four files control everything; the sample ships them filled in, so
this page is a **guided tour** — read each one, understand what it drives, and
you'll know exactly what to configure when you scaffold your own project.

Only **two values** actually need editing to run the sample. They're flagged
below with :guilabel:`edit`.

``lhp.yaml`` — the project
==========================

The root ``lhp.yaml`` holds project-wide settings that apply to every flowgroup.
Open it:

.. literalinclude:: ../_fixtures/sample_project/lhp.yaml
   :language: yaml
   :caption: lhp.yaml

Three things live here in the sample:

- **``operational_metadata.columns``** — reusable column definitions (a
  processing timestamp, the source file path and size) that any flowgroup can opt
  into by name. Defining them once here keeps the expressions consistent across
  pipelines. See :doc:`the operational-metadata reference </reference/config/operational-metadata>`.
- **``event_log``** — where Lakeflow writes each pipeline's event log table.
- **``monitoring``** — checkpoint location and a job config for the generated
  monitoring pipeline (:doc:`covered in the Operate guides </guides/ops/monitoring>`).

Nothing here needs editing for the sample. When you start your own project,
``lhp.yaml`` is where project-level defaults belong.

``substitutions/dev.yaml`` — per-environment values
===================================================

Every ``${...}`` token in your flowgroups resolves from the substitutions file
for the environment you generate against. This is the **one place**
environment-specific values live:

.. literalinclude:: ../_fixtures/sample_project/substitutions/dev.yaml
   :language: yaml
   :caption: substitutions/dev.yaml
   :emphasize-lines: 6,10

Set the two highlighted values:

1. :guilabel:`edit` ``catalog`` — a Unity Catalog catalog you can ``CREATE`` in.
   The demo makes three schemas (``sample_bronze``, ``sample_silver``,
   ``sample_gold``) and a landing volume inside it.
2. :guilabel:`edit` ``landing_volume`` — keep it in sync with the catalog above:
   ``/Volumes/<your_catalog>/sample_bronze/landing``.

``prod.yaml`` sits alongside it with production values. The flowgroups never
change between environments — only these files do. That is the whole point of
substitution: :doc:`logic separated from surroundings </concepts/substitution-and-envs>`.

``config/pipeline_config.yaml`` — catalog, schema, and compute
==============================================================

Because the sample is bundle-enabled, ``lhp generate`` needs to know the
catalog, schema, and compute to stamp into each generated pipeline resource. That
comes from a **pipeline config**, passed on every ``generate`` and ``validate``
with ``-pc``:

.. literalinclude:: ../_fixtures/sample_project/config/pipeline_config.yaml
   :language: yaml
   :caption: config/pipeline_config.yaml
   :lines: 20-31

It's a multi-document YAML: the first document sets project-wide defaults
(serverless compute, the catalog); each following document overrides one
pipeline (silver and gold get their own schema). The catalog and schema are
``${...}`` tokens, so **this file needs no edits** — it follows your
``substitutions/dev.yaml`` automatically.

``databricks.yml`` — the bundle
===============================

A project becomes a **bundle project** the moment ``databricks.yml`` sits at its
root. It defines the bundle name, where generated resources live, and one
**target** per environment:

.. literalinclude:: ../_fixtures/sample_project/databricks.yml
   :language: yaml
   :caption: databricks.yml
   :emphasize-lines: 24,42

Two things to know, and one edit:

- The ``catalog`` bundle **variable** is separate from the ``catalog``
  substitution token. The variable feeds the data-prep notebook at run time; the
  token feeds ``lhp generate`` at build time. They must agree — so set the same
  catalog in both ``databricks.yml`` and ``substitutions/dev.yaml``.
- :guilabel:`edit` ``workspace.host`` under ``targets.dev`` — your workspace URL.
- The ``sync.include`` glob ships ``generated/`` to the workspace; ``include``
  pulls in the bundle resources (both LHP-managed and hand-written).

.. admonition:: The two edits, in one place
   :class: tip

   To run the sample you set **exactly two things**: your ``workspace.host`` in
   ``databricks.yml``, and your ``catalog`` in *both* ``databricks.yml`` (the
   variable) and ``substitutions/dev.yaml`` (the token). Everything else is
   ready.

Next
====

You've seen where every setting lives. Now open the pipelines those settings
feed — :doc:`03-explore-the-pipelines`.
