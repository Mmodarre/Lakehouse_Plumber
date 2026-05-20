Configuring catalog and schema for pipelines
============================================

.. meta::
   :description: How to set Unity Catalog catalog and schema for Lakehouse Plumber pipelines via pipeline_config.yaml — per-pipeline entries, project defaults, resolution order, CLI usage, migration from databricks.yml variables, and error reference.

Every Databricks Lakeflow Declarative Pipeline targets a Unity Catalog
``catalog`` and ``schema``. In Lakehouse Plumber (LHP), both values come from
``pipeline_config.yaml`` — either as per-pipeline entries or as project-wide
defaults. ``lhp generate`` requires both values and fails fast when either is
missing.

.. versionchanged:: 0.8.7
   Catalog and schema must be defined in ``pipeline_config.yaml``. Previous
   versions wrote ``default_pipeline_catalog`` and ``default_pipeline_schema``
   variables into ``databricks.yml`` automatically and the pipeline-resource
   template fell back to ``${var.default_pipeline_*}``. Both behaviors are
   removed. See `Migration from databricks.yml variables`_.

Per-pipeline configuration
--------------------------

Set ``catalog`` and ``schema`` inside a per-pipeline entry. Each pipeline gets
its own YAML document keyed by ``pipeline:``:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   ---
   pipeline: ingestion_dev
   catalog: my_dev_catalog
   schema: bronze
   serverless: true

A per-pipeline entry wins over ``project_defaults`` for every key it sets. LHP
deep-merges nested dicts, so per-pipeline ``configuration`` and ``event_log``
blocks merge field-by-field; lists (``clusters``, ``notifications``,
``permissions``) replace the project-default list wholesale.

Project defaults
----------------

Declare ``project_defaults`` at the top of the file when several pipelines
share the same catalog and schema. The block applies to every pipeline unless
a per-pipeline entry overrides specific keys:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   project_defaults:
     catalog: shared_catalog
     schema: bronze
     serverless: true

   ---
   pipeline: ingestion_dev
   ---
   pipeline: ingestion_prod

The key is ``project_defaults`` (not ``default_pipeline_config`` or any
variant). LHP reads it once at config load and merges it into every
pipeline's resolved configuration.

Resolution order
----------------

LHP resolves each pipeline's configuration in three layers, with later layers
overriding earlier ones via deep merge:

1. ``DEFAULT_PIPELINE_CONFIG`` — LHP's built-in defaults (``serverless: true``,
   ``edition: ADVANCED``, ``channel: CURRENT``, ``continuous: false``).
2. ``project_defaults`` — the top-of-file block in ``pipeline_config.yaml``.
3. Pipeline-specific entry — the document keyed by ``pipeline: <name>``.

Per-pipeline values override ``project_defaults``; ``project_defaults`` overrides
the built-in defaults. The following config illustrates the precedence rules:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml — override behavior

   project_defaults:
     catalog: shared_dev
     schema: bronze

   ---
   pipeline: ingestion_a
   schema: silver        # overrides project_defaults; catalog inherits 'shared_dev'

   ---
   pipeline: ingestion_b
                         # both catalog and schema inherit from project_defaults

After resolution, ``ingestion_a`` runs against ``shared_dev.silver`` and
``ingestion_b`` runs against ``shared_dev.bronze``.

Substitution tokens (``${token}``, ``%{local_var}``) work inside any of these
layers. LHP resolves tokens against ``substitutions/<env>.yaml`` after the deep
merge completes. See :doc:`substitutions` for the substitution syntax.

Passing the pipeline config
---------------------------

Pass the config path with ``--pipeline-config`` (short form ``-pc``) on every
``lhp generate`` invocation:

.. code-block:: bash

   lhp generate --env dev --pipeline-config config/pipeline_config.yaml

The flag accepts paths relative to the project root or absolute paths. Without
``--pipeline-config``, LHP cannot read ``catalog`` and ``schema`` and every
pipeline fails with the error described in `Error reference`_.

For multi-environment workflows, pair one config file with each environment:

.. code-block:: bash

   lhp generate --env dev  --pipeline-config config/pipeline_config-dev.yaml
   lhp generate --env prod --pipeline-config config/pipeline_config-prod.yaml

Migration from databricks.yml variables
---------------------------------------

Prior to v0.8.7, LHP auto-populated two variables in ``databricks.yml`` for
every target:

.. code-block:: yaml
   :caption: databricks.yml (legacy, no longer written)

   targets:
     dev:
       variables:
         default_pipeline_catalog:
           default: my_dev_catalog
         default_pipeline_schema:
           default: bronze

The pipeline-resource template then emitted
``catalog: ${var.default_pipeline_catalog}`` and
``schema: ${var.default_pipeline_schema}`` when per-pipeline values were
absent. Both halves of that pathway are removed.

To migrate:

1. Delete the ``default_pipeline_catalog`` and ``default_pipeline_schema``
   entries from each target's ``variables:`` block in ``databricks.yml``. Remove
   the ``variables:`` block entirely if those were the only entries.
2. Move the values into ``pipeline_config.yaml`` — typically into
   ``project_defaults`` so they apply across every pipeline.

**Before:**

.. code-block:: yaml
   :caption: databricks.yml (before migration)

   targets:
     dev:
       variables:
         default_pipeline_catalog:
           default: my_dev_catalog
         default_pipeline_schema:
           default: bronze
     prod:
       variables:
         default_pipeline_catalog:
           default: my_prod_catalog
         default_pipeline_schema:
           default: gold

**After:**

.. code-block:: yaml
   :caption: databricks.yml (after migration)

   targets:
     dev:
       mode: development
     prod:
       mode: production

.. code-block:: yaml
   :caption: config/pipeline_config-dev.yaml (after migration)

   project_defaults:
     catalog: my_dev_catalog
     schema: bronze

.. code-block:: yaml
   :caption: config/pipeline_config-prod.yaml (after migration)

   project_defaults:
     catalog: my_prod_catalog
     schema: gold

If different pipelines need different catalogs or schemas inside one
environment, add per-pipeline entries that override the defaults. See
`Per-pipeline configuration`_.

Error reference
---------------

When catalog or schema cannot be resolved, LHP raises ``LHPConfigError`` with
code ``LHP-CFG-026`` and ``lhp generate`` exits non-zero. Every error in this
section carries a ``doc_link`` pointing to this page so that programmatic
catchers (CI tooling, editor integrations) can route users back to it.

Both missing
~~~~~~~~~~~~

**Message:** ``catalog and schema must be defined in pipeline_config.yaml for pipeline '<name>'…``

Triggered when neither the per-pipeline entry nor ``project_defaults`` sets
``catalog`` and ``schema``. The most common cause is forgetting ``--pipeline-config``
on the ``lhp generate`` command or pointing it at a file with no
``project_defaults`` block.

**Fix:** Add ``catalog`` and ``schema`` to ``project_defaults`` (for
project-wide values) or to the pipeline's own entry (for per-pipeline values),
then re-run ``lhp generate`` with ``--pipeline-config``.

Incomplete pairing
~~~~~~~~~~~~~~~~~~

**Message:** ``Incomplete catalog/schema for pipeline '<name>': catalog=…, schema=…``

Triggered when one of ``catalog`` or ``schema`` is defined but the other is
missing. LHP requires both or neither — partial configuration is treated as a
typo, not as a fallback request.

**Fix:** Add the missing key. If the intent was to inherit from
``project_defaults`` for one half, remove the other half from the per-pipeline
entry so both resolve from the same layer.

Empty after substitution
~~~~~~~~~~~~~~~~~~~~~~~~

**Message:** ``Empty catalog/schema after substitution in pipeline '<name>'. catalog=…, schema=…``

Triggered when ``catalog`` or ``schema`` references a substitution token whose
value is the empty string after ``substitutions/<env>.yaml`` resolves. The
keys exist in the config but evaluate to nothing.

**Fix:** Inspect the substitution file for the failing environment. Run
``lhp substitutions --env <env>`` to print the resolved values for every
token. Add or correct the entries that resolve to empty strings.

See also
--------

- :doc:`configure_bundles` — Bundle setup walk-through and directory layout.
- :doc:`bundle_config_reference` — Full ``pipeline_config.yaml`` schema and
  bundle sync rules.
- :doc:`substitutions` — Substitution token syntax and resolution order.
- :doc:`errors_reference` — Full LHP error code catalog.
