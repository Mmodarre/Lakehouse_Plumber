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

The flag accepts paths relative to the project root or absolute paths.

.. versionchanged:: 0.8.7
   ``--pipeline-config`` is **required** when bundle support is enabled
   (``databricks.yml`` is present in the project root and ``--no-bundle`` is
   not passed). Invocations that omit the flag now fail with
   ``LHP-CFG-023`` *before* any files are written or deleted. To skip bundle
   resource generation entirely, pass ``--no-bundle``.

Pre-flight validation
---------------------

``lhp generate`` runs catalog/schema validation **before** any side effects:
no directory is wiped, no code is generated, no bundle YAML is written until
preflight passes. Two checks fire in order:

1. **CLI flag check** (``LHP-CFG-023``) — when bundle support is enabled,
   ``--pipeline-config`` must be supplied. Fails in <1 second on a missing
   flag.
2. **Catalog/schema resolution** (``LHP-CFG-026``) — every pipeline (plus
   the synthetic monitoring pipeline if monitoring is enabled in
   ``lhp.yaml``) is checked for resolved ``catalog`` and ``schema``. All
   failures are aggregated into one error, grouped by failure type — see
   `Error reference`_ below.

Preflight runs identically under ``--dry-run``: a dry-run invocation with
missing config still surfaces the same errors, just without ever touching
the filesystem.

Without ``--pipeline-config`` and with bundle support enabled, every pipeline
fails with the error described in `Error reference`_.

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

Preflight surfaces two distinct error codes. ``LHP-CFG-023`` covers the CLI
flag check; ``LHP-CFG-026`` is the aggregated catalog/schema validation
result. Both carry a ``doc_link`` pointing to this page so that programmatic
catchers (CI tooling, editor integrations) can route users back to it.

LHP-CFG-023 — ``--pipeline-config`` is required
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Message:** ``--pipeline-config is required when bundle support is enabled``

Triggered when ``databricks.yml`` exists in the project root (bundle support
is enabled), ``--no-bundle`` was not passed, and ``--pipeline-config`` /
``-pc`` was omitted. Without that flag, ``PipelineConfigLoader`` loads empty
defaults and every pipeline would fail catalog/schema validation later — so
preflight surfaces the actionable error up front.

**Fix:** Pass ``--pipeline-config config/pipeline_config.yaml`` (or your
project's equivalent path). To skip bundle resource generation entirely
without supplying the flag, pass ``--no-bundle``.

LHP-CFG-026 — aggregated catalog/schema validation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Message:** ``Catalog/schema validation failed for N pipeline(s)``

Triggered when any pipeline (including the synthetic monitoring pipeline if
monitoring is enabled) has missing or empty ``catalog`` / ``schema`` after
config merge and substitution. Failures are aggregated across **all**
pipelines and grouped by failure type. The error's ``context["failures"]``
exposes the structured payload to programmatic consumers::

   {
     "both_missing": ["pipeline_a", "pipeline_b"],
     "incomplete": [{"pipeline_name": "...", "catalog": "...", "schema": null}],
     "empty_after_substitution": [{"pipeline_name": "...", ...}]
   }

The three failure categories below explain each bucket.

Both missing
^^^^^^^^^^^^

Pipelines listed under ``both_missing`` have neither ``catalog`` nor
``schema`` set after the deep merge (``DEFAULT_PIPELINE_CONFIG`` →
``project_defaults`` → per-pipeline). The most common cause is a
``pipeline_config.yaml`` without a ``project_defaults`` block and no
per-pipeline overrides.

**Fix:** Add ``catalog`` and ``schema`` to ``project_defaults`` (for
project-wide values) or to each pipeline's own entry (for per-pipeline values).

Incomplete pairing
^^^^^^^^^^^^^^^^^^

Pipelines listed under ``incomplete`` have one of ``catalog`` or ``schema``
defined but the other missing. LHP requires both or neither — partial
configuration is treated as a typo, not as a fallback request.

**Fix:** Add the missing key. If the intent was to inherit from
``project_defaults`` for one half, remove the other half from the
per-pipeline entry so both resolve from the same layer.

Empty after substitution
^^^^^^^^^^^^^^^^^^^^^^^^

Pipelines listed under ``empty_after_substitution`` have ``catalog`` or
``schema`` that resolved to whitespace-only strings after
``substitutions/<env>.yaml`` was applied. The keys exist in the config but
evaluate to nothing meaningful.

**Fix:** Inspect the substitution file for the failing environment. Run
``lhp substitutions --env <env>`` to print the resolved values for every
token. Add or correct the entries that resolve to empty strings.

.. note::

   Pre-0.8.7 versions raised three separate ``LHP-CFG-026`` errors — one per
   failure category, on the first failing pipeline. The current preflight
   walks every pipeline once and aggregates into a single error. Users who
   parsed the previous title strings should switch to
   ``LHPConfigError.context["failures"]``, which is the stable contract.

See also
--------

- :doc:`configure_bundles` — Bundle setup walk-through and directory layout.
- :doc:`bundle_config_reference` — Full ``pipeline_config.yaml`` schema and
  bundle sync rules.
- :doc:`substitutions` — Substitution token syntax and resolution order.
- :doc:`errors_reference` — Full LHP error code catalog.
