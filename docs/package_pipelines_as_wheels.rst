Package Pipelines as Wheels
===========================

.. meta::
   :description: How to package Lakehouse Plumber pipelines as deterministic Python wheels to cut workspace sync and deploy cost — artifact-volume setup, the packaging toggle, and what gets generated.

This how-to switches a Lakehouse Plumber (LHP) pipeline from emitting loose
``.py`` files to emitting a single deterministic Python wheel. Wheel packaging
collapses a pipeline's synced workspace footprint to one small runner file plus
one uploaded artifact, which reduces ``databricks bundle deploy`` upload time and
the file-write slice of ``lhp generate`` on large projects. Packaging is opt-in
per pipeline; the generated pipeline logic is byte-identical to source mode.

.. versionadded:: 0.9.0

Requirements
------------

* An LHP project with Declarative Automation Bundles enabled (a ``databricks.yml``
  at the project root). See :doc:`configure_bundles`.
* A Unity Catalog volume your deploy identity can write to. Serverless compute
  installs custom wheels only from a Unity Catalog volume or PyPI, so the artifact
  destination must be a ``/Volumes/...`` path.
* In wheel mode each flowgroup is packed under a sanitized import package, so a
  top-level Python ``import`` of one flowgroup module from another will not
  resolve. Relate flowgroups through the Lakeflow Spark Declarative Pipelines
  (SDP) dataset graph, not Python imports.

Set the artifact volume
-----------------------

Declare the Unity Catalog volume that built wheels upload to in a top-level
``wheel`` block in ``lhp.yaml``. The value is substitution-token-aware: LHP
resolves ``${...}`` tokens from ``substitutions/<env>.yaml`` per environment,
exactly as it resolves any other per-environment value, so one ``lhp.yaml`` entry
serves every environment.

.. code-block:: yaml
   :caption: lhp.yaml

   wheel:
     artifact_volume: "/Volumes/${catalog}/${artifact_schema}/bundle_artifacts"

Define the tokens per environment so the resolved path is environment-specific:

.. code-block:: yaml
   :caption: substitutions/prod.yaml (excerpt)

   prod:
     catalog: prod_catalog
     artifact_schema: lhp_artifacts

The **resolved** value must start with ``/Volumes/``. If a pipeline is set to
wheel mode and ``wheel.artifact_volume`` is missing, empty, or resolves to a
non-``/Volumes/`` path, ``lhp generate`` fails with ``LHP-CFG-061``. A malformed
``wheel`` block — for example ``artifact_volume`` set to a non-string — fails with
``LHP-CFG-060``.

Turn on wheel packaging
-----------------------

Set ``packaging: wheel`` for a pipeline. The toggle lives in
``pipeline_config-<env>.yaml`` and resolves with this precedence, lowest to
highest:

1. the built-in default, ``source``;
2. ``project_defaults.packaging`` — the environment-wide default;
3. ``packaging`` inside a per-pipeline block — wins for that pipeline.

Because the toggle lives in the per-environment pipeline config, a pipeline can be
``source`` in one environment and ``wheel`` in another. The only accepted values
are ``source`` and ``wheel``; any other value fails with ``LHP-VAL-062``.

To package every pipeline in an environment as a wheel, set the default and opt
individual pipelines back out:

.. code-block:: yaml
   :caption: pipeline_config-prod.yaml

   project_defaults:
     catalog: "${catalog}"
     schema: "${raw_schema}"
     packaging: wheel            # environment-wide default

   ---
   pipeline: interactive_debug_pipeline
   packaging: source             # opt this one pipeline back out

   ---
   pipeline:
     - large_ingest_a
     - large_ingest_b            # inherit project_defaults.packaging: wheel

To package a single pipeline and leave the rest as source, omit the default and
set ``packaging`` only on that pipeline:

.. code-block:: yaml
   :caption: pipeline_config-prod.yaml

   ---
   pipeline: large_ingest_a
   packaging: wheel

Generate and deploy
-------------------

Run ``lhp generate`` for the target environment. Source-mode and wheel-mode
pipelines mix freely in one run; LHP builds each wheel once, in process.

.. code-block:: bash

   lhp generate --env prod --pipeline-config config/pipeline_config-prod.yaml
   databricks bundle validate --target prod
   databricks bundle deploy --target prod

Deploy uploads each pre-built wheel to the artifact volume and references it from
the pipeline's environment; it does not trigger a second Python build.

What LHP generates
------------------

For a wheel-mode pipeline, ``lhp generate`` writes a runner and a wheel, and wires
the bundle:

``generated/<env>/<pipeline>/<import_pkg>_runner.py``
   The only file synced to the workspace. The runner is the single source file SDP
   loads; it publishes the ambient ``spark`` and ``dbutils`` globals into
   ``builtins`` and then imports the wheel's flowgroup modules. Its content
   references only the import-package name, so it is stable across content changes.

``generated/<env>/_wheels/<pipeline>/dist/<pipeline>_<env>_<hash>-<lhp-version>-py3-none-any.whl``
   The built wheel. It is gitignored and excluded from the bundle file sync, and
   reaches the workspace only as an uploaded artifact. It contains the pipeline's
   flowgroup package, the ``custom_python_functions`` package as a top-level import
   root, and any generated test or quality and reporting modules.

``resources/lhp/<pipeline>.pipeline.yml``
   The pipeline resource. LHP appends the wheel reference as the last entry under
   ``environment.dependencies``, preserving any dependencies you declared. This
   file never contains the ``packaging`` key — the toggle is consumed by LHP, not
   by Databricks.

``resources/lhp/_wheels.bundle.yml``
   An LHP-owned bundle file, regenerated on every run. It declares the wheel
   ``artifacts`` to build and upload, sets ``targets.<env>.workspace.artifact_path``
   to the resolved artifact volume, and adds a ``sync.exclude`` for the
   ``_wheels/`` staging directory. LHP does not edit your ``databricks.yml``.

The wheel is deterministic and content-addressed: its filename is a function of
the packaged ``.py`` bytes and the environment, so an unchanged pipeline produces
an identical filename and deploys as a no-op. ``lhp validate``, ``lhp show``, and
dependency analysis operate on YAML and are unaffected by the packaging mode.

Limitations
-----------

Wheel packaging in this release has the following boundaries.

* **The LHP tool version is the wheel version.** Every wheel's metadata ``Version``
  is the LHP tool version that built it. Upgrading LHP re-stamps every wheel
  filename, which triggers a one-time reinstall of all wheel-mode pipelines on the
  next deploy even where the pipeline logic is unchanged. This is intended for
  traceability.

* **Code that reads files relative to** ``__file__`` **behaves differently.** Inside
  a wheel, ``__file__`` resolves into the installed package location, not
  ``generated/``, and only ``.py`` files are packaged. A flowgroup or custom helper
  that loads a sidecar data file (CSV, JSON, SQL) by a ``__file__``-relative path,
  or that assumes a loose on-disk layout, does not find it in wheel mode. Avoid
  ``__file__``-relative reads, or keep such a pipeline in source mode.

* **Monitoring pipelines are not wheeled.** The centralized event-log monitoring
  pipeline is always emitted in source mode regardless of the packaging toggle
  (internal tracking: WHEEL-MONITORING-DEFER). See :doc:`enable_monitoring`.

.. note::
   Several runtime behaviors are confirmed against the design but pending
   verification on a live workspace (internal tracking: WHEEL-RUNTIME-O2 through
   WHEEL-RUNTIME-O5): wheel install on classic compute, the bundle honoring the
   ``sync.exclude`` for the wheels directory, deploy uploading the pre-built wheel
   without rebuilding, and serverless reusing a cached environment when a
   pipeline's dependencies are unchanged. Verify these against your own workspace
   before relying on them in production.

Related articles
----------------

* :doc:`architecture` — How LHP generates pipeline code and why packaging is a
  per-pipeline emit mode.
* :doc:`configure_bundles` — Enable Declarative Automation Bundle integration, the
  prerequisite for wheel packaging.
* :doc:`configure_catalog_schema` — Resolve ``catalog`` and ``schema`` through
  ``pipeline_config.yaml`` and ``project_defaults``.
* :doc:`cicd` — Promote the same commit across environments; rollback is a redeploy
  of a prior commit, which regenerates an identical wheel.
* :doc:`bundle_config_reference` — Full bundle and pipeline configuration schema.
* :doc:`errors_reference` — ``LHP-VAL-062``, ``LHP-CFG-060``, and ``LHP-CFG-061``
  in context.
