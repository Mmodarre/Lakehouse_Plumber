Bundle configuration
====================

.. meta::
   :description: Reference for LHP's Databricks bundle integration — the databricks.yml fields LHP needs, the config/pipeline_config.yaml schema (project_defaults and per-pipeline catalog, schema, serverless, packaging), the -pc requirement (LHP-CFG-023), wheel packaging keys, and the generated resources/lhp output.

Bundle support turns on whenever a ``databricks.yml`` file is present in the
project root. Pass ``--no-bundle`` to ``lhp generate`` or ``lhp validate`` to
disable it. LHP never reads or modifies ``databricks.yml``; it only detects the
file's presence and writes pipeline resource YAML under ``resources/lhp/``.

Activation flags
----------------

.. list-table::
   :header-rows: 1
   :widths: 28 18 54

   * - Flag
     - Command
     - Behavior
   * - ``--no-bundle``
     - ``generate``, ``validate``
     - Skip bundle detection and resource sync even when ``databricks.yml`` exists.
   * - ``-pc``, ``--pipeline-config``
     - ``generate``, ``validate``
     - Path (relative to project root) to the ``pipeline_config.yaml`` file. Required when bundle support is enabled.
   * - ``--force``
     - ``generate``
     - Deprecated no-op. Every run fully regenerates; the flag is hidden and ignored.

When bundle support is enabled and ``-pc`` / ``--pipeline-config`` is omitted,
both ``generate`` and ``validate`` fail up-front with ``LHP-CFG-023`` before any
files are written. Supply the flag, or pass ``--no-bundle`` to skip bundle
resource generation entirely.

``databricks.yml``
------------------

User-owned; LHP reads only its presence. These fields govern how the Databricks
CLI picks up LHP-generated resources.

.. list-table::
   :header-rows: 1
   :widths: 22 16 62

   * - Field
     - Type
     - Description
   * - ``bundle.name``
     - string
     - Bundle name used by the Databricks CLI for deploy paths.
   * - ``include``
     - list[string]
     - Resource-file globs. Must cover ``resources/lhp/*.yml`` so the Databricks CLI loads LHP-generated pipeline resources.
   * - ``targets``
     - mapping
     - One entry per deployment target. Each target name must match an LHP environment — a ``substitutions/<env>.yaml`` file with the same name.

.. code-block:: yaml

   bundle:
     name: my_project

   include:
     - resources/*.yml
     - resources/lhp/*.yml

   targets:
     dev:
       mode: development
       default: true
       workspace:
         host: <databricks_host>
     prod:
       mode: production
       workspace:
         host: <databricks_host>

``config/pipeline_config.yaml``
-------------------------------

A multi-document YAML file. The first document holds ``project_defaults``; each
subsequent document targets one or more pipelines via a ``pipeline`` key (a
single name or a list). Pass its path with ``-pc`` / ``--pipeline-config``.

.. code-block:: text

   project_defaults:
     <shared keys>
   ---
   pipeline: <name>          # or [name1, name2]
   <per-pipeline keys>

Top-level keys
~~~~~~~~~~~~~~

Valid under both ``project_defaults`` and a per-pipeline document.

.. list-table::
   :header-rows: 1
   :widths: 18 14 14 14 40

   * - Key
     - Type
     - Required
     - Default
     - Description
   * - ``catalog``
     - string
     - Yes
     - —
     - Unity Catalog name. Must be set with ``schema`` (both or neither) and non-empty after substitution. Supports ``${token}``.
   * - ``schema``
     - string
     - Yes
     - —
     - Schema name. Must be set with ``catalog``. Supports ``${token}``.
   * - ``serverless``
     - bool
     - No
     - ``true``
     - Pipeline compute mode.
   * - ``edition``
     - string
     - No
     - ``ADVANCED``
     - One of ``CORE``, ``PRO``, ``ADVANCED``. Ignored when ``serverless: true``.
   * - ``channel``
     - string
     - No
     - ``CURRENT``
     - One of ``CURRENT``, ``PREVIEW``.
   * - ``continuous``
     - bool
     - No
     - ``false``
     - Streaming/continuous pipeline mode.
   * - ``packaging``
     - string
     - No
     - ``source``
     - One of ``source``, ``wheel``. Selects how generated code ships (see `Wheel packaging`_). Consumed by LHP; never written to the resource YAML.

Any other top-level key (``clusters``, ``configuration``, ``notifications``,
``tags``, ``event_log``, ``environment``, ``permissions``, ``photon``, and any
Databricks Pipelines API field such as ``run_as``) is passed through verbatim
into the generated pipeline resource.

Merge precedence (lowest to highest): built-in defaults → ``project_defaults``
→ per-pipeline document. Nested mappings are deep-merged; lists are replaced
wholesale. Token substitution from ``substitutions/<env>.yaml`` applies to every
field.

.. code-block:: yaml

   project_defaults:
     catalog: "${catalog}"
     schema: "${bronze_schema}"
     serverless: true

   ---
   pipeline: bronze_load
   packaging: wheel

Wheel packaging
---------------

Set ``packaging: wheel`` on a pipeline (or under ``project_defaults``) to ship
its generated code as a built wheel instead of source files. Wheel mode requires
a ``wheel.artifact_volume`` in ``lhp.yaml``.

.. list-table::
   :header-rows: 1
   :widths: 26 14 60

   * - Key (``lhp.yaml``)
     - Type
     - Description
   * - ``wheel.artifact_volume``
     - string
     - Unity Catalog volume path where built wheels are uploaded. After substitution it must start with ``/Volumes/``; otherwise wheel packaging fails with ``LHP-CFG-061``. Supports ``${token}``.

.. code-block:: yaml

   wheel:
     artifact_volume: "/Volumes/${catalog}/artifacts/wheels"

Generated output
----------------

Each generated pipeline produces one resource file
``resources/lhp/<pipeline_name>.pipeline.yml`` carrying a
``# Generated by LakehousePlumber`` header. Every ``lhp generate`` run wipes
``resources/lhp/`` and rewrites it — manual edits are lost.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Path
     - Contents
   * - ``resources/lhp/<pipeline_name>.pipeline.yml``
     - Pipeline resource; declares a ``<pipeline_name>_pipeline`` resource key.
   * - ``resources/lhp/_wheels.bundle.yml``
     - Written only when at least one pipeline uses ``packaging: wheel``; points the bundle's artifact upload path at ``wheel.artifact_volume``.
