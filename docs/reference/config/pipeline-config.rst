pipeline_config.yaml
====================

**File:** path passed with ``-pc`` / ``--pipeline-config``.
For setup, see :doc:`/develop/bundles`.


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
     - One of ``source``, ``wheel``. Selects how generated code ships (see :doc:`bundle`). Consumed by LHP; never written to the resource YAML.

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

External pipeline settings follow the `Databricks pipeline resource reference
<https://docs.databricks.com/aws/en/dev-tools/bundles/resources#pipeline>`_.

Pipeline selection
------------------

The ``pipeline`` selector is a string or non-empty list of strings. A pipeline
may occur only once across override documents. Defaults and overrides apply to
selected pipelines by their YAML ``pipeline`` name, independently of file paths.

Use ``pipeline: __eventlog_monitoring`` to target the generated monitoring
pipeline without repeating its resolved name. Defining both that alias and the
actual monitoring pipeline name is an error. When monitoring is unconfigured
or disabled, the alias entry is ignored with a warning.

Event-log overrides
-------------------

A per-pipeline ``event_log`` mapping replaces the project's injected event-log
configuration for that pipeline. ``event_log: false`` opts that pipeline out.
The generated monitoring pipeline itself does not receive event-log injection.
See :doc:`monitoring` for the project settings and collection pipeline.
