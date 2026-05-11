Blueprints
==========

.. meta::
   :description: Reference for Lakehouse Plumber blueprints: file format, instance format, parameter resolution, discovery, generation behavior, CLI commands, and error codes.

A **blueprint** is a parametrised collection of FlowGroup specs expanded once
per *instance* file. Each ``(blueprint, instance, spec)`` triple yields one
synthetic FlowGroup that joins the regular generation pipeline alongside
hand-written FlowGroups.

This page catalogs the blueprint and instance file formats, parameter
resolution rules, discovery behavior, generation behavior, CLI surface, and
error codes. For *when* to use a blueprint versus a :term:`Preset` or :term:`Template`, see
:doc:`decisions`.

Blueprint file
--------------

Place blueprint files under the configured ``blueprint_include`` patterns
(default ``blueprints/**/*.yaml`` and ``blueprints/**/*.yml``). Each file is a
single-document YAML matching the ``Blueprint`` model.

.. code-block:: yaml
   :caption: blueprints/erp_ingestion.yaml

   name: erp_ingestion
   version: "1.0"
   description: "Standard ERP ingestion for all regional sites"

   parameters:
     - name: site_name
       required: true
       description: "Site identifier"
     - name: site_id
       required: true
     - name: partition_key
       required: false
       default: order_date

   flowgroups:
     - pipeline: "%{site_name}_erp_raw"
       flowgroup: "%{site_name}_orders_ingestion"
       actions:
         - name: load_orders
           type: load
           source:
             type: cloudfiles
             path: "/Volumes/raw/erp/%{site_name}/orders"
             format: json
           target: v_orders_raw
         - name: write_orders
           type: write
           source: v_orders_raw
           write_target:
             type: streaming_table
             catalog: ${catalog}
             schema: bronze
             table: "%{site_name}_orders_raw"
             partition_columns:
               - "%{partition_key}"

Top-level fields
~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 18 12 15 55

   * - Field
     - Type
     - Default
     - Notes
   * - ``name``
     - string
     - required
     - Unique across the project. Referenced by ``use_blueprint:`` in instance
       files. Duplicates raise ``LHP-VAL-046``.
   * - ``version``
     - string
     - ``"1.0"``
     - Free-form string for change tracking.
   * - ``description``
     - string
     - ``null``
     - Surfaced by ``lhp list-blueprints``.
   * - ``parameters``
     - list
     - ``[]``
     - Parameter declarations (see below).
   * - ``flowgroups``
     - list
     - required
     - FlowGroup specs. Each entry is expanded once per instance. Empty list
       fails Pydantic validation.

Parameter declarations
~~~~~~~~~~~~~~~~~~~~~~

Each entry in ``parameters:`` is a ``BlueprintParameter`` object.

.. list-table::
   :header-rows: 1
   :widths: 18 12 15 55

   * - Field
     - Type
     - Default
     - Notes
   * - ``name``
     - string
     - required
     - The key the instance file binds a value to. Strict-checked.
   * - ``required``
     - bool
     - ``false``
     - If ``true``, every instance must supply this parameter
       (``LHP-VAL-042`` otherwise).
   * - ``default``
     - any
     - ``null``
     - Applied when ``required: false`` and the instance omits the value.
   * - ``description``
     - string
     - ``null``
     - Surfaced by ``lhp list-blueprints --verbose``.

Unknown parameter keys in an instance file raise ``LHP-VAL-043`` with a
``difflib`` "did you mean" suggestion against the declared parameter names.

FlowGroup spec fields
~~~~~~~~~~~~~~~~~~~~~

Each entry in ``flowgroups:`` is a ``BlueprintFlowgroupSpec``. It accepts the
same fields as a regular FlowGroup file.

.. list-table::
   :header-rows: 1
   :widths: 24 14 62

   * - Field
     - Required
     - Notes
   * - ``pipeline``
     - yes
     - Template string. Only ``%{var}`` permitted (see
       :ref:`parameter-resolution`).
   * - ``flowgroup``
     - yes
     - Template string. Same restriction as ``pipeline``.
   * - ``job_name``
     - no
     - Optional job name; pass-through to the FlowGroup.
   * - ``variables``
     - no
     - Map of derived local variables. Spec ``variables`` win over instance
       parameters on key conflict.
   * - ``presets``
     - no
     - List of preset names applied to actions.
   * - ``use_template``
     - no
     - Template name. Renders into the spec's action list.
   * - ``template_parameters``
     - no
     - Map of values for the referenced template. ``%{var}`` resolves at Step
       0.5; Jinja2 ``{{ var }}`` resolves at Step 1.
   * - ``actions``
     - no
     - Inline action list.
   * - ``operational_metadata``
     - no
     - Bool or list of metadata column names.

A blueprint with N specs and M instances expands to ``N × M`` synthetic
FlowGroups.

Instance file
-------------

Place instance files anywhere matched by ``instance_include`` (default
``pipelines/**/*.yaml`` and ``pipelines/**/*.yml``). The instance file format
is strict (``extra="forbid"``); unknown top-level keys are parse errors.

Preferred syntax
~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: pipelines/sites/apac_sg.yaml

   use_blueprint: erp_ingestion
   parameters:
     site_name: apac_sg
     site_id: SG001

.. list-table::
   :header-rows: 1
   :widths: 22 12 66

   * - Field
     - Required
     - Notes
   * - ``use_blueprint``
     - yes
     - Name of the target blueprint. Unknown name raises ``LHP-VAL-041``.
   * - ``parameters``
     - no
     - Flat map of ``parameter_name: value``. Required parameters declared by
       the blueprint must appear here.
   * - ``overrides``
     - no
     - Reserved for future use. Accepted but unused.

Legacy syntax (deprecated)
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. deprecated:: 0.8
   The ``blueprint:`` + flat-parameters form is removed in V0.9. Migrate to
   ``use_blueprint:`` + nested ``parameters:``.

The legacy form uses a top-level ``blueprint:`` key with parameter values as
flat top-level siblings. A normalization validator rewrites legacy files to
the preferred shape at parse time and emits a deprecation warning once per
file path. Mixing both forms in the same file raises ``LHP-VAL-061``.

.. _parameter-resolution:

Parameter resolution
--------------------

Two substitution syntaxes apply to blueprints. They differ in scope and
resolution phase.

.. list-table::
   :header-rows: 1
   :widths: 26 36 38

   * - Token
     - Where it works
     - When it resolves
   * - ``%{var}``
     - Anywhere in the blueprint.
     - In ``pipeline:`` / ``flowgroup:`` fields at Step 0 (expansion).
       Elsewhere at Step 0.5 (per-FlowGroup, after expansion).
   * - ``${env_token}``
     - Anywhere **except** ``pipeline:`` and ``flowgroup:``.
     - Step 3 (environment substitution).
   * - ``${secret:scope/key}``
     - Same as ``${env_token}``.
     - Step 3.

The expander rejects any ``${...}`` token inside ``pipeline:`` or
``flowgroup:`` template strings with ``LHP-VAL-044``. The match regex is
``\$\{[^}]+\}``, so environment tokens and secret references are both
rejected. The resolved ``(pipeline, flowgroup)`` tuple is used as the
source-path index and state-tracking key before Step 3 runs; allowing
``${...}`` would make those keys unstable across environments.

Unresolved ``%{var}`` in ``pipeline:`` or ``flowgroup:`` raises
``LHP-VAL-055``.

Effective parameter map
~~~~~~~~~~~~~~~~~~~~~~~

For each ``(instance, spec)`` pair the expander builds a merged variable map:

1. Blueprint parameter defaults.
2. Instance ``parameters`` values (override step 1).
3. Spec ``variables`` block (override step 2).

Spec ``variables`` win on key conflict — this protects blueprint-author
intent against an instance that accidentally names a parameter the same as a
derived spec variable.

Discovery
---------

Default discovery globs:

.. code-block:: yaml

   blueprint_include: ["blueprints/**/*.yaml", "blueprints/**/*.yml"]
   instance_include:  ["pipelines/**/*.yaml",  "pipelines/**/*.yml"]

Both ``instance_include`` and the regular ``include`` glob match
``pipelines/**/*.yaml`` by default. Discovery routes by content shape, not by
path:

* File has top-level ``use_blueprint:`` or legacy ``blueprint:`` key →
  **instance**.
* File has top-level ``parameters:`` and ``flowgroups:`` and no ``actions:``
  key → **blueprint**.
* File matched by the flowgroup ``include`` glob but shaped like a blueprint
  → ``LHP-CFG-040`` (move to ``blueprints/``).
* Anything else under a flowgroup glob → **flowgroup**.

Override ``blueprint_include`` or ``instance_include`` in ``lhp.yaml`` only
to restrict discovery (for example, to gate a subset of sites during a
rollout).

Generation behavior
-------------------

Synthetic FlowGroups produced by expansion enter the regular processing
pipeline. The expansion step is the only step exclusive to blueprints.

1. **Step 0 — Blueprint expansion.** ``%{var}`` resolved in ``pipeline:`` and
   ``flowgroup:`` fields; ``${...}`` rejected there.
2. **Step 0.5 — Local variables.** ``%{var}`` resolved everywhere else
   against the merged parameter map.
3. **Step 1 — Template render.** Jinja2 ``{{ var }}`` substituted from
   ``template_parameters``.
4. **Steps 1.5 / 2 — Preset merge.** Template-level then FlowGroup-level
   presets applied as deep-merge defaults.
5. **Step 3 — Environment + secrets.** ``${env_token}`` and
   ``${secret:scope/key}`` resolved.
6. **Steps 3.5–5 — Validation.** Unresolved tokens, FlowGroup shape, secret
   references.
7. **Code generation.** Python files written under ``generated/<env>/``.

Incremental regeneration
~~~~~~~~~~~~~~~~~~~~~~~~

The state tracker injects a ``type='instance'`` dependency on each
synthetic FlowGroup pointing at its originating instance file.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - Change
     - ``lhp generate --env <env>`` behavior
   * - Edit an instance file
     - Regenerates only that instance's FlowGroups.
   * - Edit a blueprint file
     - Regenerates all instances that reference that blueprint.
   * - Add a new instance file
     - Generates the new instance's FlowGroups (novelty detection).
   * - Delete an instance file
     - Cleans up that instance's generated files via orphan detection.
   * - Edit a preset or template referenced by the blueprint
     - Regular preset/template dependency tracking applies.

CLI commands
------------

``lhp list-blueprints``
~~~~~~~~~~~~~~~~~~~~~~~

Lists discovered blueprints with parameter and instance counts.

.. code-block:: bash

   lhp list-blueprints
   lhp list-blueprints --verbose

``--verbose`` runs the expander against each instance and prints every
resolved ``(pipeline, flowgroup)`` tuple it produces.

``lhp show --instance``
~~~~~~~~~~~~~~~~~~~~~~~

Expands one instance and prints the resolved configuration. Mutually
exclusive with the positional ``FLOWGROUP`` argument.

.. code-block:: bash

   lhp show --instance pipelines/sites/apac_sg.yaml --env dev

Passing both forms raises ``LHP-CFG-057``; passing neither raises
``LHP-CFG-058``.

``lhp validate``
~~~~~~~~~~~~~~~~

Runs blueprint discovery, expansion, and full FlowGroup validation.

.. code-block:: bash

   lhp validate --env dev

``lhp deps``
~~~~~~~~~~~~

By default the dependency graph deduplicates synthetic FlowGroups by
``(blueprint_name, spec_index)`` — one logical edge per spec, not per
instance.

.. code-block:: bash

   lhp deps
   lhp deps --expand-blueprints
   lhp deps --blueprint erp_ingestion

See :doc:`dependency_analysis` for the full ``deps`` reference.

Error codes
-----------

Blueprint-related codes. The project-wide catalog lives in
:doc:`errors_reference`.

.. list-table::
   :header-rows: 1
   :widths: 18 14 68

   * - Code
     - Category
     - Meaning
   * - ``LHP-CFG-040``
     - Config
     - Blueprint file matched by a flowgroup include glob. Move to
       ``blueprints/`` or adjust ``include`` / ``blueprint_include``.
   * - ``LHP-VAL-041``
     - Validation
     - Instance references an unknown blueprint name. Error includes a "did
       you mean" suggestion.
   * - ``LHP-VAL-042``
     - Validation
     - Instance is missing a required parameter.
   * - ``LHP-VAL-043``
     - Validation
     - Instance has an unknown parameter key. Often a typo; error includes a
       suggestion.
   * - ``LHP-VAL-044``
     - Validation
     - ``${...}`` substitution token appears in a ``pipeline:`` or
       ``flowgroup:`` template string. Only ``%{var}`` permitted there.
   * - ``LHP-VAL-045``
     - Validation
     - Two instances expand to the same ``(pipeline, flowgroup)`` tuple. Both
       file paths in error context.
   * - ``LHP-VAL-046``
     - Validation
     - Two blueprint files declare the same ``name``.
   * - ``LHP-CFG-047`` … ``050``
     - Config
     - Blueprint file shape errors: empty, multi-document, wrong shape,
       failed Pydantic validation.
   * - ``LHP-CFG-051`` / ``052`` / ``054``
     - Config
     - Instance file shape errors: empty, multi-document, invalid parse.
   * - ``LHP-VAL-053``
     - Validation
     - Instance file missing ``use_blueprint:`` / ``blueprint:`` key.
   * - ``LHP-VAL-055``
     - Validation
     - Unresolved ``%{var}`` in a ``pipeline:`` or ``flowgroup:`` template.
   * - ``LHP-CFG-057``
     - Config
     - ``lhp show`` called with both ``FLOWGROUP`` and ``--instance``.
   * - ``LHP-CFG-058``
     - Config
     - ``lhp show`` called with neither ``FLOWGROUP`` nor ``--instance``.
   * - ``LHP-IO-059``
     - I/O
     - Instance file path resolves outside the project root.
   * - ``LHP-VAL-061``
     - Validation
     - Conflicting instance syntax: both ``use_blueprint:`` and legacy
       ``blueprint:``, or ``use_blueprint:`` plus unexpected top-level keys.

See also
--------

* :doc:`decisions` — when to pick a Blueprint over a Preset or Template.
* :doc:`templates_reference` — Templates referenced by
  ``use_template:`` inside FlowGroup specs.
* :doc:`presets_reference` — Presets referenced by ``presets:``.
* :doc:`substitutions` — environment tokens, local variables, secret
  references.
* :doc:`dependency_analysis` — full ``lhp deps`` reference.
* :doc:`architecture` — why expansion runs before the rest of the pipeline
  and how state tracking distinguishes synthetic FlowGroups.
* :doc:`errors_reference` — project-wide error code catalog.
