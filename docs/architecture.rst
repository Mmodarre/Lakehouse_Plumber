Architecture
============

.. meta::
   :description: How Lakehouse Plumber is built — the Pipeline/FlowGroup/Action model, the substitution layer cake, state tracking, and the four-phase generation workflow.

Lakehouse Plumber (LHP) is a code generator. You write declarative YAML; LHP produces
Databricks Lakeflow Declarative Pipelines Python code. This page explains the model that
shape sits on: what objects you compose, how those objects relate, what substitutions
resolve and in what order, and how the generation engine turns YAML into Python.

If you want to do something — build a pipeline, configure CI/CD, troubleshoot a failure —
see :doc:`how_to_index`. If you are choosing between two ways of doing something (preset
versus template, streaming versus batch, where to enable state tracking), see
:doc:`decisions`.

The composition model
---------------------

LHP has three nested objects: **Pipeline**, **FlowGroup**, **Action**. They are
authored as YAML and validated by Pydantic models in ``src/lhp/models/config.py``.

A **Pipeline** is a logical grouping label. Every FlowGroup declares ``pipeline:
<name>``, and all FlowGroups sharing that name produce Python files into the same
output folder. A Pipeline is a deployment unit — when you build a :term:`Databricks Asset
Bundle <DAB>`, one :term:`Lakeflow Declarative Pipeline` resource is generated per Pipeline name.

A **FlowGroup** is a logical slice of a Pipeline, often a single source table or
business entity. A FlowGroup file declares its Pipeline, its own name, an optional
``job_name`` for multi-job orchestration, optional local ``variables``, and an ordered
list of Actions. One YAML file can hold one or many FlowGroups (see
:doc:`multi_flowgroup_guide`).

An **Action** is a single step inside a FlowGroup. Actions have four top-level types,
declared by the ``type:`` field. Each type has a sub-type that selects the generator
backing it. The full catalogue lives in :doc:`actions/index`; the canonical list of
enum values comes from ``ActionType`` and the four sub-type enums in
``models/config.py``.

.. list-table:: Action types
   :header-rows: 1
   :widths: 14 30 56

   * - Type
     - Sub-types
     - Purpose
   * - ``load``
     - ``cloudfiles``, ``delta``, ``sql``, ``python``, ``jdbc``, ``kafka``,
       ``custom_datasource``
     - Read external data into a temporary view. One per data source.
   * - ``transform``
     - ``sql``, ``python``, ``data_quality``, ``schema``, ``temp_table``
     - Reshape or check data already loaded into a view. Zero or many per FlowGroup.
   * - ``write``
     - ``streaming_table``, ``materialized_view``, ``sink``
     - Persist the final dataset. One per output table or sink.
   * - ``test``
     - ``row_count``, ``uniqueness``, ``referential_integrity``, ``completeness``,
       ``range``, ``schema_match``, ``all_lookups_found``, ``custom_sql``,
       ``custom_expectations``
     - Assert a property of the data. Run only with ``--include-tests``.

The model is a directed acyclic graph. A typical FlowGroup is **load → zero or more
transforms → write**, with optional tests attached. Actions inside a FlowGroup do not
execute in YAML order — Lakeflow's declarative engine schedules them at runtime based
on the ``source``/``target`` view references. LHP enforces the DAG at generation time
so cycles fail before deployment.

Reuse primitives
----------------

Three primitives let you factor reuse out of FlowGroups. They layer rather than
substitute for each other; the decision matrix lives in :doc:`decisions`.

A **Preset** is a YAML file of default *values* that get deep-merged into actions
matched by type. Presets resolve before substitutions and before validation, so the
defaults flow into every action that matches and explicit FlowGroup config wins. See
:doc:`presets_reference` for the resolution rules and merging semantics.

A **Template** is a YAML file of parametrised *actions*. Where a Preset injects
values, a Template injects whole action blocks. A FlowGroup applies a template via
``use_template:`` and supplies ``template_parameters:``; LHP renders the template with
Jinja2 ``{{ }}`` placeholders and appends the rendered actions to the FlowGroup. See
:doc:`templates_reference`.

A **Blueprint** is a higher-order template that instantiates *multiple* FlowGroups at
once. Where templates parameterise actions inside one FlowGroup, blueprints
parameterise the FlowGroups themselves. Blueprint instances supply parameters via
``use_blueprint:`` + ``parameters:`` (legacy ``blueprint:`` + flat keys is deprecated;
see ``BlueprintInstance`` in ``models/config.py``). See :doc:`blueprints` for full
semantics.

The substitution layer cake
---------------------------

LHP resolves four substitution syntaxes in a fixed order. The order matters because
each layer may emit text that the next layer then sees and resolves.

.. list-table::
   :header-rows: 1
   :widths: 8 22 22 48

   * - Phase
     - Syntax
     - Source
     - Resolved by
   * - 1
     - ``%{local_var}``
     - FlowGroup ``variables:``
     - ``LocalVariableResolver`` (``utils/local_variables.py``)
   * - 2
     - ``{{ template_param }}``
     - Template ``parameters:`` + caller's ``template_parameters:``
     - Jinja2 in the template engine
   * - 3
     - ``${env_token}``
     - ``substitutions/<env>.yaml``
     - ``EnhancedSubstitutionManager`` (``utils/substitution.py``)
   * - 4
     - ``${secret:scope/key}``
     - Databricks secret scopes (resolved at runtime by ``dbutils.secrets.get``)
     - ``SecretCodeGenerator`` (``utils/secret_code_generator.py``)

The order is enforced inside ``FlowgroupProcessor.process_flowgroup`` (steps 0.5, 1,
3, 5 in ``core/services/flowgroup_processor.py``). A consequence: an env token can
expand to a string that contains a secret reference, but not the other way around. A
template parameter can expand to a string that contains an env token, but a local
variable cannot reference a template parameter that has not yet rendered.

The ``${token}`` form is canonical. The bare ``{token}`` form is **deprecated** —
treat any documentation or example that still uses it as legacy. For full syntax,
including file substitutions and Databricks Connect compatibility shims, see
:doc:`substitutions`.

The generation workflow
-----------------------

``lhp generate --env <env>`` runs four phases over every FlowGroup it discovers. The
phase names map to services in ``src/lhp/core/services/``.

.. mermaid::

   graph TD
       subgraph Discovery
           A[Scan pipelines/] --> B[Apply include patterns]
           B --> C[Parse YAML to FlowGroup models]
           C --> C2[Discover and expand Blueprints]
       end
       subgraph Resolution
           C2 --> D[Resolve local variables]
           D --> E[Expand templates]
           E --> F[Apply preset defaults]
           F --> G[Apply env substitutions]
           G --> H[Validate FlowGroup]
           H --> I[Validate secret references]
       end
       subgraph Planning
           I --> J[Build dependency DAG]
           J --> K[Compare against .lhp_state.json]
           K --> L{Changed?}
       end
       subgraph Code generation
           L -->|yes| M[Run action generators]
           L -->|no| N[Skip]
           M --> O[Inject secret calls]
           O --> P[Write Python file]
           P --> Q[Update state]
           N --> Q
       end

**Discovery** is driven by ``FlowgroupDiscoverer`` and the project-level ``include``
patterns from ``lhp.yaml``. Blueprint instances are expanded into concrete
FlowGroups by ``BlueprintExpander`` before the resolution phase sees them.

**Resolution** runs each FlowGroup through ``FlowgroupProcessor``, applying the
substitution layer cake described above, deep-merging preset defaults, and running
Pydantic validation. Unresolved tokens raise ``LHP-CFG-010`` with the unresolved
names listed.

**Planning** builds the dependency DAG (``DependencyAnalyzer``) and compares each
FlowGroup's source-YAML checksum against ``.lhp_state.json``. Only changed
FlowGroups — and their downstream dependents — re-enter code generation.
``lhp deps`` exposes the DAG for inspection (see :doc:`dependency_analysis`).

**Code generation** dispatches each action to a generator looked up in
``ActionRegistry`` (one of 7 load, 5 transform, 3 write, or 9 test generators). The
generators emit Jinja2-rendered Python; ``CodeGenerator`` injects
``dbutils.secrets.get`` calls last so secret references never leak into source files.
The final Python is written via ``SmartFileWriter``, which only touches disk when
content actually changes.

State tracking
--------------

LHP writes ``.lhp_state.json`` after every successful generation. The state maps
each generated Python file to the source YAML, environment, checksum, and the
auxiliary files (presets, templates, schemas) the FlowGroup depended on.

.. code-block:: json
   :caption: .lhp_state.json (excerpt)

   {
     "version": "1.0",
     "generated_files": {
       "customer_ingestion.py": {
         "source_yaml": "pipelines/bronze/customer_ingestion.yaml",
         "checksum": "a1b2c3d4e5f6",
         "environment": "dev",
         "dependencies": ["presets/bronze_layer.yaml"]
       }
     }
   }

State serves three purposes. It enables **incremental regeneration**: when only one
FlowGroup changes, only one Python file regenerates. It enables **orphan detection**:
when you delete a YAML file, ``lhp state`` can find and remove the now-stale
generated Python. And it enables **CI/CD short-circuits**: pipelines that have not
changed since the last commit skip generation entirely.

Multi-environment generation
----------------------------

LHP is environment-aware. The same FlowGroup YAML generates different Python files
per environment because ``${env_token}`` resolves against ``substitutions/<env>.yaml``.
State is per-environment too — the ``environment`` field in ``.lhp_state.json`` keys
the cache, so a ``dev`` and ``prod`` build maintain separate incremental state.
``substitutions/lhp.yaml`` provides shared defaults that environment files override.

Multi-job orchestration
-----------------------

A FlowGroup can declare ``job_name: <name>`` to opt into the multi-job model. When
*any* FlowGroup sets ``job_name``, *every* FlowGroup must — the all-or-nothing rule
prevents partial orchestration. LHP then generates one Databricks job per unique
``job_name`` plus a master orchestration job that chains them, and writes per-job
configuration from multi-document ``job_config.yaml``. The orchestration generator
lives in ``core/services/job_generator.py``; full configuration semantics are in
:doc:`bundle_config_reference`.

How this maps to the codebase
-----------------------------

The objects above correspond directly to Python modules:

- ``src/lhp/models/config.py`` — Pydantic models for every YAML object.
- ``src/lhp/parsers/yaml_parser.py`` — YAML → model conversion.
- ``src/lhp/core/services/flowgroup_processor.py`` — substitution + preset + template
  pipeline.
- ``src/lhp/core/action_registry.py`` — action-type → generator mapping.
- ``src/lhp/generators/{load,transform,write,test}/`` — one generator per sub-type.
- ``src/lhp/core/orchestrator.py`` — top-level coordination across phases.
- ``src/lhp/utils/smart_file_writer.py`` — content-aware file writes.

The model is intentionally narrow. The four action types and their sub-type lists are
closed enums, not extension points — adding a new sub-type means adding a generator,
registering it in ``ActionRegistry``, and adding an enum value. Reuse extension is
expected to happen through Presets, Templates, and Blueprints, not through new
action types.

.. seealso::
   :doc:`decisions` for when to use which primitive,
   :doc:`actions/index` for the full action reference, and
   :doc:`substitutions` for the full substitution syntax reference.
