Blueprints Reference
====================

.. meta::
   :description: Reusable parametrised pipeline shapes. Define one blueprint, instantiate per site/tenant/region with a tiny instance file. Eliminates copy-paste across N similar deployments.

A **blueprint** is a reusable collection of flowgroups instantiated once per
entry in an *instances* registry. Reach for a blueprint when many sites,
tenants, or regions share the same pipeline shape but vary by a handful of
parameters (site name, connection string, partition key).

A change to the shape lives in **one** file. A new site is a 5-line file.

What is a Blueprint?
--------------------

A blueprint is a YAML file under ``blueprints/`` that declares two things:

1. **Parameters** — the values that vary per deployment (e.g.
   ``site_name``, ``source_connection``).
2. **Flowgroup specs** — a list of flowgroup shapes the blueprint will
   produce, parameterised by those values.

Each *instance* file under ``pipelines/`` references the blueprint by
name and supplies parameter values. At generation time, the expander
turns each ``(blueprint × instance × spec)`` triple into a synthetic
flowgroup that flows through the same processing pipeline as a
hand-written flowgroup file. From that point on, synthetic and
hand-written flowgroups are indistinguishable.

The Five LHP Primitives
~~~~~~~~~~~~~~~~~~~~~~~

LHP has five reusability primitives. They are not alternatives — they
layer.

.. list-table::
   :header-rows: 1
   :widths: 18 42 22 18

   * - Primitive
     - What it is
     - Reuse axis
     - Where it lives
   * - **Action**
     - Atomic load / transform / write step
     - n/a
     - Inside a flowgroup
   * - **Preset**
     - A bundle of action defaults applied by deep merge to actions of a
       matching type
     - Across actions of one type within (or many) flowgroups
     - ``presets/*.yaml``
   * - **Template**
     - A parametrised list of actions rendered into a flowgroup at
       generation time
     - Inside a single flowgroup — a callable "macro" expanded into the
       flowgroup's actions
     - ``templates/*.yaml``
   * - **FlowGroup**
     - One concrete pipeline definition
     - n/a — the unit of generation
     - ``pipelines/**/*.yaml``
   * - **Blueprint**
     - A parametrised *list of flowgroup specs* expanded once per instance
     - Across N similar deployments — one shape, many sites
     - ``blueprints/*.yaml``
   * - **Instance**
     - A small file that supplies parameters and references a blueprint
     - Per-deployment instantiation of a blueprint
     - ``pipelines/**/*.yaml`` (default — co-located)

Templates vs Blueprints
~~~~~~~~~~~~~~~~~~~~~~~

Both reduce repetition, but at different levels of granularity.

.. list-table::
   :header-rows: 1
   :widths: 50 50

   * - Template
     - Blueprint
   * - Reuse **within one flowgroup**.
     - Reuse **across many flowgroups**.
   * - "I keep writing the same three actions to ingest a CSV table — let
       me parametrise them by table name."
     - "I keep duplicating the same bronze/silver shape across 10 regional
       sites — let me parametrise it by ``site_name``."
   * - One template + one flowgroup → one flowgroup's actions.
     - One blueprint + N instances → N × M synthetic flowgroups.
   * - Used via ``use_template:`` in a flowgroup file.
     - Used via ``use_blueprint:`` in an instance file.
   * - Parameters substituted as Jinja2 ``{{ var }}`` expressions.
     - Parameters substituted as ``%{var}`` (local-variable) expressions.

The two compose: a blueprint flowgroup spec can declare ``use_template:``,
exactly like a regular disk-sourced flowgroup. So a blueprint that fans
ingestion across 10 sites can still call into a CSV-ingestion template
inside each spec.

Concept Hierarchy
~~~~~~~~~~~~~~~~~

The five primitives form a top-down hierarchy from the broadest reuse
mechanism (Blueprint — one shape across many deployments) down to the
atomic unit of work (Action — one load, transform, or write step).
Templates and Presets are *cross-cutting reusables* that compose into
FlowGroups and Actions respectively.

.. mermaid::

   graph TB
       Blueprint["<b>Blueprint</b><br/><i>blueprints/*.yaml</i><br/>parameters + N flowgroup specs"]
       Instance["<b>Instance</b><br/><i>pipelines/.../site_x.yaml</i><br/>parameter values for one deployment"]
       FlowGroup["<b>FlowGroup</b><br/><i>pipelines/.../*.yaml or synthetic</i><br/>concrete pipeline definition"]
       Template["<b>Template</b><br/><i>templates/*.yaml</i><br/>reusable action sequence"]
       Action["<b>Action</b><br/>load / transform / write step"]
       Preset["<b>Preset</b><br/><i>presets/*.yaml</i><br/>action default bundle"]

       Blueprint -->|"instantiated by N×"| Instance
       Instance -->|"expands into synthetic"| FlowGroup
       FlowGroup -->|"contains 1..N"| Action
       FlowGroup -.->|"use_template:"| Template
       Template -.->|"renders into"| Action
       Action -.->|"presets: merged in"| Preset

**Reading the diagram.** Solid arrows trace the required production chain
(Blueprint → Instance → FlowGroup → Action). Dotted arrows mark optional
composition — a FlowGroup *may* call a Template (which *renders into*
Actions), and an Action *may* have one or more Presets merged in.

A FlowGroup can also exist without a Blueprint above it (a regular
hand-written ``pipelines/.../*.yaml`` file). Synthetic flowgroups
produced by Blueprint × Instance expansion and disk-sourced flowgroups
are indistinguishable downstream — they enter the same processing
pipeline.

When to Reach for Blueprints
----------------------------

Use a blueprint when **all** of these are true:

* You'd otherwise be copy-pasting the same flowgroup files across N
  sites/tenants/regions.
* The shape is identical across N — only parameter values differ.
* N is large enough that maintaining N copies is painful (typically ten
  or more).

If two sites genuinely have **different shapes** (one ingests CDC, the
other batch), make them **different blueprints**. Don't try to make one
blueprint flexible enough for both — that's the override anti-pattern,
and LHP deliberately does not support it.

For one-off pipelines, write regular flowgroup files under
``pipelines/``. Blueprints exist to eliminate fan-out repetition, not to
be a default authoring style.

How to Use Blueprints
---------------------

Project Layout
~~~~~~~~~~~~~~

A blueprint-using project looks like a normal LHP project with a
``blueprints/`` directory added:

.. code-block:: text

   your_project/
   ├── lhp.yaml
   ├── blueprints/
   │   └── erp_ingestion.yaml          ← the shape (one file)
   ├── pipelines/
   │   ├── sites/
   │   │   ├── apac_sg.yaml             ← instance — ~5 lines
   │   │   ├── emea_uk.yaml
   │   │   └── latam_br.yaml
   │   └── shared/
   │       └── orchestration.yaml       ← regular flowgroup (alongside)
   ├── templates/                       ← optional — referenced from blueprints
   ├── presets/                         ← optional — referenced from blueprints
   └── substitutions/
       └── dev.yaml

**Default discovery globs:**

.. code-block:: yaml

   blueprint_include: ["blueprints/**/*.yaml", "blueprints/**/*.yml"]
   instance_include:  ["pipelines/**/*.yaml",  "pipelines/**/*.yml"]

Note that ``instance_include`` and the regular ``include`` glob both
match ``pipelines/**/*.yaml`` by default. **This is intentional.** The
discoverer routes by *content shape*: files with a top-level
``use_blueprint:`` key are treated as instances; everything else is
treated as a flowgroup. So instance files can sit next to the regular
flowgroups they relate to without any include-list contortions.

You only need to set ``blueprint_include`` or ``instance_include`` in
``lhp.yaml`` if you want to **restrict** discovery (during phased
rollouts, for instance — see :ref:`working-example` below).

Most projects need no change. Defaults are sufficient.

.. code-block:: yaml
   :caption: lhp.yaml — minimum configuration

   name: my_project
   version: "1.0"
   include:
     - "pipelines/**/*.yaml"

   # blueprint_include and instance_include default to:
   #   blueprint_include: ["blueprints/**/*.yaml"]
   #   instance_include:  ["pipelines/**/*.yaml"]

Quickstart
~~~~~~~~~~

A complete copy-paste example: one blueprint, two instances, two
generated pipelines.

**Step 1 — Set up the directory structure:**

.. code-block:: bash

   mkdir -p my_test/{blueprints,pipelines/sites,substitutions}
   cd my_test

**Step 2 — Create the project config and substitutions:**

.. code-block:: yaml
   :caption: lhp.yaml

   name: my_test
   version: "1.0"
   include:
     - "pipelines/**/*.yaml"

.. code-block:: yaml
   :caption: substitutions/dev.yaml

   tokens: {}
   secrets: {}

**Step 3 — Define the blueprint** (the shape, written once):

.. code-block:: yaml
   :caption: blueprints/erp_ingestion.yaml

   name: erp_ingestion
   version: "1.0"
   parameters:
     - name: site_name
       required: true
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
             catalog: dev_catalog
             schema: bronze
             table: "%{site_name}_orders_raw"
             partition_columns:
               - "%{partition_key}"

**Step 4 — Add two instances** (one per site, ~5 lines each):

.. code-block:: yaml
   :caption: pipelines/sites/apac_sg.yaml

   use_blueprint: erp_ingestion
   parameters:
     site_name: apac_sg
     site_id: SG001

.. code-block:: yaml
   :caption: pipelines/sites/emea_uk.yaml

   use_blueprint: erp_ingestion
   parameters:
     site_name: emea_uk
     site_id: UK001
     partition_key: uk_order_date         # parameter override

**Step 5 — Inspect, validate, generate:**

.. code-block:: bash

   lhp list-blueprints
   lhp validate --env dev
   lhp generate --env dev
   ls generated/dev/

Two pipelines (``apac_sg_erp_raw``, ``emea_uk_erp_raw``) appear under
``generated/dev/``, each with one generated flowgroup file.

To add a third site, create a new instance file (e.g.
``pipelines/sites/latam_br.yaml``) and re-run ``lhp generate`` —
incremental regeneration produces only the new flowgroups.

.. _working-example:

Working Example (Real-World)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The repository contains a complete worked example at
``Example_Projects/performance_testing_blueprint/``:

* **One blueprint** (``blueprints/medallion_pipeline.yaml``) declares
  200 flowgroup specs across 5 medallion layers (raw → bronze → silver
  → gold → modelled) × 40 entities.
* **Many instance files** under
  ``pipelines/multisite_source_system/sites/site_*.yaml``, each ~5
  lines long, demonstrating co-location with regular flowgroup
  directories.
* **Three templates** under ``templates/`` referenced from a subset of
  specs (parquet ingestion, bronze cleanse, silver CDC).
* **One preset** (``presets/default_delta_properties.yaml``) referenced
  from many specs.
* A developer-side helper, ``build_blueprint.py``, regenerates the
  blueprint YAML when the entity list or pattern mix changes. **It does
  not generate instance files** — those are hand-written.

The example also illustrates a useful operational pattern:
``instance_include`` is overridden in ``lhp.yaml`` to limit discovery
to a subset of sites during development:

.. code-block:: yaml
   :caption: Example_Projects/performance_testing_blueprint/lhp.yaml

   instance_include:
     - "pipelines/**/site_a.yaml"
     - "pipelines/**/site_b.yaml"
     - "pipelines/**/site_c.yaml"

Run it end-to-end:

.. code-block:: bash

   cd Example_Projects/performance_testing_blueprint
   lhp list-blueprints
   lhp validate --env dev
   lhp generate --env dev

Advanced Topics
---------------

Lifecycle and Substitution Order
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

After discovery, every flowgroup — disk-sourced or synthetic — flows
through the same processing pipeline. Blueprint expansion is the only
step that runs *before* the regular flowgroup pipeline.

.. mermaid::

   graph TD
       A["Discovery<br/>blueprints/, pipelines/, templates/, presets/"]
       B["Step 0 — Blueprint expansion<br/>%{var} resolved in pipeline:/flowgroup:<br/>${...} rejected in those fields<br/>(pipeline, flowgroup) tuples deduplicated"]
       C["Step 0.5 — Local variables<br/>%{var} resolved everywhere else"]
       D["Step 1 — Template render<br/>Jinja2 {{ }} parameters substituted"]
       E["Steps 1.5 / 2 — Preset merge<br/>action defaults applied"]
       F["Step 3 — Environment + secrets<br/>${env_token}, ${secret:scope/key}"]
       G["Steps 3.5 – 5 — Validation<br/>unresolved tokens, flowgroup shape, secret refs"]
       H["Code generation<br/>Python files in generated/&lt;env&gt;/"]

       A --> B --> C --> D --> E --> F --> G --> H

The substitution rules below follow directly from this order:
``%{var}`` resolves before templates render, environment tokens resolve
after templates render, and validation runs last so it sees fully
substituted values.

Blueprint File Reference
~~~~~~~~~~~~~~~~~~~~~~~~

A more sophisticated blueprint than the Quickstart — two flowgroup
specs, multiple parameters, one with a default:

.. code-block:: yaml
   :caption: blueprints/erp_ingestion.yaml
   :linenos:

   name: erp_ingestion
   version: "1.0"
   description: "Standard ERP ingestion for all regional sites"

   # Parameters the instance must (or may) supply.
   parameters:
     - name: site_name
       required: true
       description: "Site identifier (e.g., apac_sg)"
     - name: site_id
       required: true
     - name: source_connection
       required: true
     - name: partition_key
       required: false
       default: order_date

   # Each entry below (a flowgroup spec) is expanded once per instance.
   # `pipeline` and `flowgroup` are templates — only %{var} is allowed here.
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
             catalog: ${catalog}     # ${env_token} OK inside actions
             schema: bronze
             table: "%{site_name}_orders_raw"
             partition_columns:
               - "%{partition_key}"

     - pipeline: "%{site_name}_erp_raw"
       flowgroup: "%{site_name}_customers_ingestion"
       actions:
         - name: load_customers
           type: load
           source:
             type: cloudfiles
             path: "/Volumes/raw/erp/%{site_name}/customers"
             format: json
           target: v_customers_raw
         - name: write_customers
           type: write
           source: v_customers_raw
           write_target:
             type: streaming_table
             catalog: ${catalog}
             schema: bronze
             table: "%{site_name}_customers_raw"

**Top-level fields:**

.. list-table::
   :header-rows: 1
   :widths: 18 12 70

   * - Field
     - Required
     - Notes
   * - ``name``
     - yes
     - Unique across the project. Two blueprints with the same name →
       ``LHP-VAL-046``.
   * - ``version``
     - no
     - Defaults to ``"1.0"``. Free-form string for change tracking.
   * - ``description``
     - no
     - Surfaced by ``lhp list-blueprints``.
   * - ``parameters``
     - no
     - List of parameter declarations (see below).
   * - ``flowgroups``
     - yes
     - List of flowgroup specs (see below). Empty list is a parse error.

**Parameter declarations:**

.. list-table::
   :header-rows: 1
   :widths: 18 12 70

   * - Field
     - Required
     - Notes
   * - ``name``
     - yes
     - The key the instance file uses to bind a value.
   * - ``required``
     - no
     - Defaults to ``false``. If ``true``, every instance must supply
       this parameter or fail with ``LHP-VAL-042``.
   * - ``default``
     - no
     - Used when ``required: false`` and the instance omits the value.
   * - ``description``
     - no
     - Surfaced by ``lhp list-blueprints --verbose`` and the IDE schema.

Strict mode is always on: an unknown parameter key in an instance file
raises ``LHP-VAL-043`` with a "did you mean…" suggestion based on the
declared parameter names.

**Flowgroup specs.** A *flowgroup spec* is one entry in the
blueprint's ``flowgroups:`` list — not the blueprint itself. The
blueprint is the whole file (``parameters:`` plus ``flowgroups:``); a
flowgroup spec is one item inside that ``flowgroups:`` list, describing
the shape of one flowgroup the blueprint will produce per instance. A
blueprint with three specs and five instances expands into 15
generated flowgroups (3 × 5).

A flowgroup spec accepts every field a regular flowgroup accepts:
``pipeline``, ``flowgroup``, ``job_name``, ``variables``, ``presets``,
``use_template``, ``template_parameters``, ``actions``,
``operational_metadata``. **Templates and presets work transparently
inside flowgroup specs** (see :ref:`composition` below).

The two **mandatory** fields are ``pipeline`` and ``flowgroup``. Both
are treated as templates that must resolve to a non-empty string when
``%{var}`` substitution runs at expansion time.

Substitutions and Variables
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 28 38 34

   * - Token
     - Where it works
     - When it resolves
   * - ``%{var}``
     - Anywhere in the blueprint.
     - In ``pipeline:`` / ``flowgroup:`` fields → at Step 0 (expansion).
       Elsewhere → at Step 0.5 (per-flowgroup, after expansion).
   * - ``${env_token}``
     - Anywhere **except** ``pipeline:`` and ``flowgroup:`` template
       strings.
     - Step 3 (substitution phase).
   * - ``${secret:scope/key}``
     - Same as ``${env_token}``.
     - Step 3 (substitution phase).

The expander rejects **any** ``${...}`` token in ``pipeline:`` or
``flowgroup:`` template strings with ``LHP-VAL-044``. The rejection
regex is ``\$\{[^}]+\}``, so this applies equally to environment tokens
and secret references.

The reason: resolved ``(pipeline, flowgroup)`` tuples are used as keys
in the source-path index and the state-tracking layer, both of which
are built *before* environment substitution runs. If those keys
depended on ``${env_token}`` they would be unstable across environments
and incremental regeneration would break.

Local variables vs substitution tokens
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A common first-day mistake is to put a **substitution token**
(``${catalog}``, ``${env}``, or any other ``${...}``) inside a
``pipeline:`` or ``flowgroup:`` template, expecting it to resolve at
generation time. The expander rejects this with ``LHP-VAL-044`` — only
**local variables** (``%{var}``, sourced from instance parameters and
``spec.variables``) are permitted in those two fields. Substitution
tokens work normally in every other field.

.. code-block:: yaml
   :caption: ❌ Wrong — substitution token inside pipeline:/flowgroup:

   flowgroups:
     - pipeline:  "${catalog}_${site_name}_raw"     # LHP-VAL-044
       flowgroup: "${env}_orders_${site_name}"      # LHP-VAL-044
       actions:
         - name: write_orders
           type: write
           source: v_orders_raw
           write_target:
             type: streaming_table
             catalog: ${catalog}
             schema: bronze
             table: orders_raw

The same error fires on either field, and applies equally to
``${secret:scope/key}`` references — the rejection regex
``\$\{[^}]+\}`` does not distinguish environment tokens from secret
references.

.. code-block:: yaml
   :caption: ✅ Right — pipeline:/flowgroup: use only %{var}

   flowgroups:
     - pipeline:  "%{site_name}_raw"                # only %{var} permitted
       flowgroup: "%{site_name}_orders"             # only %{var} permitted
       actions:
         - name: write_orders
           type: write
           source: v_orders_raw
           write_target:
             type: streaming_table
             catalog: ${catalog}                    # substitution token OK here
             schema: bronze
             table: "%{site_name}_orders_raw"       # %{var} OK here too

**Mnemonic.** In ``pipeline:`` and ``flowgroup:`` — local variables
(``%{var}``) only; substitution tokens (``${...}``) are rejected.
Everywhere else — both syntaxes work. ``%{var}`` resolves at expansion
(Step 0), so it is environment-invariant; ``${...}`` resolves at
substitution (Step 3), which is why it is unsafe in fields that become
index keys before Step 3 runs.

Local ``variables`` inside a flowgroup spec
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A flowgroup spec can declare its own derived local variables under a
``variables:`` key. ``%{var}`` references elsewhere in the same spec
then see both instance parameters and these derived values:

.. code-block:: yaml

   flowgroups:
     - pipeline: "%{site_name}_erp_raw"
       flowgroup: "%{site_name}_orders_ingestion"
       variables:                                  # ← inside this flowgroup spec
         raw_table: "raw_%{site_name}_orders"     # derived from instance param
       actions:
         - name: write_orders
           # ...
           write_target:
             table: "%{raw_table}"                 # resolves to raw_apac_sg_orders, etc.

**Precedence:** the flowgroup spec's ``variables`` block wins over
instance parameters on key conflict. This protects blueprint-author
intent if an instance file accidentally defines a parameter with the
same name as a derived variable inside a spec.

.. _composition:

Composition with Templates and Presets
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Blueprints, templates, and presets compose cleanly. Each addresses a
different reuse axis:

* A **template** generates a reusable list of actions inside one
  flowgroup.
* A **preset** applies action defaults inside a flowgroup.
* A **blueprint** generates a reusable set of flowgroups across
  deployments.

A blueprint flowgroup spec can use both templates and presets, exactly
like a regular disk-sourced flowgroup. The synthetic flowgroup the
expander produces flows through the same processing pipeline:

1. **Step 0** — Blueprint expansion (only step exclusive to
   blueprints). ``%{var}`` resolved in ``pipeline:`` / ``flowgroup:``
   fields; ``${...}`` rejected there.
2. **Step 0.5** — Local variables. ``%{var}`` resolved everywhere else,
   using the merged map of (instance parameters + ``spec.variables``).
3. **Step 1** — If ``use_template:`` is set, the template's actions are
   rendered with ``template_parameters`` substituted as Jinja2
   ``{{ ... }}`` expressions.
4. **Steps 1.5 / 2** — Presets are merged into actions (template-level
   presets first, then flowgroup-level presets).
5. **Step 3** — ``${env_token}`` and ``${secret:scope/key}`` resolved
   from the active environment.
6. **Steps 3.5 – 5** — Validation: unresolved tokens, flowgroup shape,
   secret references.
7. **Code generation** — Python files written under
   ``generated/<env>/``.

Worked example — blueprint that uses both a template and a preset:

.. code-block:: yaml
   :caption: presets/bronze_defaults.yaml

   name: bronze_defaults
   version: "1.0"
   defaults:
     write:
       write_target:
         type: streaming_table
         table_properties:
           "delta.appendOnly": "true"

.. code-block:: yaml
   :caption: templates/cloudfiles_csv.yaml

   name: cloudfiles_csv
   version: "1.0"
   parameters:
     - name: table_name
       type: string
       required: true
     - name: landing_path
       type: string
       required: true
   actions:
     - name: "load_{{ table_name }}_csv"
       type: load
       source:
         type: cloudfiles
         path: "{{ landing_path }}"
         format: csv
       target: "v_{{ table_name }}"
     - name: "write_{{ table_name }}"
       type: write
       source: "v_{{ table_name }}"
       write_target:
         catalog: ${catalog}
         schema: bronze
         table: "{{ table_name }}_raw"

.. code-block:: yaml
   :caption: blueprints/site_ingestion.yaml

   name: site_ingestion
   parameters:
     - name: site_name
       required: true
     - name: customer_path
       required: true
     - name: order_path
       required: true
   flowgroups:
     - pipeline: "%{site_name}_bronze"
       flowgroup: "%{site_name}_customers"
       presets:
         - bronze_defaults
       use_template: cloudfiles_csv
       template_parameters:
         table_name: "%{site_name}_customers"
         landing_path: "%{customer_path}"
     - pipeline: "%{site_name}_bronze"
       flowgroup: "%{site_name}_orders"
       presets:
         - bronze_defaults
       use_template: cloudfiles_csv
       template_parameters:
         table_name: "%{site_name}_orders"
         landing_path: "%{order_path}"

.. code-block:: yaml
   :caption: pipelines/sites/site_a.yaml

   use_blueprint: site_ingestion
   parameters:
     site_name: site_a
     customer_path: "/Volumes/raw/site_a/customers/*.csv"
     order_path: "/Volumes/raw/site_a/orders/*.csv"

Two instances of this blueprint produce four flowgroups across two
pipelines, each composed of two preset-applied actions rendered from
the template. ``%{site_name}`` resolves at expansion (Step 0 for
``pipeline:``/``flowgroup:``, Step 0.5 for ``template_parameters:``);
``{{ table_name }}`` and ``{{ landing_path }}`` resolve at Step 1 when
the template renders; ``${catalog}`` resolves at Step 3.

Instance File Reference
~~~~~~~~~~~~~~~~~~~~~~~

**Instance file fields:**

.. list-table::
   :header-rows: 1
   :widths: 22 12 66

   * - Field
     - Required
     - Notes
   * - ``use_blueprint``
     - yes
     - The ``name:`` of the target blueprint. Unknown name →
       ``LHP-VAL-041`` with a "did you mean…" suggestion.
   * - ``parameters``
     - no
     - A flat map of ``parameter_name: value``. Required parameters
       declared by the blueprint must be present here.

The instance file format is **strict** (``extra="forbid"`` on the
underlying model) — any top-level key other than the two above raises a
parse error. This strictness is what makes content-shape routing
reliable: any YAML file matching ``instance_include`` is unambiguously
classified as instance, flowgroup, or error.

**Instance file location.** By default, instance files match
``pipelines/**/*.yaml`` — the same glob used for hand-written
flowgroups. The discoverer routes by content shape:

* File has a top-level ``use_blueprint:`` key → **instance**.
* File has top-level ``flowgroup:`` and ``actions:`` (or
  ``use_template:``) → **flowgroup**.
* File has top-level ``parameters:`` and ``flowgroups:`` but lives
  under a flowgroup-include glob → **error** (``LHP-CFG-040``:
  blueprint file in flowgroup directory; move it to ``blueprints/``).

You can place instance files anywhere matched by ``instance_include``.
Common patterns:

* **Co-located with related flowgroups** —
  ``pipelines/sites/apac_sg.yaml`` next to a
  ``pipelines/sites/orchestration.yaml`` flowgroup. (This is the
  default and what the bundled example project does.)
* **Grouped under a per-site directory** —
  ``pipelines/sites/apac_sg/_instance.yaml`` plus other site-specific
  flowgroups in the same directory.
* **Held in a dedicated directory** — set ``instance_include`` to
  ``["instances/**/*.yaml"]`` and store them under ``instances/``.

CLI Commands
~~~~~~~~~~~~

**lhp list-blueprints**

.. code-block:: bash

   lhp list-blueprints              # summary table
   lhp list-blueprints --verbose    # per-instance breakdown of expansion

Verbose mode runs the expander against each discovered instance and
shows every ``(pipeline, flowgroup)`` tuple it produces. Faster than
``lhp generate`` for sanity-checking instance parameters.

**lhp show**

.. code-block:: bash

   # Show the resolved configuration for one instance
   lhp show --instance pipelines/sites/apac_sg.yaml --env dev

The ``--instance`` flag is mutually exclusive with the positional
``flowgroup`` argument. Useful for debugging a misconfigured instance —
surfaces what the expander produces *before* any code is generated.

**lhp validate**

.. code-block:: bash

   lhp validate --env dev

Runs the full blueprint + flowgroup validation stack. Surfaces every
error code listed in :ref:`error-reference` below, plus the regular
flowgroup, secret, and token validations.

**lhp deps**

.. code-block:: bash

   lhp deps                            # default — synthetic flowgroups deduplicated
   lhp deps --expand-blueprints        # show per-instance edges
   lhp deps --blueprint erp_ingestion  # restrict to one blueprint's subgraph

By default, the dependency graph deduplicates synthetic flowgroups by
``(blueprint_name, spec_index)`` — one logical edge per flowgroup spec
instead of N edges (one per instance). Pass ``--expand-blueprints`` to
see the literal expansion. See :doc:`dependency_analysis` for the full
``deps`` reference.

Incremental Regeneration
~~~~~~~~~~~~~~~~~~~~~~~~

State tracking treats blueprint-driven flowgroups carefully so that
day-to-day edits regenerate the minimum necessary.

.. list-table::
   :header-rows: 1
   :widths: 40 60

   * - What you did
     - What ``lhp generate --env dev`` does
   * - Edit ``pipelines/sites/apac_sg.yaml``
     - Regenerates **only** that instance's flowgroups. The instance
       file is tracked as a ``type='instance'`` dependency, so its
       checksum mismatch invalidates only that site.
   * - Edit ``blueprints/erp_ingestion.yaml``
     - Regenerates **all** instances that use that blueprint. The
       source-YAML checksum mismatches on every expanded flowgroup.
   * - Add ``pipelines/sites/latam_br.yaml``
     - Generates the new site's flowgroups. Smart filtering detects
       them as new even though no on-disk flowgroup checksum changed.
   * - Delete ``pipelines/sites/emea_uk.yaml``
     - Cleans up emea_uk's generated files via the orphan-detection
       fast path on the next ``generate``.
   * - Edit a preset/template referenced by the blueprint
     - Regular preset/template dependency tracking regenerates affected
       files.

Anti-Patterns
~~~~~~~~~~~~~

**Don't make one blueprint serve two genuinely different shapes.** The
override mechanism was deliberately omitted. If a site needs a
different shape, write a different blueprint. Conditional logic inside
a single blueprint becomes unmaintainable quickly.

**Don't put a substitution token (``${...}``) inside ``pipeline:`` or
``flowgroup:`` templates.** Validation will reject it
(``LHP-VAL-044``). Only local variables (``%{var}``) are permitted in
those two fields. If an env-driven catalog/schema is required, put
``${catalog}`` inside an action's ``write_target`` — it resolves there.

**Don't put a blueprint file under ``pipelines/``.** The discriminator
catches it (``LHP-CFG-040``), but the experience is clearer when
blueprints live in ``blueprints/`` from the start.

**Don't create instance files where the same parameter values produce
the same ``(pipeline, flowgroup)`` tuple.** This is typically a
copy-paste error. The expander rejects this with ``LHP-VAL-045`` and
names both files in the error message.

.. _error-reference:

Error Reference
~~~~~~~~~~~~~~~

Blueprint-related error codes. See :doc:`errors_reference` for the
project-wide reference.

.. list-table::
   :header-rows: 1
   :widths: 18 50 32

   * - Code
     - Meaning
     - Typical fix
   * - ``LHP-CFG-040``
     - Blueprint file matched by an instance or flowgroup glob.
     - Move to ``blueprints/`` or adjust ``include`` /
       ``blueprint_include``.
   * - ``LHP-VAL-041``
     - Instance references a blueprint name that doesn't exist.
     - Check ``use_blueprint:`` value against ``lhp list-blueprints``.
   * - ``LHP-VAL-042``
     - Instance is missing a required parameter.
     - Add the parameter, or mark the declaration ``required: false``
       with a ``default`` in the blueprint.
   * - ``LHP-VAL-043``
     - Instance has an unknown parameter key. Often a typo.
     - The error includes a "did you mean…" suggestion.
   * - ``LHP-VAL-044``
     - A substitution token (``${env_token}`` or ``${secret:…}``)
       appears in a ``pipeline:`` or ``flowgroup:`` template string.
     - Use only local variables (``%{var}``) in those two fields.
   * - ``LHP-VAL-045``
     - Two instances produce the same ``(pipeline, flowgroup)`` tuple
       after expansion.
     - Both file paths are in the error context — usually a copy-paste.
   * - ``LHP-VAL-046``
     - Two blueprint files declare the same ``name:`` value.
     - Rename one or remove the duplicate.
   * - ``LHP-VAL-047`` … ``050``
     - Blueprint file shape errors: empty file, multi-document YAML,
       missing ``flowgroups:``, etc.
     - Compare against the blueprint schema and fix the structure.
   * - ``LHP-VAL-051`` … ``054``
     - Instance file shape errors: empty, multi-document, or wrong
       structure.
     - Use the canonical ``use_blueprint:`` + ``parameters:`` form.
   * - ``LHP-VAL-055``
     - Unresolved ``%{var}`` in a ``pipeline:`` or ``flowgroup:``
       template.
     - Confirm the parameter is declared and the instance supplies a
       value.
   * - ``LHP-CFG-057`` / ``058``
     - ``lhp show`` was called with both ``--instance`` and a
       positional flowgroup argument (or neither).
     - Pick exactly one form.
   * - ``LHP-VAL-059``
     - Instance file resolves to a path outside the project root.
     - Move the instance file inside the project directory.

See Also
--------

* :doc:`templates_reference` — reusable action sequences referenced by
  ``use_template:``.
* :doc:`dynamic_templates_guide` — Jinja2 conditionals and loops inside
  templates.
* :doc:`presets_reference` — reusable action defaults referenced by
  ``presets:``.
* :doc:`concepts` — the overall pipeline / flowgroup / action model.
* :doc:`substitutions` — environment tokens, local variables, secret
  references.
* :doc:`errors_reference` — full project-wide error code reference.
