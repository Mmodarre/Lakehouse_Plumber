Dynamic Templates Guide
=======================

.. meta::
   :description: Build dynamic templates with conditional logic, loops, and advanced Jinja2 features for complex pipeline patterns.

This guide covers the **dynamic capabilities** of LHP templates — conditionals, loops, filters, and advanced parameter patterns that go beyond simple ``{{ variable }}`` substitution. For basic template structure and parameter types, see :doc:`templates_reference`.


How Template Rendering Works
-----------------------------

LHP templates use **Jinja2** as their rendering engine. When LHP processes a template, every string value in the ``actions:`` section is checked for Jinja2 syntax. If a string contains ``{{`` and ``}}``, the entire string is rendered through Jinja2, which means **all** Jinja2 features are available within that string — including ``{% if %}``, ``{% for %}``, filters, and more.

**Processing pipeline:**

.. code-block:: text

   Flowgroup YAML
     │
     ├─ 1. Local variables (%{var}) resolved first
     ├─ 2. Template expansion (Jinja2 renders {{ }} and {% %} blocks)
     ├─ 3. Presets applied (template-level, then flowgroup-level)
     ├─ 4. Environment tokens (${token}) resolved
     └─ 5. Validation

.. seealso::
   For full details on each substitution stage, see :doc:`substitutions`.

.. important::

   Jinja2 processing is triggered when a string contains **both** ``{{`` and ``}}``.
   If a string only contains ``{% %}`` block tags without any ``{{ }}`` expression,
   it will **not** be processed by Jinja2. Always include at least one ``{{ }}``
   expression in any string that uses ``{% if %}`` or ``{% for %}``.

Parameter Substitution Basics
-----------------------------

The simplest dynamic feature is parameter substitution using ``{{ parameter_name }}``:

.. code-block:: yaml
   :caption: Template definition

   name: simple_ingestion
   parameters:
     - name: table_name
       type: string
       required: true
     - name: landing_folder
       type: string
       required: true

   actions:
     - name: load_{{ table_name }}_csv
       type: load
       source:
         type: cloudfiles
         path: "${landing_path}/{{ landing_folder }}/"
       target: vw_{{ table_name }}_raw

.. code-block:: yaml
   :caption: Flowgroup using the template

   pipeline: my_pipeline
   flowgroup: customer_ingestion
   use_template: simple_ingestion
   template_parameters:
     table_name: customer
     landing_folder: customer_data

**Result:** LHP generates actions with ``load_customer_csv``, paths pointing to ``customer_data/``, and target ``vw_customer_raw``.

Notice how ``{{ template_param }}`` and ``${env_token}`` coexist in the same string — template parameters are resolved first (step 2), environment tokens later (step 4).


Conditional Logic with ``{% if %}``
------------------------------------

Use Jinja2 ``{% if %}`` blocks to conditionally include or exclude parts of a value based on template parameters.

Basic Conditional
~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: Template with conditional surrogate key generation (TMPL004 pattern)

   name: raw_to_bronze_template
   parameters:
     - name: raw_table_name
       required: true
     - name: bronze_table_name
       required: true
     - name: generate_surrogate_key
       required: false
       default: false
       description: "Whether to generate a surrogate key using xxhash64"
     - name: surrogate_key_name
       required: false
       default: "surrogate_key"

   actions:
     - name: cleanse_{{ raw_table_name }}_delta
       type: transform
       transform_type: sql
       source: vw_{{ bronze_table_name }}_DQE
       target: vw_{{ bronze_table_name }}_cleaned
       sql: |
         SELECT
           {% if generate_surrogate_key %}xxhash64(* except (_processing_timestamp, _source_file_path)) as {{ surrogate_key_name }},
           {% endif %}* except (_rescued_data)
         FROM stream(vw_{{ bronze_table_name }}_DQE)

**How it works:**

- The ``sql`` field is a multi-line string containing both ``{{ }}`` and ``{% %}``.
- Because ``{{ surrogate_key_name }}`` is present, Jinja2 processes the entire string.
- When ``generate_surrogate_key`` is ``true``, the ``xxhash64(...)`` line is included.
- When ``false`` (the default), it is omitted entirely.

**Flowgroup with surrogate key enabled:**

.. code-block:: yaml

   pipeline: bronze_sap
   flowgroup: bronze_sap_prd
   use_template: raw_to_bronze_template
   template_parameters:
     raw_table_name: raw_sap_prd_snapshot
     bronze_table_name: bronze_sap_prd
     generate_surrogate_key: true
     surrogate_key_name: prd_key

**Generated SQL:**

.. code-block:: sql

   SELECT
     xxhash64(* except (_processing_timestamp, _source_file_path)) as prd_key,
     * except (_rescued_data)
   FROM stream(vw_bronze_sap_prd_DQE)

**Flowgroup without surrogate key (using defaults):**

.. code-block:: yaml

   pipeline: bronze_sfcc
   flowgroup: bronze_sfcc_sls_ord
   use_template: raw_to_bronze_template
   template_parameters:
     raw_table_name: raw_sfcc_sls_ord
     bronze_table_name: bronze_sfcc_sls_ord

**Generated SQL:**

.. code-block:: sql

   SELECT
     * except (_rescued_data)
   FROM stream(vw_bronze_sfcc_sls_ord_DQE)

``{% if %}`` / ``{% elif %}`` / ``{% else %}``
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can use the full if/elif/else chain:

.. code-block:: yaml
   :caption: Template with multiple conditional branches

   actions:
     - name: transform_{{ table_name }}
       type: transform
       transform_type: sql
       source: vw_{{ table_name }}_raw
       target: vw_{{ table_name }}_cleaned
       sql: |
         SELECT *
         {% if dedup_strategy == "latest" %}, ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ timestamp_col }} DESC) as rn
         {% elif dedup_strategy == "earliest" %}, ROW_NUMBER() OVER (PARTITION BY {{ primary_key }} ORDER BY {{ timestamp_col }} ASC) as rn
         {% else %}
         {% endif %}
         FROM stream(vw_{{ table_name }}_raw)

Truthiness Check on Collections
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

You can check if a list or dict parameter is non-empty:

.. code-block:: yaml

   sql: |
     SELECT
       {{ primary_key }}{% if additional_columns %},
       {% for col in additional_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}{% endif %}
     FROM {{ source_table }}

- An empty list ``[]`` evaluates as falsy in Jinja2.
- A non-empty list evaluates as truthy.
- Same applies to empty dicts ``{}`` and empty strings ``""``.


Loops with ``{% for %}``
------------------------

Use ``{% for %}`` to iterate over list (array) parameters, generating repeated SQL fragments, column lists, or other patterns.

Iterating Over a List
~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: Template with dynamic column generation (TMPL007 pattern)

   name: accumulating_fact_template
   parameters:
     - name: table_name
       type: string
       required: true
     - name: source_system
       type: string
       required: true
     - name: group_by_columns
       type: array
       required: true
       description: "Columns for GROUP BY clause"
     - name: status_column
       type: string
       required: true
     - name: status_values
       type: array
       required: true
       description: "Status values to pivot into date columns"
     - name: additional_columns
       type: array
       required: false
       default: []

   actions:
     - name: "create_{{ table_name }}_snapshot_mv"
       type: write
       readMode: batch
       write_target:
         type: materialized_view
         database: "${catalog}.${silver_schema}"
         table: "fct_{{ source_system }}_{{ table_name }}_snapshot"
         sql: |
           SELECT
             {% for col in group_by_columns %}{{ col }},
             {% endfor %}{% for status in status_values %}MIN(CASE WHEN {{ status_column }} = '{{ status }}' THEN last_update_dttm END) as {{ status }}_date,
             {% endfor %}MAX(CASE WHEN __end_at IS NULL THEN {{ status_column }} END) as current_status,
             MAX(CASE WHEN __end_at IS NULL THEN last_update_dttm END) as last_update_dttm{% if additional_columns %},
             {% for col in additional_columns %}MAX(CASE WHEN __end_at IS NULL THEN {{ col }} END) as {{ col }}{% if not loop.last %},
             {% endif %}{% endfor %}{% endif %}
           FROM ${catalog}.${silver_schema}.fct_{{ source_system }}_{{ table_name }}_hist
           GROUP BY {% for col in group_by_columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}

**Flowgroup invocation:**

.. code-block:: yaml

   pipeline: silver_facts_sap
   flowgroup: prch_ord_hdr_silver
   use_template: accumulating_fact_template
   template_parameters:
     table_name: prch_ord_hdr
     source_system: sap
     status_column: status
     status_values:
       - "pending"
       - "received"
     group_by_columns:
       - "po_id"
     additional_columns:
       - "currency"
       - "order_date"
       - "expected_date"

**Generated SQL:**

.. code-block:: sql
   :force:

   SELECT
     po_id,
     MIN(CASE WHEN status = 'pending' THEN last_update_dttm END) as pending_date,
     MIN(CASE WHEN status = 'received' THEN last_update_dttm END) as received_date,
     MAX(CASE WHEN __end_at IS NULL THEN status END) as current_status,
     MAX(CASE WHEN __end_at IS NULL THEN last_update_dttm END) as last_update_dttm,
     MAX(CASE WHEN __end_at IS NULL THEN currency END) as currency,
     MAX(CASE WHEN __end_at IS NULL THEN order_date END) as order_date,
     MAX(CASE WHEN __end_at IS NULL THEN expected_date END) as expected_date
   FROM ${catalog}.${silver_schema}.fct_sap_prch_ord_hdr_hist
   GROUP BY po_id

.. note::

   Environment tokens like ``${catalog}`` and ``${silver_schema}`` remain as-is after template
   rendering — they are resolved in step 4 of the pipeline, after Jinja2 has finished.

The ``loop`` Variable
~~~~~~~~~~~~~~~~~~~~~

Inside ``{% for %}``, Jinja2 provides a ``loop`` variable with useful attributes:

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Variable
     - Description
   * - ``loop.index``
     - Current iteration (1-indexed)
   * - ``loop.index0``
     - Current iteration (0-indexed)
   * - ``loop.first``
     - ``True`` if first iteration
   * - ``loop.last``
     - ``True`` if last iteration
   * - ``loop.length``
     - Total number of items

**Common pattern — comma-separated column lists:**

.. code-block:: yaml

   sql: |
     SELECT {% for col in columns %}{{ col }}{% if not loop.last %}, {% endif %}{% endfor %}
     FROM {{ source_table }}

This produces ``SELECT col_a, col_b, col_c`` with commas only between items (no trailing comma).


Jinja2 Filters
--------------

Jinja2 built-in filters are available inside ``{{ }}`` expressions. These let you transform parameter values during rendering.

.. note::

   LHP's template engine uses a bare Jinja2 ``Environment()`` — the custom ``tojson`` and ``toyaml``
   filters are only available in internal code-generation templates, not in user YAML templates.
   Standard Jinja2 built-in filters work normally.

Common Filters
~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 35 40

   * - Filter
     - Usage
     - Result
   * - ``lower``
     - ``{{ name | lower }}``
     - Converts to lowercase
   * - ``upper``
     - ``{{ name | upper }}``
     - Converts to uppercase
   * - ``title``
     - ``{{ name | title }}``
     - Capitalizes first letter of each word
   * - ``replace``
     - ``{{ name | replace('_', '-') }}``
     - String replacement
   * - ``default``
     - ``{{ opt_param | default('fallback') }}``
     - Default value if undefined/empty
   * - ``join``
     - ``{{ columns | join(', ') }}``
     - Join list into string
   * - ``length``
     - ``{{ items | length }}``
     - Number of items in list/string
   * - ``trim``
     - ``{{ value | trim }}``
     - Strip leading/trailing whitespace
   * - ``sort``
     - ``{{ items | sort }}``
     - Sort a list

Examples with Filters
~~~~~~~~~~~~~~~~~~~~~

**Generating comma-separated column lists with ``join``:**

.. code-block:: yaml

   sql: |
     SELECT {{ columns | join(', ') }}
     FROM {{ source_table }}

**Default values for optional parameters:**

.. code-block:: yaml

   parameters:
     - name: file_format
       required: false

   actions:
     - name: load_{{ table_name }}
       source:
         format: "{{ file_format | default('csv') }}"

**String manipulation:**

.. code-block:: yaml

   actions:
     - name: load_{{ table_name | lower }}_raw
       description: "Load {{ table_name | title }} data from {{ source_system | upper }}"


Parameter Type Handling
-----------------------

LHP automatically converts Jinja2-rendered strings back to their native types. This lets you pass complex data structures as template parameters.

Automatic Type Conversion
~~~~~~~~~~~~~~~~~~~~~~~~~

After Jinja2 renders a ``{{ parameter }}`` expression to a string, LHP's template engine converts it back:

.. list-table::
   :header-rows: 1
   :widths: 30 30 40

   * - Rendered String
     - Converted To
     - Example
   * - ``"None"``
     - ``None``
     - Missing/null values
   * - ``"{}"``
     - ``{}`` (empty dict)
     - Empty configuration
   * - ``"[]"``
     - ``[]`` (empty list)
     - Empty list
   * - ``"{'key': 'val'}"``
     - ``dict``
     - Parsed via JSON then ``ast.literal_eval``
   * - ``"['a', 'b']"``
     - ``list``
     - Parsed via JSON then ``ast.literal_eval``
   * - ``"true"`` / ``"false"``
     - ``bool``
     - Boolean flags
   * - ``"42"``
     - ``int``
     - Numeric values
   * - Anything else
     - ``str``
     - Strings pass through

Passing Complex Types
~~~~~~~~~~~~~~~~~~~~~

This means you can pass dicts and lists as template parameters using native YAML syntax:

.. code-block:: yaml
   :caption: Template expecting dict and list parameters

   name: configurable_write
   parameters:
     - name: table_name
       type: string
       required: true
     - name: table_properties
       type: object
       required: false
       default: {}
     - name: cluster_columns
       type: array
       required: false
       default: []

   actions:
     - name: write_{{ table_name }}
       type: write
       write_target:
         type: streaming_table
         database: "${catalog}.${schema}"
         table: "{{ table_name }}"
         table_properties: "{{ table_properties }}"
         cluster_columns: "{{ cluster_columns }}"

.. code-block:: yaml
   :caption: Flowgroup passing native YAML objects and arrays

   use_template: configurable_write
   template_parameters:
     table_name: customer
     table_properties:
       delta.enableChangeDataFeed: "true"
       PII: "true"
     cluster_columns:
       - customer_id
       - region

The dict and list parameters are serialized by Jinja2 during rendering and then converted back to native Python types by LHP's type coercion.


Combining Substitution Systems
-------------------------------

LHP has four substitution systems that are processed in strict order. Understanding this order is key to writing effective dynamic templates.

.. code-block:: text

   1. %{local_var}        → Flowgroup-scoped variables (regex, before templates)
   2. {{ template_param }} → Jinja2 template parameters (during template expansion)
   3. ${token}              → Environment tokens from substitutions/<env>.yaml
   4. ${secret:scope/key}  → Secret references → dbutils.secrets.get()

For detailed examples of mixing substitution types — including local variables feeding into
template parameters and environment tokens coexisting with Jinja2 expressions — see
:doc:`substitutions`.


Advanced Patterns
-----------------

Multi-Action Templates
~~~~~~~~~~~~~~~~~~~~~~

Templates can define multiple actions that form a complete processing pipeline:

.. code-block:: yaml
   :caption: Template with load → DQE → cleanse → write pipeline

   name: bronze_ingestion_pipeline
   parameters:
     - name: raw_table_name
       required: true
     - name: bronze_table_name
       required: true
     - name: generate_surrogate_key
       required: false
       default: false
     - name: surrogate_key_name
       required: false
       default: "surrogate_key"

   actions:
     # Step 1: Load from raw
     - name: load_{{ raw_table_name }}_delta
       type: load
       readMode: stream
       operational_metadata: ["_processing_timestamp"]
       source:
         type: delta
         database: "${catalog}.${raw_schema}"
         table: "{{ raw_table_name }}"
       target: vw_{{ raw_table_name }}
       description: "Load {{ raw_table_name }} from raw schema"

     # Step 2: Data quality checks
     - name: DQE_{{ raw_table_name }}_delta
       type: transform
       transform_type: data_quality
       source: vw_{{ raw_table_name }}
       target: vw_{{ bronze_table_name }}_DQE
       readMode: stream
       expectations_file: "expectations/check_no_rescued_data.json"

     # Step 3: Cleanse with optional surrogate key
     - name: cleanse_{{ raw_table_name }}_delta
       type: transform
       transform_type: sql
       source: vw_{{ bronze_table_name }}_DQE
       target: vw_{{ bronze_table_name }}_cleaned
       sql: |
         SELECT
           {% if generate_surrogate_key %}xxhash64(* except (_processing_timestamp, _source_file_path)) as {{ surrogate_key_name }},
           {% endif %}* except (_rescued_data)
         FROM stream(vw_{{ bronze_table_name }}_DQE)

     # Step 4: Write to bronze
     - name: write_{{ bronze_table_name }}_delta
       type: write
       source: vw_{{ bronze_table_name }}_cleaned
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: "{{ bronze_table_name }}"
         table_properties:
           quality: "bronze"

This single template generates 4 actions. With 28 different flowgroup files referencing it, you define the pattern once and reuse it everywhere.

Template + Preset Layering
~~~~~~~~~~~~~~~~~~~~~~~~~~

Templates can declare their own presets, and flowgroups can add more on top:

.. code-block:: yaml
   :caption: Template with built-in preset

   name: ingestion_with_defaults
   presets:
     - cloudfiles_defaults        # Applied to all actions from this template

   parameters:
     - name: table_name
       required: true

   actions:
     - name: load_{{ table_name }}
       type: load
       source:
         type: cloudfiles
         path: "${landing_path}/{{ table_name }}/"
       target: vw_{{ table_name }}_raw

.. code-block:: yaml
   :caption: Flowgroup adding additional presets

   pipeline: my_pipeline
   flowgroup: customer_load
   presets:
     - write_defaults              # Stacks on top of template's cloudfiles_defaults
   use_template: ingestion_with_defaults
   template_parameters:
     table_name: customer

**Preset application order:**

1. Template-level presets (``cloudfiles_defaults``) applied first
2. Flowgroup-level presets (``write_defaults``) applied second (can override)
3. Explicit action config always wins over both presets

Multi-FlowGroup with Templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Use multi-document syntax to define multiple flowgroups from the same template in one file:

.. code-block:: yaml
   :caption: Three flowgroups in one file using ``---`` separator

   pipeline: raw_ingestion
   flowgroup: lineitem_ingestion_middle_east
   use_template: parquet_ingestion_template
   template_parameters:
     table_name: lineitem_middle_east_raw
     landing_folder: lineitem/region_MIDDLE_EAST
   ---
   pipeline: raw_ingestion
   flowgroup: orders_ingestion_middle_east
   use_template: parquet_ingestion_template
   template_parameters:
     table_name: orders_middle_east_raw
     landing_folder: orders/region_MIDDLE_EAST
   ---
   pipeline: raw_ingestion
   flowgroup: supplier_ingestion_middle_east
   use_template: parquet_ingestion_template
   template_parameters:
     table_name: supplier_middle_east_raw
     landing_folder: supplier/region_MIDDLE_EAST

Or use array syntax with field inheritance:

.. code-block:: yaml
   :caption: Array syntax with inherited pipeline and template

   pipeline: raw_ingestion
   use_template: parquet_ingestion_template

   flowgroups:
     - flowgroup: lineitem_ingestion
       template_parameters:
         table_name: lineitem_raw
         landing_folder: lineitem
     - flowgroup: orders_ingestion
       template_parameters:
         table_name: orders_raw
         landing_folder: orders
     - flowgroup: supplier_ingestion
       template_parameters:
         table_name: supplier_raw
         landing_folder: supplier


Template Organization
---------------------

Templates support subdirectory organization. Place your templates logically:

.. code-block:: text

   templates/
     ├── ingestion/
     │   ├── csv_ingestion_template.yaml
     │   ├── json_ingestion_template.yaml
     │   └── parquet_ingestion_template.yaml
     ├── bronze/
     │   └── raw_to_bronze_standard_template.yaml
     ├── silver/
     │   ├── dimension_scd2_template.yaml
     │   └── accumulating_fact_template.yaml
     └── analytics/
         └── gold_aggregate_template.yaml

Reference with the full path (without the ``.yaml`` extension):

.. code-block:: yaml

   use_template: ingestion/csv_ingestion_template
   use_template: bronze/raw_to_bronze_standard_template
   use_template: silver/dimension_scd2_template


Gotchas and Best Practices
--------------------------

The ``{{ }}`` Requirement
~~~~~~~~~~~~~~~~~~~~~~~~~~

.. warning::

   ``{% if %}`` and ``{% for %}`` blocks are only processed when the **same string value**
   also contains at least one ``{{ }}`` expression. This is the most common mistake.

**This works** (``{{ }}`` present):

.. code-block:: yaml

   sql: |
     SELECT
       {% if add_key %}xxhash64(*) as {{ key_name }},{% endif %}
       *
     FROM {{ source_view }}

**This does NOT work** (no ``{{ }}``):

.. code-block:: yaml

   # BUG: No {{ }} expression, so {% if %} is treated as a literal string!
   sql: |
     {% if use_full_load %}
     SELECT * FROM my_table
     {% else %}
     SELECT * FROM my_table WHERE updated > current_date - 1
     {% endif %}

**Workaround** — add a no-op ``{{ }}`` expression or refactor to include a variable:

.. code-block:: yaml

   # Option 1: Use a parameter in the string
   sql: |
     {% if use_full_load %}SELECT * FROM {{ table_name }}{% else %}SELECT * FROM {{ table_name }} WHERE updated > current_date - 1{% endif %}

   # Option 2: Design around it — make the WHERE clause a parameter
   parameters:
     - name: where_clause
       required: false
       default: ""

   sql: |
     SELECT * FROM {{ table_name }}{% if where_clause %} WHERE {{ where_clause }}{% endif %}

Whitespace Management
~~~~~~~~~~~~~~~~~~~~~

YAML's ``|`` (literal block) preserves newlines, which helps with readability of multi-line SQL. Be mindful of extra blank lines that Jinja2 block tags can introduce:

.. code-block:: yaml

   # Good: Tight formatting, minimal extra whitespace
   sql: |
     SELECT
       {% if add_surrogate_key %}xxhash64(*) as {{ key_name }},
       {% endif %}* except (_rescued_data)
     FROM stream({{ source_view }})

   # Avoid: Extra blank lines from Jinja2 blocks
   sql: |
     SELECT
       {% if add_surrogate_key %}
       xxhash64(*) as {{ key_name }},
       {% endif %}
       * except (_rescued_data)
     FROM stream({{ source_view }})

Use ``{%- %}`` (with dashes) for whitespace trimming if needed:

- ``{%- if ... %}`` strips whitespace **before** the tag
- ``{% if ... -%}`` strips whitespace **after** the tag

Boolean Parameters
~~~~~~~~~~~~~~~~~~

Boolean defaults in YAML can be ``true``/``false`` (no quotes). LHP's type coercion handles the conversion:

.. code-block:: yaml

   parameters:
     - name: generate_surrogate_key
       required: false
       default: false              # YAML native boolean

   # In the flowgroup:
   template_parameters:
     generate_surrogate_key: true  # YAML native boolean — works with {% if %}

Avoid quoting boolean values (``"true"``/``"false"``) in template parameters — the type coercion will still work, but native YAML booleans are cleaner.

Template Parameters vs Environment Tokens
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Keep the separation clear:

- Use ``{{ template_param }}`` for values that **vary per flowgroup** (table names, column lists, flags)
- Use ``${env_token}`` for values that **vary per environment** (catalog names, schema names, paths)
- Use ``%{local_var}`` for computed values **within a single flowgroup** (derived names, path segments)

.. code-block:: yaml

   # Good separation of concerns
   actions:
     - name: write_{{ table_name }}     # table_name varies per flowgroup
       write_target:
         database: "${catalog}.${schema}" # catalog/schema vary per environment
         table: "{{ table_name }}"

Testing Templates
~~~~~~~~~~~~~~~~~

Use ``lhp generate --dry-run --verbose --env dev`` to preview generated code without writing files. This lets you verify that your conditional logic and loops produce the expected output.

.. code-block:: bash

   # Preview what a template generates
   lhp generate --env dev --dry-run --verbose

   # Validate configuration without generating
   lhp validate --env dev


Quick Reference
---------------

.. list-table:: Jinja2 Features in LHP Templates
   :header-rows: 1
   :widths: 30 70

   * - Feature
     - Syntax
   * - Variable substitution
     - ``{{ variable_name }}``
   * - Conditional
     - ``{% if condition %}...{% elif other %}...{% else %}...{% endif %}``
   * - For loop
     - ``{% for item in list %}...{% endfor %}``
   * - Loop last check
     - ``{% if not loop.last %}, {% endif %}``
   * - Truthiness check
     - ``{% if my_list %}`` (non-empty = true)
   * - Filter
     - ``{{ value | lower }}``, ``{{ items | join(', ') }}``
   * - Default value
     - ``{{ param | default('fallback') }}``
   * - Whitespace trim
     - ``{%- ... -%}`` (strips surrounding whitespace)
   * - String comparison
     - ``{% if mode == "cdc" %}``

.. list-table:: Substitution Systems Summary
   :header-rows: 1
   :widths: 15 25 30 30

   * - Order
     - Syntax
     - Scope
     - Defined In
   * - 1st
     - ``%{var}``
     - Single flowgroup
     - ``variables:`` block in flowgroup
   * - 2nd
     - ``{{ param }}``
     - Template actions
     - ``template_parameters:`` in flowgroup
   * - 3rd
     - ``${token}``
     - All flowgroups in environment
     - ``substitutions/<env>.yaml``
   * - 4th
     - ``${secret:scope/key}``
     - Runtime only
     - ``substitutions/<env>.yaml`` secrets section
