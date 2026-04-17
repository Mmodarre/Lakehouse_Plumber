========================
Error Reference
========================

.. meta::
   :description: Troubleshooting guide with all Lakehouse Plumber error codes, causes, and resolution steps.

Overview
========

When Lakehouse Plumber encounters a problem, it displays a structured error with a unique
code in the format ``LHP-{CATEGORY}-{NUMBER}``:

- **LHP** — Lakehouse Plumber prefix
- **CATEGORY** — The error category (e.g. ``CFG`` for configuration, ``VAL`` for validation)
- **NUMBER** — A unique number within that category

Here is what an error looks like in your terminal:

.. code-block:: text
   :caption: Example error output

   ❌ Error [LHP-VAL-001]: Missing required field 'source'
   ======================================================================

   The Load action 'load_customers' requires a 'source' field. This field
   specifies where to read data from.

   📍 Context:
      • Component Type: Load action
      • Component Name: load_customers
      • Missing Field: source

   💡 How to fix:
      1. Add the 'source' field to your configuration
      2. Check the example below for the correct format

   📝 Example:
      actions:
        - name: load_customers
          type: load
          source:
            type: cloudfiles
            path: /data/customers/

   📚 More info: https://docs.lakehouseplumber.com/errors/lhp-val-001
   ======================================================================

Each error includes the cause, relevant context, numbered fix suggestions, and a
configuration example where applicable. Search this page for your error code to find
detailed resolution steps.

.. tip::

   Use the ``--verbose`` flag with any LHP command to see additional debug information
   that can help diagnose the issue.

Error Categories
================

.. list-table::
   :header-rows: 1
   :widths: 15 15 70

   * - Category
     - Prefix
     - Description
   * - Configuration
     - ``CFG``
     - Invalid or conflicting settings in YAML files, presets, templates, or bundle configuration
   * - Validation
     - ``VAL``
     - Missing required fields, invalid field values, or structural problems in action definitions
   * - I/O
     - ``IO``
     - Files not found, read/write failures, or file format issues
   * - Action
     - ``ACT``
     - Unknown action types, subtypes, or presets
   * - Dependency
     - ``DEP``
     - Circular dependencies between views or preset inheritance cycles

Configuration Errors (LHP-CFG)
==============================

Configuration errors indicate problems with your YAML files, presets, templates,
or Databricks Asset Bundle setup. They are the most common error category.

LHP-CFG-001: Configuration Conflict
------------------------------------

**When it occurs:** You have specified the same configuration option in multiple ways,
typically when both legacy and new-format fields are present in the same action.

**Common causes:**

- Using both a top-level field and its equivalent under ``options``
- A preset defines a value that conflicts with an explicit value in the action

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-001)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
         format: json              # Legacy top-level field
         options:
           cloudFiles.format: json  # Same setting in new format

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
         options:
           cloudFiles.format: json  # Use only the new format

.. seealso::

   :doc:`actions/index` for the current configuration format for each action type.

LHP-CFG-006: Invalid Event Log Configuration
---------------------------------------------

**When it occurs:** The ``event_log`` section in ``lhp.yaml`` is not a valid YAML mapping,
or its field values have incorrect types.

**Common causes:**

- Providing a scalar value (string, number, boolean) instead of a mapping
- Using invalid types for fields (e.g., a list for ``catalog``)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-006)

   # lhp.yaml
   event_log: "enable_logging"     # String, but a mapping is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

.. seealso::

   :doc:`monitoring` for all available event log and monitoring configuration options.

LHP-CFG-007: Incomplete Event Log Configuration
------------------------------------------------

**When it occurs:** The ``event_log`` section in ``lhp.yaml`` is enabled but missing
required fields (``catalog`` and/or ``schema``).

**Common causes:**

- Defining ``event_log`` without specifying ``catalog`` or ``schema``
- Forgetting to add both required fields when enabling event logging

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-007)

   # lhp.yaml
   event_log:
     name_suffix: "_event_log"
     # Missing: catalog and schema!

.. code-block:: yaml
   :caption: After (fixed) — provide both required fields

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

.. code-block:: yaml
   :caption: Alternative fix — disable event logging

   # lhp.yaml
   event_log:
     enabled: false
     name_suffix: "_event_log"

.. note::

   When ``enabled`` is ``true`` (the default), both ``catalog`` and ``schema`` are required.
   Set ``enabled: false`` to define the section without activating event log injection.

LHP-CFG-008: Invalid Monitoring Configuration
----------------------------------------------

**When it occurs:** The ``monitoring`` section in ``lhp.yaml`` is not a valid YAML mapping,
its field values have incorrect types, or it fails cross-validation with the ``event_log``
section. This error code covers several monitoring validation scenarios:

**Scenario 1: monitoring is not a mapping**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   monitoring: "enable_monitoring"     # String, but a mapping is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

**Scenario 2: materialized_views is not a list**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views: "events_summary"   # String, but a list is expected

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"
     materialized_views:
       - name: "events_summary"
         sql: "SELECT * FROM all_pipelines_event_log"

**Scenario 3: monitoring requires event_log**

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   # event_log is missing or disabled!
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. code-block:: yaml
   :caption: After (fixed) — add event_log

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
     name_suffix: "_event_log"

   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

.. code-block:: yaml
   :caption: Alternative fix — disable monitoring

   # lhp.yaml
   monitoring:
     enabled: false

**Scenario 4: monitoring.checkpoint_path is missing**

``checkpoint_path`` is required whenever ``monitoring.enabled`` is ``true``. Each
streaming query in the generated union notebook writes to its own checkpoint directory
at ``{checkpoint_path}/{pipeline_name}/``.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-008)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
   monitoring: {}      # Missing checkpoint_path

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml
   event_log:
     catalog: "${catalog}"
     schema: _meta
   monitoring:
     checkpoint_path: "/Volumes/${catalog}/_meta/checkpoints/event_logs"

**Additional validation (all LHP-CFG-008):**

- Each materialized view entry must be a YAML mapping with at least a ``name`` field
- MV names must be unique within the ``materialized_views`` list
- Each MV must specify either ``sql`` or ``sql_path``, not both

.. seealso::

   :doc:`monitoring` for complete monitoring configuration reference and examples.

LHP-CFG-009: YAML Parsing Error
--------------------------------

**When it occurs:** A YAML file contains invalid syntax that cannot be parsed.

**Common causes:**

- Incorrect indentation (mixing tabs and spaces)
- Missing colons after keys
- Unquoted strings containing special characters (``:`` ``#`` ``{`` ``}`` ``[`` ``]``)
- Unclosed quotes or brackets

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-009)

   actions:
     - name: load_events
       type: load
       source:
         path: /data/{date}/events  # Braces need quoting
         comment: Load events: raw  # Colon in value needs quoting

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         path: "/data/{date}/events"     # Quoted
         comment: "Load events: raw"     # Quoted

.. tip::

   Use a YAML linter (``yamllint`` or your IDE's YAML extension) to catch syntax
   issues before running ``lhp validate``.

LHP-CFG-010: Deprecated Field
------------------------------

**When it occurs:** Your configuration uses a field name that has been removed
or replaced in a newer version of Lakehouse Plumber.

**Common causes:**

- Using a field name from an older version of LHP
- Copy-pasting examples from outdated documentation

The error message tells you exactly which field to use instead. Replace the
deprecated field with the suggested replacement.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-010)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         schemaHints: "id BIGINT, name STRING"   # Deprecated field

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         options:
           cloudFiles.schemaHints: "id BIGINT, name STRING"   # New format

LHP-CFG-012: Missing Template Parameters
-----------------------------------------

**When it occurs:** A template requires parameters that were not provided in
the ``template_parameters`` section of the flowgroup.

**Common causes:**

- Forgetting to add ``template_parameters`` when using a template
- Misspelling a parameter name
- Using a template that was recently updated with new required parameters

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-012)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load
   # Missing template_parameters!
   actions:
     - name: load_data
       type: load

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load
   template_parameters:
     source_path: /data/raw/events    # Required by template
     file_format: json                # Required by template
   actions:
     - name: load_data
       type: load

.. seealso::

   :doc:`templates_reference` for how templates and parameters work.

LHP-CFG-020: Bundle Resource Error
-----------------------------------

**When it occurs:** An error occurred while generating or syncing Databricks Asset Bundle
resource files during ``lhp generate``.

**Common causes:**

- Invalid YAML in existing bundle resource files under ``resources/lhp/``
- File permission issues preventing writes to the resources directory
- Corrupted resource files from a previous interrupted generation

**Resolution:**

1. Run ``lhp validate --env <env>`` to check your configuration
2. Check that files under ``resources/lhp/`` are valid YAML
3. If files are corrupted, delete them and re-run ``lhp generate --env <env> --force``

.. seealso::

   :doc:`databricks_bundles` for bundle setup and configuration.

LHP-CFG-021: Bundle YAML Processing Error
------------------------------------------

**When it occurs:** A bundle-related YAML file (such as ``databricks.yml`` or a resource
file) could not be processed.

**Common causes:**

- Invalid YAML syntax in ``databricks.yml``
- Malformed resource YAML files under ``resources/lhp/``
- Encoding issues (file is not UTF-8)

**Resolution:**

1. Validate ``databricks.yml`` with a YAML linter
2. Check for syntax errors at the line number shown in the error details
3. Ensure all YAML files use UTF-8 encoding

LHP-CFG-022: Missing databricks.yml
------------------------------------

**When it occurs:** Bundle operations require a ``databricks.yml`` file, but it was
not found in the project root.

**Common causes:**

- Running bundle commands in a project that was not initialized with bundle support
- Running commands from the wrong directory

.. code-block:: bash
   :caption: Resolution options

   # Option 1: Initialize a new project with bundle support
   lhp init my_project

   # Option 2: Skip bundle operations
   lhp generate --env dev --no-bundle

LHP-CFG-023: Missing Databricks Bundle Targets
-----------------------------------------------

**When it occurs:** Substitution files exist for environments (e.g., ``substitutions/dev.yaml``)
but the corresponding targets are not defined in ``databricks.yml``.

**Common causes:**

- Adding a new substitution file without updating ``databricks.yml``
- Renaming a target in ``databricks.yml`` without updating the substitution file name

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-023) — substitutions/staging.yaml exists but...

   # databricks.yml
   targets:
     dev:
       default: true
     prod:
       mode: production
     # Missing: staging target!

.. code-block:: yaml
   :caption: After (fixed)

   # databricks.yml
   targets:
     dev:
       default: true
     staging:                    # Added to match substitutions/staging.yaml
       mode: development
     prod:
       mode: production

.. seealso::

   :doc:`databricks_bundles` for target configuration details.

LHP-CFG-024: Bundle Template Error
-----------------------------------

**When it occurs:** An error occurred while fetching or processing bundle templates
during project initialization.

**Common causes:**

- Network connectivity issues when downloading templates
- Invalid template URL or path
- Template file is corrupted or in unexpected format

**Resolution:**

1. Check your internet connection if using remote templates
2. Verify the template path or URL is correct
3. Try running the command again

LHP-CFG-025: Bundle Configuration Error
----------------------------------------

**When it occurs:** The ``databricks.yml`` file or bundle configuration has structural
problems that prevent LHP from processing it.

**Common causes:**

- Missing required fields in ``databricks.yml``
- Invalid YAML structure in the bundle configuration
- Incompatible bundle configuration format

**Resolution:**

1. Review your ``databricks.yml`` against the Databricks Asset Bundles documentation
2. Run ``lhp validate`` for detailed diagnostics
3. Compare with a working project's ``databricks.yml``

LHP-CFG-027: Template Not Found
--------------------------------

**When it occurs:** The flowgroup references a template name that does not exist
in the ``templates/`` directory.

**Common causes:**

- Typo in the template name
- The template file is missing from the ``templates/`` directory
- Using a template name without the correct file extension

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-027)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: stanard_cloudfiles_load    # Typo!

.. code-block:: yaml
   :caption: After (fixed)

   pipeline: my_pipeline
   flowgroup: bronze_load
   template: standard_cloudfiles_load   # Correct spelling

.. tip::

   Run ``lhp list_templates`` to see all available template names.

Validation Errors (LHP-VAL)
============================

Validation errors indicate that your configuration is syntactically valid YAML but
contains values that are structurally incorrect, missing, or incompatible.

LHP-VAL-001: Missing Required Field
------------------------------------

**When it occurs:** An action is missing a field that is required for its type.

**Common causes:**

- Forgetting to add ``source``, ``target``, or ``type`` to an action
- Incomplete action definition after copy-pasting from another flowgroup
- Template expansion that does not provide all required fields

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-001)

   actions:
     - name: load_customers
       type: load
       # Missing: source configuration!
       target: v_raw_customers

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_customers
       type: load
       source:
         type: cloudfiles
         path: /data/customers/
         options:
           cloudFiles.format: csv
       target: v_raw_customers

.. tip::

   The error message includes the specific field name that is missing and an
   example of the correct configuration.

LHP-VAL-002: Validation Failed
-------------------------------

**When it occurs:** An action or component has multiple validation issues that
were detected together during ``lhp validate`` or ``lhp generate``.

**Common causes:**

- Missing source view reference
- Invalid target reference
- Circular dependency in view definitions
- Multiple structural issues in a single action

The error details list each individual issue with a ``✗`` marker. Address each
item in the list to resolve this error.

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-002)

   actions:
     - name: process_data
       type: transform
       sub_type: sql
       # Missing: source
       # Missing: target
       sql: |
         SELECT * FROM somewhere

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: process_data
       type: transform
       sub_type: sql
       source: v_raw_data
       target: v_processed
       sql: |
         SELECT * FROM $source

.. note::

   In SQL transforms, ``$source`` is automatically replaced with the view name
   specified in the ``source`` field. See :doc:`actions/index` for details
   on SQL transform syntax.

LHP-VAL-006: Invalid Field Value
---------------------------------

**When it occurs:** A field has a value that is not in the set of allowed values.

**Common causes:**

- Typo in a value (e.g., ``streeming`` instead of ``streaming``)
- Using a value from a different context (e.g., a write target type in a load action)
- Case sensitivity issues

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-006)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
       target: v_events
       write_target:
         type: streeming_table    # Typo!

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:
         type: cloudfiles
         path: /data/events/
       target: v_events
       write_target:
         type: streaming_table    # Correct spelling

LHP-VAL-007: Invalid readMode
------------------------------

**When it occurs:** The ``readMode`` value is not valid for the action type.

**Common causes:**

- Using an unsupported readMode value
- Applying a readMode that is incompatible with the action's source type

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-007)

   actions:
     - name: load_events
       type: load
       readMode: continuous       # Not a valid readMode
       source:
         type: cloudfiles
         path: /data/events/

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       readMode: stream           # Valid: 'stream' or 'batch'
       source:
         type: cloudfiles
         path: /data/events/

.. note::

   Valid readMode values are ``stream`` (for ``spark.readStream``, the default) and
   ``batch`` (for ``spark.read``). SQL transforms reading from streaming sources must
   use ``stream(view_name)`` in the SQL expression.

LHP-VAL-008: Invalid Field Type
--------------------------------

**When it occurs:** A field has the wrong data type (e.g., a string where a list is
expected, or a number where a boolean is expected).

**Common causes:**

- Providing a string where a dictionary/object is expected
- YAML auto-conversion (e.g., ``yes``/``no`` converting to boolean)
- Passing a single value where a list is required

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-008)

   actions:
     - name: load_events
       type: load
       source: /data/events/      # String, but cloudfiles expects a dict

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_events
       type: load
       source:                     # Dictionary with required fields
         type: cloudfiles
         path: /data/events/

LHP-VAL-010: Duplicate Monitoring Pipeline Configuration
---------------------------------------------------------

**When it occurs:** Both the ``__eventlog_monitoring`` alias and the actual monitoring
pipeline name are defined as separate entries in ``pipeline_config.yaml``.

**Common causes:**

- Using the alias while also explicitly targeting the monitoring pipeline by its real name
- Copy-pasting a config document and forgetting to remove the duplicate

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-010)

   ---
   pipeline: __eventlog_monitoring
   serverless: false

   ---
   pipeline: acme_edw_event_log_monitoring
   serverless: true

.. code-block:: yaml
   :caption: After (fixed) — use only one

   ---
   pipeline: __eventlog_monitoring
   serverless: false

.. seealso::

   :doc:`monitoring` for details on the ``__eventlog_monitoring`` reserved keyword
   and monitoring pipeline configuration.

LHP-VAL-011: Monitoring Alias in Pipeline List / Schema Syntax Error
---------------------------------------------------------------------

This error code covers two validation scenarios.

**Scenario 1: Monitoring Alias in Pipeline List**

**When it occurs:** The ``__eventlog_monitoring`` alias is used inside a pipeline list
(e.g., ``pipeline: [bronze, __eventlog_monitoring]``) instead of as a standalone entry.

**Common causes:**

- Grouping the monitoring alias with other pipelines in a list
- Attempting to share configuration between regular pipelines and the monitoring pipeline

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-011)

   ---
   pipeline:
     - bronze_pipeline
     - __eventlog_monitoring
   serverless: false

.. code-block:: yaml
   :caption: After (fixed) — separate documents

   ---
   pipeline: bronze_pipeline
   serverless: false

   ---
   pipeline: __eventlog_monitoring
   serverless: false

.. seealso::

   :doc:`monitoring` for details on the ``__eventlog_monitoring`` reserved keyword
   and monitoring pipeline configuration.

**Scenario 2: Schema Syntax Error**

**When it occurs:** A schema file has invalid syntax or structure.

**Common causes:**

- Incorrect column type names
- Missing required fields in column definitions
- Malformed schema YAML structure

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-011)

   name: customer_schema
   columns:
     - name: id
       type: BIGINTT              # Typo in type name
     - name: email
                                   # Missing: type field

.. code-block:: yaml
   :caption: After (fixed)

   name: customer_schema
   columns:
     - name: id
       type: BIGINT
     - name: email
       type: STRING

.. tip::

   Valid schema types include: ``STRING``, ``BIGINT``, ``INT``, ``INTEGER``,
   ``LONG``, ``DOUBLE``, ``FLOAT``, ``BOOLEAN``, ``DATE``, ``TIMESTAMP``,
   ``BINARY``, ``BYTE``, ``SHORT``, and ``DECIMAL(precision,scale)``.

LHP-VAL-012: Invalid Source Format
-----------------------------------

**When it occurs:** The ``source`` configuration for an action is not in the
expected format for its type.

**Common causes:**

- Providing a plain string where a configuration object is needed
- Missing the ``type`` field in the source configuration
- Using source configuration from one action type in another

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-012)

   actions:
     - name: load_api_data
       type: load
       source: custom_datasource   # String, but custom_datasource needs a dict

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: load_api_data
       type: load
       source:
         type: custom_datasource
         module_path: "data_sources/api_source.py"
         custom_datasource_class: "APIDataSource"

.. seealso::

   :doc:`actions/index` for the correct source configuration format for each
   action type.

I/O Errors (LHP-IO)
====================

I/O errors indicate problems reading or writing files referenced in your configuration.

LHP-IO-001: File Not Found
---------------------------

**When it occurs:** A file referenced in your configuration does not exist at the
expected path.

**Common causes:**

- Typo in the file path
- Relative path that resolves to the wrong location
- File was moved or deleted after the configuration was written
- Missing file extension

.. code-block:: yaml
   :caption: Before (triggers LHP-IO-001)

   actions:
     - name: transform_orders
       type: transform
       sub_type: sql
       source: v_raw_orders
       target: v_orders
       sql_file: sqls/transform_orders.sql   # File doesn't exist!

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: transform_orders
       type: transform
       sub_type: sql
       source: v_raw_orders
       target: v_orders
       sql_file: sql/transform_orders.sql    # Correct path

.. note::

   File paths are resolved relative to your flowgroup YAML file's directory.
   The error message shows the full resolved path and lists the locations that
   were searched.

LHP-IO-003: Invalid Document Count
-----------------------------------

**When it occurs:** A YAML file that is expected to contain a single document
has zero documents (empty file) or multiple documents separated by ``---``.

**Common causes:**

- An empty YAML file
- Using ``---`` document separators in a file that is loaded as single-document
- Copy-pasting content that includes extra ``---`` separators

.. code-block:: yaml
   :caption: Before (triggers LHP-IO-003) — extra separator in schema file

   name: customer_schema
   columns:
     - name: id
       type: BIGINT
   ---
   name: order_schema
   columns:
     - name: order_id
       type: BIGINT

.. code-block:: yaml
   :caption: After (fixed) — one schema per file

   # schemas/customer_schema.yaml
   name: customer_schema
   columns:
     - name: id
       type: BIGINT

.. tip::

   Multi-document YAML (``---`` separators) is supported for **flowgroup files**
   only. Schema files, expectations files, and substitution files must contain
   exactly one document. See :doc:`multi_flowgroup_guide` for multi-document
   flowgroup syntax.

Action Errors (LHP-ACT)
========================

Action errors indicate that an action type, subtype, or preset name is not recognized.

LHP-ACT-001: Unknown Type
--------------------------

**When it occurs:** A value you provided is not recognized. This covers unknown
action types, subtypes, sink types, source types, and preset names.

**Common causes:**

- Typo in the action type or subtype
- Using an action type that does not exist
- Referencing a preset that is not defined in the ``presets/`` directory

.. code-block:: yaml
   :caption: Before (triggers LHP-ACT-001) — unknown action type

   actions:
     - name: clean_data
       type: transfrm               # Typo!
       sub_type: sql
       source: v_raw

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: clean_data
       type: transform               # Correct spelling
       sub_type: sql
       source: v_raw

The error includes "Did you mean?" suggestions when the provided value is close
to a valid option. It also lists all valid values.

.. tip::

   Run ``lhp list_presets`` to see all available preset names, or check the
   :doc:`actions/index` for valid action types and subtypes.

Dependency Errors (LHP-DEP)
============================

Dependency errors indicate circular references in your pipeline's view graph
or preset inheritance chain.

LHP-DEP-001: Circular Dependency Detected
------------------------------------------

**When it occurs:** Two or more views form a dependency cycle where
view A depends on view B, which depends on view C, which depends back on view A.

**Common causes:**

- A transform action's source references a view that (directly or indirectly)
  depends on the transform's own target
- Copy-paste errors where source and target views were swapped
- Complex multi-step transformations that accidentally create loops

.. code-block:: yaml
   :caption: Before (triggers LHP-DEP-001) — circular dependency

   actions:
     - name: enrich_customers
       type: transform
       sub_type: sql
       source: v_enriched_orders     # Depends on enriched orders
       target: v_enriched_customers
       sql: |
         SELECT * FROM $source

     - name: enrich_orders
       type: transform
       sub_type: sql
       source: v_enriched_customers  # Depends on enriched customers!
       target: v_enriched_orders
       sql: |
         SELECT * FROM $source

.. code-block:: yaml
   :caption: After (fixed) — break the cycle

   actions:
     - name: enrich_customers
       type: transform
       sub_type: sql
       source: v_raw_customers       # Use raw source instead
       target: v_enriched_customers
       sql: |
         SELECT * FROM $source

     - name: enrich_orders
       type: transform
       sub_type: sql
       source: v_enriched_customers
       target: v_enriched_orders
       sql: |
         SELECT * FROM $source

The error message shows the full cycle path (e.g., ``A → B → C → A``) to help
you identify which dependency to remove or redirect.

.. tip::

   Run ``lhp deps --format dot --env <env>`` to generate a visual dependency
   graph that makes cycles easier to spot. See :doc:`dependency_analysis`
   for details.

General Troubleshooting
=======================

State Management
----------------

.. code-block:: bash
   :caption: Debugging state issues

   # Force regeneration of all files
   lhp generate --force-all --env dev

   # Clear state and regenerate everything
   rm .lhp_state.json
   lhp generate --env dev

   # Check what files would be regenerated
   lhp generate --dry-run --env dev --verbose

Dependency Debugging
--------------------

.. code-block:: bash
   :caption: Dependency debugging

   # Show dependency graph
   lhp validate --env dev --show-dependencies

   # Validate for circular dependencies
   lhp validate --env dev --check-cycles

Performance Optimization
------------------------

- Use **include patterns** to limit file scanning scope
- Keep **FlowGroups focused** — avoid overly large YAML files
- Leverage **state management** — don't force regeneration unless needed
- Use **specific targets** when possible instead of full pipeline generation

Getting Help
============

**Error not listed here?** This page documents the most common errors you will
encounter as a pipeline author. Some error codes are used internally for rare
edge cases and are not listed above.

If you encounter an error that is not documented here:

1. Read the error message carefully — every LHP error includes a description,
   context, and numbered fix suggestions directly in the terminal output
2. Run the command again with ``--verbose`` for additional diagnostic information
3. Run ``lhp validate --env <env>`` to check your full configuration
4. Report the issue at https://github.com/MehdiDataHandcraft/LakehousePlumber/issues
