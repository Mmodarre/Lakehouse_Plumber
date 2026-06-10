========================
Error Reference
========================

.. meta::
   :description: Troubleshooting guide with all Lakehouse Plumber error codes, causes, and resolution steps.

Overview
--------

When Lakehouse Plumber encounters a problem, it displays a structured error with a unique
code in the format ``LHP-{CATEGORY}-{NUMBER}``:

- **LHP** — Lakehouse Plumber prefix
- **CATEGORY** — The error category (e.g. ``CFG`` for configuration, ``VAL`` for validation)
- **NUMBER** — A unique number within that category

Here is what an error looks like in your terminal (Rich-rendered Panel on
stderr; file logs receive the same content as plain ASCII):

.. code-block:: text
   :caption: Example error output

   ╭─ LHP-VAL-001   Validation ──────────────────────────────────────────╮
   │ Missing required field 'source'                                     │
   │                                                                    │
   │ The Load action 'load_customers' requires a 'source' field. This    │
   │ field specifies where to read data from.                            │
   │                                                                    │
   │ Context                                                             │
   │   Component Type: Load action                                       │
   │   Component Name: load_customers                                    │
   │   Missing Field: source                                             │
   │                                                                    │
   │ Suggestions                                                         │
   │   -> Add the 'source' field to your configuration                   │
   │   -> Check the example below for the correct format                 │
   │                                                                    │
   │ Example                                                             │
   │   actions:                                                          │
   │     - name: load_customers                                          │
   │       type: load                                                    │
   │       source:                                                       │
   │         type: cloudfiles                                            │
   │         path: /data/customers/                                      │
   │                                                                    │
   │ More info: https://docs.lakehouseplumber.com/errors/lhp-val-001     │
   ╰─────────────────────────────────────────────────────────────────────╯

Each error includes the cause, relevant context, numbered fix suggestions, and a
configuration example where applicable. Search this page for your error code to find
detailed resolution steps.

.. tip::

   Use the ``--verbose`` flag with any LHP command to see additional debug information
   that can help diagnose the issue.

Error Categories
----------------

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
   * - Deprecation
     - ``DEPR``
     - Soft-deprecation warnings for fields/syntax scheduled for removal; surfaced as warnings, not failures

Configuration Errors (LHP-CFG)
------------------------------

Configuration errors indicate problems with your YAML files, presets, templates,
or Declarative Automation Bundles setup. They are the most common error category.

LHP-CFG-001: Configuration Conflict
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   :doc:`monitoring_reference` for all available event log and monitoring configuration options.

LHP-CFG-007: Incomplete Event Log Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   :doc:`monitoring_reference` for complete monitoring configuration reference and examples.

LHP-CFG-009: YAML Parsing Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** An error occurred while generating or syncing Declarative Automation Bundles
resource files during ``lhp generate``.

**Common causes:**

- Invalid YAML in existing bundle resource files under ``resources/lhp/``
- File permission issues preventing writes to the resources directory
- Corrupted resource files from a previous interrupted generation

**Resolution:**

1. Run ``lhp validate --env <env>`` to check your configuration
2. Check that files under ``resources/lhp/`` are valid YAML
3. If files are corrupted, delete them and re-run ``lhp generate --env <env>``

.. seealso::

   :doc:`configure_bundles` for bundle setup and configuration.

LHP-CFG-021: Bundle YAML Processing Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

LHP-CFG-024: Bundle Template Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** The ``databricks.yml`` file or bundle configuration has structural
problems that prevent LHP from processing it.

**Common causes:**

- Missing required fields in ``databricks.yml``
- Invalid YAML structure in the bundle configuration
- Incompatible bundle configuration format

**Resolution:**

1. Review your ``databricks.yml`` against the Declarative Automation Bundles documentation
2. Run ``lhp validate`` for detailed diagnostics
3. Compare with a working project's ``databricks.yml``

LHP-CFG-027: Template Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

LHP-CFG-032: Test Reporting File Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** ``lhp.yaml`` declares a ``test_reporting`` section, but the
provider module at ``module_path`` (or the optional ``config_file``) does not
exist at the resolved path.

**Common causes:**

- Typo in the ``module_path`` or ``config_file`` path
- The provider module has not been created yet
- A path that resolves relative to the wrong directory (paths are relative to
  the project root)

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-032)

   # lhp.yaml
   test_reporting:
     module_path: py_functions/test_reporting_publisher.py   # File does not exist
     function_name: publish_results

.. code-block:: yaml
   :caption: After (fixed) — create the file, or correct the path

   # lhp.yaml
   test_reporting:
     module_path: py_functions/delta_test_reporter.py        # File exists
     function_name: publish_results

.. note::

   This is a project preflight check. It runs on both ``lhp validate`` and
   ``lhp generate``, **independent of** ``--include-tests`` — a project with a
   missing provider file fails ``lhp generate`` even without the flag.

.. seealso::

   :doc:`actions/test_reporting` for the provider module contract and built-in
   providers.

LHP-CFG-031: Generated Source Failed to Parse
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** LHP generated Python source that could not be parsed
(``ast.parse`` raised a ``SyntaxError``). The in-worker syntax guard runs over
every flowgroup's generated code before the output is committed, and the error
names the offending flowgroup.

**Common causes:**

- A bug in an LHP generator or template that emitted syntactically invalid
  Python — this is almost never a problem with your YAML.
- A custom template or a snapshot-CDC ``source_function`` that embeds Python
  which does not parse.

**How to resolve:**

- File a bug report against LHP with the failing flowgroup YAML.
- Turn on DEBUG logging to inspect the generated source that failed to parse.
- If you author a custom template or a snapshot-CDC ``source_function``, verify
  the embedded Python parses with ``python -m py_compile``.

LHP-CFG-033: Ruff Failed to Format Generated Code
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** The terminal ``ruff format`` pass over the generated output
directory exited non-zero. The generated code was written to disk but could not
be formatted. The error carries ruff's exit code, stdout, and stderr so the
failure is diagnosable.

**Common causes:**

- A generated file is syntactically invalid (an LHP generator/template bug),
  so ruff could not parse it.
- The ruff invocation itself failed to read a file in the output tree.

**How to resolve:**

- Inspect ruff's error output (carried in the error context) for the offending
  file.
- Confirm ruff is installed and the generated tree is valid Python.
- Re-run with ``--no-format`` to skip formatting and inspect the raw generated
  code; if a generated file is invalid, file a bug report with the flowgroup
  YAML.

LHP-CFG-034: Ruff Executable Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** LHP could not locate the ``ruff`` executable required for
the generated-code formatting pass. ruff ships as a runtime dependency of LHP
but was not found in the active environment's scripts directory or on ``PATH``.

**How to resolve:**

- Install ruff into the active environment: ``pip install ruff``.
- Reinstall LHP with its dependencies: ``pip install lakehouse-plumber``.
- If you use an isolated or custom environment, ensure ruff is on ``PATH`` or
  installed alongside LHP.

LHP-CFG-054: Invalid Instance Definition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** A blueprint instance file has a malformed blueprint
reference. The ``use_blueprint:`` (or legacy ``blueprint:``) value must name the
blueprint as a single, non-empty string. A list, mapping, empty string, or
missing/null value triggers this error. It also fires when the instance document
otherwise fails to parse into a blueprint instance.

**Common causes:**

- ``use_blueprint:`` (or ``blueprint:``) given a list or mapping instead of a
  single string.
- The blueprint reference is an empty string or ``null``.
- The instance file's shape cannot be parsed into a valid blueprint instance.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-054)

   # Instance file — blueprint reference is a list, not a string
   use_blueprint:
     - bronze_ingestion
   parameters:
     table_name: customers

.. code-block:: yaml
   :caption: After (fixed)

   # Instance file — blueprint reference is a single non-empty string
   use_blueprint: bronze_ingestion
   parameters:
     table_name: customers

.. seealso::

   :doc:`blueprints` for the full set of blueprint and instance file errors
   (``LHP-CFG-047``–``058``, ``LHP-VAL-041``–``061``).

.. _lhp-cfg-060:

LHP-CFG-060: Invalid Wheel Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** The top-level ``wheel`` block in ``lhp.yaml`` is malformed.
The block must be a mapping, and its optional ``artifact_volume`` key, when
present, must be a string. The ``/Volumes/...`` shape of the resolved value is
checked separately and later — see :ref:`lhp-cfg-061`.

**Common causes:**

- ``wheel`` given a scalar or list instead of a mapping.
- ``wheel.artifact_volume`` set to a non-string (for example a number or a list).
- The ``wheel`` block otherwise fails to parse into the expected shape.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-060)

   # lhp.yaml — artifact_volume is a list, not a string
   wheel:
     artifact_volume:
       - /Volumes/prod_catalog/lhp_artifacts/bundle_artifacts

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml — wheel is a mapping; artifact_volume is a single string
   wheel:
     artifact_volume: /Volumes/prod_catalog/lhp_artifacts/bundle_artifacts

.. seealso::

   :doc:`package_pipelines_as_wheels` for the full wheel-packaging setup,
   including the ``wheel`` block and the artifact-volume requirement.

.. _lhp-cfg-061:

LHP-CFG-061: Wheel Packaging Requires a ``/Volumes/...`` Artifact Volume
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** A pipeline is configured for ``packaging: wheel``, but the
project's ``wheel.artifact_volume`` is missing, empty, or — after substitution —
resolves to a path that does not start with ``/Volumes/``. Serverless compute
installs custom wheels only from a Unity Catalog volume, so wheel packaging needs
a valid ``/Volumes/...`` destination to resolve the wheel's install path.

**Common causes:**

- No ``wheel`` block (or no ``artifact_volume``) declared in ``lhp.yaml`` while a
  pipeline opts into ``packaging: wheel``.
- A ``${token}`` in ``artifact_volume`` that resolves to a non-``/Volumes/`` path
  for the target environment.
- ``artifact_volume`` set to an empty or whitespace-only string.

.. code-block:: yaml
   :caption: Before (triggers LHP-CFG-061)

   # lhp.yaml — no artifact_volume, but a pipeline uses packaging: wheel
   wheel:
     artifact_volume: ""

.. code-block:: yaml
   :caption: After (fixed)

   # lhp.yaml — artifact_volume resolves to a /Volumes/... path
   wheel:
     artifact_volume: /Volumes/${catalog}/${artifact_schema}/bundle_artifacts

**How to resolve:**

- Add a ``wheel.artifact_volume`` that resolves to a ``/Volumes/...`` path for
  the environment you are generating.
- Verify any ``${tokens}`` in the path resolve to a ``/Volumes/...`` value for
  that environment.
- Or set the pipeline's ``packaging`` back to ``source``.

.. seealso::

   :doc:`package_pipelines_as_wheels` for the artifact-volume requirement and the
   per-environment ``packaging`` toggle.

Validation Errors (LHP-VAL)
----------------------------

Validation errors indicate that your configuration is syntactically valid YAML but
contains values that are structurally incorrect, missing, or incompatible.

LHP-VAL-001: Missing Required Field
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   :doc:`monitoring_reference` for details on the ``__eventlog_monitoring`` reserved keyword
   and monitoring pipeline configuration.

LHP-VAL-011: Monitoring Alias in Pipeline List / Schema Syntax Error
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

   :doc:`monitoring_reference` for details on the ``__eventlog_monitoring`` reserved keyword
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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

.. _lhp-val-062:

LHP-VAL-062: Invalid Pipeline Packaging Mode
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** A pipeline's ``packaging`` field is set to a value other than
``source`` or ``wheel``. The toggle lives in ``pipeline_config-<env>.yaml`` (per
pipeline or under ``project_defaults``) and accepts only those two values.

**Common causes:**

- A typo such as ``wheels`` or ``whl`` instead of ``wheel``.
- A case mismatch such as ``Wheel`` or ``Source`` (the values are
  case-sensitive).

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-062)

   ---
   pipeline: large_ingest_a
   packaging: wheels   # not a valid mode

.. code-block:: yaml
   :caption: After (fixed)

   ---
   pipeline: large_ingest_a
   packaging: wheel

.. seealso::

   :doc:`package_pipelines_as_wheels` for the ``packaging`` toggle, its
   precedence, and the per-environment behavior.

.. _lhp-val-063:

LHP-VAL-063: Invalid ``depends_on`` Entry
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** An action's optional ``depends_on`` field contains a
malformed entry. ``depends_on`` is a list of upstream table references used to
declare dependency-graph edges explicitly; each entry must be a non-empty
string of at most three dot-separated parts
(``catalog.schema.table``, ``schema.table``, or ``table``) with no blank parts.

**Common causes:**

- An entry that is not a string, or an empty / whitespace-only string
- A reference with more than three dot-separated parts
- A reference with a blank part (for example ``catalog..table`` or a trailing dot)

.. code-block:: yaml
   :caption: Before (triggers LHP-VAL-063)

   actions:
     - name: build_summary
       type: transform
       transform_type: python
       source: v_orders
       module_path: "transforms/build_summary.py"
       function_name: run
       depends_on:
         - "a.b.c.d"   # four parts — too many
         - ""          # empty string

.. code-block:: yaml
   :caption: After (fixed)

   actions:
     - name: build_summary
       type: transform
       transform_type: python
       source: v_orders
       module_path: "transforms/build_summary.py"
       function_name: run
       depends_on:
         - my_catalog.my_schema.my_table
         - my_schema.my_table

.. seealso::

   :doc:`dependency_analysis` for when and how to use ``depends_on`` to declare
   edges the analyzer cannot parse from your sources.

I/O Errors (LHP-IO)
--------------------

I/O errors indicate problems reading or writing files referenced in your configuration.

LHP-IO-001: File Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

.. _lhp-io-022:

LHP-IO-022: Wheel File Not Found
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** ``lhp inspect-wheel`` was given a wheel **path** that does not
exist. Only the path form raises this; a pipeline-name selector locates the wheel
under ``generated/<env>/_wheels/<pipeline>/dist/`` and reports a missing build as
``LHP-GEN-001`` instead.

**Common causes:**

- A typo in the ``.whl`` path, or a shell glob that matched nothing and was passed
  through literally.
- The pipeline has not been generated yet, so no wheel has been built.
- Running from the wrong working directory, so a relative path misresolves.

.. code-block:: bash
   :caption: Before (triggers LHP-IO-022)

   lhp inspect-wheel generated/dev/_wheels/orders/dist/missing.whl

.. code-block:: bash
   :caption: After (fixed) — build first, then inspect by pipeline name

   lhp generate -e dev
   lhp inspect-wheel orders -e dev

.. seealso::

   :doc:`package_pipelines_as_wheels` (the "Inspect a built wheel" section) and
   :doc:`cli` for ``lhp inspect-wheel`` usage and exit codes.

.. _lhp-io-023:

LHP-IO-023: Not a Wheel File
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** ``lhp inspect-wheel`` was given a path that exists but is not a
usable ``.whl`` file — it is a directory, or a file whose name does not end in
``.whl``.

**Common causes:**

- Passing the pipeline's ``dist`` directory instead of the ``.whl`` inside it.
- Pointing at the runner ``.py`` or another generated file rather than the wheel.
- A selector that looks like a path (contains a separator) but targets the wrong
  file.

.. code-block:: bash
   :caption: Before (triggers LHP-IO-023)

   lhp inspect-wheel generated/dev/_wheels/orders/dist/

.. code-block:: bash
   :caption: After (fixed) — name the .whl, or inspect by pipeline name

   lhp inspect-wheel orders -e dev

.. _lhp-io-024:

LHP-IO-024: Corrupt Wheel Archive
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When it occurs:** ``lhp inspect-wheel`` reached a file that ends in ``.whl`` but
is not a valid zip archive, so it cannot be opened (a wheel is a zip file).

**Common causes:**

- A truncated or partially written wheel — for example a build interrupted
  mid-write, or a partial copy or download.
- The file was overwritten with non-wheel content.
- On-disk corruption.

.. code-block:: bash
   :caption: Recovery — rebuild the wheel

   lhp generate -e dev
   lhp inspect-wheel orders -e dev

.. note::

   LHP-built wheels are deterministic and content-addressed, so regenerating from
   the same YAML reproduces a byte-identical wheel. See
   :doc:`package_pipelines_as_wheels`.

Action Errors (LHP-ACT)
------------------------

Action errors indicate that an action type, subtype, or preset name is not recognized.

LHP-ACT-001: Unknown Type
~~~~~~~~~~~~~~~~~~~~~~~~~~

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
----------------------------

Dependency errors indicate circular references in your pipeline's view graph
or preset inheritance chain.

LHP-DEP-001: Circular Dependency Detected
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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

Deprecation Warnings (LHP-DEPR)
-------------------------------

Deprecation codes are **warnings, not failures** — generation and validation
still succeed. They flag fields and syntax that are scheduled for removal in a
future release so you can migrate ahead of time.

.. list-table::
   :header-rows: 1
   :widths: 20 80

   * - Code
     - Deprecated usage and replacement
   * - ``LHP-DEPR-001``
     - Bare-braces ``{token}`` substitution syntax. Use ``${token}`` instead
       (the only valid non-``$`` braces form is ``%{local_var}`` for local
       variables).
   * - ``LHP-DEPR-002``
     - The ``database`` field. Use ``catalog`` and ``schema`` instead.
   * - ``LHP-DEPR-003``
     - The schema-transform ``enforcement`` key.
   * - ``LHP-DEPR-004``
     - The ``database_suffix`` field. Use ``catalog`` and ``schema`` instead.

.. note::

   The legacy ``blueprint:`` / ``use_blueprint:`` syntax is **not** a soft
   deprecation — mixing it with the current syntax is a hard ``LHP-VAL-061``
   error.

General Troubleshooting
-----------------------

Dependency Debugging
~~~~~~~~~~~~~~~~~~~~

.. code-block:: bash
   :caption: Dependency debugging

   # Show dependency graph
   lhp validate --env dev --show-dependencies

   # Validate for circular dependencies
   lhp validate --env dev --check-cycles

Performance Optimization
~~~~~~~~~~~~~~~~~~~~~~~~

- Use **include patterns** to limit file scanning scope
- Keep **FlowGroups focused** — avoid overly large YAML files
- Use **specific targets** when possible instead of full pipeline generation

Getting Help
------------

**Error not listed here?** This page documents the most common errors you will
encounter as a pipeline author. Some error codes are used internally for rare
edge cases and are not listed above.

If you encounter an error that is not documented here:

1. Read the error message carefully — every LHP error includes a description,
   context, and numbered fix suggestions directly in the terminal output
2. Run the command again with ``--verbose`` for additional diagnostic information
3. Run ``lhp validate --env <env>`` to check your full configuration
4. Report the issue at https://github.com/MehdiDataHandcraft/LakehousePlumber/issues
