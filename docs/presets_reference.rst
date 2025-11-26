===================
Presets Reference
===================

Overview
========

Presets provide reusable configuration defaults that are automatically merged with
explicit configurations in your FlowGroups and Templates. They enable consistent
settings across your data platform without repetition.

**Key Benefits:**

- Enforce organizational standards (error handling, retention policies)
- Reduce configuration duplication
- Simplify updates to common settings
- Support configuration inheritance and precedence

**How Presets Work:**

Presets use **implicit type matching** to apply defaults. When a preset defines
``load_actions.cloudfiles``, those defaults automatically apply to any action
where ``source.type == "cloudfiles"``. No conditional logic or explicit matching
is required.

Preset Structure
================

Basic Structure
---------------

.. code-block:: yaml
   :caption: Basic preset structure
   
   name: my_preset
   version: "1.0"
   description: "Preset description"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           key: value
     
     write_actions:
       streaming_table:
         table_properties:
           key: value

**Required Fields:**

- **name**: Unique identifier for the preset
- **defaults**: Configuration defaults organized by action type

**Optional Fields:**

- **version**: Version tracking for change management
- **description**: Documentation about preset purpose
- **extends**: Parent preset name for inheritance

Configuration Defaults
=======================

Load Actions
------------

CloudFiles Defaults
~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: presets/cloudfiles_defaults.yaml
   
   name: cloudfiles_defaults
   version: "1.0"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.rescuedDataColumn: "_rescued_data"
           ignoreCorruptFiles: "true"
           ignoreMissingFiles: "true"
           cloudFiles.useStrictGlobber: "false"
           cloudFiles.maxFilesPerTrigger: 200
           cloudFiles.schemaEvolutionMode: "addNewColumns"

**How it works:**

- Options are deep-merged into ``source.options`` of CloudFiles actions
- Explicit options in flowgroup/template override preset defaults on conflicts
- Non-conflicting options from both sources are preserved

**Generated Code Example:**

When a template defines ``cloudFiles.format: csv`` and the preset defines the above,
the generated code contains ALL options:

.. code-block:: python

   df = (
       spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "csv")  # From template
       .option("cloudFiles.rescuedDataColumn", "_rescued_data")  # From preset
       .option("ignoreCorruptFiles", "true")  # From preset
       .option("ignoreMissingFiles", "true")  # From preset
       .option("cloudFiles.useStrictGlobber", "false")  # From preset
       .option("cloudFiles.maxFilesPerTrigger", 200)  # From preset
       .load(path)
   )

Write Actions
-------------

Streaming Table Defaults
~~~~~~~~~~~~~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: presets/bronze_layer.yaml
   
   name: bronze_layer
   version: "1.0"
   
   defaults:
     write_actions:
       streaming_table:
         table_properties:
           delta.enableRowTracking: "true"
           delta.autoOptimize.optimizeWrite: "true"
           delta.enableChangeDataFeed: "true"
           quality: "bronze"

**How it works:**

- Properties are deep-merged into ``write_target.table_properties``
- Preset properties + explicit properties are combined
- Explicit properties override preset values on conflicts

**Generated Code Example:**

.. code-block:: python

   dp.create_streaming_table(
       name="catalog.schema.table",
       table_properties={
           "PII": "true",  # From explicit config
           "delta.enableRowTracking": "true",  # From preset
           "delta.autoOptimize.optimizeWrite": "true",  # From preset
           "delta.enableChangeDataFeed": "true",  # From preset
           "quality": "bronze",  # From preset
       }
   )

Preset Application
==================

How Presets Match Actions
--------------------------

Presets use **implicit type-based matching**:

1. **Load Actions:** ``load_actions.{source_type}`` matches actions where ``source.type == {source_type}``

   - ``load_actions.cloudfiles`` → applies to CloudFiles load actions
   - ``load_actions.delta`` → applies to Delta load actions
   - ``load_actions.jdbc`` → applies to JDBC load actions

2. **Write Actions:** ``write_actions.{target_type}`` matches actions where ``write_target.type == {target_type}``

   - ``write_actions.streaming_table`` → applies to streaming table writes
   - ``write_actions.materialized_view`` → applies to materialized view writes

No ``when`` conditions or explicit selectors are needed. The system automatically
applies the appropriate defaults based on action types.

Precedence Rules
----------------

When the same configuration is defined at multiple levels:

1. **Flowgroup explicit config** (highest precedence)
2. **Flowgroup preset**
3. **Template explicit config**
4. **Template preset** (lowest precedence)

Merge Behavior
--------------

**Deep Merge for Nested Objects:**

Options and properties are deep-merged, not replaced:

.. code-block:: yaml
   :caption: Preset Configuration
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.rescuedDataColumn: "_rescued_data"
           ignoreCorruptFiles: "true"

.. code-block:: yaml
   :caption: Action Configuration
   
   source:
     type: cloudfiles
     options:
       cloudFiles.format: csv
       cloudFiles.inferColumnTypes: "true"

**Result:** ALL options present in generated code:

.. code-block:: python

   .option("cloudFiles.format", "csv")
   .option("cloudFiles.inferColumnTypes", "true")
   .option("cloudFiles.rescuedDataColumn", "_rescued_data")
   .option("ignoreCorruptFiles", "true")

Usage Examples
==============

Template with Preset
---------------------

Templates can include presets that apply to all generated actions:

.. code-block:: yaml
   :caption: templates/ingestion_template.yaml
   
   name: ingestion_template
   version: "1.0"
   
   presets:
     - cloudfiles_defaults  # Applied to all template actions
   
   parameters:
     - name: table_name
       type: string
       required: true
   
   actions:
     - name: "load_{{ table_name }}"
       type: load
       source:
         type: cloudfiles
         options:
           cloudFiles.format: parquet  # Merges with preset options
       target: "v_{{ table_name }}_raw"

Flowgroup with Multiple Presets
--------------------------------

FlowGroups can apply multiple presets in order:

.. code-block:: yaml
   :caption: flowgroup with multiple presets
   
   pipeline: data_pipeline
   flowgroup: customer_ingestion
   
   presets:
     - cloudfiles_defaults
     - bronze_layer
   
   actions:
     - name: load_customers
       type: load
       source:
         type: cloudfiles
         path: "/data/customers/*.csv"
         options:
           cloudFiles.format: csv
       target: v_customers_raw
     
     - name: write_customers
       type: write
       source: v_customers_raw
       write_target:
         type: streaming_table
         database: "{catalog}.{schema}"
         table: "customers"

Preset Inheritance
------------------

Presets can extend other presets:

.. code-block:: yaml
   :caption: presets/base_config.yaml
   
   name: base_config
   version: "1.0"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.rescuedDataColumn: "_rescued_data"

.. code-block:: yaml
   :caption: presets/bronze_cloudfiles.yaml
   
   name: bronze_cloudfiles
   version: "1.0"
   extends: base_config  # Inherits from base_config
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.maxFilesPerTrigger: 200
           ignoreCorruptFiles: "true"

Best Practices
==============

1. **Structure Correctly**

   Always nest CloudFiles options under ``options:`` key:
   
   .. code-block:: yaml
      
      # ✅ CORRECT
      load_actions:
        cloudfiles:
          options:
            cloudFiles.rescuedDataColumn: "_rescued_data"
      
      # ❌ WRONG - Missing 'options' nesting
      load_actions:
        cloudfiles:
          cloudFiles.rescuedDataColumn: "_rescued_data"

2. **Use Descriptive Names**

   - Good: ``cloudfiles_error_handling``, ``bronze_layer_defaults``
   - Bad: ``preset1``, ``my_preset``

3. **Version Presets**

   Track breaking changes with version numbers

4. **Document Clearly**

   Explain what each preset configures and why

5. **Test Merging**

   Verify preset + explicit configs merge correctly by inspecting generated code

Common Patterns
===============

Error Handling Preset
---------------------

.. code-block:: yaml
   :caption: presets/error_handling.yaml
   
   name: error_handling
   version: "1.0"
   description: "Standard error handling for all data sources"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           ignoreCorruptFiles: "true"
           ignoreMissingFiles: "true"
           cloudFiles.rescuedDataColumn: "_rescued_data"

Bronze Layer Preset
-------------------

.. code-block:: yaml
   :caption: presets/bronze_layer.yaml
   
   name: bronze_layer
   version: "1.0"
   description: "Standard configuration for bronze layer tables"
   
   defaults:
     write_actions:
       streaming_table:
         table_properties:
           delta.enableRowTracking: "true"
           delta.autoOptimize.optimizeWrite: "true"
           delta.enableChangeDataFeed: "true"
           quality: "bronze"

Performance Tuning Preset
--------------------------

.. code-block:: yaml
   :caption: presets/performance_tuning.yaml
   
   name: performance_tuning
   version: "1.0"
   description: "Optimized settings for large-scale ingestion"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.maxFilesPerTrigger: 1000
           cloudFiles.useStrictGlobber: "false"

Troubleshooting
===============

Preset Options Not Appearing
-----------------------------

**Problem:** Preset options don't appear in generated code

**Solutions:**

1. **Verify correct nesting structure:**
   
   Check that CloudFiles options are under ``options:`` key:
   
   .. code-block:: yaml
      
      defaults:
        load_actions:
          cloudfiles:
            options:  # ← This level is CRITICAL
              cloudFiles.rescuedDataColumn: "_rescued_data"

2. **Check source type matches:**
   
   The source type in your action must match the preset key:
   
   - Action has ``source.type: cloudfiles``
   - Preset must have ``load_actions.cloudfiles``

3. **Inspect generated code:**
   
   Look at the ``.option()`` calls in generated Python files to confirm merge

Property Conflicts
------------------

**Problem:** Explicit config value being ignored

**Expected Behavior:** Explicit configurations override preset values (by design)

**Solution:** This is correct behavior. If you want the preset value to win,
remove the explicit configuration.

Preset Not Found Error
-----------------------

**Problem:** ``ValueError: Preset 'my_preset' not found``

**Solutions:**

1. Verify preset file exists in ``presets/`` directory
2. Check preset filename matches the ``name`` field in the YAML
3. Ensure preset file has ``.yaml`` extension

Limitations
===========

**No Conditional Logic:**

Presets do NOT support:

- ``when`` conditions or conditional application
- Dynamic value selection based on action properties
- Runtime evaluation of expressions

Presets use simple type-based matching only. For conditional behavior,
use separate presets for different scenarios.

**No Field-Level Merging for Non-Dict Values:**

Preset merge is deep for nested dictionaries, but not for:

- Lists (preset list replaces explicit list entirely)
- Scalar values (explicit value wins on conflict)

Summary
=======

**Key Takeaways:**

1. Presets provide defaults that merge with explicit configs
2. Use correct structure with ``options`` nesting for CloudFiles
3. Presets match actions by type (implicit matching)
4. Explicit configs override preset defaults
5. Non-conflicting values from both sources are preserved
6. No conditional logic - use separate presets for different cases

**Related Documentation:**

- :doc:`concepts` - Presets overview and basic examples
- :doc:`templates_reference` - Template documentation
- :doc:`actions_reference` - Action configuration reference

