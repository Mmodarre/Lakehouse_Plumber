Multi-Flowgroup YAML Files
===========================

Overview
--------

LakehousePlumber supports defining **multiple flowgroups in a single YAML file**, reducing file proliferation and improving project organization. This is particularly useful when you have many similar flowgroups that share common configuration.

**Key Benefits:**

* Reduce file count (e.g., 93 files → 20-30 files)
* Group related flowgroups logically (e.g., all SAP master data tables)
* Share common configuration across flowgroups
* Maintain cleaner project structure
* Easier to maintain and review

Two Syntaxes Available
-----------------------

LakehousePlumber supports two syntaxes for multi-flowgroup files:

1. **Multi-Document Syntax** using ``---`` separators (traditional YAML)
2. **Array Syntax** with ``flowgroups:`` list (with inheritance)

Both syntaxes work identically for generation, validation, and all LHP commands.

Multi-Document Syntax
----------------------

Use YAML's multi-document feature with ``---`` separators to define multiple flowgroups in one file.

**Example: SAP Master Data Ingestions**

.. code-block:: yaml

   # Brand master data ingestion from SAP
   pipeline: raw_ingestions_sap
   flowgroup: sap_brand_ingestion
   use_template: TMPL003_parquet_ingestion_template
   template_parameters:
     table_name: raw_sap_brand
     landing_folder: brand
   ---
   # Category master data ingestion from SAP
   pipeline: raw_ingestions_sap
   flowgroup: sap_cat_ingestion
   use_template: TMPL003_parquet_ingestion_template
   template_parameters:
     table_name: raw_sap_cat
     landing_folder: category
   ---
   # Carrier master data ingestion from SAP
   pipeline: raw_ingestions_sap
   flowgroup: sap_carrier_ingestion
   use_template: TMPL003_parquet_ingestion_template
   template_parameters:
     table_name: raw_sap_carrier
     landing_folder: carrier

**Characteristics:**

* Each flowgroup is a complete, self-contained document
* Use ``---`` to separate documents
* No field inheritance between documents
* Best for flowgroups with different structures

Array Syntax with Inheritance
------------------------------

Use the ``flowgroups:`` array syntax to define multiple flowgroups with shared configuration at the document level.

**Example: SAP Master Data Ingestions (with inheritance)**

.. code-block:: yaml

   # SAP Master Data Ingestions
   pipeline: raw_ingestions_sap
   use_template: TMPL003_parquet_ingestion_template
   
   flowgroups:
     - flowgroup: sap_brand_ingestion
       template_parameters:
         table_name: raw_sap_brand
         landing_folder: brand
     
     - flowgroup: sap_cat_ingestion
       template_parameters:
         table_name: raw_sap_cat
         landing_folder: category
     
     - flowgroup: sap_carrier_ingestion
       template_parameters:
         table_name: raw_sap_carrier
         landing_folder: carrier

**Characteristics:**

* Shared fields at document level are inherited by all flowgroups
* Individual flowgroups can override inherited values
* More concise when many flowgroups share configuration
* Best for similar flowgroups with common settings

Inheritance Rules
-----------------

When using array syntax, flowgroups inherit document-level fields:

**Inheritable Fields:**

* ``pipeline``
* ``use_template``
* ``presets``
* ``operational_metadata``

**Inheritance Behavior:**

1. **Key Not Present** → Inherits document-level value
2. **Key Present (any value)** → Uses flowgroup-level value (overrides)
3. **Empty List ``[]``** → Explicit override (no inheritance)

**Example: Inheritance with Override**

.. code-block:: yaml

   pipeline: bronze_layer
   presets:
     - bronze_preset
     - data_quality
   operational_metadata: true
   
   flowgroups:
     - flowgroup: table1_bronze
       # Inherits: pipeline, presets, operational_metadata
     
     - flowgroup: table2_bronze
       presets:
         - silver_preset  # Override presets
       # Inherits: pipeline, operational_metadata
     
     - flowgroup: table3_bronze
       presets: []  # Explicit: no presets
       # Inherits: pipeline, operational_metadata

Type-Agnostic Override
^^^^^^^^^^^^^^^^^^^^^^

Different types can override each other (e.g., bool → list):

.. code-block:: yaml

   operational_metadata: true
   
   flowgroups:
     - flowgroup: table1
       # Inherits: operational_metadata: true
     
     - flowgroup: table2
       operational_metadata: ["col1", "col2", "col3"]
       # Override with list

Migration Guide
---------------

Converting Multiple Files to One
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

**Before: 3 Separate Files**

``pipelines/bronze/brand.yaml``:

.. code-block:: yaml

   pipeline: bronze_sap
   flowgroup: bronze_sap_brand
   use_template: standard_bronze
   template_parameters:
     table_name: brand

``pipelines/bronze/category.yaml``:

.. code-block:: yaml

   pipeline: bronze_sap
   flowgroup: bronze_sap_category
   use_template: standard_bronze
   template_parameters:
     table_name: category

``pipelines/bronze/carrier.yaml``:

.. code-block:: yaml

   pipeline: bronze_sap
   flowgroup: bronze_sap_carrier
   use_template: standard_bronze
   template_parameters:
     table_name: carrier

**After: 1 Combined File**

``pipelines/bronze/sap_master_data.yaml``:

.. code-block:: yaml

   pipeline: bronze_sap
   use_template: standard_bronze
   
   flowgroups:
     - flowgroup: bronze_sap_brand
       template_parameters:
         table_name: brand
     
     - flowgroup: bronze_sap_category
       template_parameters:
         table_name: category
     
     - flowgroup: bronze_sap_carrier
       template_parameters:
         table_name: carrier

**Result:** 3 files → 1 file (67% reduction)

When to Use Multi-Flowgroup Files
----------------------------------

**Use Multi-Flowgroup Files When:**

* Multiple flowgroups share common configuration
* Tables belong to same logical group (e.g., all SAP master data)
* Flowgroups use same template with different parameters
* Managing many similar, small flowgroups

**Use Separate Files When:**

* Flowgroups have significantly different structures
* Flowgroups belong to different logical domains
* Independent ownership/maintenance is important
* File would become too large (>500 lines as guideline)

**Use Multi-Document Syntax When:**

* Flowgroups have different base configurations
* You prefer traditional YAML multi-document approach
* No shared fields to inherit

**Use Array Syntax When:**

* Many flowgroups share same configuration
* You want to minimize repetition
* Flowgroups are variations on a theme

Real-World Example: ACME Supermarkets
--------------------------------------

The ACME Supermarkets project originally had **93 flowgroup files** across raw, bronze, and silver layers. Using multi-flowgroup files:

**Before:**

.. code-block:: text

   pipelines/01_raw_ingestion/SAP/
     ├── brand_ingestion.yaml
     ├── category_ingestion.yaml
     ├── carrier_ingestion.yaml
     ├── product_ingestion.yaml
     ├── supplier_ingestion.yaml
     ├── warehouse_ingestion.yaml
     ├── uom_ingestion.yaml
     ├── user_ingestion.yaml
     └── ... (15+ more files)

**After:**

.. code-block:: text

   pipelines/01_raw_ingestion/SAP/
     ├── master_data_ingestions.yaml       # 8 flowgroups
     ├── transactional_data_ingestions.yaml  # 6 flowgroups
     └── inventory_ingestions.yaml          # 4 flowgroups

**Result:** 27 files → 3 files (89% reduction)

Complete Example
----------------

Here's a complete example combining multiple concepts:

.. code-block:: yaml

   # Bronze Layer - NCR Transaction Data
   pipeline: bronze_ncr
   use_template: TMPL004_raw_to_bronze_standard
   presets:
     - bronze_layer
     - data_quality_checks
   operational_metadata: true
   
   flowgroups:
     - flowgroup: pos_transaction_bronze
       template_parameters:
         raw_table_name: raw_ncr_pos_transaction
         bronze_table_name: bronze_ncr_pos_transaction
         generate_surrogate_key: true
         surrogate_key_name: transaction_key
     
     - flowgroup: payment_method_bronze
       template_parameters:
         raw_table_name: raw_ncr_payment_method
         bronze_table_name: bronze_ncr_payment_method
         generate_surrogate_key: true
         surrogate_key_name: payment_method_key
     
     - flowgroup: location_bronze
       template_parameters:
         raw_table_name: raw_ncr_location
         bronze_table_name: bronze_ncr_location
         generate_surrogate_key: true
         surrogate_key_name: location_key
       presets:
         - bronze_layer
         - data_quality_checks
         - scd_type2  # Additional preset for this flowgroup
     
     - flowgroup: customer_bronze
       template_parameters:
         raw_table_name: raw_ncr_customer
         bronze_table_name: bronze_ncr_customer
         generate_surrogate_key: true
         surrogate_key_name: customer_key
       operational_metadata: ["col1", "col2", "col3"]  # Override with specific columns

Validation and Error Handling
------------------------------

Duplicate Flowgroup Names
^^^^^^^^^^^^^^^^^^^^^^^^^^

LakehousePlumber detects duplicate flowgroup names within the same file:

.. code-block:: yaml

   flowgroups:
     - flowgroup: my_flowgroup
     - flowgroup: my_flowgroup  # ERROR: Duplicate name

**Error:**

.. code-block:: text

   ValueError: Duplicate flowgroup name 'my_flowgroup' in file 
   pipelines/bronze/data.yaml

Mixed Syntax Not Allowed
^^^^^^^^^^^^^^^^^^^^^^^^^

You cannot mix multi-document and array syntax in the same file:

.. code-block:: yaml

   # This is NOT allowed
   flowgroups:
     - flowgroup: fg1
   ---
   flowgroup: fg2

**Error:**

.. code-block:: text

   ValueError: Mixed syntax detected in pipelines/bronze/data.yaml: 
   cannot use both multi-document (---) and flowgroups array syntax 
   in the same file

Backward Compatibility
----------------------

**All existing single-flowgroup files continue to work without changes.**

LakehousePlumber automatically detects:

* Single-flowgroup files (original behavior)
* Multi-document files (new)
* Array syntax files (new)

All commands (``generate``, ``validate``, ``show``, etc.) work identically regardless of syntax.

CLI Commands
------------

All LakehousePlumber commands work seamlessly with multi-flowgroup files:

Generate
^^^^^^^^

.. code-block:: bash

   # Generate from multi-flowgroup file
   lhp generate bronze_sap --env dev
   
   # Discovers all flowgroups automatically
   # Output: Generates one Python file per flowgroup

Validate
^^^^^^^^

.. code-block:: bash

   # Validate multi-flowgroup files
   lhp validate bronze_sap --env dev
   
   # Validates all flowgroups in all files

Show
^^^^

.. code-block:: bash

   # Show specific flowgroup (works regardless of file structure)
   lhp show sap_brand_ingestion --env dev
   
   # Finds flowgroup even in multi-flowgroup files

State Tracking
--------------

State tracking works identically with multi-flowgroup files:

* Each flowgroup tracked individually by ``(pipeline, flowgroup)`` tuple
* Source YAML file path tracked for each flowgroup
* Multiple flowgroups from same file tracked with same source path
* Smart regeneration based on individual flowgroup changes

Best Practices
--------------

1. **Group Logically:** Combine flowgroups that belong together conceptually
2. **Keep Manageable:** Aim for 5-15 flowgroups per file
3. **Use Clear Names:** Name files to reflect their contents (e.g., ``sap_master_data.yaml``)
4. **Add Comments:** Document the purpose of each section
5. **Consistent Style:** Choose one syntax per project for consistency
6. **Test First:** Validate with ``lhp validate`` after combining files

Troubleshooting
---------------

Flowgroup Not Found
^^^^^^^^^^^^^^^^^^^

If ``lhp show`` cannot find your flowgroup:

1. Check the flowgroup name is spelled correctly
2. Verify the file is in the ``pipelines/`` directory
3. Check ``lhp.yaml`` include patterns (if used)
4. Ensure no YAML syntax errors

Generation Issues
^^^^^^^^^^^^^^^^^

If generation fails with multi-flowgroup files:

1. Validate syntax: ``lhp validate <pipeline> --env <env>``
2. Check for duplicate flowgroup names
3. Verify template parameters are correct for each flowgroup
4. Test with ``--dry-run`` first

Performance
^^^^^^^^^^^

Multi-flowgroup files are parsed once per file, not once per flowgroup, making discovery more efficient for projects with many flowgroups.

Summary
-------

Multi-flowgroup YAML support in LakehousePlumber:

* ✅ Reduces file proliferation
* ✅ Improves project organization  
* ✅ Maintains backward compatibility
* ✅ Works with all LHP commands
* ✅ Two flexible syntaxes available
* ✅ Smart inheritance in array syntax
* ✅ Validated and tested

For questions or issues, refer to the main documentation or open an issue on GitHub.

