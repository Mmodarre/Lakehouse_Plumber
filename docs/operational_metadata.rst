Operational Metadata
====================

.. meta::
   :description: Configure operational metadata columns for lineage, provenance, and processing context in Lakehouse Plumber pipelines.

Concept
-------

**What.** Operational metadata columns are auto-injected provenance and lineage
columns that Lakehouse Plumber (LHP) adds to your generated tables. You define
each column once in ``lhp.yaml`` — a name, a Spark expression, and the target
table types it applies to — then opt actions in by listing the column name.
LHP appends a ``withColumn(...)`` call for every selected column to the
generated Python.

**When.** Enable operational metadata when you need a row-level audit trail
(processing timestamp, pipeline name, run ID) or file-level lineage
(source file path, modification time, size) on Bronze loads. Disable it on
Silver and Gold tables when those columns no longer carry meaning downstream.

**Minimum example.** One preset turns on a single processing-timestamp column:

.. code-block:: yaml
   :caption: presets/bronze_layer.yaml

   name: bronze_layer
   version: "1.0"

   defaults:
     operational_metadata: ["_processing_timestamp"]

The column definition itself (the Spark expression behind
``_processing_timestamp``) lives at project level — see the reference body below.

Reference
---------

Column catalogue
~~~~~~~~~~~~~~~~

Define operational metadata columns in the project-level configuration file
under the ``operational_metadata`` key. Each column declares its Spark
expression, a description, the target types it applies to, and any extra
imports the expression needs.

.. code-block:: yaml
   :caption: lhp.yaml - Project operational metadata configuration
   :linenos:

   # LakehousePlumber Project Configuration
   name: my_lakehouse_project
   version: "1.0"

   operational_metadata:
     columns:
       _processing_timestamp:
         expression: "F.current_timestamp()"
         description: "When the record was processed by the pipeline"
         applies_to: ["streaming_table", "materialized_view", "view"]

       _source_file_path:
         expression: "F.col('_metadata.file_path')"
         description: "Source file path for lineage tracking"
         applies_to: ["view"]

       _record_hash:
         expression: "F.xxhash64(*[F.col(c) for c in df.columns])"
         description: "Hash of all record fields for change detection"
         applies_to: ["streaming_table", "materialized_view", "view"]
         additional_imports:
           - "from pyspark.sql.functions import xxhash64"

       _pipeline_name:
         expression: "F.lit('${pipeline_name}')"
         description: "Name of the processing pipeline"
         applies_to: ["streaming_table", "materialized_view", "view"]

Target type compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~

The ``applies_to`` field controls which DLT table types can use each operational metadata column.
LHP automatically filters columns based on the target type to prevent runtime errors.

**Purpose of target type restrictions:**

When defining operational metadata columns at the project level, the ``applies_to`` field serves as a
**safeguard mechanism** to protect end users from accidentally using incompatible columns in their
pipeline configurations. This is a defensive design pattern that prevents common mistakes.

**Best practice for project administrators:**

- Set restrictive ``applies_to`` values for source-specific columns (e.g., CloudFiles metadata)
- Use broader ``applies_to`` values for universal columns (e.g., timestamps, pipeline names)
- This protects pipeline developers from runtime failures and provides clear usage guidance

**Target types:**

- **``view``** - Source views created by load actions (``@dp.temporary_view()``)
- **``streaming_table``** - Live tables with streaming updates (``@dp.materialized_view()``)
- **``materialized_view``** - Batch-computed views for analytics (``@dp.temporary_view()``)

**Source-specific metadata limitations:**

.. warning::
   - Metadata columns that depend on CloudFiles features (like ``_metadata.file_path``) are **only available in views** that load data from CloudFiles sources. These columns will cause runtime errors if used with JDBC, SQL, Delta, or custom_datasource sources.
   - Custom data sources may provide their own metadata columns depending on their implementation, but CloudFiles-specific metadata will not be available.

.. seealso::
   For complete details on file metadata columns available in Databricks CloudFiles, refer to the Databricks documentation:
   `File Metadata Columns <https://docs.databricks.com/aws/en/ingestion/file-metadata-column>`_


**Examples of source-restricted columns:**

.. code-block:: yaml
   :caption: CloudFiles-only operational metadata
   :linenos:
   :emphasize-lines: 6

   operational_metadata:
     columns:
       _source_file_name:
         expression: "F.col('_metadata.file_name')"
         description: "Original file name with extension"
         applies_to: ["view"]  # Only views, and only CloudFiles sources

       _file_modification_time:
         expression: "F.col('_metadata.file_modification_time')"
         description: "When the source file was last modified"
         applies_to: ["view"]  # Only views, and only CloudFiles sources

       _processing_timestamp:
         expression: "F.current_timestamp()"
         description: "When record was processed (works everywhere)"
         applies_to: ["streaming_table", "materialized_view", "view"]

**Safe usage patterns:**

.. code-block:: yaml
   :caption: Source-aware metadata configuration
   :linenos:

   # CloudFiles load action - can use file metadata
   - name: load_files
     type: load
     source:
       type: cloudfiles
       path: "/mnt/data/*.json"
     operational_metadata:
       - "_source_file_name"        # ✓ Available in CloudFiles
       - "_file_modification_time"  # ✓ Available in CloudFiles
       - "_processing_timestamp"    # ✓ Available everywhere
     target: v_file_data

   # JDBC load action - file metadata not available
   - name: load_database
     type: load
     source:
       type: jdbc
       table: "customers"
     operational_metadata:
       - "_processing_timestamp"    # ✓ Available everywhere
       # DO NOT USE: "_source_file_name" would cause runtime error
     target: v_database_data

   # Custom data source - metadata depends on implementation
   - name: load_api_data
     type: load
     module_path: "data_sources/api_source.py"
     custom_datasource_class: "APIDataSource"
     options:
       api_endpoint: "https://api.example.com/data"
     operational_metadata:
       - "_processing_timestamp"    # ✓ Available everywhere
       # Custom metadata depends on DataSource implementation
     target: v_api_data

Multi-level configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Operational metadata can be configured at multiple levels with **additive behavior** - columns from all levels are combined together.

**Additive behavior:** Operational metadata columns are **never overridden** between levels.
Instead, columns from preset + flowgroup + action levels are **combined together**. The
only exception is ``operational_metadata: false`` at action level, which disables **all**
metadata.

**Preset level**

.. code-block:: yaml
   :caption: presets/bronze_layer.yaml
   :linenos:

   name: bronze_layer
   version: "1.0"

   defaults:
     operational_metadata: ["_processing_timestamp", "_source_file_path"]

**FlowGroup level**

.. code-block:: yaml
   :caption: pipelines/customer_ingestion/load_customers.yaml
   :linenos:
   :emphasize-lines: 4

   pipeline: customer_ingestion
   flowgroup: load_customers
   presets: ["bronze_layer"]
   operational_metadata: ["_record_hash"]  # Adds to preset columns

   actions:
     - name: load_customer_files
       type: load
       source:
         type: cloudfiles
         path: "/mnt/landing/customers/*.json"
         format: json
       target: v_customers_raw

**Action level**

.. code-block:: yaml
   :caption: Action-specific metadata configuration
   :linenos:
   :emphasize-lines: 8-11

   actions:
     - name: load_with_custom_metadata
       type: load
       source:
         type: cloudfiles
         path: "/mnt/data/*.parquet"
         format: parquet
       operational_metadata:  # Adds to flowgroup + preset columns
         - "_pipeline_name"
         - "_custom_business_logic"
       target: v_enriched_data

     - name: load_without_metadata
       type: load
       source:
         type: sql
         sql: "SELECT * FROM source_table"
               operational_metadata: false  # Disables all metadata
        target: v_clean_data

**Additive behavior example:**

.. code-block:: yaml
   :caption: Complete example showing additive behavior
   :linenos:
   :emphasize-lines: 4, 9, 18-20

   # Preset defines base columns
   # presets/bronze_layer.yaml
   defaults:
     operational_metadata: ["_processing_timestamp"]

   # FlowGroup adds more columns
   pipeline: customer_ingestion
   flowgroup: load_customers
   operational_metadata: ["_source_file_path", "_record_hash"]

   actions:
     - name: load_customer_files
       type: load
       source:
         type: cloudfiles
         path: "/mnt/data/*.json"
       # Action adds even more columns
       operational_metadata:
         - "_pipeline_name"
         - "_custom_business_logic"
       target: v_customers_raw

   # Final result: ALL columns combined
   # ✓ _processing_timestamp      (from preset)
   # ✓ _source_file_path          (from flowgroup)
   # ✓ _record_hash               (from flowgroup)
   # ✓ _pipeline_name             (from action)
   # ✓ _custom_business_logic     (from action)

Usage patterns
~~~~~~~~~~~~~~

**Enable all available columns:**

.. code-block:: yaml

   operational_metadata: true

**Select specific columns:**

.. code-block:: yaml

   operational_metadata:
     - "_processing_timestamp"
     - "_source_file_path"
     - "_record_hash"

**Disable metadata:**

.. code-block:: yaml

   operational_metadata: false

**Generated Python code:**

.. code-block:: python
   :caption: Generated DLT code with operational metadata
   :linenos:
   :emphasize-lines: 8-11

   @dp.temporary_view()
   def v_customers_raw():
       """Load customer files from landing zone"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "json") \
           .load("/mnt/landing/customers/*.json")

       # Add operational metadata columns
       df = df.withColumn('_processing_timestamp', F.current_timestamp())
       df = df.withColumn('_source_file_path', F.col('_metadata.file_path'))
       df = df.withColumn('_record_hash', F.xxhash64(*[F.col(c) for c in df.columns]))

       return df


.. danger::
   - When you add operational metadata columns to an upstream action,
     if your downstream action is a transformation, for example SQL transform,
     you need to make sure they are included in the SQL query.

Internal Implementation Note
----------------------------

The codebase maintains strict semantic separation between single and multi-document YAML files:

- ``load_yaml_file()`` - For single-document files (configs, templates, presets)

  * Validates exactly one document exists
  * Raises ``MultiDocumentError`` (LHP-IO-003) for empty files or files with multiple documents
  * Used for templates, presets, configs, and other single-document files

- ``load_yaml_documents_all()`` - For multi-document files (flowgroup files only)

  * Returns list of all documents
  * Used exclusively for flowgroup YAML files that may contain multiple flowgroups

This strict validation prevents accidental misuse and catches bugs early. If you encounter a
``MultiDocumentError``, the error message will guide you to the correct loading method.
