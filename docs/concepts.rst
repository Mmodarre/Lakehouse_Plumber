Concepts & Architecture
=======================

At its core Lakehouse Plumber converts **declarative YAML** into regular
Databricks Lakeflow Declarative Pipelines (ETL) Python code.  The YAML files are intentionally
simple and the heavy-lifting happens inside the Plumber engine at generation time.
This page explains the key building blocks you will interact with.

.. contents:: Page outline
   :depth: 2
   :local:

FlowGroups
----------
A **FlowGroup** represents a logical slice of your pipeline often a single
source table or business entity.  Each YAML file contains exactly one
FlowGroup.

Required keys in the FlowGroup YAML file

.. code-block:: yaml

   pipeline:  bronze_raw                 # pipeline name (logical)
   flowgroup: customer_bronze_ingestion  # unique name for the flowgroup (logical)
   actions:                              # list of steps in the flowgroup


.. note::
   **FlowGroup vs Pipeline:**
   - A **FlowGroup** represents a logical slice of your pipeline often a single source table or business entity.  Each YAML file contains exactly one
   FlowGroup.

   - A **Pipeline** is a logical grouping of FlowGroups. It is used to group the generated python files in the same folder.

   - Lakeflow Declarative Pipelines are **declarative** (as the name suggests) hence the order of the actions is determind at runtime by the Lakeflow engine based on the dependencies between the tables/views.

Actions
-------
Every FlowGroup lists one or more **Actions** 
Actions come in three top-level types:

+----------------+----------------------------------------------------------+
| Type           | Purpose                                                  |
+================+==========================================================+
|| **Load**      || Bring data into a temporary **view** (e.g. CloudFiles,  |
||               || Delta, JDBC, SQL, Python, custom_datasource).           |
+----------------+----------------------------------------------------------+
|| **Transform** || Manipulate data in one or more steps (SQL, Python,      |
||               || schema adjustments, data-quality checks, temp tables…). |
+----------------+----------------------------------------------------------+
|| **Write**     || Persist the final dataset to a *streaming_table* or     |
||               || *materialized_view*.                                    |
+----------------+----------------------------------------------------------+


.. note::
   - You may chain **zero or many Transform actions** between a Load and a Write.

.. important::
   - the order of the actions is determind at runtime by the Lakeflow engine based on the dependencies between the tables/views, Not the order in the YAML file or the generated Python file.


For a complete catalogue of Action sub-types and their options see
:doc:`actions_reference`.

Presets
-------
A **Preset** is a YAML file that provides default configuration snippets you can
reuse across FlowGroups.  Typical examples:

* Standardised table properties for all Bronze streaming tables.
* Standardised CloudFiles properties

Usage inside a FlowGroup YAML file:


.. code-block:: yaml
   
   presets:
     - bronze_layer

Templates
---------
While presets inject reusable **values**, **Templates** inject reusable **action
patterns** think of them as parametrised macros.

In a template file you define parameters and a list of actions that reference
those parameters.  Inside a FlowGroup you apply the template and provide actual
arguments

**Example of a template file:**

.. code-block:: yaml
   :caption: templates/csv_ingestion_template.yaml
   :linenos:

   # This is a template for ingesting CSV files with schema enforcement
   # It is used to generate the actions for the pipeline
   # within the pipeline all it need to defined are the parameters for the table name and landing folder
   # the template will generate the actions for the pipeline

   name: csv_ingestion_template
   version: "1.0"
   description: "Standard template for ingesting CSV files with schema enforcement"

   presets:
   - bronze_layer

   parameters:
   - name: table_name
      required: true
      description: "Name of the table to ingest"
   - name: landing_folder
      required: true
      description: "Name of the landing folder"

   actions:
   - name: load_{{ table_name }}_csv
      type: load
      readMode : "stream"
      operational_metadata: ["_source_file_path","_source_file_size","_source_file_modification_time","_record_hash"]
      source:
         type: cloudfiles
         path: "{landing_volume}/{{ landing_folder }}/*.csv"
         format: csv
         options:
         cloudFiles.format: csv
         header: True
         delimiter: "|"
         cloudFiles.maxFilesPerTrigger: 11
         cloudFiles.inferColumnTypes: False
         cloudFiles.schemaEvolutionMode: "addNewColumns"
         cloudFiles.rescuedDataColumn: "_rescued_data"
         cloudFiles.schemaHints: "schemas/{{ table_name }}_schema.yaml"

      target: v_{{ table_name }}_cloudfiles
      description: "Load {{ table_name }} CSV files from landing volume"

   - name: write_{{ table_name }}_cloudfiles
      type: write
      source: v_{{ table_name }}_cloudfiles
      write_target:
         type: streaming_table
         database: "{catalog}.{raw_schema}"
         table: "{{ table_name }}"
         description: "Write {{ table_name }} to raw layer" 

**Example of a flowgroup using the template:**

.. code-block:: yaml
   :caption: pipelines/01_raw_ingestion/csv_ingestions/customer_ingestion.yaml
   :linenos:
   :emphasize-lines: 11-14

   # This pipeline is used to ingest the customer table from the csv files into the raw schema
   # Pipeline variable puts the generate files in the same folder for the pipeline to pick up
   pipeline: raw_ingestions
   # Flowgroup are conceptual artifacts and has no functional purpose
   # there are used to group actions together in the generated files
   flowgroup: customer_ingestion

   # Use the template to generate the actions for the pipeline
   # Template parameters are used to pass in the table name and landing folder
   # The template will generate the actions for the pipeline
   use_template: csv_ingestion_template
   template_parameters:
   table_name: customer
   landing_folder: customer



Substitutions & Secrets
-----------------------

Environment Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Tokens wrapped in ``{token}`` or ``${token}`` are replaced at generation time
using files under ``substitutions/<env>.yaml``. This enables environment-specific
configurations while keeping pipeline definitions portable.

**Example substitution file:**

.. code-block:: yaml
   :caption: substitutions/dev.yaml
   :linenos:
   :emphasize-lines: 10-15

   # Environment-specific tokens
   dev:
     catalog: dev_catalog
     bronze_schema: bronze
     silver_schema: silver
     landing_path: /mnt/dev/landing
     checkpoint_path: /mnt/dev/checkpoints

   # Secret configuration
   secrets:
     default_scope: dev_secrets
     scopes:
       database_secrets: dev_db_secrets
       storage_secrets: dev_azure_secrets
       api_secrets: dev_external_apis


Secret Management
~~~~~~~~~~~~~~~~~

**Secret references** use the ``${secret:scope/key}`` syntax and are converted to
secure ``dbutils.secrets.get()`` calls in generated Python code. LHP validates
scope aliases and collects every secret used by the pipeline, making security
reviews and approvals easier.

**Secret reference formats:**

- ``${secret:scope_alias/key}`` - Uses specific scope alias (resolved to actual Databricks scope)
- ``${secret:key}`` - Uses default_scope if configured

.. note::
   Scope aliases (like ``database_secrets``) are mapped to actual Databricks secret scope 
   names (like ``dev_db_secrets``) in the substitution file. This provides flexibility 
   to use different scope names across environments while keeping pipeline definitions portable.


File Substitution Support
~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: Latest

LakehousePlumber now supports substitutions in external files, providing the same environment-specific flexibility for Python functions and SQL files that you have in YAML configurations.

**Supported File Types:**

================== ==================================================
File Type          Where Used
================== ==================================================
**Python Files**   • Snapshot CDC ``source_function`` files
                   • Python transform ``module_path`` files
                   • Custom datasource ``module_path`` files
**SQL Files**      • SQL load actions with ``sql_path``
                   • SQL transform actions with ``sql_path``
================== ==================================================

**Example Python Function with Substitutions:**

.. code-block:: python
   :caption: py_functions/customer_snapshot.py
   :linenos:
   :emphasize-lines: 4-5,10

   from typing import Optional, Tuple
   from pyspark.sql import DataFrame

   catalog = "{catalog}"
   schema = "{bronze_schema}"

   def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
       if latest_version is None:
           df = spark.sql(f"""
               SELECT * FROM {catalog}.{schema}.customers 
               WHERE snapshot_id = 1
           """)
           return (df, 1)
       return None

**Example SQL File with Substitutions:**

.. code-block:: sql
   :caption: sql/customer_metrics.sql
   :linenos:
   :emphasize-lines: 4-6

   SELECT 
       customer_id,
       customer_name,
       '{environment}' as source_env
   FROM {catalog}.{bronze_schema}.customers
   WHERE created_date >= '{cutoff_date}'

**Secret Support in Files:**

Both Python and SQL files support secret substitutions with the same syntax as YAML:

.. code-block:: python
   :caption: Example with secrets

   # Environment token
   api_endpoint = "{api_base_url}"
   
   # Secret reference  
   api_key = "${secret:api_keys/service_key}"
   db_password = "${secret:database/password}"

**Processing Behavior:**

- **Tokens and secrets** are processed before the file content is used
- **Python files** have substitutions applied before import management
- **SQL files** have substitutions applied before query execution
- **Backward compatible** - files without substitution variables work unchanged
- **Same syntax** as YAML substitutions for consistency

**Example pipeline with secrets:**

.. code-block:: yaml
   :caption: pipelines/customer_ingestion/external_load.yaml
   :linenos:
   :emphasize-lines: 9-12

   pipeline: customer_ingestion
   flowgroup: external_load

   actions:
     - name: load_from_postgres
       type: load
       source:
         type: jdbc
         url: "jdbc:postgresql://${secret:database_secrets/host}:5432/customers"
         user: "${secret:database_secrets/username}"
         password: "${secret:database_secrets/password}"
         driver: "org.postgresql.Driver"
         table: "customers"
       target: v_customers_raw

**Generated Python code:**

.. code-block:: python
   :caption: Generated DLT code with secure secret handling
   :linenos:
   :emphasize-lines: 6-8

   @dlt.view()
   def v_customers_raw():
       """Load from external database"""
       df = spark.read \
           .format("jdbc") \
           .option("url", f"jdbc:postgresql://{dbutils.secrets.get(scope='dev_db_secrets', key='host')}:5432/customers") \
           .option("user", f"{dbutils.secrets.get(scope='dev_db_secrets', key='username')}") \
           .option("password", f"{dbutils.secrets.get(scope='dev_db_secrets', key='password')}") \
           .option("driver", "org.postgresql.Driver") \
           .option("dbtable", "customers") \
           .load()
       
       return df


Operational Metadata
---------------------

Column Definitions
~~~~~~~~~~~~~~~~~~

Operational metadata are automatically generated columns that provide lineage, data
provenance, and processing context. These columns are added to your tables without
requiring manual SQL modifications.

.. note::
   Operational metadata columns are defined in the project level configuration file. under the ``operational_metadata`` key.

**Project-level configuration:**

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

Version Requirements
~~~~~~~~~~~~~~~~~~~~

LakehousePlumber supports version enforcement to ensure consistent code generation across development and CI environments. This prevents "works on my machine" issues and ensures reproducible builds.

**Basic configuration:**

.. code-block:: yaml
   :caption: lhp.yaml - Version enforcement examples
   :linenos:

   # LakehousePlumber Project Configuration
   name: my_lakehouse_project
   version: "1.0"
   
   # Enforce version requirements (optional)
   required_lhp_version: ">=0.4.1,<0.5.0"  # Allow patch updates within 0.4.x

**Version specification formats:**

.. code-block:: yaml
   :caption: Version requirement examples

   # Exact version pin (strict)
   required_lhp_version: "==0.4.1"
   
   # Allow patch updates only
   required_lhp_version: "~=0.4.1"          # Equivalent to >=0.4.1,<0.5.0
   
   # Range with exclusions
   required_lhp_version: ">=0.4.1,<0.5.0,!=0.4.3"  # Exclude known bad version
   
   # Allow minor updates
   required_lhp_version: ">=0.4.0,<1.0.0"

**Behavior:**

- When ``required_lhp_version`` is set, ``lhp validate`` and ``lhp generate`` will fail if the installed version doesn't satisfy the requirement
- Informational commands like ``lhp show`` skip version checking to allow inspection even with mismatches
- Version checking uses `PEP 440 <https://peps.python.org/pep-0440/>`_ version specifiers

**Emergency bypass:**

.. code-block:: bash
   :caption: Bypass version checking in emergencies

   # Temporarily bypass version checking
   export LHP_IGNORE_VERSION=1
   lhp generate -e dev
   
   # Or inline
   LHP_IGNORE_VERSION=1 lhp validate -e prod

**CI/CD integration:**

.. code-block:: bash
   :caption: CI pipeline with version enforcement

   # Install exact version matching project requirements
   pip install "lakehouse-plumber$(yq -r .required_lhp_version lhp.yaml | sed 's/^//')"
   
   # Or use range-compatible version
   pip install "lakehouse-plumber>=0.4.1,<0.5.0"
   
   # Validate and generate (will fail if version mismatch)
   lhp validate -e prod
   lhp generate -e prod

.. note::
   Version enforcement is **optional**. Projects without ``required_lhp_version`` work normally with any installed LakehousePlumber version.

.. warning::
   Use the bypass environment variable (``LHP_IGNORE_VERSION=1``) only in emergencies. It's not recommended for production environments as it defeats the purpose of version consistency.

Target Type Compatibility
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

- **``view``** - Source views created by load actions (``@dlt.view()``)
- **``streaming_table``** - Live tables with streaming updates (``@dlt.table()``)  
- **``materialized_view``** - Batch-computed views for analytics (``@dlt.view()``)

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

Usage in YAML Files
~~~~~~~~~~~~~~~~~~~

Operational metadata can be configured at multiple levels with **additive behavior** - columns from all levels are combined together:

.. important::
   **Additive Behavior**: Operational metadata columns are **never overridden** between levels. 
   Instead, columns from preset + flowgroup + action levels are **combined together**. 
   The only exception is ``operational_metadata: false`` at action level, which disables **all** metadata.

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

Usage Patterns
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

   @dlt.view()
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

What's Next?
============

Now that you understand the core building blocks of Lakehouse Plumber, explore these advanced features:

* **Dependency Analysis** - Understand how your pipelines depend on each other and generate orchestration jobs automatically. See :doc:`dependency_analysis`.
* **Templates & Presets** - Reuse common patterns across your pipelines. See :doc:`templates_reference`.
* **Databricks Bundles** - Deploy and manage your pipelines as code. See :doc:`databricks_bundles`.

For hands-on examples and complete workflows, check out :doc:`getting_started`.
