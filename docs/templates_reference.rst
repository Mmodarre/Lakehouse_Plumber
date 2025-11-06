Templates Reference
===================

Templates are reusable action patterns that eliminate repetitive configuration and standardize common data pipeline workflows. They use parameter substitution to generate customized actions from a single template definition.

.. contents:: Table of Contents
   :depth: 2
   :local:

Templates Overview
------------------

Templates transform **parametrized action patterns** into **concrete pipeline actions** through variable substitution. Think of them as functions that accept parameters and return a list of actions configured for your specific use case.

**Key Benefits:**

+---------------------+----------------------------------------------------------+
| Benefit             | Description                                              |
+=====================+==========================================================+
|| **Code Reuse**     || Define once, use many times across different tables     |
||                    || and data sources                                        |
+---------------------+----------------------------------------------------------+
|| **Standardization**|| Enforce consistent patterns and configurations across   |
||                    || your data platform                                      |
+---------------------+----------------------------------------------------------+
|| **Maintainability**|| Update logic in one place, automatically propagate      |
||                    || changes to all users of the template                    |
+---------------------+----------------------------------------------------------+
|| **Simplification** || Reduce complex 100+ line configurations to simple       |
||                    || 5-line parameter definitions                            |
+---------------------+----------------------------------------------------------+

**Template vs FlowGroup:**

- **Templates** are reusable patterns stored in ``templates/`` directory
- **FlowGroups** are concrete pipeline definitions that may use templates
- **Template Parameters** customize the template for each specific use case

Template Structure
------------------

Every template file contains these core elements:

.. code-block:: yaml
   :caption: Basic template structure
   :linenos:

   name: my_template                    # Unique template identifier
   version: "1.0"                      # Template version for tracking
   description: "Template description" # Documentation
   
   presets: []                         # Optional presets to apply
   
   parameters:                         # Parameter definitions
     - name: param_name
       type: string
       required: true
       description: "Parameter description"
       default: "default_value"
   
   actions:                           # Action patterns with {{ }} expressions
     - name: "load_{{ table_name }}"
       type: load
       source:
         type: cloudfiles
         path: "{{ data_path }}/*.csv"
       target: "v_{{ table_name }}_raw"

**Required Fields:**

- **name**: Unique identifier for the template across your project
- **actions**: List of action patterns that will be generated

**Optional Fields:**

- **version**: Template version for change tracking and compatibility
- **description**: Human-readable explanation of template purpose
- **presets**: List of preset names to apply to generated actions
- **parameters**: Parameter definitions with types and validation

Parameter Types
---------------

Templates support multiple parameter types with automatic type conversion and validation:

string
~~~~~~

String parameters are the most common type for names, paths, and configuration values:

.. code-block:: yaml
   :caption: String parameter examples
   :linenos:

   parameters:
     - name: table_name
       type: string
       required: true
       description: "Name of the target table"
     
     - name: file_format
       type: string
       required: false
       default: "parquet"
       description: "Input file format (csv, json, parquet)"

**Usage in templates:**

.. code-block:: yaml
   
   actions:
     - name: "load_{{ table_name }}_data"
       source:
         type: cloudfiles
         format: "{{ file_format }}"
         path: "/data/{{ table_name }}/*.{{ file_format }}"

object
~~~~~~

Object parameters accept complex nested configurations as natural YAML objects:

.. code-block:: yaml
   :caption: Object parameter examples
   :linenos:

   parameters:
     - name: table_properties
       type: object
       required: false
       default: {}
       description: "Delta table properties for optimization"
     
     - name: spark_conf
       type: object
       required: false
       default: {}
       description: "Spark configuration for the streaming operation"

**Usage in FlowGroup (Natural YAML):**

.. code-block:: yaml
   :caption: FlowGroup using object parameters
   :linenos:

   use_template: advanced_streaming_template
   template_parameters:
     table_name: customer_data
     table_properties:
       delta.enableChangeDataFeed: true
       delta.autoOptimize.optimizeWrite: true
       delta.autoOptimize.autoCompact: true
       custom.business.owner: "data_team"
     spark_conf:
       spark.sql.streaming.stateStore.rebalancing.enabled: true
       spark.sql.adaptive.coalescePartitions.enabled: true

**Template usage:**

.. code-block:: yaml
   :caption: Template usage
   :linenos:

   actions:
     - name: "write_{{ table_name }}_table"
       type: write
       write_target:
         type: streaming_table
         table_properties: "{{ table_properties }}"
         spark_conf: "{{ spark_conf }}"

array
~~~~~

Array parameters accept lists of values using natural YAML array syntax:

.. code-block:: yaml
   :caption: Array parameter examples
   :linenos:

   parameters:
     - name: partition_columns
       type: array
       required: false
       default: []
       description: "Columns to partition the table by"
     
     - name: cluster_columns
       type: array
       required: false
       default: []
       description: "Columns for Liquid Clustering optimization"

**Usage in FlowGroup (Natural YAML):**

.. code-block:: yaml
   :caption: FlowGroup using array parameters
   :linenos:

   use_template: partitioned_table_template
   template_parameters:
     table_name: sales_transactions
     partition_columns:
       - "year"
       - "month"
       - "region"
     cluster_columns:
       - "customer_id"
       - "product_id"

**Template usage:**

.. code-block:: yaml
   :caption: Template usage
   :linenos:

   actions:
     - name: "write_{{ table_name }}_table"
       type: write
       write_target:
         type: streaming_table
         partition_columns: "{{ partition_columns }}"
         cluster_columns: "{{ cluster_columns }}"

boolean
~~~~~~~

Boolean parameters control conditional behavior with true/false values:

.. code-block:: yaml
   :caption: Boolean parameter examples
   :linenos:

   parameters:
     - name: enable_cdc
       type: boolean
       required: false
       default: true
       description: "Enable Change Data Feed on the target table"
     
     - name: create_table
       type: boolean
       required: false
       default: true
       description: "Whether to create the target table"

**Usage in FlowGroup:**

.. code-block:: yaml
   :caption: FlowGroup using boolean parameters
   :linenos:

   use_template: configurable_table_template
   template_parameters:
     table_name: customer_master
     enable_cdc: true
     create_table: false  # Append to existing table

**Template usage:**

.. code-block:: yaml
   :caption: Template usage
   :linenos:

   actions:
     - name: "write_{{ table_name }}_table"
       type: write
       write_target:
         type: streaming_table
         create_table: "{{ create_table }}"
         table_properties:
           delta.enableChangeDataFeed: "{{ enable_cdc }}"

number
~~~~~~

Number parameters accept integer and floating-point values:

.. code-block:: yaml
   :caption: Number parameter examples
   :linenos:

   parameters:
     - name: max_files_per_trigger
       type: number
       required: false
       default: 1000
       description: "Maximum files to process per streaming trigger"
     
     - name: batch_size
       type: number
       required: false
       default: 50000
       description: "Number of records to process in each batch"

**Usage in FlowGroup:**

.. code-block:: yaml
   :caption: FlowGroup using number parameters

   use_template: optimized_ingestion_template
   template_parameters:
     table_name: transaction_logs
     max_files_per_trigger: 500
     batch_size: 100000

**Template usage:**

.. code-block:: yaml
   :caption: Template usage
   :linenos:

   actions:
     - name: "load_{{ table_name }}_files"
       type: load
       source:
         type: cloudfiles
         options:
           cloudFiles.maxFilesPerTrigger: "{{ max_files_per_trigger }}"

Template Examples
-----------------

Simple Ingestion Template
~~~~~~~~~~~~~~~~~~~~~~~~~

A basic template for standardized CSV ingestion with schema hints:

.. code-block:: yaml
   :caption: templates/csv_ingestion_template.yaml
   :linenos:

   name: csv_ingestion_template
   version: "1.0"
   description: "Standard template for ingesting CSV files with schema enforcement"

   presets:
     - bronze_layer

   parameters:
     - name: table_name
       type: string
       required: true
       description: "Name of the table to ingest"
     - name: landing_folder
       type: string
       required: true
       description: "Name of the landing folder"
     - name: table_properties
       type: object
       required: false
       description: "Optional table properties as key-value pairs"
       default: {}
     - name: cluster_columns
       type: array
       required: false
       description: "Optional Liquid clustering columns"
       default: []

   actions:
     - name: "load_{{ table_name }}_csv"
       type: load
       readMode: stream
       operational_metadata:
         - "_source_file_path"
         - "_processing_timestamp"
       source:
         type: cloudfiles
         path: "{landing_volume}/{{ landing_folder }}/*.csv"
         format: csv
         options:
           cloudFiles.format: csv
           header: true
           delimiter: ","
           cloudFiles.maxFilesPerTrigger: 50
           cloudFiles.inferColumnTypes: false
           cloudFiles.schemaEvolutionMode: addNewColumns
           cloudFiles.rescuedDataColumn: _rescued_data
           cloudFiles.schemaHints: "schemas/{{ table_name }}_schema.yaml"
       target: "v_{{ table_name }}_cloudfiles"
       description: "Load {{ table_name }} CSV files from landing volume"

     - name: "write_{{ table_name }}_bronze"
       type: write
       source: "v_{{ table_name }}_cloudfiles"
       write_target:
         type: streaming_table
         database: "{catalog}.{bronze_schema}"
         table: "{{ table_name }}"
         cluster_columns: "{{ cluster_columns }}"
         table_properties: "{{ table_properties }}"
       description: "Write {{ table_name }} to bronze layer"

**Using the CSV Ingestion Template**

.. code-block:: yaml
   :caption: pipelines/ingestion/customer_ingestion.yaml
   :linenos:

   pipeline: raw_ingestions
   flowgroup: customer_ingestion

   use_template: csv_ingestion_template
   template_parameters:
     table_name: customer
     landing_folder: customer_data
     cluster_columns:
       - "customer_id"
       - "region"
     table_properties:
       delta.autoOptimize.optimizeWrite: true
       custom.business.domain: "customer_data"

**The above template usage generates this Python code:**

.. code-block:: python
   :caption: Generated customer_ingestion.py
   :linenos:

   # Generated by LakehousePlumber
   # Pipeline: raw_ingestions
   # FlowGroup: customer_ingestion

   from pyspark.sql import functions as F
   from pyspark import pipelines as dp

   # Schema hints for customer_cloudfiles table
   customer_cloudfiles_schema_hints = """
       customer_id BIGINT,
       name STRING,
       email STRING,
       region STRING,
       registration_date DATE
   """.strip().replace("\n", " ")

   @dp.view()
   def v_customer_cloudfiles():
       """Load customer CSV files from landing volume"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "csv") \
           .option("header", True) \
           .option("delimiter", ",") \
           .option("cloudFiles.maxFilesPerTrigger", 50) \
           .option("cloudFiles.inferColumnTypes", False) \
           .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
           .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
           .option("cloudFiles.schemaHints", customer_cloudfiles_schema_hints) \
           .load("/Volumes/dev/raw/landing_volume/customer_data/*.csv")
       
       # Add operational metadata columns
       df = df.withColumn('_source_file_path', F.col('_metadata.file_path'))
       df = df.withColumn('_processing_timestamp', F.current_timestamp())
       
       return df

   # Create the streaming table
   dp.create_streaming_table(
       name="dev_catalog.bronze.customer",
       comment="Write customer to bronze layer",
       table_properties={
           "delta.autoOptimize.optimizeWrite": True,
           "custom.business.domain": "customer_data"
       },
       cluster_by=["customer_id", "region"]
   )

   @dp.append_flow(
       target="dev_catalog.bronze.customer",
       name="f_customer_bronze"
   )
   def f_customer_bronze():
       """Write customer to bronze layer"""
       df = spark.readStream.table("v_customer_cloudfiles")
       return df

Multi-Format Ingestion Template
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A more advanced template supporting multiple file formats with format-specific configurations:

.. code-block:: yaml
   :caption: templates/multi_format_ingestion_template.yaml
   :linenos:

   name: multi_format_ingestion_template
   version: "2.0"
   description: "Advanced template supporting multiple file formats with custom configurations"

   parameters:
     - name: table_name
       type: string
       required: true
       description: "Name of the target table"
     
     - name: file_format
       type: string
       required: true
       description: "File format: csv, json, parquet, avro"
     
     - name: source_path
       type: string
       required: true
       description: "Source data path pattern"
     
     - name: format_options
       type: object
       required: false
       default: {}
       description: "Format-specific reader options"
     
     - name: cloudfiles_options
       type: object
       required: false
       default: {}
       description: "CloudFiles-specific options"
     
     - name: enable_dqe
       type: boolean
       required: false
       default: false
       description: "Enable data quality expectations"
     
     - name: expectation_file
       type: string
       required: false
       description: "Path to data quality expectations file"
     
     - name: partition_columns
       type: array
       required: false
       default: []
       description: "Columns to partition the target table by"

   actions:
     - name: "load_{{ table_name }}_{{ file_format }}"
       type: load
       readMode: stream
       operational_metadata:
         - "_source_file_path"
         - "_source_file_modification_time"
         - "_processing_timestamp"
       source:
         type: cloudfiles
         path: "{{ source_path }}"
         format: "{{ file_format }}"
         format_options: "{{ format_options }}"
         options: "{{ cloudfiles_options }}"
       target: "v_{{ table_name }}_raw"
       description: "Load {{ table_name }} {{ file_format }} files from {{ source_path }}"

     - name: "validate_{{ table_name }}_quality"
       type: transform
       transform_type: data_quality
       source: "v_{{ table_name }}_raw"
       target: "v_{{ table_name }}_validated"
       readMode: stream
       expectations_file: "{{ expectation_file }}"
       description: "Apply data quality validations to {{ table_name }}"
       # This action only gets generated if enable_dqe is true

     - name: "write_{{ table_name }}_bronze"
       type: write
       source: "{% if enable_dqe %}v_{{ table_name }}_validated{% else %}v_{{ table_name }}_raw{% endif %}"
       write_target:
         type: streaming_table
         database: "{catalog}.{bronze_schema}"
         table: "{{ table_name }}"
         partition_columns: "{{ partition_columns }}"
         table_properties:
           delta.enableChangeDataFeed: true
           delta.autoOptimize.optimizeWrite: true
           source.format: "{{ file_format }}"
           source.path: "{{ source_path }}"
       description: "Write {{ table_name }} to bronze streaming table"

**Using the Multi-Format Template for JSON data:**

.. code-block:: yaml
   :caption: pipelines/ingestion/events_ingestion.yaml
   :linenos:

   pipeline: event_ingestion
   flowgroup: user_events

   use_template: multi_format_ingestion_template
   template_parameters:
     table_name: user_events
     file_format: json
     source_path: "/Volumes/prod/landing/events/user_events/*.json"
     format_options:
       multiline: true
       allowComments: false
       timestampFormat: "yyyy-MM-dd HH:mm:ss"
     cloudfiles_options:
       cloudFiles.maxFilesPerTrigger: 100
       cloudFiles.schemaEvolutionMode: addNewColumns
       cloudFiles.rescuedDataColumn: "_rescued_data"
     enable_dqe: true
     expectation_file: "expectations/user_events_quality.json"
     partition_columns:
       - "event_date"
       - "event_type"

**Using the Multi-Format Template for Parquet data:**

.. code-block:: yaml
   :caption: pipelines/ingestion/sales_ingestion.yaml
   :linenos:

   pipeline: sales_ingestion
   flowgroup: sales_transactions

   use_template: multi_format_ingestion_template
   template_parameters:
     table_name: sales_transactions
     file_format: parquet
     source_path: "/Volumes/prod/landing/sales/*.parquet"
     cloudfiles_options:
       cloudFiles.maxFilesPerTrigger: 200
       cloudFiles.schemaEvolutionMode: rescue
     enable_dqe: false
     partition_columns:
       - "transaction_date"
       - "store_region"

CDC Template with SCD Type 2
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

A template for implementing Change Data Capture with Slowly Changing Dimensions:

.. code-block:: yaml
   :caption: templates/scd_type2_template.yaml
   :linenos:

   name: scd_type2_template
   version: "1.0"
   description: "Template for SCD Type 2 implementation with CDC"

   parameters:
     - name: table_name
       type: string
       required: true
       description: "Name of the dimension table"
     
     - name: source_table
       type: string
       required: true
       description: "Source table for CDC changes"
     
     - name: primary_keys
       type: array
       required: true
       description: "Primary key columns for the dimension"
     
     - name: track_history_column_list
       type: array
       required: false
       default: []
       description: "Columns to track history for (empty = all columns)"
     
     - name: sequence_column
       type: string
       required: true
       description: "Column to determine order of changes"
     
     - name: ignore_null_updates
       type: boolean
       required: false
       default: true
       description: "Ignore updates where all tracked columns are null"

   actions:
     - name: "load_{{ table_name }}_changes"
       type: load
       readMode: stream
       source:
         type: delta
         database: "{catalog}.{bronze_schema}"
         table: "{{ source_table }}"
         read_change_feed: true
       target: "v_{{ table_name }}_changes"
       description: "Load change data from {{ source_table }}"

     - name: "write_{{ table_name }}_dimension"
       type: write
       source: "v_{{ table_name }}_changes"
       write_target:
         type: streaming_table
         database: "{catalog}.{silver_schema}"
         table: "dim_{{ table_name }}"
         mode: cdc
                 cdc_config:
          keys: "{{ primary_keys }}"
          sequence_by: "{{ sequence_column }}"
          scd_type: 2
          track_history_column_list: "{{ track_history_column_list }}"
          ignore_null_updates: "{{ ignore_null_updates }}"
         table_properties:
           delta.enableChangeDataFeed: true
           table.type: "dimension"
           scd.type: "2"
       description: "Create SCD Type 2 dimension for {{ table_name }}"

**Using the SCD Type 2 Template:**

.. code-block:: yaml
   :caption: pipelines/dimensions/customer_dimension.yaml
   :linenos:

   pipeline: silver_dimensions
   flowgroup: customer_dimension

   use_template: scd_type2_template
   template_parameters:
     table_name: customer
     source_table: customer_bronze
         primary_keys:
      - "customer_id"
    track_history_column_list:
      - "name"
      - "address"
      - "phone"
      - "email"
      - "market_segment"
     sequence_column: "_commit_timestamp"
     ignore_null_updates: true

Environment and Secret Substitutions
------------------------------------

In addition to template parameters, both template definitions and flowgroup YAML files support environment-specific substitutions and secret references. These use different syntax than template parameters and are resolved at generation time.

Substitution Types
~~~~~~~~~~~~~~~~~~

**Environment Substitutions**: ``{token}`` or ``${token}``
   Replaced with values from ``substitutions/{env}.yaml`` files

**Secret References**: ``${secret:scope/key}`` or ``${secret:key}``
   Converted to secure ``dbutils.secrets.get()`` calls in generated Python

**Template Parameters**: ``{{ parameter }}``
   Replaced with values from ``template_parameters`` in flowgroups

.. important::
   **Syntax Distinction:**
   
   - ``{catalog}`` or ``${catalog}`` = Environment substitution (from substitutions/env.yaml)
   - ``${secret:scope/key}`` = Secret reference (Databricks secrets)
   - ``{{ table_name }}`` = Template parameter (from template_parameters)

.. note::
   **Processing Order**: LHP processes substitutions in this order:
   
   1. **Template parameters** (``{{ }}``) are resolved first when templates are applied
   2. **Environment substitutions** (``{ }``) are resolved at generation time  
   3. **Secret references** (``${secret:}``) are converted to ``dbutils.secrets.get()`` calls
   
   This allows templates to dynamically reference environment-specific values and secrets.

Using Substitutions in Templates
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Templates can include environment and secret substitutions alongside template parameters:

.. code-block:: yaml
   :caption: templates/secure_jdbc_template.yaml
   :linenos:

   name: secure_jdbc_template
   version: "1.0"
   description: "Template for secure JDBC ingestion with environment and secret support"

   parameters:
     - name: table_name
       type: string
       required: true
       description: "Name of the source table"
     
     - name: query_filter
       type: string
       required: false
       description: "Optional WHERE clause filter"

   actions:
     - name: "load_{{ table_name }}_from_database"
       type: load
       readMode: batch
       source:
         type: jdbc
         # Environment substitution - resolved from substitutions/{env}.yaml
         url: "{jdbc_url}"
         driver: "{jdbc_driver}"
         # Secret substitutions - resolved to dbutils.secrets.get() calls
         user: "${secret:database_secrets/username}"
         password: "${secret:database_secrets/password}"
         # Template parameter - resolved from template_parameters
         query: |
           SELECT * FROM {{ table_name }}
           {% if query_filter %}WHERE {{ query_filter }}{% endif %}
       target: "v_{{ table_name }}_raw"
       description: "Load {{ table_name }} from external database"

     - name: "write_{{ table_name }}_bronze"
       type: write
       source: "v_{{ table_name }}_raw"
       write_target:
         type: streaming_table
         # Environment substitutions for database targeting
         database: "{catalog}.{bronze_schema}"
         table: "{{ table_name }}"
         table_properties:
           # Mixed substitutions and template parameters
           source.database: "{source_database}"
           source.table: "{{ table_name }}"
           ingestion.environment: "{environment}"
       description: "Write {{ table_name }} to bronze layer"

**Example substitutions/dev.yaml:**

.. code-block:: yaml
   :caption: substitutions/dev.yaml
   :linenos:

   dev:
     catalog: "dev_catalog"
     bronze_schema: "bronze"
     environment: "development"
     source_database: "external_prod_db"
     jdbc_url: "jdbc:postgresql://dev-db.company.com:5432/analytics"
     jdbc_driver: "org.postgresql.Driver"

   secrets:
     default_scope: "dev_secrets"
     scopes:
       database_secrets: "dev_database_secrets"

**Using the template in a flowgroup:**

.. code-block:: yaml
   :caption: pipelines/external_ingestion/customers_from_postgres.yaml
   :linenos:

   pipeline: external_ingestion
   flowgroup: customer_data_load

   use_template: secure_jdbc_template
   template_parameters:
     table_name: customers
     query_filter: "status = 'active' AND created_date >= CURRENT_DATE - INTERVAL '30 days'"

**Generated Python code shows all three substitution types resolved:**

.. code-block:: python
   :caption: Generated customer_data_load.py
   :linenos:

   @dp.view()
   def v_customers_raw():
       """Load customers from external database"""
       df = spark.read \
           .format("jdbc") \
           .option("url", "jdbc:postgresql://dev-db.company.com:5432/analytics") \
           .option("driver", "org.postgresql.Driver") \
           .option("user", dbutils.secrets.get(scope="dev_database_secrets", key="username")) \
           .option("password", dbutils.secrets.get(scope="dev_database_secrets", key="password")) \
           .option("query", """
               SELECT * FROM customers
               WHERE status = 'active' AND created_date >= CURRENT_DATE - INTERVAL '30 days'
           """) \
           .load()
       return df

   # Create the streaming table
   dp.create_streaming_table(
       name="dev_catalog.bronze.customers",
       comment="Write customers to bronze layer",
       table_properties={
           "source.database": "external_prod_db",
           "source.table": "customers",
           "ingestion.environment": "development"
       }
   )

   @dp.append_flow(target="dev_catalog.bronze.customers", name="f_customers_bronze")
   def f_customers_bronze():
       """Write customers to bronze layer"""
       return spark.readStream.table("v_customers_raw")

Using Substitutions in FlowGroups
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

FlowGroups can also use environment and secret substitutions directly without templates:

.. code-block:: yaml
   :caption: pipelines/direct_ingestion/events_load.yaml
   :linenos:

   pipeline: event_ingestion
   flowgroup: user_events_direct

   actions:
     - name: load_events_from_api
       type: load
       readMode: batch
       source:
         type: python
         module_path: "extractors/events_api.py"
         function_name: "fetch_events"
         parameters:
           # Environment substitution
           api_endpoint: "{events_api_endpoint}"
           # Secret substitution
           api_key: "${secret:api_secrets/events_api_key}"
           # Direct value
           batch_size: 1000
       target: v_events_raw
       description: "Load events from external API"

     - name: write_events_bronze
       type: write
       source: v_events_raw
       write_target:
         type: streaming_table
         # Environment substitutions
         database: "{catalog}.{bronze_schema}"
         table: user_events
         table_properties:
           # Mix of environment substitutions and direct values
           source.api: "{events_api_endpoint}"
           ingestion.frequency: "hourly"
           environment: "{environment}"
       description: "Write events to bronze layer"

Multi-Environment Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~

The same template or flowgroup works across environments by changing substitution files:

**Development Environment:**

.. code-block:: yaml
   :caption: substitutions/dev.yaml
   :linenos:

   dev:
     catalog: "dev_catalog"
     bronze_schema: "bronze_dev"
     events_api_endpoint: "https://dev-api.company.com/events"
     environment: "development"

   secrets:
     default_scope: "dev_secrets"
     scopes:
       api_secrets: "dev_api_secrets"
       database_secrets: "dev_db_secrets"

**Production Environment:**

.. code-block:: yaml
   :caption: substitutions/prod.yaml
   :linenos:

   prod:
     catalog: "prod_catalog"
     bronze_schema: "bronze"
     events_api_endpoint: "https://api.company.com/events"
     environment: "production"

   secrets:
     default_scope: "prod_secrets"
     scopes:
       api_secrets: "prod_api_secrets"
       database_secrets: "prod_db_secrets"

**Same template generates different configurations:**

.. code-block:: bash

   # Development deployment
   lhp generate --env dev
   # Uses dev_catalog.bronze_dev, dev API endpoint, dev secrets

   # Production deployment  
   lhp generate --env prod
   # Uses prod_catalog.bronze, prod API endpoint, prod secrets

Advanced Substitution Patterns
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Conditional Secret Usage**

Templates can conditionally use secrets based on environment:

.. code-block:: text
   :caption: Template with conditional secrets
   :linenos:

   actions:
     - name: "load_{{ table_name }}_data"
       type: load
       source:
         type: cloudfiles
         path: "{data_path}/{{ table_name }}/*.parquet"
         {% if environment == "prod" %}
         # Only use encryption in production
         reader_options:
           spark.sql.parquet.encryption.kms.client.class: "org.apache.parquet.crypto.keytools.KmsClient"
           spark.sql.parquet.encryption.key.retrieval.kms.instance.id: "${secret:encryption_secrets/kms_instance}"
         {% endif %}

**Dynamic Database Targeting**

Use substitutions for flexible database targeting:

.. code-block:: yaml
   :caption: Environment-aware database targeting
   :linenos:

   write_target:
     type: streaming_table
     # Dynamic catalog and schema based on environment and data classification
     database: "{catalog}.{bronze_schema}_{data_classification}"
     table: "{{ table_name }}"
     table_properties:
       data.classification: "{data_classification}"
       governance.retention: "{retention_policy}"

**Secret Scope Aliases**

Use scope aliases for flexible secret management:

.. code-block:: yaml
   :caption: substitutions/staging.yaml
   :linenos:

   staging:
     catalog: "staging_catalog"
     bronze_schema: "bronze_staging"

   secrets:
     default_scope: "staging_secrets"
     scopes:
       # Alias mapping for different secret scope organization
       external_apis: "staging_external_secrets"
       databases: "staging_rds_secrets"  
       storage: "staging_azure_secrets"

.. code-block:: yaml
   :caption: Template using scope aliases

   source:
     type: jdbc
     url: "{jdbc_url}"
     # Uses mapped scope from substitutions
     user: "${secret:databases/readonly_user}"
     password: "${secret:databases/readonly_password}"

Best Practices for Substitutions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**When to Use Each Type:**

.. list-table::
   :header-rows: 1
   :widths: 25 50 25

   * - Substitution Type
     - Use Case
     - Example
   * - **Template Parameters** ``{{ }}``
     - Values that change per template usage within the same environment
     - ``{{ table_name }}``, ``{{ file_format }}``
   * - **Environment** ``{token}``
     - Values that change between dev/staging/prod but stay consistent within an environment
     - ``{catalog}``, ``{bronze_schema}``
   * - **Secret References** ``${secret:}``
     - Sensitive data like passwords, API keys, connection strings
     - ``${secret:db/password}``, ``${secret:apis/key}``

**Security Guidelines:**

.. warning::
   **Never put secrets in template parameters or direct values:**
   
   .. code-block:: yaml
      :caption: ❌ NEVER do this
      
      template_parameters:
        api_key: "sk-1234567890abcdef"  # ❌ Exposed in YAML
        password: "mypassword"          # ❌ Stored in plain text
   
   .. code-block:: yaml
      :caption: ✅ Always use secret substitutions
      
      source:
        user: "${secret:database_secrets/username}"     # ✅ Secure
        password: "${secret:database_secrets/password}" # ✅ Secure

**Organization Tips:**

1. **Group related substitutions** in your environment files
2. **Use consistent naming** across environments (dev/staging/prod)
3. **Document secret scope mappings** in your substitution files
4. **Validate secret references** using ``lhp validate --env {env}``

.. seealso::
   - For complete secret management documentation: :doc:`concepts`
   - For substitution file format: :doc:`concepts`
   - For environment-specific deployment: :doc:`databricks_bundles`

Template Expressions
--------------------

Template expressions use Jinja2-style ``{{ }}`` syntax for parameter substitution and support advanced templating features:

Basic Substitution
~~~~~~~~~~~~~~~~~~

Simple parameter replacement:

.. code-block:: yaml

   # Template parameter
   parameters:
     - name: table_name
       type: string
       required: true

   # Template usage
   actions:
     - name: "process_{{ table_name }}_data"
       target: "v_{{ table_name }}_processed"
       source:
         path: "/data/{{ table_name }}/*.parquet"

Conditional Logic
~~~~~~~~~~~~~~~~~

Use conditional expressions for dynamic action generation:

.. code-block:: text

   # Template with conditional logic
   actions:
     - name: "load_{{ table_name }}_data"
       type: load
       source:
         type: cloudfiles
         path: "{{ data_path }}"
         {% if file_format == "csv" %}
         options:
           header: true
           delimiter: ","
         {% elif file_format == "json" %}
         options:
           multiline: true
         {% endif %}
       target: "v_{{ table_name }}_raw"

**Note**: Complex conditional logic should be used sparingly. Consider creating separate templates for significantly different patterns.

String Operations
~~~~~~~~~~~~~~~~~

Jinja2 filters for string manipulation:

.. code-block:: yaml

   # Template with string operations
   actions:
     - name: "{{ table_name | lower }}_processing"
       target: "v_{{ table_name | upper }}_CLEANED"
       description: "Process {{ table_name | title }} data from {{ source_path | basename }}"

Natural YAML Syntax
-------------------

Templates support natural YAML syntax for complex parameters, eliminating the need for JSON strings:

Object Parameters
~~~~~~~~~~~~~~~~~

**Traditional approach (JSON strings):**

.. code-block:: yaml
   :caption: ❌ Old way - JSON strings (avoid this)

   template_parameters:
     table_properties: '{"delta.enableChangeDataFeed": "true", "delta.autoOptimize.optimizeWrite": "true"}'

**Natural YAML approach:**

.. code-block:: yaml
   :caption: ✅ New way - Natural YAML objects

   template_parameters:
     table_properties:
       delta.enableChangeDataFeed: true
       delta.autoOptimize.optimizeWrite: true
       delta.autoOptimize.autoCompact: true
       custom.business.domain: "customer_data"

Array Parameters
~~~~~~~~~~~~~~~~

**Traditional approach (JSON strings):**

.. code-block:: yaml
   :caption: ❌ Old way - JSON strings (avoid this)

   template_parameters:
     partition_columns: '["year", "month", "region"]'

**Natural YAML approach:**

.. code-block:: yaml
   :caption: ✅ New way - Natural YAML arrays

   template_parameters:
     partition_columns:
       - "year"
       - "month"
       - "region"

Mixed Complex Parameters
~~~~~~~~~~~~~~~~~~~~~~~~

Natural YAML syntax enables readable complex configurations:

.. code-block:: yaml
   :caption: Complex template parameters with natural YAML
   :linenos:

   use_template: advanced_data_platform_template
   template_parameters:
     table_name: customer_360
     
     # Natural YAML array
     partition_columns:
       - "year"
       - "month"
       - "region"
     
     # Natural YAML object
     table_properties:
       delta.enableChangeDataFeed: true
       delta.autoOptimize.optimizeWrite: true
       delta.autoOptimize.autoCompact: true
       delta.deletedFileRetentionDuration: "interval 30 days"
       custom.business.owner: "customer_analytics_team"
       custom.data.classification: "sensitive"
       custom.refresh.frequency: "daily"
     
     # Natural YAML object with nested structure
     cloudfiles_options:
       cloudFiles.maxFilesPerTrigger: 100
       cloudFiles.schemaEvolutionMode: addNewColumns
       cloudFiles.rescuedDataColumn: "_rescued_data"
       cloudFiles.inferColumnTypes: false
     
     # Natural YAML array of objects
     operational_metadata:
       - "_source_file_path"
       - "_processing_timestamp"
       - "_record_hash"
     
     # Simple boolean
     enable_data_quality: true
     
     # Simple number
     max_files_per_trigger: 250

Best Practices
--------------

Template Design Principles
~~~~~~~~~~~~~~~~~~~~~~~~~~

**Single Responsibility**
   Each template should solve one specific pattern or use case. Avoid overly generic templates that try to handle every scenario.

**Clear Parameter Naming**
   Use descriptive parameter names that clearly indicate their purpose and expected values.

**Sensible Defaults**
   Provide reasonable default values for optional parameters to minimize required configuration.

**Documentation**
   Include comprehensive descriptions for the template and all parameters.

Parameter Validation
~~~~~~~~~~~~~~~~~~~~

**Use Strong Typing**

.. code-block:: yaml
   :caption: ✅ Good parameter definitions

   parameters:
     - name: file_format
       type: string
       required: true
       description: "File format: csv, json, parquet, avro, orc"
     
     - name: max_files_per_trigger
       type: number
       required: false
       default: 1000
       description: "Maximum files to process per trigger (1-10000)"
     
     - name: partition_columns
       type: array
       required: false
       default: []
       description: "Table partitioning columns (recommended: 2-4 columns max)"

**Provide Examples**

.. code-block:: yaml
   :caption: Parameter documentation with examples

   parameters:
     - name: cdc_config
       type: object
       required: false
       default: {}
       description: |
         CDC configuration for change data capture.
         Example:
           keys: ["customer_id"]
           sequence_by: "_commit_timestamp"
           scd_type: 2

Template Organization
~~~~~~~~~~~~~~~~~~~~~

**File Structure**

.. code-block:: text

   templates/
   ├── ingestion/
   │   ├── csv_ingestion_template.yaml
   │   ├── json_ingestion_template.yaml
   │   └── multi_format_template.yaml
   ├── transformation/
   │   ├── bronze_to_silver_template.yaml
   │   └── data_quality_template.yaml
   ├── dimension/
   │   ├── scd_type1_template.yaml
   │   └── scd_type2_template.yaml
   └── analytics/
       ├── materialized_view_template.yaml
       └── aggregation_template.yaml

**Naming Conventions**
   - Use descriptive names that indicate the template's purpose
   - Include the layer or function in the name (e.g., ``bronze_ingestion_template``)
   - Add version numbers for breaking changes (e.g., ``csv_ingestion_template_v2.yaml``)

Error Handling
~~~~~~~~~~~~~~

**Parameter Validation**

Templates should validate critical parameters and provide clear error messages:

.. code-block:: yaml

   parameters:
     - name: primary_keys
       type: array
       required: true
       description: "Primary key columns (at least one column required)"

**Defensive Defaults**

Use safe defaults that won't cause runtime errors:

.. code-block:: yaml

   parameters:
     - name: cloudfiles_options
       type: object
       required: false
       default:
         cloudFiles.maxFilesPerTrigger: 1000
         cloudFiles.schemaEvolutionMode: addNewColumns
       description: "CloudFiles options with safe defaults"

Integration with Presets
------------------------

Templates and presets work together to provide maximum reusability:

**Template with Preset**

.. code-block:: yaml
   :caption: templates/bronze_ingestion_template.yaml
   :linenos:

   name: bronze_ingestion_template
   version: "1.0"
   description: "Bronze layer ingestion with standard configurations"

   presets:
     - bronze_layer_defaults  # Applies to all generated actions

   parameters:
     - name: table_name
       type: string
       required: true

   actions:
     # Preset values are automatically applied to these actions
     - name: "load_{{ table_name }}"
       type: load
       # ... action configuration

**Preset Definition**

.. code-block:: yaml
   :caption: presets/bronze_layer_defaults.yaml
   :linenos:

   name: bronze_layer_defaults
   version: "1.0"
   description: "Standard defaults for bronze layer operations"

   defaults:
     operational_metadata:
       - "_processing_timestamp"
       - "_source_file_path"
     
     write_target:
       table_properties:
         delta.enableChangeDataFeed: true
         delta.autoOptimize.optimizeWrite: true
         quality: bronze

**Combination Result**

When the template is used, actions automatically inherit both template parameters and preset defaults, providing consistent configuration across your platform.

Troubleshooting Templates
-------------------------

Common Issues
~~~~~~~~~~~~~

**Parameter Type Mismatches**

.. code-block:: text

   Error: Expected array for parameter 'partition_columns', got string

**Solution:** Ensure parameter types match template expectations:

.. code-block:: yaml
   :caption: ✅ Correct usage

   template_parameters:
     partition_columns:  # Array type
       - "year" 
       - "month"

.. code-block:: yaml
   :caption: ❌ Incorrect usage

   template_parameters:
     partition_columns: "year,month"  # String type

**Missing Required Parameters**

.. code-block:: text

   Error: Required parameter 'table_name' not provided

**Solution:** Check template parameter definitions and provide all required parameters.

**Template Not Found**

.. code-block:: text

   Error: Template 'my_template' not found

**Solution:** Verify template file exists in ``templates/`` directory and has correct name.

Debugging Template Rendering
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

**Use Dry Run Mode**

.. code-block:: bash

   # Preview generated actions without creating files
   lhp generate --env dev --dry-run --verbose

**Check Template Syntax**

.. code-block:: bash

   # Validate template files
   lhp validate --env dev --templates-only

**Inspect Generated Actions**

Enable verbose logging to see parameter substitution details:

.. code-block:: bash

   lhp generate --env dev --verbose


.. seealso::
   - For complete template examples see the `Example Projects <https://github.com/Mmodarre/Lakehouse_Plumber/tree/main/Example_Projects>`_
   - Template syntax: :doc:`concepts`  
   - Action reference: :doc:`actions_reference`
   - Using presets: :doc:`concepts` 