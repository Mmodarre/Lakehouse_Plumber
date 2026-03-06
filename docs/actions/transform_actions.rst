Transform Actions
=================

.. meta::
   :description: Complete reference for LHP Transform action types: SQL, Python, data quality, schema, and temp table transforms.


+--------------+---------------------------------------------------------------+
| Sub-type     | Purpose                                                       |
+==============+===============================================================+
|| sql         || Run an inline SQL statement or external ``.sql`` file.       |
+--------------+---------------------------------------------------------------+
|| python      || Apply arbitrary PySpark code; useful for complex logic.      |
+--------------+---------------------------------------------------------------+
|| schema      || Add, drop, or rename columns, or change data types.          |
+--------------+---------------------------------------------------------------+
|| data_quality|| Attach *expectations* (fail, warn, drop) to validate data.   |
+--------------+---------------------------------------------------------------+
|| temp_table  || Create an intermediate temp table or view for re-use.        |
+--------------+---------------------------------------------------------------+

sql
-------------------------------------------
SQL transform actions execute SQL queries to transform data between views. They support both **inline SQL** and **external SQL files**.

**Option 1: Inline SQL**

.. code-block:: yaml

  actions:
    - name: customer_bronze_cleanse
      type: transform
      transform_type: sql
      source: v_customer_raw
      target: v_customer_bronze_cleaned
      sql: |
        SELECT 
          c_custkey as customer_id,
          c_name as name,
          c_address as address,
          c_nationkey as nation_id,
          c_phone as phone,
          c_acctbal as account_balance,
          c_mktsegment as market_segment,
          c_comment as comment,
          _source_file_path,
          _source_file_size,
          _source_file_modification_time,
          _record_hash,
          _processing_timestamp
        FROM stream(v_customer_raw)
      description: "Cleanse and standardize customer data for bronze layer"

**Option 2: External SQL File**

.. code-block:: yaml

  actions:
    - name: customer_enrichment
      type: transform
      transform_type: sql
      source: v_customer_bronze
      target: v_customer_enriched
      sql_path: "sql/customer_enrichment.sql"
      description: "Enrich customer data with additional attributes"

**Anatomy of an SQL transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - transforms data from one view to another
- **transform_type**: Specifies this is an SQL-based transformation
- **source**: Name of the input view to transform
- **target**: Name of the output view to create
- **sql**: SQL statement that defines the transformation logic (inline option)
- **sql_path**: Path to external .sql file (external file option)
- **description**: Optional documentation for the action

.. seealso::
  - For SQL syntax see the `Databricks SQL documentation <https://docs.databricks.com/en/sql/index.html>`_.
  - Stream syntax: Use ``stream(view_name)`` for streaming transformations

.. Important::
  SQL transforms can use ``stream()`` function for streaming data or direct view references for batch processing.
  Column aliasing and data type transformations are common patterns in bronze layer cleansing.

.. note:: **File Substitution Support**
   
   Substitution variables work in both inline SQL and external SQL files (``sql_path``). 
   The same ``{token}`` and ``${secret:scope/key}`` syntax from YAML works in ``.sql`` files.
   Files are processed for substitutions before query execution.

.. Warning::
  When writing SQL statements, if your source or target is a streaming table you must use the ``stream()`` function.
  For example: `` FROM stream(v_customer_raw) ``

.. note::
  **File Organization**: When using ``sql_path``, the path is relative to your YAML file location.
  Common practice is to create a ``sql/`` folder alongside your pipeline YAML files.

**The above YAML examples translate to the following PySpark code**

**For inline SQL:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view(comment="Cleanse and standardize customer data for bronze layer")
  def v_customer_bronze_cleaned():
      """Cleanse and standardize customer data for bronze layer"""
      return spark.sql("""
          SELECT 
            c_custkey as customer_id,
            c_name as name,
            c_address as address,
            c_nationkey as nation_id,
            c_phone as phone,
            c_acctbal as account_balance,
            c_mktsegment as market_segment,
            c_comment as comment,
            _source_file_path,
            _source_file_size,
            _source_file_modification_time,
            _record_hash,
            _processing_timestamp
          FROM stream(v_customer_raw)
      """)

**For external SQL file:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view(comment="Enrich customer data with additional attributes")
  def v_customer_enriched():
      """Enrich customer data with additional attributes"""
      return spark.sql("""
          -- Content from sql/customer_enrichment.sql file
          SELECT 
            c.*,
            n.name as nation_name,
            r.name as region_name,
            CASE 
              WHEN account_balance > 5000 THEN 'High Value'
              WHEN account_balance > 1000 THEN 'Medium Value'
              ELSE 'Standard'
            END as customer_tier
          FROM {catalog}.{bronze_schema}.customer c
          LEFT JOIN {catalog}.{bronze_schema}.nation n ON c.nation_id = n.nation_id
          LEFT JOIN {catalog}.{bronze_schema}.region r ON n.region_id = r.region_id
      """)

python
-------------------------------------------
Python transform actions call custom Python functions to apply complex transformation logic that goes beyond SQL capabilities. 

.. tip::
  The framework automatically copies your Python functions into the generated pipeline and handles import management.

.. code-block:: yaml

  actions:
    - name: customer_advanced_enrichment
      type: transform
      transform_type: python
      source: v_customer_bronze 
      module_path: "transformations/customer_transforms.py"
      function_name: "enrich_customer_data"
      parameters:
        api_endpoint: "https://api.example.com/geocoding"
        api_key: "${secret:apis/geocoding_key}"
        batch_size: 1000
      target: v_customer_enriched
      readMode: batch
      operational_metadata: ["_processing_timestamp"]
      description: "Apply advanced customer enrichment using external APIs"

**Multiple Source Views Example:**

.. code-block:: yaml

  actions:
    - name: customer_order_analysis
      type: transform
      transform_type: python
      source: ["v_customer_bronze", "v_orders_bronze"]
      module_path: "analytics/customer_analysis.py"
      function_name: "analyze_customer_orders"
      parameters:
        analysis_window_days: 90
        min_order_count: 5
      target: v_customer_order_insights
      readMode: batch
      description: "Analyze customer order patterns from multiple sources"

**Python Function (transformations/customer_transforms.py):**

.. code-block:: python
  :linenos:

  import requests
  from pyspark.sql import DataFrame
  from pyspark.sql.functions import col, when, lit, udf
  from pyspark.sql.types import StringType

  def enrich_customer_data(df: DataFrame, spark, parameters: dict) -> DataFrame:
      """Apply advanced customer enrichment using external APIs.
      
      Args:
          df: Input DataFrame from source view
          spark: SparkSession instance
          parameters: Configuration parameters from YAML
          
      Returns:
          DataFrame: Enriched customer data
      """
      # Extract parameters from YAML configuration
      api_endpoint = parameters.get("api_endpoint")
      api_key = parameters.get("api_key")
      batch_size = parameters.get("batch_size", 1000)
      
      # Define UDF for geocoding
      def geocode_address(address):
          if not address:
              return None
          try:
              response = requests.get(
                  f"{api_endpoint}?address={address}&key={api_key}",
                  timeout=5
              )
              if response.status_code == 200:
                  data = response.json()
                  return data.get("coordinates", {}).get("lat")
              return None
          except:
              return None
      
      geocode_udf = udf(geocode_address, StringType())
      
      # Apply transformations
      enriched_df = df.withColumn(
          "latitude", geocode_udf(col("address"))
      ).withColumn(
          "customer_risk_score",
          when(col("account_balance") < 0, lit("High"))
          .when(col("account_balance") < 1000, lit("Medium"))
          .otherwise(lit("Low"))
      ).withColumn(
          "address_normalized",
          col("address").cast("string").alias("address")
      )
      
      return enriched_df

**Multiple Sources Function Example (analytics/customer_analysis.py):**

.. code-block:: python
  :linenos:

  from pyspark.sql import DataFrame
  from pyspark.sql.functions import col, count, sum, avg, datediff, current_date
  from typing import List

  def analyze_customer_orders(dataframes: List[DataFrame], spark, parameters: dict) -> DataFrame:
      """Analyze customer order patterns from multiple source views.
      
      Args:
          dataframes: List of DataFrames [customers_df, orders_df]
          spark: SparkSession instance
          parameters: Configuration parameters from YAML
          
      Returns:
          DataFrame: Customer order insights
      """
      customers_df, orders_df = dataframes
      analysis_window_days = parameters.get("analysis_window_days", 90)
      min_order_count = parameters.get("min_order_count", 5)
      
      # Join customers with their orders
      customer_orders = customers_df.alias("c").join(
          orders_df.alias("o"),
          col("c.customer_id") == col("o.customer_id"),
          "left"
      )
      
      # Filter orders within analysis window
      recent_orders = customer_orders.filter(
          datediff(current_date(), col("o.order_date")) <= analysis_window_days
      )
      
      # Calculate customer insights
      insights = recent_orders.groupBy(
          col("c.customer_id"),
          col("c.customer_name"),
          col("c.market_segment")
      ).agg(
          count("o.order_id").alias("order_count"),
          sum("o.total_price").alias("total_spent"),
          avg("o.total_price").alias("avg_order_value")
      ).filter(
          col("order_count") >= min_order_count
      )
      
      return insights

**Anatomy of a Python transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - transforms data from one view to another
- **transform_type**: Specifies this is a Python-based transformation
- **source**: Source view name(s) to transform (string for single view, list for multiple views)
- **module_path**: Path to Python file containing the transformation function (relative to project root)
- **function_name**: Name of function to call (required)
- **parameters**: Dictionary of parameters to pass to the function (optional)
- **target**: Name of the output view to create
- **readMode**: Either *batch* or *stream* - determines execution mode
- **operational_metadata**: Add custom metadata columns (optional)
- **description**: Optional documentation for the action

**File Management & Copying Process**

Lakehouse Plumber automatically handles Python function deployment:

1. **Automatic File Copying**: Your Python functions are copied to ``generated/pipeline_name/custom_python_functions/`` during generation
2. **Substitution Processing**: Files are processed for ``{token}`` and ``${secret:scope/key}`` substitutions before copying
3. **Import Management**: Imports are automatically generated as ``from custom_python_functions.module_name import function_name``
4. **Warning Headers**: Copied files include prominent warnings not to edit them directly
5. **State Tracking**: All copied files are tracked and cleaned up when source YAML is removed
6. **Package Structure**: A ``__init__.py`` file is automatically created to make the directory a Python package

.. note:: **File Substitution Support**
   
   Python transform files support the same substitution syntax as YAML:
   
   - **Environment tokens**: ``{catalog}``, ``{schema}``, ``{environment}``
   - **Secret references**: ``${secret:scope/key}`` or ``${secret:key}``
   
   Substitutions are applied before the file is copied and imported.

.. seealso::
  - For PySpark DataFrame operations see the `Databricks PySpark documentation <https://docs.databricks.com/en/spark/latest/spark-sql/index.html>`_.
  - Custom functions: :doc:`../concepts`

.. Important::
  **Function Requirements**: Python functions must accept the appropriate parameters based on source configuration:
  
  - **Single source**: ``function_name(df: DataFrame, spark: SparkSession, parameters: dict)``
  - **Multiple sources**: ``function_name(dataframes: List[DataFrame], spark: SparkSession, parameters: dict)``  
  - **No sources**: ``function_name(spark: SparkSession, parameters: dict)`` (for data generators)

.. note::
  **File Organization Tips**:
  
  - Keep your Python functions in a dedicated folder (e.g., ``transformations/``, ``functions/``)
  - Use descriptive function names that clearly indicate their purpose
  - Always edit the original files in your project, never the copied files in ``generated/``
  - The ``module_path`` is relative to your project root directory
  - Multiple transforms can reference the same Python file with different functions

.. Warning::
  **DO NOT Edit Generated Files**: The copied Python files in ``custom_python_functions/`` are automatically regenerated and include warning headers. Always edit your original source files.

**Generated File Structure**

After generation, your Python functions appear in the pipeline output with warning headers:

.. code-block:: text

  generated/
  └── pipeline_name/
      ├── flowgroup_name.py
      └── custom_python_functions/
          ├── __init__.py
          └── customer_transforms.py

**Example of Generated File with Warning Header:**

.. code-block:: python

  # ╔══════════════════════════════════════════════════════════════════════════════╗
  # ║                                    WARNING                                   ║
  # ║                          DO NOT EDIT THIS FILE DIRECTLY                      ║
  # ╠══════════════════════════════════════════════════════════════════════════════╣
  # ║ This file was automatically copied from: transformations/customer_transforms.py ║
  # ║ during pipeline generation. Any changes made here will be OVERWRITTEN        ║
  # ║ on the next generation cycle.                                                ║
  # ║                                                                              ║
  # ║ To make changes:                                                             ║
  # ║ 1. Edit the original file: transformations/customer_transforms.py           ║
  # ║ 2. Regenerate the pipeline                                                   ║
  # ╚══════════════════════════════════════════════════════════════════════════════╝

  import requests
  from pyspark.sql import DataFrame
  # ... rest of your original function code ...

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp
  from custom_python_functions.customer_transforms import enrich_customer_data

  @dp.temporary_view()
  def v_customer_enriched():
      """Apply advanced customer enrichment using external APIs"""
      # Load source view(s)
      v_customer_bronze_df = spark.read.table("v_customer_bronze")
      
      # Apply Python transformation
      parameters = {
          "api_endpoint": "https://api.example.com/geocoding",
          "api_key": "{{ secret_substituted_api_key }}",
          "batch_size": 1000
      }
      df = enrich_customer_data(v_customer_bronze_df, spark, parameters)
      
      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', current_timestamp())
      
      return df

**For multiple source views:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from custom_python_functions.customer_analysis import analyze_customer_orders

  @dp.temporary_view()
  def v_customer_order_insights():
      """Analyze customer order patterns from multiple sources"""
      # Load source views
      v_customer_bronze_df = spark.read.table("v_customer_bronze")
      v_orders_bronze_df = spark.read.table("v_orders_bronze")
      
      # Apply Python transformation with multiple sources
      parameters = {
          "analysis_window_days": 90,
          "min_order_count": 5
      }
      dataframes = [v_customer_bronze_df, v_orders_bronze_df]
      df = analyze_customer_orders(dataframes, spark, parameters)
      
      return df

data_quality
-------------------------------------------
Data quality transform actions apply data validation rules using Databricks DLT expectations. They automatically handle data that fails validation based on configured actions.

.. code-block:: yaml

  actions:
    - name: customer_bronze_DQE
      type: transform
      transform_type: data_quality
      source: v_customer_bronze_cleaned
      target: v_customer_bronze_DQE
      readMode: stream  
      expectations_file: "expectations/customer_quality.json"
      description: "Apply data quality checks to customer data"

**Expectations File (expectations/customer_quality.json):**

.. code-block:: json
  :linenos:

  {
    "version": "1.0",
    "table": "customer",
    "expectations": [
      {
        "name": "valid_custkey",
        "expression": "customer_id IS NOT NULL AND customer_id > 0",
        "failureAction": "fail"
      },
      {
        "name": "valid_customer_name",
        "expression": "name IS NOT NULL AND LENGTH(TRIM(name)) > 0",
        "failureAction": "fail"
      },
      {
        "name": "valid_phone_format",
        "expression": "phone IS NULL OR LENGTH(phone) >= 10",
        "failureAction": "warn"
      },
      {
        "name": "valid_account_balance",
        "expression": "account_balance IS NULL OR account_balance >= -10000",
        "failureAction": "warn"
      },
      {
        "name": "suspicious_balance",
        "expression": "account_balance IS NULL OR account_balance < 50000",
        "failureAction": "drop"
      }
    ]
  }

**Anatomy of a data quality transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - transforms data with quality validation
- **transform_type**: Specifies this is a data quality transformation
- **source**: Name of the input view to validate
- **target**: Name of the output view after validation
- **readMode**: Must be *stream* - data quality transforms require streaming mode
- **expectations_file**: Path to JSON file containing validation rules
- **description**: Optional documentation for the action

**Expectation Actions:**
- **fail**: Stop the pipeline if any records violate the rule
- **warn**: Log warnings but continue processing all records  
- **drop**: Remove records that violate the rule but continue processing

.. seealso::
  - For DLT expectations see the `Databricks DLT expectations documentation <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_.
  - Data quality patterns: :doc:`../concepts`

.. Important::
  Data quality transforms require ``readMode: stream`` and generate DLT streaming tables with built-in quality monitoring.
  Use **fail** for critical business rules, **warn** for monitoring, and **drop** for data cleansing.

.. note::
  **File Organization**: Expectations files are typically stored in an ``expectations/`` folder.
  JSON format allows for version control and reuse across multiple pipelines.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view()
  # These expectations will fail the pipeline if violated
  @dp.expect_all_or_fail({
      "valid_custkey": "customer_id IS NOT NULL AND customer_id > 0",
      "valid_customer_name": "name IS NOT NULL AND LENGTH(TRIM(name)) > 0"
  })
  # These expectations will drop rows that violate them
  @dp.expect_all_or_drop({
      "suspicious_balance": "account_balance IS NULL OR account_balance < 50000"
  })
  # These expectations will log warnings but not drop rows
  @dp.expect_all({
      "valid_phone_format": "phone IS NULL OR LENGTH(phone) >= 10",
      "valid_account_balance": "account_balance IS NULL OR account_balance >= -10000"
  })
  def v_customer_bronze_DQE():
      """Apply data quality checks to customer data"""
      df = spark.readStream.table("v_customer_bronze_cleaned")
      
      return df

schema
-------------------------------------------
Schema transform actions apply column mapping, type casting, and schema enforcement to standardize data structures.

**Action Format Structure**

Schema transform actions use a flat structure with schema definition at the action level:

.. code-block:: yaml

  actions:
    - name: standardize_customer_schema
      type: transform
      transform_type: schema
      source: v_customer_raw                              # Simple view name (string)
      target: v_customer_standardized
      schema_file: "schemas/bronze/customer_transform.yaml"  # OR use inline schema
      enforcement: strict                                  # Optional: strict or permissive (default: permissive)
      readMode: stream                                     # Optional: stream (default) or batch
      description: "Standardize customer schema and data types"

**Key Points:**

- **source**: Must be a simple string (view name), not a dictionary
- **schema_file** OR **schema**: Choose one (external file or inline definition)
- **enforcement**: Action-level field (strict or permissive)
- **readMode**: Defaults to **stream** (not batch)

**External Schema Files (Recommended)**

External schema files contain only column definitions (no enforcement):

.. code-block:: yaml

  # In schemas/bronze/customer_transform.yaml:
  columns:
    - "c_custkey -> customer_id: BIGINT"     # Rename and cast
    - "c_name -> customer_name"               # Rename only
    - "c_address -> address"                  # Rename only
    - "account_balance: DECIMAL(18,2)"        # Cast only
    - "phone_number: STRING"                  # Cast only

**Arrow Format Syntax:**

- ``"old_col -> new_col: TYPE"`` - Rename and cast in one line
- ``"old_col -> new_col"`` - Rename only
- ``"col: TYPE"`` - Cast only (no rename)
- ``"col"`` - Pass-through (strict mode only, explicitly keep column)

**Schema File Paths:**

Schema files can be organized in subdirectories relative to your project root:

.. code-block:: yaml

  # Root level
  schema_file: "customer_transform.yaml"
  
  # In schemas/ directory
  schema_file: "schemas/customer_transform.yaml"
  
  # Nested subdirectories (recommended for organization by layer)
  schema_file: "schemas/bronze/dimensions/customer_transform.yaml"
  
  # Absolute paths also supported
  schema_file: "/absolute/path/to/schema.yaml"

**Inline Schema (Arrow Format)**

For simple transformations, use inline schema with arrow syntax:

.. code-block:: yaml

  - name: standardize_customer_schema
    type: transform
    transform_type: schema
    source: v_customer_raw
    target: v_customer_standardized
    enforcement: strict
    schema_inline: |
      c_custkey -> customer_id: BIGINT
      c_name -> customer_name
      account_balance: DECIMAL(18,2)
      phone_number: STRING

**Inline Schema (Full YAML Format)**

For more complex schemas, use full YAML structure inline:

.. code-block:: yaml

  - name: standardize_customer_schema
    type: transform
    transform_type: schema
    source: v_customer_raw
    target: v_customer_standardized
    enforcement: strict
    schema_inline: |
      columns:
        - "c_custkey -> customer_id: BIGINT"
        - "c_name -> customer_name"
        - "account_balance: DECIMAL(18,2)"

**Schema Enforcement Modes:**

Schema transforms support two enforcement modes (specified at action level):

- **strict**: Only explicitly defined columns are kept in the output. All other columns are dropped. This ensures data quality and prevents schema drift.
- **permissive** (default): Explicitly defined columns are transformed, but all other columns pass through unchanged. Useful when you only want to standardize specific columns.

.. code-block:: yaml

  # Strict enforcement example
  - name: transform_strict
    type: transform
    transform_type: schema
    source: v_raw
    target: v_clean
    enforcement: strict
    schema_file: "schemas/transform.yaml"
  
  # Input:  c_custkey, c_name, c_address, c_phone, extra_col
  # Output: customer_id, customer_name (+ operational metadata)
  #         ↑ All unmapped columns dropped

.. code-block:: yaml

  # Permissive enforcement example (default)
  - name: transform_permissive
    type: transform
    transform_type: schema
    source: v_raw
    target: v_clean
    enforcement: permissive  # or omit (permissive is default)
    schema_file: "schemas/transform.yaml"
  
  # Input:  c_custkey, c_name, c_address, c_phone, extra_col
  # Output: customer_id, customer_name, c_address, c_phone, extra_col
  #         ↑ All unmapped columns kept

**Breaking Change: Old Format No Longer Supported**

.. warning::
  The nested format with schema definition inside ``source`` is no longer supported and will raise an error:
  
  .. code-block:: yaml
  
    # OLD FORMAT (NO LONGER WORKS):
    source:
      view: v_customer_raw
      schema_file: "path.yaml"
    
    # NEW FORMAT (REQUIRED):
    source: v_customer_raw
    schema_file: "path.yaml"
    enforcement: strict
  
  If you see an error about "deprecated nested format", move ``schema_inline`` or ``schema_file`` to the top level of the action.

**Anatomy of a schema transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Must be ``transform``
- **transform_type**: Must be ``schema``
- **source**: Name of the input view to transform (simple string, not a dictionary)
- **target**: Name of the output view with transformed schema
- **schema_file**: Path to external schema file (arrow or legacy format) - use this OR schema_inline
- **schema_inline**: Inline schema definition (arrow or YAML format) - use this OR schema_file
- **enforcement**: Optional - Schema enforcement mode (``strict`` or ``permissive``, default: ``permissive``)
- **readMode**: Optional - Either ``stream`` (default) or ``batch`` - determines execution mode
- **description**: Optional documentation for the action

.. seealso::
  - For Spark data types see the `PySpark SQL types documentation <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_.
  - Schema evolution: :doc:`../concepts`

.. Important::
  Schema transforms preserve operational metadata columns automatically. These columns are never renamed or cast, and are always included in the output regardless of enforcement mode. Use schema transforms for standardizing column names and ensuring consistent data types across your lakehouse.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql import functions as F
  from pyspark.sql.types import StructType

  @dp.temporary_view()
  def v_customer_standardized():
      """Standardize customer schema and data types"""
      df = spark.readStream.table("v_customer_raw")  # Default is stream mode
      
      # Apply column renaming
      df = df.withColumnRenamed("c_custkey", "customer_id")
      df = df.withColumnRenamed("c_name", "customer_name")
      df = df.withColumnRenamed("c_address", "address")
      
      # Apply type casting
      df = df.withColumn("customer_id", F.col("customer_id").cast("BIGINT"))
      df = df.withColumn("account_balance", F.col("account_balance").cast("DECIMAL(18,2)"))
      df = df.withColumn("phone_number", F.col("phone_number").cast("STRING"))
      
      # Strict schema enforcement - select only specified columns
      # Schema-defined columns (will fail if missing)
      columns_to_select = [
          "customer_id",
          "customer_name",
          "address",
          "account_balance",
          "phone_number"
      ]
      
      # Add operational metadata columns only if they exist (optional)
      available_columns = set(df.columns)
      metadata_columns = [
          "_ingestion_timestamp",
          "_source_file"
      ]
      for meta_col in metadata_columns:
          if meta_col in available_columns:
              columns_to_select.append(meta_col)
      
      df = df.select(*columns_to_select)
      
      return df

Temporary Tables
-------------------------------------------
Temp table transform actions create temporary streaming tables for intermediate processing and reuse across multiple downstream actions.

**Option 1: Simple Passthrough**

.. code-block:: yaml

  actions:
    - name: create_customer_temp
      type: transform
      transform_type: temp_table
      source: v_customer_processed
      target: customer_intermediate
      readMode: stream
      description: "Create temporary table for customer intermediate processing"

**Option 2: With SQL Transformation**

.. code-block:: yaml

  actions:
    - name: create_temp_aggregate
      type: transform
      transform_type: temp_table
      source: v_customer_raw
      target: temp_daily_summary
      readMode: stream
      sql: |
        SELECT 
          DATE(order_date) as date,
          COUNT(*) as order_count,
          SUM(total_amount) as total_amount
        FROM stream({source})
        GROUP BY DATE(order_date)
      description: "Create temporary aggregate table with daily summaries"

**Anatomy of a temp table transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - creates temporary table
- **transform_type**: Specifies this is a temporary table transformation
- **source**: Name of the input view to materialize as temporary table
- **target**: Name of the temporary table to create
- **readMode**: Either *batch* or *stream* - determines table type
- **sql**: Optional SQL transformation to apply (inline option)
- **description**: Optional documentation for the action

.. seealso::
  - For SDP table types see the `Databricks SDP table types documentation <https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-table>`_.
  - Intermediate processing: :doc:`../concepts`

.. Important::
  Temp tables are automatically cleaned up when the pipeline completes.
  Use for complex multi-step transformations where intermediate materialization improves performance.
  
  For instance, if you have a complex transformation that will be used by several downstream actions,
  you can create a temporary table to prevent the transformation from being recomputed each time.

.. Warning::
  When using the ``sql`` property with streaming tables (``readMode: stream``), you must use the 
  ``stream()`` function in your SQL query to maintain streaming semantics. Without it, the query 
  will process data in batch mode.

**The above YAML examples translate to the following PySpark code**

**For simple passthrough:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.table(
      temporary=True,
  )
  def customer_intermediate():
      """Create temporary table for customer intermediate processing"""
      df = spark.readStream.table("v_customer_processed")
      
      return df

**For SQL transformation:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.table(
      temporary=True,
  )
  def temp_daily_summary():
      """Create temporary aggregate table with daily summaries"""
      df = spark.sql("""
          SELECT 
            DATE(order_date) as date,
            COUNT(*) as order_count,
            SUM(total_amount) as total_amount
          FROM stream(v_customer_raw)
          GROUP BY DATE(order_date)
      """)
      
      return df

