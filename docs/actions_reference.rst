Actions Reference
=================

Actions are the building block of Lakehouse Plumber flowgroups.

.. contents:: Table of Contents
   :depth: 2
   :local:


Action Types
------------

Actions come in three top-level types:

+----------------+----------------------------------------------------------+
| Type           | Purpose                                                  |
+================+==========================================================+
|| **Load**      || Bring data into a temporary **view** (e.g. CloudFiles,  |
||               || Delta, JDBC, SQL, Python).                              |
+----------------+----------------------------------------------------------+
|| **Transform** || Manipulate data in one or more steps (SQL, Python,      |
||               || schema adjustments, data-quality checks, temp tables…). |
+----------------+----------------------------------------------------------+
|| **Write**     || Persist the final dataset to a *streaming_table* or     |
||               || *materialized_view*.                                    |
+----------------+----------------------------------------------------------+





Load Actions
~~~~~~~~~~~~

.. note::
  - At this time the framework supports the following load sub-types.
  - Coming soon: It will support more sources and target types through **plugins**.

+-------------+------------------------------------------------------------+
| Sub-type    | Purpose & Source                                           |
+=============+============================================================+
|| cloudfiles || Databricks *Auto Loader* (CloudFiles) – stream files from |
||            || object storage (CSV, JSON, Parquet, etc.).                |
+-------------+------------------------------------------------------------+
|| delta      || Read from an existing Delta table or Change Data Feed     |
||            || (CDC).                                                    |
+-------------+------------------------------------------------------------+
|| sql        || Execute an arbitrary SQL query and load the result as a   |
||            || view.                                                     |
+-------------+------------------------------------------------------------+
|| jdbc       || Ingest from an external RDBMS via JDBC with secret        |
||            || handling.                                                 |
+-------------+------------------------------------------------------------+
|| python     || Call custom Python code (path or inline), returning a     |
||            || DataFrame.                                                |
+-------------+------------------------------------------------------------+

cloudFiles
++++++++++
.. code-block:: yaml

  actions:
    - name: load_csv_file_from_cloudfiles
      type: load
      readMode : "stream"
      operational_metadata: ["_source_file_path","_source_file_size","_source_file_modification_time"]
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
      target: v_customer_cloudfiles
      description: "Load customer CSV files from landing volume"

**Anatomy of a cloudFiles load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: is eiather *batch* or *stream* 
  this will translate to either ``spark.read.format("cloudFiles")`` or ``spark.readStream.format("cloudFiles")``
- **operational_metadata**: Add custom metadata columns
- **source**:
      - **type**: Use Databricks Auto Loader (CloudFiles)
      - **path**: File path pattern with substitution variables
      - **format**: Specify the file format as CSV, JSON, Parquet, etc.
      - **options**: 
            - **cloudFiles.format**: Explicitly set CloudFiles format to CSV
            - **header**: First row contains column headers
            - **delimiter**: Use pipe character as field separator
            - **cloudFiles.maxFilesPerTrigger**: Limit number of files processed per trigger
            - **cloudFiles.schemaHints**: the path to the schema file
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action
            
.. seealso::
  - For full list of options see the `Databricks Auto Loader documentation <https://docs.databricks.com/en/data/data-sources/cloud-files/auto-loader/index.html>`_.
  - Operational metadata: :doc:`concepts`
  
  .. TODO: add link to schema hints
    - Schema Hints: :doc:`schema_hints`

.. Important::
  Lakehouse Plumber uses syntax consistent with Databricks, making it easy to transfer knowledge between the two.
  All options available here mirror those of Databricks Auto Loader.


**The above Yaml translates to the following Pyspark code**

.. code-block:: python
  :linenos:

  import dlt
  from pyspark.sql.functions import F

  customer_cloudfiles_schema_hints = """
      c_custkey BIGINT,
      c_name STRING,
      c_address STRING,
      c_nationkey BIGINT,
      c_phone STRING,
      c_acctbal DECIMAL(18,2),
      c_mktsegment STRING,
      c_comment STRING
  """.strip().replace("\n", " ")


  @dlt.view()
  def v_customer_cloudfiles():
      """Load customer CSV files from landing volume"""
      df = spark.readStream \
          .format("cloudFiles") \
          .option("cloudFiles.format", "csv") \
          .option("header", True) \
          .option("delimiter", "|") \
          .option("cloudFiles.maxFilesPerTrigger", 11) \
          .option("cloudFiles.inferColumnTypes", False) \
          .option("cloudFiles.schemaEvolutionMode", "addNewColumns") \
          .option("cloudFiles.rescuedDataColumn", "_rescued_data") \
          .option("cloudFiles.schemaHints", customer_cloudfiles_schema_hints) \
          .load("/Volumes/acmi_edw_dev/edw_raw/landing_volume/customer/*.csv")


      # Add operational metadata columns
      df = df.withColumn('_source_file_size', F.col('_metadata.file_size'))
      df = df.withColumn('_source_file_modification_time', F.col('_metadata.file_modification_time'))
      df = df.withColumn('_source_file_path', F.col('_metadata.file_path'))

      return df

delta
++++++
.. code-block:: yaml

  actions:
    - name: customer_raw_load
      type: load
      operational_metadata: ["_processing_timestamp"]
      readMode: stream
      source:
        type: delta
        database: "{catalog}.{raw_schema}"
        table: customer
      target: v_customer_raw
      description: "Load customer table from raw schema" 

**Anatomy of a delta load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **operational_metadata**: Add custom metadata columns (e.g., processing timestamp)
- **readMode**: Either *batch* or *stream* - translates to ``spark.read.table()`` or ``spark.readStream.table()``
- **source**:
      - **type**: Use Delta table as source
      - **database**: Target database using substitution variables for catalog and schema
      - **table**: Name of the Delta table to read from
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

.. Important::
  Delta load actions can read from both regular Delta tables and Change Data Feed (CDC) enabled tables.
  Use readMode: stream for real-time processing or readMode: batch for one-time loads.

.. seealso::
  - For ``stream`` readMode seet the Databricks documentation on `Change Data Feed <https://docs.databricks.com/en/data/data-sources/delta/change-data-feed.html>`_
  - Operational metadata: :doc:`concepts`


**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt
  from pyspark.sql.functions import current_timestamp

  @dlt.view()
  def v_customer_raw():
      """Load customer table from raw schema"""
      df = spark.readStream.table("acmi_edw_dev.edw_raw.customer")
      
      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', current_timestamp())
      
      return df

sql
+++
SQL load actions support both **inline SQL** and **external SQL files**.

**Option 1: Inline SQL**

.. code-block:: yaml

  actions:
    - name: load_customer_summary
      type: load
      readMode: batch
      source:
        type: sql
        sql: |
          SELECT 
            c_custkey,
            c_name,
            c_mktsegment,
            COUNT(*) as order_count,
            SUM(o_totalprice) as total_spent
          FROM {catalog}.{raw_schema}.customer c
          LEFT JOIN {catalog}.{raw_schema}.orders o 
            ON c.c_custkey = o.o_custkey
          GROUP BY c_custkey, c_name, c_mktsegment
      target: v_customer_summary
      description: "Load customer summary with order statistics"

**Option 2: External SQL File**

.. code-block:: yaml

  actions:
    - name: load_customer_metrics
      type: load
      readMode: batch
      source:
        type: sql
        sql_path: "sql/customer_metrics.sql"
      target: v_customer_metrics
      description: "Load customer metrics from external SQL file"

**Anatomy of an SQL load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: Either *batch* or *stream* - determines execution mode
- **source**:
      - **type**: Use SQL query as source
      - **sql**: SQL statement with substitution variables for dynamic values (inline option)
      - **sql_path**: Path to external .sql file (external file option)
- **target**: Name of the temporary view created from query results
- **description**: Optional documentation for the action

.. seealso::
  - For SQL syntax see the `Databricks SQL documentation <https://docs.databricks.com/en/sql/index.html>`_.
  - Substitution variables: :doc:`concepts`

.. Important::
  SQL load actions allow you to create complex views from multiple tables using standard SQL.
  Use substitution variables like ``{catalog}`` and ``{schema}`` for environment-specific values.
  
.. note::
  **File Organization**: When using ``sql_path``, the path is relative to your YAML file location. 
  Common practice is to create a ``sql/`` folder alongside your pipeline YAML files.

**The above YAML examples translate to the following PySpark code**

**For inline SQL:**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.view()
  def v_customer_summary():
      """Load customer summary with order statistics"""
      return spark.sql("""
          SELECT 
            c_custkey,
            c_name,
            c_mktsegment,
            COUNT(*) as order_count,
            SUM(o_totalprice) as total_spent
          FROM acmi_edw_dev.edw_raw.customer c
          LEFT JOIN acmi_edw_dev.edw_raw.orders o 
            ON c.c_custkey = o.o_custkey
          GROUP BY c_custkey, c_name, c_mktsegment
      """)

**For external SQL file:**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.view()
  def v_customer_metrics():
      """Load customer metrics from external SQL file"""
      return spark.sql("""
          -- Content from sql/customer_metrics.sql file
          SELECT 
            customer_id,
            total_orders,
            avg_order_value,
            last_order_date
          FROM {catalog}.{silver_schema}.customer_analytics
          WHERE last_order_date >= current_date() - INTERVAL 90 DAYS
      """)

jdbc
+++++
JDBC load actions connect to external relational databases using JDBC drivers. They support both **table queries** and **custom SQL queries**.

**Option 1: Query-based JDBC**

.. code-block:: yaml

  actions:
    - name: load_external_customers
      type: load
      readMode: batch
      operational_metadata: ["_extraction_timestamp"]
      source:
        type: jdbc
        url: "jdbc:postgresql://db.example.com:5432/production"
        driver: "org.postgresql.Driver"
        user: "${secret:database/username}"
        password: "${secret:database/password}"
        query: |
          SELECT 
            customer_id,
            first_name,
            last_name,
            email,
            registration_date,
            country
          FROM customers 
          WHERE status = 'active'
          AND registration_date >= CURRENT_DATE - INTERVAL '7 days'
      target: v_external_customers
      description: "Load active customers from external PostgreSQL database"

**Option 2: Table-based JDBC**

.. code-block:: yaml

  actions:
    - name: load_external_products
      type: load
      readMode: batch
      source:
        type: jdbc
        url: "jdbc:mysql://mysql.example.com:3306/catalog"
        driver: "com.mysql.cj.jdbc.Driver"
        user: "${secret:mysql/username}"
        password: "${secret:mysql/password}"
        table: "products"
      target: v_external_products
      description: "Load products table from external MySQL database"

**Anatomy of a JDBC load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: Either *batch* or *stream* - JDBC typically uses batch mode
- **operational_metadata**: Add custom metadata columns (e.g., extraction timestamp)
- **source**:
      - **type**: Use JDBC connection as source
      - **url**: JDBC connection string with database server details
      - **driver**: JDBC driver class name (database-specific)
      - **user**: Database username (supports secret substitution)
      - **password**: Database password (supports secret substitution)
      - **query**: Custom SQL query to execute (query option)
      - **table**: Table name to read entirely (table option)
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

.. seealso::
  - For JDBC drivers see the `Databricks JDBC documentation <https://docs.databricks.com/en/connect/external-systems/jdbc.html>`_.
  - Secret management: :doc:`concepts`

.. Important::
  JDBC load actions require either a ``query`` or ``table`` field, but not both.
  Use secret substitution (``${secret:scope/key}``) for secure credential management.
  Ensure the appropriate JDBC driver is available in your Databricks cluster.

.. note::
  **Secret Management**: Always use ``${secret:scope/key}`` syntax for database credentials.
  The framework automatically handles secret substitution during code generation.

**The above YAML examples translate to the following PySpark code**

**For query-based JDBC:**

.. code-block:: python
  :linenos:

  import dlt
  from pyspark.sql.functions import current_timestamp

  @dlt.view()
  def v_external_customers():
      """Load active customers from external PostgreSQL database"""
      df = spark.read \
          .format("jdbc") \
          .option("url", "jdbc:postgresql://db.example.com:5432/production") \
          .option("user", "{{ secret_substituted_username }}") \
          .option("password", "{{ secret_substituted_password }}") \
          .option("driver", "org.postgresql.Driver") \
          .option("query", """
              SELECT 
                customer_id,
                first_name,
                last_name,
                email,
                registration_date,
                country
              FROM customers 
              WHERE status = 'active'
              AND registration_date >= CURRENT_DATE - INTERVAL '7 days'
          """) \
          .load()
      
      # Add operational metadata columns
      df = df.withColumn('_extraction_timestamp', current_timestamp())
      
      return df

**For table-based JDBC:**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.view()
  def v_external_products():
      """Load products table from external MySQL database"""
      df = spark.read \
          .format("jdbc") \
          .option("url", "jdbc:mysql://mysql.example.com:3306/catalog") \
          .option("user", "{{ secret_substituted_username }}") \
          .option("password", "{{ secret_substituted_password }}") \
          .option("driver", "com.mysql.cj.jdbc.Driver") \
          .option("dbtable", "products") \
          .load()
      
      return df

python
++++++
Python load actions call custom Python functions that return DataFrames. This allows for complex data extraction logic, API calls, or custom data processing.

**YAML Configuration:**

.. code-block:: yaml

  actions:
    - name: load_api_data
      type: load
      readMode: batch
      operational_metadata: ["_api_call_timestamp"]
      source:
        type: python
        module_path: "extractors/api_extractor.py"
        function_name: "extract_customer_data"
        parameters:
          api_endpoint: "https://api.example.com/customers"
          api_key: "${secret:apis/customer_api_key}"
          batch_size: 1000
          start_date: "2024-01-01"
      target: v_api_customers
      description: "Load customer data from external API"

**Python Function (extractors/api_extractor.py):**

.. code-block:: python
  :linenos:

  import requests
  from pyspark.sql import DataFrame
  from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType

  def extract_customer_data(spark, parameters: dict) -> DataFrame:
      """Extract customer data from external API.
      
      Args:
          spark: SparkSession instance
          parameters: Configuration parameters from YAML
          
      Returns:
          DataFrame: Customer data as PySpark DataFrame
      """
      # Extract parameters from YAML configuration
      api_endpoint = parameters.get("api_endpoint")
      api_key = parameters.get("api_key")
      batch_size = parameters.get("batch_size", 1000)
      start_date = parameters.get("start_date")
      
      # Call external API
      headers = {"Authorization": f"Bearer {api_key}"}
      response = requests.get(
          f"{api_endpoint}?start_date={start_date}&limit={batch_size}",
          headers=headers
      )
      response.raise_for_status()
      
      # Convert API response to DataFrame
      data = response.json()["customers"]
      
      # Define schema for the DataFrame
      schema = StructType([
          StructField("customer_id", IntegerType(), True),
          StructField("first_name", StringType(), True),
          StructField("last_name", StringType(), True),
          StructField("email", StringType(), True),
          StructField("registration_date", TimestampType(), True)
      ])
      
      # Create and return DataFrame
      return spark.createDataFrame(data, schema)

**Anatomy of a Python load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: Either *batch* or *stream* - Python actions typically use batch mode
- **operational_metadata**: Add custom metadata columns
- **source**:
      - **type**: Use Python function as source
      - **module_path**: Path to Python file containing the extraction function
      - **function_name**: Name of function to call (defaults to "get_df" if not specified)
      - **parameters**: Dictionary of parameters to pass to the function
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

.. seealso::
  - For PySpark DataFrame operations see the `Databricks PySpark documentation <https://docs.databricks.com/en/spark/latest/spark-sql/index.html>`_.
  - Custom functions: :doc:`concepts`

.. Important::
  Python functions must accept two parameters: ``spark`` (SparkSession) and ``parameters`` (dict).
  The function must return a PySpark DataFrame that will be used as the view source.

.. note::
  **File Organization**: When using ``module_path``, the path is relative to your YAML file location.
  Common practice is to create an ``extractors/`` or ``functions/`` folder alongside your pipeline YAML files.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt
  from pyspark.sql.functions import current_timestamp
  from extractors.api_extractor import extract_customer_data

  @dlt.view()
  def v_api_customers():
      """Load customer data from external API"""
      # Call the external Python function with spark and parameters
      parameters = {
          "api_endpoint": "https://api.example.com/customers",
          "api_key": "{{ secret_substituted_api_key }}",
          "batch_size": 1000,
          "start_date": "2024-01-01"
      }
      df = extract_customer_data(spark, parameters)
      
      # Add operational metadata columns
      df = df.withColumn('_api_call_timestamp', current_timestamp())
      
      return df

Transform Actions
~~~~~~~~~~~~~~~~~

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
+++
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

  import dlt

  @dlt.view(comment="Cleanse and standardize customer data for bronze layer")
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

  import dlt

  @dlt.view(comment="Enrich customer data with additional attributes")
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
++++++
Python transform actions call custom Python functions to apply complex transformation logic that goes beyond SQL capabilities.

.. code-block:: yaml

  actions:
    - name: customer_advanced_enrichment
      type: transform
      transform_type: python
      source:
        module_path: "transformations/customer_transforms.py"
        function_name: "enrich_customer_data"
        sources: ["v_customer_bronze"]
        parameters:
          api_endpoint: "https://api.example.com/geocoding"
          api_key: "${secret:apis/geocoding_key}"
          batch_size: 1000
      target: v_customer_enriched
      readMode: batch
      description: "Apply advanced customer enrichment using external APIs"

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

**Anatomy of a Python transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - transforms data from one view to another
- **transform_type**: Specifies this is a Python-based transformation
- **source**:
      - **module_path**: Path to Python file containing the transformation function
      - **function_name**: Name of function to call (defaults to "transform" if not specified)
      - **sources**: List of source view names to pass to the function
      - **parameters**: Dictionary of parameters to pass to the function
- **target**: Name of the output view to create
- **readMode**: Either *batch* or *stream* - determines execution mode
- **description**: Optional documentation for the action

.. seealso::
  - For PySpark DataFrame operations see the `Databricks PySpark documentation <https://docs.databricks.com/en/spark/latest/spark-sql/index.html>`_.
  - Custom functions: :doc:`concepts`

.. Important::
  Python functions must accept three parameters: ``df`` (DataFrame), ``spark`` (SparkSession), and ``parameters`` (dict).
  For multiple source views, the function receives a list of DataFrames as the first parameter.

.. note::
  **File Organization**: When using ``module_path``, the path is relative to your YAML file location.
  Common practice is to create a ``transformations/`` folder alongside your pipeline YAML files.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt
  from transformations.customer_transforms import enrich_customer_data

  @dlt.view()
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
      return enrich_customer_data(v_customer_bronze_df, spark, parameters)

data_quality
++++++++++++
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
  - For DLT expectations see the `Databricks DLT documentation <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_.
  - Data quality patterns: :doc:`concepts`

.. Important::
  Data quality transforms require ``readMode: stream`` and generate DLT streaming tables with built-in quality monitoring.
  Use **fail** for critical business rules, **warn** for monitoring, and **drop** for data cleansing.

.. note::
  **File Organization**: Expectations files are typically stored in an ``expectations/`` folder.
  JSON format allows for version control and reuse across multiple pipelines.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.view()
  # These expectations will fail the pipeline if violated
  @dlt.expect_all_or_fail({
      "valid_custkey": "customer_id IS NOT NULL AND customer_id > 0",
      "valid_customer_name": "name IS NOT NULL AND LENGTH(TRIM(name)) > 0"
  })
  # These expectations will drop rows that violate them
  @dlt.expect_all_or_drop({
      "suspicious_balance": "account_balance IS NULL OR account_balance < 50000"
  })
  # These expectations will log warnings but not drop rows
  @dlt.expect_all({
      "valid_phone_format": "phone IS NULL OR LENGTH(phone) >= 10",
      "valid_account_balance": "account_balance IS NULL OR account_balance >= -10000"
  })
  def v_customer_bronze_DQE():
      """Apply data quality checks to customer data"""
      df = spark.readStream.table("v_customer_bronze_cleaned")
      
      return df

schema
++++++
Schema transform actions apply column mapping, type casting, and schema enforcement to standardize data structures.

.. code-block:: yaml

  actions:
    - name: standardize_customer_schema
      type: transform
      transform_type: schema
      source:
        view: v_customer_raw
        schema:
          enforcement: strict
          column_mapping:
            c_custkey: customer_id
            c_name: customer_name
            c_address: address
            c_phone: phone_number
          type_casting:
            customer_id: "BIGINT"
            account_balance: "DECIMAL(18,2)"
            phone_number: "STRING"
      target: v_customer_standardized
      readMode: batch
      description: "Standardize customer schema and data types"

**Anatomy of a schema transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - transforms data schema and types
- **transform_type**: Specifies this is a schema transformation
- **source**:
      - **view**: Name of the input view to transform
      - **schema**: Schema transformation configuration
        - **enforcement**: Schema enforcement level ("strict" or "permissive")
        - **column_mapping**: Dictionary of old_name -> new_name mappings
        - **type_casting**: Dictionary of column_name -> new_data_type castings
- **target**: Name of the output view with transformed schema
- **readMode**: Either *batch* or *stream* - determines execution mode
- **description**: Optional documentation for the action

.. seealso::
  - For Spark data types see the `PySpark SQL types documentation <https://spark.apache.org/docs/latest/sql-ref-datatypes.html>`_.
  - Schema evolution: :doc:`concepts`

.. Important::
  Schema transforms preserve operational metadata columns automatically.
  Use for standardizing column names and ensuring consistent data types across your lakehouse.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt
  from pyspark.sql import functions as F
  from pyspark.sql.types import StructType

  @dlt.view()
  def v_customer_standardized():
      """Standardize customer schema and data types"""
      df = spark.read.table("v_customer_raw")
      
      # Apply column renaming
      df = df.withColumnRenamed("c_custkey", "customer_id")
      df = df.withColumnRenamed("c_name", "customer_name")
      df = df.withColumnRenamed("c_address", "address")
      df = df.withColumnRenamed("c_phone", "phone_number")
      
      # Apply type casting
      df = df.withColumn("customer_id", F.col("customer_id").cast("BIGINT"))
      df = df.withColumn("account_balance", F.col("account_balance").cast("DECIMAL(18,2)"))
      df = df.withColumn("phone_number", F.col("phone_number").cast("STRING"))
      
      return df

Temporary Tables
++++++++++++++++
Temp table transform actions create temporary streaming tables for intermediate processing and reuse across multiple downstream actions.

.. code-block:: yaml

  actions:
    - name: create_customer_temp
      type: transform
      transform_type: temp_table
      source: v_customer_processed
      target: customer_intermediate
      readMode: stream
      description: "Create temporary table for customer intermediate processing"

**Anatomy of a temp table transform action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - creates temporary table
- **transform_type**: Specifies this is a temporary table transformation
- **source**: Name of the input view to materialize as temporary table
- **target**: Name of the temporary table to create
- **readMode**: Either *batch* or *stream* - determines table type
- **description**: Optional documentation for the action

.. seealso::
  - For DLT table types see the `Databricks DLT documentation <https://docs.databricks.com/aws/en/dlt-ref/dlt-python-ref-table>`_.
  - Intermediate processing: :doc:`concepts`

.. Important::
  Temp tables are automatically cleaned up when the pipeline completes.
  Use for complex multi-step transformations where intermediate materialization improves performance.
  
  For instance, if you have a complex transformation that will be used by several downstream actions,
  you can create a temporary table to prevent the transformation from being recomputed each time.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.table(
      temporary=True,
  )
  def customer_intermediate():
      """Create temporary table for customer intermediate processing"""
      df = spark.readStream.table("v_customer_processed")
      
      return df

Write Actions
~~~~~~~~~~~~~

+-------------------+--------------------------------------------------------------------------+
| Sub-type          | Purpose                                                                  |
+===================+==========================================================================+
|| streaming_table  || Create or append to a Delta streaming table in Unity Catalog.           |
||                  || Supports Change Data Feed (CDF), CDC modes, and append flows.           |
+-------------------+--------------------------------------------------------------------------+
|| materialized_view|| Create a Lakeflow *materialized view* for batch-computed analytics.     |
+-------------------+--------------------------------------------------------------------------+

streaming_table
+++++++++++++++
Streaming table write actions create or append to Delta streaming tables. They support three modes: **standard** (append flows), **cdc** (change data capture), and **snapshot_cdc** (snapshot-based CDC).

**Standard Mode (Default)**

.. code-block:: yaml

  actions:
    - name: write_customer_bronze
      type: write
      source: v_customer_cleansed
      write_target:
        type: streaming_table
        database: "{catalog}.{bronze_schema}"
        table: customer
        create_table: true
        table_properties:
          delta.enableChangeDataFeed: "true"
          delta.autoOptimize.optimizeWrite: "true"
          quality: "bronze"
        partition_columns: ["region", "year"]
        cluster_columns: ["customer_id"]
        #spark_conf:
         # if you need to set spark conf, you can do it here
        table_schema: |
          customer_id BIGINT NOT NULL,
          name STRING,
          email STRING,
          region STRING,
          registration_date DATE,
          _source_file_path STRING,
          _processing_timestamp TIMESTAMP
        row_filter: "ROW FILTER catalog.schema.customer_access_filter ON (region)"
      description: "Write customer data to bronze streaming table"

**Anatomy of a streaming table write action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - persists data to a streaming table
- **source**: Source view(s) to read from (string or list of strings)
- **write_target**: Streaming table configuration
      - **type**: Use streaming table as target
      - **database**: Target database using substitution variables
      - **table**: Target table name
      - **create_table**: Whether to create the table (true) or append to existing (false)
      - **table_properties**: Delta table properties for optimization and metadata
      - **partition_columns**: Columns to partition the table by
      - **cluster_columns**: Columns to cluster/z-order the table by
      - **spark_conf**: Streaming-specific Spark configuration
      - **table_schema**: DDL schema definition for the table
      - **row_filter**: Row-level security filter using SQL UDF (format: "ROW FILTER function_name ON (column_names)")
      - **comment**: Table comment for documentation
      - **mode**: Streaming mode - "standard" (default), "cdc", or "snapshot_cdc"
- **description**: Optional documentation for the action

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt

  # Create the streaming table
  dlt.create_streaming_table(
      name="catalog.bronze.customer",
      comment="Write customer data to bronze streaming table",
      table_properties={
          "delta.enableChangeDataFeed": "true",
          "delta.autoOptimize.optimizeWrite": "true",
          "quality": "bronze"
      },
      spark_conf={
          "spark.sql.streaming.checkpointLocation": "/checkpoints/customer_bronze"
      },
      partition_cols=["region", "year"],
      cluster_by=["customer_id"],
      row_filter="ROW FILTER catalog.schema.customer_access_filter ON (region)",
      schema="""customer_id BIGINT NOT NULL,
        name STRING,
        email STRING,
        region STRING,
        registration_date DATE,
        _source_file_path STRING,
        _processing_timestamp TIMESTAMP"""
  )

  # Define append flow
  @dlt.append_flow(
      target="catalog.bronze.customer",
      name="f_customer_bronze",
      comment="Append flow to catalog.bronze.customer from v_customer_cleansed"
  )
  def f_customer_bronze():
      """Append flow to catalog.bronze.customer from v_customer_cleansed"""
      # Streaming flow
      df = spark.readStream.table("v_customer_cleansed")
      return df

**CDC Mode**

CDC mode enables Change Data Capture using DLT's auto CDC functionality for SCD Type 1 and Type 2 processing.

.. code-block:: yaml

  actions:
    - name: write_customer_scd
      type: write
      source: v_customer_changes
      write_target:
        type: streaming_table
        database: "{catalog}.{silver_schema}"
        table: dim_customer
        mode: "cdc"
        table_properties:
          delta.enableChangeDataFeed: "true"
          quality: "silver"
        row_filter: "ROW FILTER catalog.schema.customer_region_filter ON (region)"
        cdc_config:
          keys: ["customer_id"]
          sequence_by: "_commit_timestamp"
          scd_type: 2
          track_history_columns: ["name", "address", "phone"]
          ignore_null_updates: true
      description: "Track customer changes with CDC and SCD Type 2"

**The CDC YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  import dlt

  # Create the streaming table for CDC
  dlt.create_streaming_table(
      name="catalog.silver.dim_customer",
      comment="Track customer changes with CDC and SCD Type 2",
      table_properties={
          "delta.enableChangeDataFeed": "true",
          "quality": "silver"
      },
      row_filter="ROW FILTER catalog.schema.customer_region_filter ON (region)"
  )

  # CDC mode using auto_cdc
  dlt.create_auto_cdc_flow(
      target="catalog.silver.dim_customer",
      source="v_customer_changes",
      keys=["customer_id"],
      sequence_by="_commit_timestamp",
      stored_as_scd_type=2,
      track_history_column_list=["name", "address", "phone"],
      ignore_null_updates=True
  )

.. seealso::
  - For more information on ``create_auto_cdc_flow`` see the `Databricks official documentation <https://docs.databricks.com/en/delta-live-tables/dlt-python-ref-apply-changes.html>`_

**Snapshot CDC Mode**

Snapshot CDC mode creates CDC flows from full snapshots of data using DLT's `create_auto_cdc_from_snapshot_flow()`. It supports two source approaches: direct table references or custom Python functions.

**Option 1: Table Source**

.. code-block:: yaml

  actions:
    - name: write_customer_snapshot_simple
      type: write
      write_target:
        type: streaming_table
        database: "{catalog}.{silver_schema}"
        table: dim_customer_simple
        mode: "snapshot_cdc"
        snapshot_cdc_config:
          source: "catalog.bronze.customer_snapshots"
          keys: ["customer_id"]
          stored_as_scd_type: 1
        table_properties:
          delta.enableChangeDataFeed: "true"
          custom.data.owner: "data_team"
        partition_columns: ["region"]
        cluster_columns: ["customer_id"]
        row_filter: "ROW FILTER catalog.schema.region_access_filter ON (region)"
      description: "Create customer dimension from snapshot table"

**Option 2: Function Source with SCD Type 2**

.. code-block:: yaml

  actions:
    - name: write_customer_snapshot_advanced
      type: write
      write_target:
        type: streaming_table
        database: "{catalog}.{silver_schema}"
        table: dim_customer_advanced
        mode: "snapshot_cdc"
        snapshot_cdc_config:
          source_function:
            file: "customer_snapshot_functions.py"
            function: "next_customer_snapshot"
          keys: ["customer_id", "region"]
          stored_as_scd_type: 2
          track_history_column_list: ["name", "email", "address", "phone"]
        table_properties:
          delta.enableChangeDataFeed: "true"
      description: "Advanced customer dimension with function-based snapshots"

**Option 3: Exclude Columns from History Tracking**

.. code-block:: yaml

  actions:
    - name: write_product_snapshot
      type: write
      write_target:
        type: streaming_table
        database: "{catalog}.{silver_schema}"
        table: dim_product
        mode: "snapshot_cdc"
        snapshot_cdc_config:
          source: "catalog.bronze.product_snapshots"
          keys: ["product_id"]
          stored_as_scd_type: 2
          track_history_except_column_list: ["created_at", "updated_at", "_metadata"]
      description: "Product dimension excluding audit columns from history"

**Anatomy of snapshot CDC configuration**

- **snapshot_cdc_config**: Required configuration block for snapshot CDC
      - **source**: Source table name (mutually exclusive with source_function)
      - **source_function**: Python function configuration (mutually exclusive with source)
        - **file**: Path to Python file containing the function
        - **function**: Name of the function to call
      - **keys**: Primary key columns for CDC (required, list of strings)
      - **stored_as_scd_type**: SCD type - "1" or "2" (required)
      - **track_history_column_list**: Specific columns to track history for (optional)
      - **track_history_except_column_list**: Columns to exclude from history tracking (optional, mutually exclusive with track_history_column_list)

**Example Python Function for source_function**

Create file `customer_snapshot_functions.py`:

.. code-block:: python
  :linenos:

  from typing import Optional, Tuple
  from pyspark.sql import DataFrame

  def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
      """
      Snapshot processing function for customer data.
      
      Args:
          latest_version: Most recent version processed, or None for first run
          
      Returns:
          Tuple of (DataFrame, version_number) or None if no more data
      """
      if latest_version is None:
          # First run - load initial snapshot
          df = spark.read.table("catalog.bronze.customer_snapshots")
          return (df, 1)
      
             # Subsequent runs - check for new snapshots
       # Add your logic here to determine if new snapshots are available
       return None  # No more snapshots available
.. seealso::
  - For more information on ``create_auto_cdc_from_snapshot_flow`` see the `Databricks official documentation <https://docs.databricks.com/en/delta-live-tables/python-ref.html#create_auto_cdc_from_snapshot_flow>`_

**The above YAML examples translate to the following PySpark code**

**For table source (Option 1):**

.. code-block:: python
  :linenos:

  import dlt

  # Create the streaming table for snapshot CDC
  dlt.create_streaming_table(
      name="catalog.silver.dim_customer_simple",
      comment="Create customer dimension from snapshot table",
      table_properties={
          "delta.enableChangeDataFeed": "true",
          "custom.data.owner": "data_team"
      },
      partition_cols=["region"],
      cluster_by=["customer_id"],
      row_filter="ROW FILTER catalog.schema.region_access_filter ON (region)"
  )

  # Snapshot CDC mode using create_auto_cdc_from_snapshot_flow
  dlt.create_auto_cdc_from_snapshot_flow(
      target="catalog.silver.dim_customer_simple",
      source="catalog.bronze.customer_snapshots",
      keys=["customer_id"],
      stored_as_scd_type=1
  )

**For function source (Option 2):**

.. code-block:: python
  :linenos:

  import dlt
  from typing import Optional, Tuple
  from pyspark.sql import DataFrame

  # Snapshot function embedded directly in generated code
  def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
      """
      Snapshot processing function for customer data.
      
      Args:
          latest_version: Most recent version processed, or None for first run
          
      Returns:
          Tuple of (DataFrame, version_number) or None if no more data
      """
      if latest_version is None:
          # First run - load initial snapshot
          df = spark.read.table("catalog.bronze.customer_snapshots")
          return (df, 1)
      
      # Subsequent runs - check for new snapshots
      # Add your logic here to determine if new snapshots are available
      return None  # No more snapshots available

  # Create the streaming table for snapshot CDC
  dlt.create_streaming_table(
      name="catalog.silver.dim_customer_advanced",
      comment="Advanced customer dimension with function-based snapshots",
      table_properties={
          "delta.enableChangeDataFeed": "true"
      }
  )

  # Snapshot CDC mode using create_auto_cdc_from_snapshot_flow
  dlt.create_auto_cdc_from_snapshot_flow(
      target="catalog.silver.dim_customer_advanced",
      source=next_customer_snapshot,
      keys=["customer_id", "region"],
      stored_as_scd_type=2,
      track_history_column_list=["name", "email", "address", "phone"]
  )

**For exclude columns (Option 3):**

.. code-block:: python
  :linenos:

  import dlt

  # Create the streaming table for snapshot CDC
  dlt.create_streaming_table(
      name="catalog.silver.dim_product",
      comment="Product dimension excluding audit columns from history"
  )

  # Snapshot CDC mode using create_auto_cdc_from_snapshot_flow
  dlt.create_auto_cdc_from_snapshot_flow(
      target="catalog.silver.dim_product",
      source="catalog.bronze.product_snapshots",
      keys=["product_id"],
      stored_as_scd_type=2,
      track_history_except_column_list=["created_at", "updated_at", "_metadata"]
  )

.. Warning::
  **Table Creation Control**: Each streaming table must have exactly one action with `create_table: true` across the entire pipeline.
  Additional actions targeting the same table should use `create_table: false` to append data.

  By default, Lakehouse Plumber will create a streaming table with `create_table: true` if you do not specify otherwise.
  If you want to append to an existing streaming table, you can set `create_table: false`.

  **CDC Requirements**: CDC modes automatically set `create_table: true` and require specific source configurations. Standard mode supports multiple source views through append flows.

  **Snapshot CDC Requirements**: 
  - Must have either `source` OR `source_function` (mutually exclusive)
  - `keys` field is required and must be a list of column names
  - `stored_as_scd_type` must be "1" or "2" 
  - Can use either `track_history_column_list` OR `track_history_except_column_list` (mutually exclusive)
  - When using `source_function`, the Python function is embedded directly into the generated DLT code
  - Function file paths are relative to the YAML file location

materialized_view
++++++++++++++++++
Materialized view write actions create Databricks materialized views
for pre-computed analytics tables based on the output of a query.

**Option 1: Source View Based**

.. code-block:: yaml

  actions:
    - name: create_customer_summary_mv
      type: write
      source: v_customer_aggregated
      write_target:
        type: materialized_view
        database: "{catalog}.{gold_schema}"
        table: customer_summary
        table_properties:
          delta.autoOptimize.optimizeWrite: "true"
          custom.refresh.frequency: "daily"
        partition_columns: ["region"]
        cluster_columns: ["customer_segment"]
        refresh_schedule: "0 2 * * *"
        row_filter: "ROW FILTER catalog.schema.region_access_filter ON (region)"
        comment: "Daily customer summary materialized view"
      description: "Create daily customer summary for analytics"

**Option 2: SQL Query Based**

.. code-block:: yaml

  actions:
    - name: create_sales_summary_mv
      type: write
      write_target:
        type: materialized_view
        database: "{catalog}.{gold_schema}"
        table: daily_sales_summary
        sql: |
          SELECT 
            region,
            product_category,
            DATE(transaction_date) as sales_date,
            COUNT(*) as transaction_count,
            SUM(amount) as total_sales,
            AVG(amount) as avg_transaction_amount
          FROM {catalog}.{silver_schema}.sales_transactions
          WHERE DATE(transaction_date) >= CURRENT_DATE - INTERVAL 90 DAYS
          GROUP BY region, product_category, DATE(transaction_date)
        table_properties:
          delta.autoOptimize.optimizeWrite: "true"
          custom.business.domain: "sales_analytics"
        partition_columns: ["sales_date"]
        refresh_schedule: "0 1 * * *"
        row_filter: "ROW FILTER catalog.schema.region_access_filter ON (region)"
      description: "Daily sales summary by region and category"

**Anatomy of a materialized view write action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - creates a materialized view
- **source**: Source view to read from (optional if SQL provided in write_target)
- **write_target**: Materialized view configuration
      - **type**: Use materialized view as target
      - **database**: Target database using substitution variables
      - **table**: Target table name
      - **sql**: SQL query to define the view (alternative to source)
      - **table_properties**: Delta table properties for optimization
      - **partition_columns**: Columns to partition the view by
      - **cluster_columns**: Columns to cluster/z-order the view by
      - **refresh_schedule**: Cron expression for refresh schedule
      - **table_schema**: DDL schema definition for the view
      - **row_filter**: Row-level security filter using SQL UDF (format: "ROW FILTER function_name ON (column_names)")
      - **comment**: Table comment for documentation
- **description**: Optional documentation for the action

**The above YAML examples translate to the following PySpark code**

**For source view-based:**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.table(
      name="catalog.gold.customer_summary",
      comment="Daily customer summary materialized view",
      table_properties={
          "delta.autoOptimize.optimizeWrite": "true",
          "custom.refresh.frequency": "daily"
      },
      partition_cols=["region"],
      cluster_by=["customer_segment"],
      refresh_schedule="0 2 * * *",
      row_filter="ROW FILTER catalog.schema.region_access_filter ON (region)"
  )
  def customer_summary():
      """Create daily customer summary for analytics"""
      # Materialized views use batch processing
      df = spark.read.table("v_customer_aggregated")
      return df

**For SQL query-based:**

.. code-block:: python
  :linenos:

  import dlt

  @dlt.table(
      name="catalog.gold.daily_sales_summary",
      comment="Daily sales summary by region and category",
      table_properties={
          "delta.autoOptimize.optimizeWrite": "true",
          "custom.business.domain": "sales_analytics"
      },
      partition_cols=["sales_date"],
      refresh_schedule="0 1 * * *",
      row_filter="ROW FILTER catalog.schema.region_access_filter ON (region)"
  )
  def daily_sales_summary():
      """Daily sales summary by region and category"""
      # Materialized views use batch processing
      df = spark.sql("""SELECT 
        region,
        product_category,
        DATE(transaction_date) as sales_date,
        COUNT(*) as transaction_count,
        SUM(amount) as total_sales,
        AVG(amount) as avg_transaction_amount
      FROM catalog.silver.sales_transactions
      WHERE DATE(transaction_date) >= CURRENT_DATE - INTERVAL 90 DAYS
      GROUP BY region, product_category, DATE(transaction_date)""")
      return df


.. Important::
  Materialized views are designed for analytics workloads and always use batch processing.
  Use `refresh_schedule` to control when the view refreshes. 
  Materialized views can either read from source views or execute custom SQL queries.

Row-Level Security with row_filter
+++++++++++++++++++++++++++++++++++

The `row_filter` option enables row-level security for both streaming tables and materialized views. Row filters use SQL user-defined functions (UDFs) to control which rows users can see based on their identity, group membership, or other criteria.

**Creating a Row Filter Function**

Before applying a row filter to a table, you must create a SQL UDF that returns a boolean value:

.. code-block:: sql

  -- Example: Region-based access control
  CREATE FUNCTION catalog.schema.region_access_filter(region STRING)
  RETURN 
    CASE 
      WHEN IS_ACCOUNT_GROUP_MEMBER('admin') THEN TRUE
      WHEN IS_ACCOUNT_GROUP_MEMBER('na_users') THEN region IN ('US', 'Canada')
      WHEN IS_ACCOUNT_GROUP_MEMBER('emea_users') THEN region IN ('UK', 'Germany', 'France')
      ELSE FALSE
    END;

  -- Example: User-specific customer access
  CREATE FUNCTION catalog.schema.customer_access_filter(customer_id BIGINT)
  RETURN 
    IS_ACCOUNT_GROUP_MEMBER('admin') OR 
    EXISTS(
      SELECT 1 FROM catalog.access_control.user_customer_mapping 
      WHERE username = CURRENT_USER() AND customer_id_access = customer_id
    );

**Key Functions for Row Filters:**

- **CURRENT_USER()**: Returns the username of the current user
- **IS_ACCOUNT_GROUP_MEMBER('group_name')**: Returns true if user is in the specified group
- **EXISTS()**: Checks for existence in mapping tables for complex access control

**Row Filter Syntax**

The row filter format is: ``"ROW FILTER function_name ON (column_names)"``

- **function_name**: Name of the SQL UDF that implements the filtering logic
- **column_names**: Comma-separated list of columns to pass to the function

.. seealso::
  - For complete row filter documentation see the `Databricks Row Filters and Column Masks documentation <https://docs.databricks.com/aws/en/dlt/unity-catalog#row-filters-and-column-masks>`_.


Further Reading
---------------
* :doc:`concepts` – deeper dive into how Actions fit inside FlowGroups.
* `Reference templates <https://github.com/.../Reference_Templates>`_ – fully-
  documented YAML files covering every option. 