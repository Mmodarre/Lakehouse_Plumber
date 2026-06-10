Load Actions
============

.. meta::
   :description: Complete reference for LHP Load action types: cloudfiles, delta, sql, jdbc, python, kafka, and custom_datasource.

Concept
-------

What
~~~~

A Load action reads data from a source — files, a Delta table, a SQL query,
an RDBMS, a Python function, a Kafka topic, or a custom PySpark DataSource —
and exposes the result as a temporary view inside a FlowGroup. Downstream
Transform and Write actions reference the view by its ``target`` name. Each
Load action declares one source via the ``source.type`` field; the rest of
``source`` carries the type-specific configuration.

When
~~~~

Every FlowGroup that produces data starts with a Load action. Pick the
sub-type by how the source is delivered:

.. list-table::
   :header-rows: 1
   :widths: 22 78

   * - Sub-type
     - Use when…
   * - ``cloudfiles``
     - Files arrive in object storage (S3, ADLS, GCS, Unity Catalog
       volumes); incremental ingestion with checkpoints and schema
       evolution. Streaming only.
   * - ``delta``
     - Reading an existing Delta table or its Change Data Feed (CDF).
       Batch or streaming.
   * - ``sql``
     - An arbitrary SQL query materialised as a temporary view.
   * - ``jdbc``
     - Pulling from an external RDBMS; credentials via Databricks secrets.
   * - ``python``
     - The format is not covered by a built-in sub-type, or you need
       custom pre-processing in Python before the flow sees the data.
   * - ``kafka``
     - Streaming from Apache Kafka, AWS MSK, or Azure Event Hubs.
   * - ``custom_datasource``
     - You have a PySpark ``DataSource`` implementation and want LHP to
       register and invoke it.

Minimum example
~~~~~~~~~~~~~~~

The smallest working Load action reads a Delta table and exposes it as a
temporary view:

.. code-block:: yaml
   :caption: pipelines/bronze/customer.yaml

   actions:
     - name: customer_raw_load
       type: load
       readMode: stream
       source:
         type: delta
         catalog: "${catalog}"
         schema: "${raw_schema}"
         table: customer
       target: v_customer_raw

The reference body below documents every sub-type and every option.

Reference
---------

LHP supports seven Load sub-types: ``delta``, ``cloudfiles``, ``sql``,
``jdbc``, ``python``, ``kafka``, and ``custom_datasource``. Additional
sources arrive through the plugin mechanism.

delta
~~~~~

Use when reading an existing Delta table or its Change Data Feed (CDF).
Batch or streaming.

.. deprecated:: 0.7.8
   The ``database`` field (e.g., ``database: "${catalog}.${schema}"``) is deprecated
   for delta sources. Use explicit ``catalog`` and ``schema`` fields instead. The old
   format is auto-converted with a deprecation warning. Removal in v1.0.0.

.. code-block:: yaml

  actions:
    - name: customer_raw_load
      type: load
      operational_metadata: ["_processing_timestamp"]
      readMode: stream
      source:
        type: delta
        catalog: "${catalog}"
        schema: "${raw_schema}"
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
      - **catalog**: Target catalog using substitution variables
      - **schema**: Target schema using substitution variables
      - **table**: Name of the Delta table to read from
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

Delta load actions read from both regular Delta tables and Change Data Feed (:term:`CDC`) enabled tables.
Use ``readMode: stream`` for real-time processing or ``readMode: batch`` for one-time loads.

**Delta Options**

Delta load actions support the ``options`` field to configure Delta-specific reader options:

.. code-block:: yaml

  actions:
    - name: load_orders_cdc
      type: load
      readMode: stream
      source:
        type: delta
        catalog: "${catalog}"
        schema: "bronze"
        table: orders
        options:
          readChangeFeed: "true"
          startingVersion: "0"
          ignoreDeletes: "true"
      target: v_orders_changes
      description: "Stream order changes using Delta Change Data Feed"

**Supported Delta Options:**

+-------------------------+------------------+---------------------------------------------------+
| Option                  | Type             | Description                                       |
+=========================+==================+===================================================+
| **readChangeFeed**      | string/boolean   | Enable Change Data Feed (stream or batch)         |
+-------------------------+------------------+---------------------------------------------------+
| **startingVersion**     | string           | Starting version for CDC or time travel           |
+-------------------------+------------------+---------------------------------------------------+
| **startingTimestamp**   | string           | Starting timestamp for CDC (ISO 8601 format)      |
+-------------------------+------------------+---------------------------------------------------+
| **endingVersion**       | string           | Ending version for batch CDF reads                |
+-------------------------+------------------+---------------------------------------------------+
| **endingTimestamp**     | string           | Ending timestamp for batch CDF reads              |
+-------------------------+------------------+---------------------------------------------------+
| **versionAsOf**         | string           | Read specific table version (time travel)         |
+-------------------------+------------------+---------------------------------------------------+
| **timestampAsOf**       | string           | Read table at specific timestamp (time travel)    |
+-------------------------+------------------+---------------------------------------------------+
| **ignoreDeletes**       | boolean          | Ignore delete operations in CDC                   |
+-------------------------+------------------+---------------------------------------------------+
| **skipChangeCommits**   | string/boolean   | Skip change commits in CDC stream                 |
+-------------------------+------------------+---------------------------------------------------+
| **maxFilesPerTrigger**  | number           | Maximum files to process per trigger              |
+-------------------------+------------------+---------------------------------------------------+

.. note::
  - ``readChangeFeed`` works in both **stream** and **batch** mode. In batch mode, a starting bound (``startingVersion`` or ``startingTimestamp``) is required.
  - ``endingVersion`` and ``endingTimestamp`` are only valid in batch mode.
  - ``readChangeFeed`` and ``skipChangeCommits`` are **mutually exclusive** — one reads all changes, the other skips them.
  - ``readChangeFeed`` cannot be combined with time-travel options (``versionAsOf`` / ``timestampAsOf``).
  - ``startingVersion`` and ``startingTimestamp`` are mutually exclusive.
  - ``versionAsOf`` and ``timestampAsOf`` are mutually exclusive.
  - All option values are validated and cannot be ``None`` or empty strings.

**Batch CDF Example:**

.. code-block:: yaml

  actions:
    - name: load_order_changes
      type: load
      readMode: batch
      source:
        type: delta
        catalog: "${catalog}"
        schema: "bronze"
        table: orders
        options:
          readChangeFeed: "true"
          startingVersion: "5"
          endingVersion: "20"
      target: v_order_changes
      description: "Read order changes between version 5 and 20"

.. warning::
  When using ``startingVersion``, the specified version may become unavailable after
  ``VACUUM`` runs. Prefer ``startingTimestamp`` for durable references, or use
  checkpoint-managed streaming for production workloads.

**readChangeFeed vs skipChangeCommits:**

- ``readChangeFeed: "true"`` — reads the Change Data Feed, exposing row-level changes
  (inserts, updates, deletes) with metadata columns. Use this when you need to process
  individual changes (e.g., CDC into a downstream table).
- ``skipChangeCommits: "true"`` — skips commits that contain data-changing operations
  (useful when a table has CDF enabled but you only want the latest state, ignoring
  change events). **Cannot be combined with readChangeFeed.**

**CDF Metadata Columns:**

When ``readChangeFeed`` is enabled, the resulting DataFrame includes three additional
columns:

- ``_change_type`` — the type of change: ``insert``, ``update_preimage``, ``update_postimage``, or ``delete``
- ``_commit_version`` — the Delta version of the commit
- ``_commit_timestamp`` — the timestamp of the commit

An ``UPDATE`` operation produces **two rows**: one with ``_change_type = "update_preimage"``
(the old values) and one with ``_change_type = "update_postimage"`` (the new values).

If writing CDF data to a non-CDC streaming table, you should filter or drop these columns
in a transform action:

.. code-block:: sql

  SELECT * EXCEPT (_change_type, _commit_version, _commit_timestamp)
  FROM stream(v_order_changes)
  WHERE _change_type != 'delete'

**Full Refresh Resilience:**

When a Delta table undergoes a full refresh (e.g., ``TRUNCATE`` followed by reload), the
CDF stream emits a large batch of ``delete`` rows followed by ``insert`` rows. This can
overwhelm downstream consumers. Mitigation strategies:

- Use ``ignoreDeletes: "true"`` if deletes are not relevant to your pipeline.
- Use ``skipChangeCommits: "true"`` on non-CDF consumers that share the same source table.
- For CDC targets, rely on Databricks checkpointing to handle reprocessing gracefully.

**Time Travel Example:**

.. code-block:: yaml

  actions:
    - name: load_customers_snapshot
      type: load
      readMode: batch
      source:
        type: delta
        catalog: "${catalog}"
        schema: "silver"
        table: customers
        options:
          versionAsOf: "10"
        where_clause: ["status = 'active'"]
        select_columns: ["customer_id", "name", "email"]
      target: v_customers_snapshot
      description: "Load customers at version 10"

.. seealso::
  - For ``stream`` readMode see the Databricks documentation on `Change Data Feed <https://docs.databricks.com/aws/en/delta/delta-change-data-feed>`_
  - For time travel see `Delta Time Travel <https://docs.databricks.com/en/delta/history.html>`_
  - Operational metadata: :doc:`../operational_metadata`


**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp

  @dp.temporary_view()
  def v_customer_raw():
      """Load customer table from raw schema"""
      df = spark.readStream.table("acmi_edw_dev.edw_raw.customer")

      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', current_timestamp())

      return df

cloudfiles
~~~~~~~~~~

Use when files arrive in object storage and you want incremental ingestion with
checkpoints and schema evolution. Streaming only. For an end-to-end walkthrough,
see :doc:`../ingest_with_autoloader`.

.. code-block:: yaml

  actions:
    - name: load_csv_file_from_cloudfiles
      type: load
      readMode : "stream"
      operational_metadata: ["_source_file_path","_source_file_size","_source_file_modification_time"]
      source:
        type: cloudfiles
        path: "${landing_volume}/{{ landing_folder }}/*.csv"
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
- **readMode**: must be *stream* (CloudFiles only supports streaming mode).
  This translates to ``spark.readStream.format("cloudFiles")``
- **operational_metadata**: Add custom metadata columns
- **source**:
      - **type**: Use Databricks Auto Loader (CloudFiles)
      - **path**: File path pattern with substitution variables
      - **format**: Specify the file format as CSV, JSON, Parquet, etc.
      - **schema**: Path to a YAML schema file for full schema enforcement (see below)
      - **options**:
            - **cloudFiles.format**: Explicitly set CloudFiles format to CSV
            - **header**: First row contains column headers
            - **delimiter**: Use pipe character as field separator
            - **cloudFiles.maxFilesPerTrigger**: Limit number of files processed per trigger
            - **cloudFiles.schemaHints**: Schema definition for Auto Loader (supports multiple formats - see below)
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

**cloudFiles.schemaHints Format Options**

The ``cloudFiles.schemaHints`` option supports three formats, automatically detected by the framework:

**Option 1: Inline DDL String** (for simple schemas)

.. code-block:: yaml

  cloudFiles.schemaHints: "customer_id BIGINT, name STRING, email STRING"

**Option 2: External YAML File** (recommended for complex schemas with metadata)

.. code-block:: yaml

  cloudFiles.schemaHints: "schemas/customer_schema.yaml"

**Option 3: External DDL/SQL File** (for pre-defined DDL statements)

.. code-block:: yaml

  cloudFiles.schemaHints: "schemas/customer_schema.ddl"
  # or
  cloudFiles.schemaHints: "schemas/customer_schema.sql"

**File Path Organization**: Organize schema files in subdirectories relative to your project root:

- Root level: ``"customer_schema.yaml"``
- Single directory: ``"schemas/customer_schema.yaml"``
- Nested subdirectories: ``"schemas/bronze/dimensions/customer_schema.yaml"``

The framework automatically detects whether the value is an inline DDL string or a file path based on common file indicators (``.yaml``, ``.yml``, ``.ddl``, ``.sql``, or path separators).

**YAML Schema Conversion**: When using YAML schema files (Option 2), the ``nullable`` field is respected during conversion to DDL:

- Columns with ``nullable: false`` are converted to include ``NOT NULL`` constraint
- Columns with ``nullable: true`` (or omitted, default is true) are converted without constraints

Example: A YAML column defined as ``{name: c_custkey, type: BIGINT, nullable: false}`` will generate ``c_custkey BIGINT NOT NULL`` in the schema hints.

**source.schema — Full Schema Enforcement**

The ``source.schema`` field provides full schema enforcement by applying a ``StructType`` schema
on the ``DataStreamReader`` before ``.load()``. This disables schema inference entirely, ensuring
the data conforms exactly to the specified schema.

**When to use ``schema`` vs ``schemaHints``:**

- Use ``schema`` when you want to **enforce** a complete, exact schema and disable inference.
- Use ``schemaHints`` when you want to **guide** Auto Loader's inference while still allowing it to discover additional columns.

.. code-block:: yaml

  actions:
    - name: load_customer_with_schema
      type: load
      readMode: stream
      source:
        type: cloudfiles
        path: "/data/customers/*.csv"
        format: csv
        schema: schemas/customer_schema.yaml
        options:
          cloudFiles.format: csv
      target: v_customer_raw
      description: "Load customer CSV with explicit schema enforcement"

The schema file uses the same YAML format as ``schemaHints`` files:

.. code-block:: yaml

  name: customer
  version: "1.0"
  columns:
    - name: c_custkey
      type: BIGINT
      nullable: false
    - name: c_name
      type: STRING
      nullable: true

This generates code with ``.schema()`` applied on the reader chain before ``.load()``:

.. code-block:: python

  customer_schema = StructType([
      StructField("c_custkey", LongType(), False),
      StructField("c_name", StringType(), True),
  ])

  @dp.temporary_view()
  def v_customer_raw():
      df = spark.readStream \
          .format("cloudFiles") \
          .schema(customer_schema) \
          .option("cloudFiles.format", "csv") \
          .load("/data/customers/*.csv")
      return df

When you provide ``source.schema``, ``cloudFiles.schemaEvolutionMode`` defaults to ``none``
because inference is disabled. Do not combine ``source.schema`` with ``cloudFiles.schemaHints``
— these are mutually exclusive approaches.

Lakehouse Plumber uses syntax consistent with Databricks so you can transfer knowledge between
the two. All options available here mirror those of Databricks Auto Loader.

.. seealso::
  - For the end-to-end how-to see :doc:`../ingest_with_autoloader`.
  - For full list of options see the `Databricks Auto Loader documentation <https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/>`_.
  - Operational metadata: :doc:`../operational_metadata`


**The above Yaml translates to the following Pyspark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import F

  customer_cloudfiles_schema_hints = """
      c_custkey BIGINT NOT NULL,
      c_name STRING NOT NULL,
      c_address STRING,
      c_nationkey BIGINT NOT NULL,
      c_phone STRING,
      c_acctbal DECIMAL(18,2),
      c_mktsegment STRING,
      c_comment STRING
  """.strip().replace("\n", " ")


  @dp.temporary_view()
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

sql
~~~

Use when you need an arbitrary SQL query (often joins or windowed aggregates across
already-loaded sources) materialised as a temporary view. SQL load actions support
both **inline SQL** and **external SQL files**.

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
          FROM ${catalog}.${raw_schema}.customer c
          LEFT JOIN ${catalog}.${raw_schema}.orders o
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
  - Substitution variables: :doc:`../substitutions`

SQL load actions let you create complex views from multiple tables using standard SQL.
Use substitution variables like ``${catalog}`` and ``${schema}`` for environment-specific values.

**File Substitution Support**

Substitution variables work in both inline SQL and external SQL files (``sql_path``).
The same ``${token}`` and ``${secret:scope/key}`` syntax from YAML works in ``.sql`` files.
Files are processed for substitutions before query execution.

**File Organization**: When using ``sql_path``, the path is relative to your YAML file location.
Common practice is to create a ``sql/`` folder alongside your pipeline YAML files.

**The above YAML examples translate to the following PySpark code**

**For inline SQL:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view()
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

  from pyspark import pipelines as dp

  @dp.temporary_view()
  def v_customer_metrics():
      """Load customer metrics from external SQL file"""
      return spark.sql("""
          -- Content from sql/customer_metrics.sql file
          SELECT
            customer_id,
            total_orders,
            avg_order_value,
            last_order_date
          FROM ${catalog}.${silver_schema}.customer_analytics
          WHERE last_order_date >= current_date() - INTERVAL 90 DAYS
      """)

jdbc
~~~~

Use when pulling from an external RDBMS (Oracle, SQL Server, Postgres, MySQL).
Credentials flow through Databricks secrets. JDBC load actions support both
**table queries** and **custom SQL queries**.

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
  - Secret management: :doc:`../substitutions`

JDBC load actions require either a ``query`` or ``table`` field, but not both — providing
both raises an error. Use secret substitution (``${secret:scope/key}``) for secure credential
management, and ensure the appropriate JDBC driver is available on your Databricks cluster.

**Secret Management**: Always use ``${secret:scope/key}`` syntax for database credentials.
The framework automatically handles secret substitution during code generation.

**The above YAML examples translate to the following PySpark code**

**For query-based JDBC:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp

  @dp.temporary_view()
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

  from pyspark import pipelines as dp

  @dp.temporary_view()
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
~~~~~~

Use when the source format is not covered by a built-in sub-type, or you need
custom pre-processing in Python before the flow sees the data. Python load
actions call custom Python functions that return DataFrames.

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
  - Custom functions: :doc:`../architecture`

Python functions must accept two parameters: ``spark`` (SparkSession) and ``parameters`` (dict).
The function must return a PySpark DataFrame that will be used as the view source.

**File Organization**: When using ``module_path``, the path is relative to the project root.
Common practice is to create an ``extractors/`` or ``functions/`` folder at the project root.

**Parameter Substitution**: The ``parameters`` dictionary supports ``${token}``
substitution for environment-specific values:

.. code-block:: yaml

   parameters:
     catalog: "${catalog}"
     table_name: "${schema}.users"
     api_endpoint: "${api_url}"
     batch_size: 1000                     # No substitution needed

All tokens are replaced with values from ``substitutions/{env}.yaml`` at generation time.
Secret references (``${secret:scope/key}``) are converted to ``dbutils.secrets.get()`` calls.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp
  from extractors.api_extractor import extract_customer_data

  @dp.temporary_view()
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

**Importing local helper modules**

A Python load function (or a custom data source) may import local helper modules that
live alongside it. LHP follows those imports and copies the whole transitive closure of
local helpers into ``custom_python_functions/``, preserving sub-package structure, so the
entry module imports cleanly at runtime.

The directory that holds your entry file is the **import root** against which "local" is
decided:

- **Rule A — the import root must not itself be a package.** It may not contain an
  ``__init__.py`` at its top level. If it does, generation fails with
  ``LHP-VAL-023`` (put helpers in a sub-directory instead and keep the entry file flat).
- **Rule B — referenced helper packages are copied in full.** If your entry imports from
  a local sub-package, the *entire* sub-package (every ``.py`` under it) is copied with its
  structure preserved, so ``__init__.py`` side effects and intra-package imports keep working.

Imports inside copied files are reconciled as follows:

- **Absolute local imports are prefix-rewritten.** ``from helpers.dates import to_date``
  becomes ``from custom_python_functions.helpers.dates import to_date`` (aliases preserved).
- **Relative imports are preserved unchanged.** ``from .sibling import x`` inside a helper
  package is first-class and copied verbatim — structure preservation keeps it resolvable.
- **External and standard-library imports are left untouched** (``import os``,
  ``from pyspark.sql import functions``).
- **Plain dotted imports of a local module are rejected** with ``LHP-VAL-024``:
  ``import helpers.dates`` cannot be rewritten safely — use ``from helpers.dates import ...``
  (or ``from helpers import dates``) instead.
- A local import that points at a file or package member that does not exist on disk fails
  with ``LHP-VAL-025``.

Because a referenced helper package is copied whole, a syntactically broken sibling module
inside that package surfaces ``LHP-IO-003`` at generate time.

kafka
~~~~~

Use when streaming from Apache Kafka, AWS Managed Streaming for Apache Kafka
(MSK), or Azure Event Hubs via the Kafka protocol.

.. code-block:: yaml

  actions:
    - name: load_kafka_events
      type: load
      readMode: stream
      operational_metadata: ["_processing_timestamp"]
      source:
        type: kafka
        bootstrap_servers: "kafka1.example.com:9092,kafka2.example.com:9092"
        subscribe: "events,logs,metrics"
        options:
          startingOffsets: "latest"
          failOnDataLoss: false
          kafka.group.id: "lhp-consumer-group"
          kafka.session.timeout.ms: 30000
          kafka.ssl.truststore.location: "/path/to/truststore.jks"
          kafka.ssl.truststore.password: "${secret:scope/truststore-password}"
      target: v_kafka_events_raw
      description: "Load events from Kafka topics"

**Anatomy of a Kafka load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: Must be *stream* - Kafka is always streaming
- **operational_metadata**: Add custom metadata columns (e.g., processing timestamp)
- **source**:
      - **type**: Use Apache Kafka as source
      - **bootstrap_servers**: Comma-separated list of Kafka broker addresses (host:port)
      - **subscribe**: Comma-separated list of topics to subscribe to (choose ONE subscription method)
      - **subscribePattern**: Java regex pattern for topic subscription (alternative to subscribe)
      - **assign**: JSON string specifying specific topic partitions (alternative to subscribe)
      - **options**:
            - **startingOffsets**: Starting offset position (earliest/latest/JSON)
            - **failOnDataLoss**: Whether to fail on potential data loss (default: true)
            - **kafka.group.id**: Consumer group ID (use with caution)
            - **kafka.session.timeout.ms**: Session timeout in milliseconds
            - **kafka.ssl.***: SSL/TLS configuration options for secure connections
            - **kafka.sasl.***: SASL authentication options
            - All other kafka.* options from Databricks Kafka connector
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

.. seealso::
  - For full list of Kafka options see the `Databricks Kafka documentation <https://docs.databricks.com/aws/en/connect/streaming/kafka.html>`_.
  - Operational metadata: :doc:`../operational_metadata`

Kafka always returns a fixed 7-column schema with binary key/value columns:
``key``, ``value``, ``topic``, ``partition``, ``offset``, ``timestamp``, ``timestampType``.
Explicitly deserialize the key and value columns using transform actions.

.. warning::
  **Subscription Methods**: Specify exactly ONE of:

  - ``subscribe``: Comma-separated list of specific topics
  - ``subscribePattern``: Java regex pattern for topic names
  - ``assign``: JSON with specific topic partitions

  Using multiple subscription methods will result in an error.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp

  @dp.temporary_view()
  def v_kafka_events_raw():
      """Load events from Kafka topics"""
      df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "kafka1.example.com:9092,kafka2.example.com:9092") \
          .option("subscribe", "events,logs,metrics") \
          .option("startingOffsets", "latest") \
          .option("failOnDataLoss", False) \
          .option("kafka.group.id", "lhp-consumer-group") \
          .option("kafka.session.timeout.ms", 30000) \
          .option("kafka.ssl.truststore.location", "/path/to/truststore.jks") \
          .option("kafka.ssl.truststore.password", dbutils.secrets.get("scope", "truststore-password")) \
          .load()

      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', current_timestamp())

      return df

**Example: Deserializing Kafka Data**

Since Kafka returns binary data, you typically need a transform action to deserialize:

.. code-block:: yaml

  actions:
    # Load from Kafka (returns binary key/value)
    - name: load_kafka_events
      type: load
      readMode: stream
      source:
        type: kafka
        bootstrap_servers: "localhost:9092"
        subscribe: "events"
      target: v_kafka_events_raw

    # Deserialize and parse JSON
    - name: parse_kafka_events
      type: transform
      transform_type: sql
      source: v_kafka_events_raw
      target: v_kafka_events_parsed
      sql: |
        SELECT
          CAST(key AS STRING) as message_key,
          from_json(CAST(value AS STRING), 'event_type STRING, timestamp BIGINT, data STRING') as parsed_value,
          topic,
          partition,
          offset,
          timestamp as kafka_timestamp
        FROM $source

**Advanced Authentication: AWS MSK IAM**

AWS Managed Streaming for Apache Kafka (MSK) supports IAM authentication for secure, credential-free access.

**Prerequisites:**

1. AWS MSK cluster configured with IAM authentication enabled
2. Databricks cluster with IAM role/instance profile with MSK permissions
3. IAM policy granting ``kafka-cluster:Connect``, ``kafka-cluster:DescribeCluster``, and topic/group permissions

**YAML Configuration:**

.. code-block:: yaml

  actions:
    - name: load_msk_orders
      type: load
      readMode: stream
      source:
        type: kafka
        bootstrap_servers: "b-1.msk-cluster.abc123.kafka.us-east-1.amazonaws.com:9098"
        subscribe: "orders"
        options:
          kafka.security.protocol: "SASL_SSL"
          kafka.sasl.mechanism: "AWS_MSK_IAM"
          kafka.sasl.jaas.config: "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;"
          kafka.sasl.client.callback.handler.class: "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
          startingOffsets: "earliest"
          failOnDataLoss: "false"
      target: v_msk_orders_raw
      description: "Load orders from MSK using IAM authentication"

**With Specific IAM Role:**

.. code-block:: yaml

  options:
    kafka.security.protocol: "SASL_SSL"
    kafka.sasl.mechanism: "AWS_MSK_IAM"
    kafka.sasl.jaas.config: 'shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required awsRoleArn="${msk_role_arn}";'
    kafka.sasl.client.callback.handler.class: "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"

**Generated PySpark Code:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view()
  def v_msk_orders_raw():
      """Load orders from MSK using IAM authentication"""
      df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "b-1.msk-cluster.abc123.kafka.us-east-1.amazonaws.com:9098") \
          .option("subscribe", "orders") \
          .option("kafka.security.protocol", "SASL_SSL") \
          .option("kafka.sasl.mechanism", "AWS_MSK_IAM") \
          .option("kafka.sasl.jaas.config", "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;") \
          .option("kafka.sasl.client.callback.handler.class", "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler") \
          .option("startingOffsets", "earliest") \
          .option("failOnDataLoss", "false") \
          .load()

      return df

.. seealso::
  For complete MSK IAM documentation see `AWS MSK IAM Access Control <https://docs.aws.amazon.com/msk/latest/developerguide/iam-access-control.html>`_.

**MSK IAM Requirements:**

- Use port 9098 for IAM authentication (not the standard 9092).
- Provide all four required options: ``kafka.security.protocol``, ``kafka.sasl.mechanism``, ``kafka.sasl.jaas.config``, and ``kafka.sasl.client.callback.handler.class``.
- Grant the IAM role appropriate ``kafka-cluster:*`` permissions.
- Rely on IAM for authentication — no credentials are stored.
- Ensure your Databricks cluster has network access to the MSK cluster.

**Advanced Authentication: Azure Event Hubs OAuth**

Azure Event Hubs provides Kafka protocol support with OAuth 2.0 authentication using Azure Active Directory.

**Prerequisites:**

1. Azure Event Hubs namespace (Premium or Standard tier)
2. Azure AD App Registration (Service Principal) with appropriate permissions
3. Service Principal granted "Azure Event Hubs Data Receiver" role on the namespace
4. Databricks secrets configured for client credentials

**YAML Configuration:**

.. code-block:: yaml

  actions:
    - name: load_event_hubs_data
      type: load
      readMode: stream
      source:
        type: kafka
        bootstrap_servers: "my-namespace.servicebus.windows.net:9093"
        subscribe: "my-event-hub"
        options:
          kafka.security.protocol: "SASL_SSL"
          kafka.sasl.mechanism: "OAUTHBEARER"
          kafka.sasl.jaas.config: 'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="${secret:azure_secrets/client_id}" clientSecret="${secret:azure_secrets/client_secret}" scope="https://${event_hubs_namespace}/.default" ssl.protocol="SSL";'
          kafka.sasl.oauthbearer.token.endpoint.url: "https://login.microsoft.com/${azure_tenant_id}/oauth2/v2.0/token"
          kafka.sasl.login.callback.handler.class: "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
          startingOffsets: "earliest"
      target: v_event_hubs_data_raw
      description: "Load data from Azure Event Hubs using OAuth"

**Generated PySpark Code:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.temporary_view()
  def v_event_hubs_data_raw():
      """Load data from Azure Event Hubs using OAuth"""
      df = spark.readStream \
          .format("kafka") \
          .option("kafka.bootstrap.servers", "my-namespace.servicebus.windows.net:9093") \
          .option("subscribe", "my-event-hub") \
          .option("kafka.security.protocol", "SASL_SSL") \
          .option("kafka.sasl.mechanism", "OAUTHBEARER") \
          .option("kafka.sasl.jaas.config",
                  f'kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required clientId="{dbutils.secrets.get(scope="azure-secrets", key="client_id")}" clientSecret="{dbutils.secrets.get(scope="azure-secrets", key="client_secret")}" scope="https://my-namespace.servicebus.windows.net/.default" ssl.protocol="SSL";') \
          .option("kafka.sasl.oauthbearer.token.endpoint.url", "https://login.microsoft.com/12345678-1234-1234-1234-123456789012/oauth2/v2.0/token") \
          .option("kafka.sasl.login.callback.handler.class", "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler") \
          .option("startingOffsets", "earliest") \
          .load()

      return df

.. seealso::
  For complete Event Hubs Kafka documentation see `Azure Event Hubs for Apache Kafka <https://learn.microsoft.com/en-us/azure/event-hubs/event-hubs-for-kafka-ecosystem-overview>`_.

**Event Hubs OAuth Requirements:**

- Always use port 9093 for the Kafka protocol with Event Hubs.
- Specify the Event Hubs namespace in the format ``<namespace>.servicebus.windows.net``.
- Match the scope in JAAS config to ``https://<namespace>.servicebus.windows.net/.default``.
- Provide all five required options: ``kafka.security.protocol``, ``kafka.sasl.mechanism``, ``kafka.sasl.jaas.config``, ``kafka.sasl.oauthbearer.token.endpoint.url``, and ``kafka.sasl.login.callback.handler.class``.
- Assign the Service Principal the "Azure Event Hubs Data Receiver" role.
- Rely on the callback handler to refresh OAuth tokens automatically.
- Always use secrets for client credentials — never hardcode them in YAML.

custom_datasource
~~~~~~~~~~~~~~~~~

Use when you have or want a PySpark ``DataSource`` implementation and want LHP
to register and invoke it. Custom data source load actions use PySpark's
DataSource API to implement specialised data ingestion from APIs, custom
protocols, or any external system that requires custom logic.

**YAML Configuration:**

.. code-block:: yaml

  actions:
    - name: load_currency_exchange
      type: load
      readMode: stream
      operational_metadata: ["_processing_timestamp"]
      source:
        type: custom_datasource
        module_path: "data_sources/currency_api_source.py"
        custom_datasource_class: "CurrencyAPIStreamingDataSource"
        options:
          apiKey: "${secret:apis/currency_key}"
          baseCurrencies: "USD,EUR,GBP"
          progressPath: "/Volumes/catalog/schema/checkpoints/"
          minCallIntervalSeconds: "300"
          workspaceUrl: "adb-XYZ.azuredatabricks.net"
      target: v_currency_bronze
      description: "Load live currency exchange rates from external API"

**Custom DataSource Implementation (data_sources/currency_api_source.py):**

.. code-block:: python
  :linenos:

  from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
  from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, BooleanType
  from typing import Iterator, Tuple
  import requests
  import time
  import json

  class CurrencyInputPartition(InputPartition):
      """Input partition for currency API data source"""
      def __init__(self, start_time, end_time):
          self.start_time = start_time
          self.end_time = end_time

  class CurrencyAPIStreamingDataSource(DataSource):
      """
      Custom data source for live currency exchange rates.
      Fetches data from external API with rate limiting and progress tracking.
      """

      @classmethod
      def name(cls):
          return "currency_api_stream"

      def schema(self):
          return """
              base_currency string,
              target_currency string,
              exchange_rate double,
              api_timestamp timestamp,
              fetch_timestamp timestamp,
              rate_change_1h double,
              is_crypto boolean,
              data_source string,
              pipeline_run_id string
          """

      def streamReader(self, schema: StructType):
          return CurrencyAPIStreamingReader(schema, self.options)

  class CurrencyAPIStreamingReader(DataSourceStreamReader):
      """Streaming reader implementation with API calls and progress tracking"""

      def __init__(self, schema, options):
          self.schema = schema
          self.options = options
          self.api_key = options.get("apiKey")
          self.base_currencies = options.get("baseCurrencies", "USD").split(",")
          self.progress_path = options.get("progressPath")
          self.min_interval = int(options.get("minCallIntervalSeconds", "300"))

      def initialOffset(self) -> dict:
          return {"fetch_time": int(time.time() * 1000)}

      def latestOffset(self) -> dict:
          return {"fetch_time": int(time.time() * 1000)}

      def partitions(self, start: dict, end: dict):
          return [CurrencyInputPartition(start.get("fetch_time", 0), end.get("fetch_time", 0))]

      def read(self, partition) -> Iterator[Tuple]:
          """Fetch data from external API and yield as tuples"""
          # API call logic here
          for base_currency in self.base_currencies:
              # Make API calls and yield data
              yield (base_currency, "USD", 1.0, time.time(), time.time(), 0.0, False, "API", "run_1")

**Anatomy of a custom data source load action**

- **name**: Unique name for this action within the FlowGroup
- **type**: Action type - brings data into a temporary view
- **readMode**: Either *batch* or *stream* - determines if custom DataSource uses batch or stream reader
- **operational_metadata**: Add custom metadata columns (e.g., processing timestamp)
- **source**: Custom data source configuration
      - **type**: Use custom_datasource as source type
      - **module_path**: Path to Python file containing the custom DataSource implementation
      - **custom_datasource_class**: Name of the DataSource class to register and use
      - **options**: Dictionary of parameters passed to the DataSource (available via self.options)
- **target**: Name of the temporary view created
- **description**: Optional documentation for the action

.. seealso::
  - For PySpark DataSource API see the `PySpark DataSource documentation <https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.datasource.DataSource.html>`_.
  - Custom integrations: :doc:`../architecture`

Custom DataSources require implementing the DataSource interface with appropriate reader methods.
The framework copies your file to a ``custom_python_functions/`` subdirectory next to the generated
pipeline file and imports the class by name — the user file is not inlined into the pipeline.
Use the ``options`` dictionary to pass configuration parameters from YAML to your DataSource.

**File Substitution Support**

Custom DataSource Python files support substitution variables:

- **Environment tokens**: ``${catalog}``, ``${api_endpoint}``, ``${environment}``
- **Secret references**: ``${secret:scope/key}`` for API keys and credentials

Substitutions are applied to the file's contents as it is copied to ``custom_python_functions/``.

**Key Implementation Requirements:**

- Your DataSource class must implement the ``name()`` class method returning the format name used in ``.format()``
- The framework uses the return value of ``name()`` method, not the class name, for the format string
- The class is imported from the copied module; the registration call (``spark.dataSource.register``)
  runs at module load before the pipeline body
- PySpark's *vendored* cloudpickle is registered (``register_pickle_by_value``) so the class survives
  serialization to executors

**File Organization**: The ``module_path`` is relative to the project root.
Common practice is to create a ``data_sources/`` folder at the project root.

**Schema Definition**: Define your schema in the ``schema()`` method using DDL string format as shown in the example.
This schema should match the data structure returned by your ``read()`` method.

**Import Management**: The framework automatically handles import deduplication and conflict resolution.
If your custom source uses wildcard imports (e.g., ``from pyspark.sql.functions import *``),
they will take precedence over alias imports, and operational metadata expressions will adapt accordingly.

**The above YAML translates to the following PySpark code**

The user's ``data_sources/currency_api_source.py`` is copied verbatim into a
``custom_python_functions/`` subdirectory beside the generated pipeline file.
The pipeline file imports the class by name and registers it on the local
Spark session at module load:

.. code-block:: python
  :linenos:

  # Generated by LakehousePlumber
  # Pipeline: unirate_api_ingestion
  # FlowGroup: api_unirate_ingestion_bronze

  from pyspark import cloudpickle as _lhp_cloudpickle
  from pyspark.sql import functions as F
  from pyspark import pipelines as dp
  from custom_python_functions.currency_api_source import CurrencyAPIStreamingDataSource
  import custom_python_functions

  _lhp_cloudpickle.register_pickle_by_value(custom_python_functions)

  # Pipeline Configuration
  PIPELINE_ID = "unirate_api_ingestion"
  FLOWGROUP_ID = "api_unirate_ingestion_bronze"

  # ============================================================================
  # SOURCE VIEWS
  # ============================================================================

  # Try to register the custom data source
  try:
      spark.dataSource.register(CurrencyAPIStreamingDataSource)
  except Exception:
      pass  # Ignore if already registered

  @dp.temporary_view()
  def v_currency_bronze():
      """Load live currency exchange rates from external API"""
      df = spark.readStream \
          .format("currency_api_stream") \
          .option("apiKey", dbutils.secrets.get(scope='apis', key='currency_key')) \
          .option("baseCurrencies", "USD,EUR,GBP") \
          .option("progressPath", "/Volumes/catalog/schema/checkpoints/") \
          .option("minCallIntervalSeconds", "300") \
          .option("workspaceUrl", "adb-XYZ.azuredatabricks.net") \
          .load()

      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', F.current_timestamp())

      return df

The ``register_pickle_by_value(custom_python_functions)`` line registers the
package with PySpark's *vendored* cloudpickle so the ``DataSource`` class
survives serialization when SDP ships it to the executors. (The system
``cloudpickle`` is silently inert here because PySpark uses its own bundled
copy — this is the one-line fix that makes the copy-and-import pattern work
across local + executor boundaries.)

See also
--------

* :doc:`../decisions` — load source decision matrix, streaming-vs-batch matrix.
* :doc:`../ingest_with_autoloader` — end-to-end ``cloudfiles`` how-to.
* :doc:`../operational_metadata` — audit column catalog.
* :doc:`../substitutions` — ``${token}`` and ``${secret:scope/key}`` syntax.
