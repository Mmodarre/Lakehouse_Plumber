Actions Reference
=================

Actions are the building block of Lakehouse Plumber flowgroups.

.. contents:: Table of Contents
   :depth: 2
   :local:


Actions Overview
----------------

Actions come in four top-level types:

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
|| **Test**      || Validate data quality using DLT expectations in         |
||               || temporary tables (uniqueness, referential integrity…).  |
+----------------+----------------------------------------------------------+





Load Actions
------------

.. note::
  - At this time the framework supports the following load sub-types.
  - Coming soon: It will support more sources and target types through **plugins**.

+----------------------------+------------------------------------------------------------+
| Sub-type                   | Purpose & Source                                           |
+============================+============================================================+
|| cloudfiles                || Databricks *Auto Loader* (CloudFiles) – stream files from |
||                           || object storage (CSV, JSON, Parquet, etc.).                |
+----------------------------+------------------------------------------------------------+
|| delta                     || Read from an existing Delta table or Change Data Feed     |
||                           || (CDC).                                                    |
+----------------------------+------------------------------------------------------------+
|| sql                       || Execute an arbitrary SQL query and load the result as a   |
||                           || view.                                                     |
+----------------------------+------------------------------------------------------------+
|| jdbc                      || Ingest from an external RDBMS via JDBC with secret        |
||                           || handling.                                                 |
+----------------------------+------------------------------------------------------------+
|| python                    || Call custom Python code (path or inline), returning a     |
||                           || DataFrame.                                                |
+----------------------------+------------------------------------------------------------+
|| custom_datasource(PySpark)|| Configured under ``source`` block with automatic import   |
||                           || management and registration.                              |
+----------------------------+------------------------------------------------------------+

cloudFiles
~~~~~~~~~~
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

  from pyspark import pipelines as dp
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

delta
~~~~~~
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

  from pyspark import pipelines as dp
  from pyspark.sql.functions import current_timestamp

  @dp.temporary_view()
  def v_customer_raw():
      """Load customer table from raw schema"""
      df = spark.readStream.table("acmi_edw_dev.edw_raw.customer")
      
      # Add operational metadata columns
      df = df.withColumn('_processing_timestamp', current_timestamp())
      
      return df

kafka
~~~~~
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
  - Operational metadata: :doc:`concepts`

.. Important::
  Kafka always returns a fixed 7-column schema with binary key/value columns:
  ``key``, ``value``, ``topic``, ``partition``, ``offset``, ``timestamp``, ``timestampType``.
  You must explicitly deserialize the key and value columns using transform actions.

.. Warning::
  **Subscription Methods**: You must specify exactly ONE of:
  
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

.. Important::
  **MSK IAM Requirements:**
  
  - Port 9098 is used for IAM authentication (not the standard 9092)
  - All three options are required: ``kafka.security.protocol``, ``kafka.sasl.mechanism``, ``kafka.sasl.jaas.config``, ``kafka.sasl.client.callback.handler.class``
  - IAM role must have appropriate kafka-cluster:* permissions
  - No credentials are stored - authentication is via IAM
  - Ensure Databricks cluster has network access to MSK cluster

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

.. Important::
  **Event Hubs OAuth Requirements:**
  
  - Port 9093 is always used for Kafka protocol with Event Hubs
  - Event Hubs namespace must be in format: ``<namespace>.servicebus.windows.net``
  - The scope in JAAS config must match: ``https://<namespace>.servicebus.windows.net/.default``
  - All four options are required: ``kafka.security.protocol``, ``kafka.sasl.mechanism``, ``kafka.sasl.jaas.config``, ``kafka.sasl.oauthbearer.token.endpoint.url``, ``kafka.sasl.login.callback.handler.class``
  - Service Principal needs "Azure Event Hubs Data Receiver" role assignment
  - OAuth token refresh is handled automatically by the callback handler
  - Always use secrets for client credentials - never hardcode in YAML

sql
~~~
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

.. note:: **File Substitution Support**
   
   Substitution variables work in both inline SQL and external SQL files (``sql_path``). 
   The same ``{token}`` and ``${secret:scope/key}`` syntax from YAML works in ``.sql`` files.
   Files are processed for substitutions before query execution.
  
.. note::
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
          FROM {catalog}.{silver_schema}.customer_analytics
          WHERE last_order_date >= current_date() - INTERVAL 90 DAYS
      """)

jdbc
~~~~
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

PySpark Custom DataSource
~~~~~~~~~~~~~~~~~~~~~~~~~
Custom data source load actions use PySpark's DataSource API to implement specialized data ingestion from APIs, custom protocols, or any external system that requires custom logic. This allows for highly flexible data ingestion patterns.

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
  - Custom integrations: :doc:`concepts`

.. Important::
  Custom DataSources require implementing the DataSource interface with appropriate reader methods.
  The framework automatically registers your DataSource and copies the implementation to the generated pipeline.
  Use options dictionary to pass configuration parameters from YAML to your DataSource.

.. note:: **File Substitution Support**
   
   Custom DataSource Python files support substitution variables:
   
   - **Environment tokens**: ``{catalog}``, ``{api_endpoint}``, ``{environment}``
   - **Secret references**: ``${secret:scope/key}`` for API keys and credentials
   
   Substitutions are applied before the class is embedded in the generated code.

  **Key Implementation Requirements:**
  - Your DataSource class must implement the ``name()`` class method returning the format name used in ``.format()``
  - The framework uses the return value of ``name()`` method, not the class name, for the format string
  - The custom source code is placed *before* the registration call to ensure proper class definition order
  - Import management is handled automatically to resolve conflicts between source file imports and generated code

.. note::
  **File Organization**: The ``module_path`` is relative to your YAML file location.
  Common practice is to create a ``data_sources/`` folder alongside your pipeline YAML files.
  
  **Schema Definition**: Define your schema in the ``schema()`` method using DDL string format as shown in the example.
  This schema should match the data structure returned by your ``read()`` method.

  **Import Management**: The framework automatically handles import deduplication and conflict resolution.
  If your custom source uses wildcard imports (e.g., ``from pyspark.sql.functions import *``), 
  they will take precedence over alias imports, and operational metadata expressions will adapt accordingly.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  # Generated by LakehousePlumber
  # Pipeline: unirate_api_ingestion
  # FlowGroup: api_unirate_ingestion_bronze

  from pyspark.sql.datasource import DataSource, DataSourceStreamReader, InputPartition
  from pyspark.sql.functions import *
  from pyspark.sql.types import *
  from typing import Iterator, Tuple
  from pyspark import pipelines as dp
  import json
  import os
  import requests
  import time

  # Pipeline Configuration
  PIPELINE_ID = "unirate_api_ingestion"
  FLOWGROUP_ID = "api_unirate_ingestion_bronze"

  # ============================================================================
  # CUSTOM DATA SOURCE IMPLEMENTATIONS
  # ============================================================================
  # The following code was automatically copied from: data_sources/currency_api_source.py
  # Used by action: load_currency_exchange

  class CurrencyInputPartition(InputPartition):
      """Input partition for currency API data source"""
      def __init__(self, start_time, end_time):
          self.start_time = start_time
          self.end_time = end_time

  class CurrencyAPIStreamingDataSource(DataSource):
      """
      Real currency exchange data source powered by UniRateAPI.
      Fetches live exchange rates on each triggered pipeline run.
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

  # ... rest of custom data source implementation ...

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
      df = df.withColumn('_processing_timestamp', current_timestamp())

      return df

Transform Actions
------------------

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
~~~
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
~~~~~~
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
  - Custom functions: :doc:`concepts`

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
~~~~~~~~~~~~
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
~~~~~~
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

  from pyspark import pipelines as dp
  from pyspark.sql import functions as F
  from pyspark.sql.types import StructType

  @dp.temporary_view()
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
~~~~~~~~~~~~~~~~
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
  - For DLT table types see the `Databricks DLT table types documentation <https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-table>`_.
  - Intermediate processing: :doc:`concepts`

.. Important::
  Temp tables are automatically cleaned up when the pipeline completes.
  Use for complex multi-step transformations where intermediate materialization improves performance.
  
  For instance, if you have a complex transformation that will be used by several downstream actions,
  you can create a temporary table to prevent the transformation from being recomputed each time.

**The above YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  @dp.materialized_view(
      temporary=True,
  )
  def customer_intermediate():
      """Create temporary table for customer intermediate processing"""
      df = spark.readStream.table("v_customer_processed")
      
      return df

Write Actions
--------------

+-------------------+--------------------------------------------------------------------------+
| Sub-type          | Purpose                                                                  |
+===================+==========================================================================+
|| streaming_table  || Create or append to a Delta streaming table in Unity Catalog.           |
||                  || Supports Change Data Feed (CDF), CDC modes, and append flows.           |
+-------------------+--------------------------------------------------------------------------+
|| materialized_view|| Create a Lakeflow *materialized view* for batch-computed analytics.     |
+-------------------+--------------------------------------------------------------------------+

streaming_table
~~~~~~~~~~~~~~~
Streaming table write actions create or append to Delta streaming tables. They support three modes: **standard** (append flows), **cdc** (change data capture), and **snapshot_cdc** (snapshot-based CDC).

Append Streaming Table Write
++++++++++++++++++++++++++++

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

  from pyspark import pipelines as dp

  # Create the streaming table
  dp.create_streaming_table(
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
  @dp.append_flow(
      target="catalog.bronze.customer",
      name="f_customer_bronze",
      comment="Append flow to catalog.bronze.customer from v_customer_cleansed"
  )
  def f_customer_bronze():
      """Append flow to catalog.bronze.customer from v_customer_cleansed"""
      # Streaming flow
      df = spark.readStream.table("v_customer_cleansed")
      return df

CDC Mode
++++++++


**Incremental CDC**

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
          track_history_column_list: ["name", "address", "phone"]
          ignore_null_updates: true
      description: "Track customer changes with CDC and SCD Type 2"

**The CDC YAML translates to the following PySpark code**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  # Create the streaming table for CDC
  dp.create_streaming_table(
      name="catalog.silver.dim_customer",
      comment="Track customer changes with CDC and SCD Type 2",
      table_properties={
          "delta.enableChangeDataFeed": "true",
          "quality": "silver"
      },
      row_filter="ROW FILTER catalog.schema.customer_region_filter ON (region)"
  )

  # CDC mode using auto_cdc
  dp.create_auto_cdc_flow(
      target="catalog.silver.dim_customer",
      source="v_customer_changes",
      keys=["customer_id"],
      sequence_by="_commit_timestamp",
      stored_as_scd_type=2,
      track_history_column_list=["name", "address", "phone"],
      ignore_null_updates=True
  )

.. seealso::
  - For more information on ``create_auto_cdc_flow`` see the `Databricks CDC documentation <https://docs.databricks.com/aws/en/ldp/developer/ldp-python-ref-apply-changes-from-snapshot>`_

**Snapshot CDC**

Snapshot CDC mode creates CDC flows from full snapshots of data using DLT's `create_auto_cdc_from_snapshot_flow()`. It supports two source approaches: direct table references or custom Python functions.

.. note::
  **Recent Improvements**: Snapshot CDC actions using ``source_function`` are now **self-contained** and automatically handle:
  
  - **Dependency Management**: No false dependency errors when using ``source_function``
  - **FlowGroup Validation**: Exempt from "must have at least one Load action" requirement
  - **Source Field Handling**: Action-level ``source`` field is redundant and should be omitted

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

**Option 2: Function Source with SCD Type 2 (Self-Contained)**

.. code-block:: yaml

  actions:
    - name: write_part_silver_snapshot
      type: write
      write_target:
        type: streaming_table
        database: "{catalog}.{silver_schema}"
        table: "part_dim"
        mode: "snapshot_cdc"
        snapshot_cdc_config:
          source_function:
            file: "py_functions/part_snapshot_func.py"
            function: "next_snapshot_and_version"
          keys: ["part_id"]
          stored_as_scd_type: 2
          track_history_except_column_list: ["_source_file_path", "_processing_timestamp"]
      description: "Create part dimension with function-based snapshots"

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

.. Important::
  **Source Configuration for snapshot CDC**: 

  - **With source_function**: The action becomes **self-contained** and does not require external dependencies. 
    Any ``source`` field at the action level is **redundant** and should be omitted.
  - **With source table**: The action depends on the specified source table and requires proper dependency management.
  
  **FlowGroup Requirements**: Self-contained snapshot CDC actions (using ``source_function``) are exempt from the 
  "FlowGroup must have at least one Load action" requirement, as they provide their own data source.

**Example Python Function for source_function**

Create file `py_functions/part_snapshot_func.py`:

.. code-block:: python
  :linenos:

  from typing import Optional, Tuple
  from pyspark.sql import DataFrame

  def next_snapshot_and_version(latest_snapshot_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
      """
      Snapshot processing function for part dimension data.
      
      Args:
          latest_snapshot_version: Most recent snapshot version processed, or None for first run
          
      Returns:
          Tuple of (DataFrame, snapshot_version) or None if no more snapshots available
      """
      if latest_snapshot_version is None:
          # First run - load initial snapshot
          df = spark.sql("""
              SELECT * FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id = (SELECT min(snapshot_id) FROM acme_edw_dev.edw_bronze.part)
          """)
          
          min_snapshot_id = spark.sql("""
              SELECT min(snapshot_id) as min_id FROM acme_edw_dev.edw_bronze.part
          """).collect()[0].min_id
          
          return (df, min_snapshot_id)
      
      else:
          # Subsequent runs - check for new snapshots
          next_snapshot_result = spark.sql(f"""
              SELECT min(snapshot_id) as next_id 
              FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id > '{latest_snapshot_version}'
          """).collect()[0]
          
          if next_snapshot_result.next_id is None:
              return None  # No more snapshots available
          
          next_snapshot_id = next_snapshot_result.next_id
          df = spark.sql(f"""
              SELECT * FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id = '{next_snapshot_id}'
          """)
          
          return (df, next_snapshot_id)
.. seealso::
  - For more information on ``create_auto_cdc_from_snapshot_flow`` see the `Databricks snapshot CDC documentation <https://docs.databricks.com/en/delta-live-tables/python-ref.html#create_auto_cdc_from_snapshot_flow>`_

**The above YAML examples translate to the following PySpark code**

**For table source (Option 1):**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  # Create the streaming table for snapshot CDC
  dp.create_streaming_table(
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
  dp.create_auto_cdc_from_snapshot_flow(
      target="catalog.silver.dim_customer_simple",
      source="catalog.bronze.customer_snapshots",
      keys=["customer_id"],
      stored_as_scd_type=1
  )

**For function source (Option 2):**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  from typing import Optional, Tuple
  from pyspark.sql import DataFrame

  # Snapshot function embedded directly in generated code
  def next_snapshot_and_version(latest_snapshot_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
      """
      Snapshot processing function for part dimension data.
      
      Args:
          latest_snapshot_version: Most recent snapshot version processed, or None for first run
          
      Returns:
          Tuple of (DataFrame, snapshot_version) or None if no more snapshots available
      """
      if latest_snapshot_version is None:
          # First run - load initial snapshot
          df = spark.sql("""
              SELECT * FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id = (SELECT min(snapshot_id) FROM acme_edw_dev.edw_bronze.part)
          """)
          
          min_snapshot_id = spark.sql("""
              SELECT min(snapshot_id) as min_id FROM acme_edw_dev.edw_bronze.part
          """).collect()[0].min_id
          
          return (df, min_snapshot_id)
      
      else:
          # Subsequent runs - check for new snapshots
          next_snapshot_result = spark.sql(f"""
              SELECT min(snapshot_id) as next_id 
              FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id > '{latest_snapshot_version}'
          """).collect()[0]
          
          if next_snapshot_result.next_id is None:
              return None  # No more snapshots available
          
          next_snapshot_id = next_snapshot_result.next_id
          df = spark.sql(f"""
              SELECT * FROM acme_edw_dev.edw_bronze.part 
              WHERE snapshot_id = '{next_snapshot_id}'
          """)
          
          return (df, next_snapshot_id)

  # Create the streaming table for snapshot CDC
  dp.create_streaming_table(
      name="catalog.silver.part_dim",
      comment="Create part dimension with function-based snapshots"
  )

  # Snapshot CDC mode using create_auto_cdc_from_snapshot_flow
  dp.create_auto_cdc_from_snapshot_flow(
      target="catalog.silver.part_dim",
      source=next_snapshot_and_version,
      keys=["part_id"],
      stored_as_scd_type=2,
      track_history_except_column_list=["_source_file_path", "_processing_timestamp"]
  )

**For exclude columns (Option 3):**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp

  # Create the streaming table for snapshot CDC
  dp.create_streaming_table(
      name="catalog.silver.dim_product",
      comment="Product dimension excluding audit columns from history"
  )

  # Snapshot CDC mode using create_auto_cdc_from_snapshot_flow
  dp.create_auto_cdc_from_snapshot_flow(
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
  - **Substitution support**: Python functions support ``{token}`` and ``${secret:scope/key}`` substitutions
  
  **⚠️ Source Field Redundancy**: When using ``source_function`` in snapshot CDC configuration, do NOT include a ``source`` field at the action level. The ``source`` field becomes redundant and may cause false dependency errors. The ``source_function`` provides the data source internally.

  **✅ Correct pattern (self-contained)**:
  
  .. code-block:: yaml
  
    - name: write_part_silver_snapshot
      type: write
      # No source field needed
      write_target:
        mode: "snapshot_cdc" 
        snapshot_cdc_config:
          source_function: # This provides the data
            file: "py_functions/part_snapshot_func.py"
            function: "next_snapshot_and_version"
  
  **❌ Incorrect pattern (redundant source)**:
  
  .. code-block:: yaml
  
    - name: write_part_silver_snapshot
      type: write
      source: v_part_bronze_snapshot  # ← REDUNDANT, causes false dependencies
      write_target:
        mode: "snapshot_cdc"
        snapshot_cdc_config:
          source_function:
            file: "py_functions/part_snapshot_func.py"
            function: "next_snapshot_and_version"

materialized_view
~~~~~~~~~~~~~~~~~
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

  from pyspark import pipelines as dp

  @dp.materialized_view(
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

  from pyspark import pipelines as dp

  @dp.materialized_view(
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

sink
~~~~
Sink write actions enable streaming data to external destinations beyond traditional 
DLT-managed streaming tables. Sinks provide flexible output for real-time data 
distribution to external systems, Unity Catalog external tables, and event streaming 
services.

**Supported Sink Types:**

+---------------+------------------------------------------------------------+
| Sink Type     | Purpose                                                    |
+===============+============================================================+
|| delta        || Write to Unity Catalog external tables or external Delta |
+---------------+------------------------------------------------------------+
|| kafka        || Stream to Apache Kafka or Azure Event Hubs topics        |
+---------------+------------------------------------------------------------+
|| custom       || Write to custom destinations via Python DataSink class   |
+---------------+------------------------------------------------------------+

**When to Use Sinks:**

* **Operational use cases** - Fraud detection, real-time analytics, customer recommendations with low-latency requirements
* **External Delta tables** - Write to Unity Catalog external tables or Delta instances managed outside DLT
* **Reverse ETL** - Export processed data to external systems like Kafka for downstream consumption
* **Custom formats** - Use Python custom data sources to write to any destination not directly supported by Databricks

Delta Sink
++++++++++

Write processed data to Delta tables in external Unity Catalog locations or shared 
analytics databases managed outside of DLT pipelines.

**Use Cases:**
- Export aggregated metrics to shared analytics catalog
- Sync data to external reporting systems
- Write to tables managed outside DLT pipelines

.. code-block:: yaml

  # Example: Export to external analytics catalog
  actions:
    # Read processed silver data
    - name: load_silver_sales_metrics
      type: load
      source:
        type: delta
        table: acme_catalog.silver.fact_sales_order_line
      target: v_sales_metrics
      readMode: stream

    # Aggregate for external reporting
    - name: aggregate_daily_sales
      type: transform
      transform_type: sql
      source: v_sales_metrics
      target: v_daily_sales_summary
      sql: |
        SELECT 
          DATE(order_date) as sales_date,
          store_id,
          product_id,
          SUM(quantity) as total_quantity,
          SUM(net_amount) as total_revenue,
          COUNT(DISTINCT order_id) as order_count,
          AVG(unit_price) as avg_unit_price,
          current_timestamp() as export_timestamp
        FROM v_sales_metrics
        GROUP BY DATE(order_date), store_id, product_id

    # Write to external Delta sink
    - name: export_to_analytics_catalog
      type: write
      source: v_daily_sales_summary
      write_target:
        type: sink
        sink_type: delta
        sink_name: analytics_catalog_export
        comment: "Export daily sales summary to shared analytics catalog"
        options:
          tableName: "analytics_shared_catalog.reporting.daily_sales_summary"
          checkpointLocation: "/tmp/checkpoints/analytics_export"
          mergeSchema: "true"
          optimizeWrite: "true"

**Anatomy of a Delta sink write action:**

- **write_target.type**: Must be ``sink``
- **write_target.sink_type**: Must be ``delta``
- **write_target.sink_name**: Unique identifier for this sink
- **write_target.comment**: Description of the sink's purpose
- **write_target.options**:
  
  - **tableName**: Fully qualified table name (``catalog.schema.table``) - Required (use this OR path)
  - **path**: File system path (``/mnt/delta/table``) - Required (use this OR tableName)
  - Other options can be specified and will be passed to DLT (currently not all options are supported by DLT)

.. Important::
  Delta sinks require EITHER ``tableName`` OR ``path`` (not both).
  
  - Use ``tableName`` for Unity Catalog tables (``catalog.schema.table``) or Hive metastore (``schema.table``)
  - Use ``path`` for file-based Delta tables
  
  Additional options like ``checkpointLocation`` can be included in YAML for future compatibility, 
  but verify current DLT support before relying on them.

**The above YAML translates to the following PySpark code:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  
  # Create the Delta sink
  dp.create_sink(
      name="analytics_catalog_export",
      format="delta",
      options={
          "tableName": "analytics_shared_catalog.reporting.daily_sales_summary",
          "checkpointLocation": "/tmp/checkpoints/analytics_export",
          "mergeSchema": "true",
          "optimizeWrite": "true"
      }
  )
  
  # Write to the sink using append flow
  @dp.append_flow(
      name="export_to_analytics_catalog",
      target="analytics_catalog_export",
      comment="Export daily sales summary to shared analytics catalog"
  )
  def export_to_analytics_catalog():
      df = spark.readStream.table("v_daily_sales_summary")
      return df

**Path-based Delta Sink Example:**

.. code-block:: yaml

  # Example: Delta sink with path
  - name: export_to_delta_path
    type: write
    source: v_processed_data
    write_target:
      type: sink
      sink_type: delta
      sink_name: delta_path_export
      comment: "Export to file-based Delta table"
      options:
        path: "/mnt/delta_exports/my_table"

Kafka Sink
++++++++++

Stream data to Apache Kafka topics for real-time consumption by downstream applications,
microservices, or event-driven architectures.

**Use Cases:**
- Stream events to microservices
- Feed real-time dashboards and monitoring systems
- Integrate with event-driven architectures

.. Important::
  **Kafka sinks require explicit ``key`` and ``value`` columns.** You must create these 
  columns in a transform action before writing to Kafka. The ``value`` column is mandatory, 
  while ``key``, ``partition``, and ``headers`` are optional.

.. code-block:: yaml

  # Example: Stream order events to Kafka
  actions:
    # Load order data
    - name: load_order_fulfillment_data
      type: load
      source:
        type: delta
        table: acme_catalog.silver.fact_sales_order_header
      target: v_order_data
      readMode: stream

    # Transform to Kafka format with key/value columns
    - name: prepare_kafka_message
      type: transform
      transform_type: sql
      source: v_order_data
      target: v_kafka_ready
      sql: |
        SELECT 
          -- Kafka key: use order_id for partitioning
          CAST(order_id AS STRING) as key,
          
          -- Kafka value: JSON structure with order details
          to_json(struct(
            order_id,
            customer_id,
            store_id,
            order_date,
            order_status,
            total_amount,
            current_timestamp() as event_timestamp,
            'order_fulfillment' as event_type
          )) as value,
          
          -- Optional: Kafka headers (as map)
          map(
            'source', 'acme_lakehouse',
            'event_version', '1.0'
          ) as headers
        FROM v_order_data
        WHERE order_status IN ('PENDING', 'PROCESSING', 'SHIPPED')

    # Write to Kafka sink
    - name: stream_to_kafka
      type: write
      source: v_kafka_ready
      write_target:
        type: sink
        sink_type: kafka
        sink_name: order_events_kafka
        bootstrap_servers: "${KAFKA_BOOTSTRAP_SERVERS}"
        topic: "acme.orders.fulfillment"
        comment: "Stream order fulfillment events to Kafka"
        options:
          # Security settings
          kafka.security.protocol: "${KAFKA_SECURITY_PROTOCOL}"
          kafka.sasl.mechanism: "${KAFKA_SASL_MECHANISM}"
          kafka.sasl.jaas.config: "${KAFKA_JAAS_CONFIG}"
          
          # Performance tuning
          kafka.batch.size: "16384"
          kafka.compression.type: "snappy"
          kafka.acks: "1"
          
          # Checkpointing
          checkpointLocation: "/tmp/checkpoints/kafka_orders"

**Anatomy of a Kafka sink write action:**

- **write_target.type**: Must be ``sink``
- **write_target.sink_type**: Must be ``kafka``
- **write_target.sink_name**: Unique identifier for this sink
- **write_target.bootstrap_servers**: Kafka broker addresses (comma-separated)
- **write_target.topic**: Target Kafka topic name
- **write_target.comment**: Description of the sink's purpose
- **write_target.options**: Kafka producer settings

  - **kafka.security.protocol**: Security protocol (e.g., ``SASL_SSL``, ``PLAINTEXT``)
  - **kafka.sasl.mechanism**: SASL mechanism (e.g., ``PLAIN``, ``SCRAM-SHA-256``)
  - **kafka.sasl.jaas.config**: JAAS configuration for authentication
  - **kafka.batch.size**: Batch size in bytes for producer
  - **kafka.compression.type**: Compression type (``none``, ``gzip``, ``snappy``, ``lz4``)
  - **kafka.acks**: Acknowledgment mode (``0``, ``1``, ``all``)
  - **checkpointLocation**: Required checkpoint location for streaming

**Required Columns in Source Data:**

+------------+----------+------------------------------------------------------------+
| Column     | Type     | Purpose                                                    |
+============+==========+============================================================+
| value      | STRING   | Message payload (mandatory) - typically JSON               |
+------------+----------+------------------------------------------------------------+
| key        | STRING   | Message key for partitioning (optional)                    |
+------------+----------+------------------------------------------------------------+
| headers    | MAP      | Message headers as MAP<STRING,STRING> (optional)           |
+------------+----------+------------------------------------------------------------+
| partition  | INT      | Explicit partition assignment (optional)                   |
+------------+----------+------------------------------------------------------------+

**The above YAML translates to the following PySpark code:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  
  # Create the Kafka sink
  dp.create_sink(
      name="order_events_kafka",
      format="kafka",
      options={
          "kafka.bootstrap.servers": "kafka-broker.example.com:9092",
          "topic": "acme.orders.fulfillment",
          "kafka.security.protocol": "SASL_SSL",
          "kafka.sasl.mechanism": "PLAIN",
          "kafka.sasl.jaas.config": "...",
          "kafka.batch.size": "16384",
          "kafka.compression.type": "snappy",
          "kafka.acks": "1",
          "checkpointLocation": "/tmp/checkpoints/kafka_orders"
      }
  )
  
  # Write to the sink using append flow
  @dp.append_flow(
      name="stream_to_kafka",
      target="order_events_kafka",
      comment="Stream order fulfillment events to Kafka"
  )
  def stream_to_kafka():
      df = spark.readStream.table("v_kafka_ready")
      return df

Azure Event Hubs Sink
++++++++++++++++++++++

Azure Event Hubs is Kafka-compatible, so you use ``sink_type: kafka`` with OAuth 
configuration for authentication.

**Key Configuration:**

- Use ``kafka.sasl.mechanism: "OAUTHBEARER"`` for Event Hubs OAuth authentication
- Use Unity Catalog service credentials for secure authentication
- Bootstrap servers format: ``{namespace}.servicebus.windows.net:9093``

.. code-block:: yaml

  # Example: Stream to Azure Event Hubs
  actions:
    - name: prepare_event_hubs_message
      type: transform
      transform_type: sql
      source: v_alerts
      target: v_event_hubs_ready
      sql: |
        SELECT 
          CONCAT(store_id, '-', product_id) as key,
          to_json(struct(
            store_id,
            product_id,
            alert_type,
            alert_timestamp
          )) as value
        FROM v_alerts

    - name: stream_to_event_hubs
      type: write
      source: v_event_hubs_ready
      write_target:
        type: sink
        sink_type: kafka
        sink_name: alerts_eventhubs
        bootstrap_servers: "${EVENT_HUBS_NAMESPACE}:9093"
        topic: "${EVENT_HUBS_TOPIC}"
        options:
          # OAuth for Event Hubs
          kafka.sasl.mechanism: "OAUTHBEARER"
          kafka.security.protocol: "SASL_SSL"
          kafka.sasl.jaas.config: "${EVENT_HUBS_JAAS_CONFIG}"
          checkpointLocation: "/tmp/checkpoints/eventhubs_alerts"

For more details on Event Hubs authentication, see the 
`Databricks Event Hubs documentation <https://docs.databricks.com/structured-streaming/streaming-event-hubs.html>`_.

Custom Sink
+++++++++++

Implement custom Python DataSink classes to write data to any destination not directly 
supported by Databricks, including REST APIs, databases, file systems, or proprietary 
data stores.

**Use Cases:**
- Push updates to external REST APIs or webhooks
- Write to non-Spark data stores
- Implement custom retry/error handling logic
- Interface with proprietary systems

.. code-block:: yaml

  # Example: Push customer updates to external CRM API
  actions:
    # Prepare customer data for API
    - name: prepare_api_payload
      type: transform
      transform_type: sql
      source: v_customer_changes
      target: v_api_ready_customers
      sql: |
        SELECT 
          customer_id,
          first_name,
          last_name,
          email,
          phone,
          loyalty_tier,
          total_lifetime_value,
          customer_status,
          current_timestamp() as sync_timestamp
        FROM v_customer_changes
        WHERE customer_status = 'ACTIVE'
          AND email IS NOT NULL

    # Write to custom API sink
    - name: push_to_crm_api
      type: write
      source: v_api_ready_customers
      write_target:
        type: sink
        sink_type: custom
        sink_name: customer_crm_api
        module_path: "sinks/customer_api_sink.py"
        custom_sink_class: "CustomerAPIDataSource"
        comment: "Push customer updates to external CRM API"
        options:
          endpoint: "${CRM_API_ENDPOINT}"
          apiKey: "${CRM_API_KEY}"
          batchSize: "100"
          timeout: "30"
          maxRetries: "3"
          checkpointLocation: "/tmp/checkpoints/crm_api_sink"

**Anatomy of a custom sink write action:**

- **write_target.type**: Must be ``sink``
- **write_target.sink_type**: Must be ``custom``
- **write_target.sink_name**: Unique identifier for this sink
- **write_target.module_path**: Path to Python file containing the DataSink class
- **write_target.custom_sink_class**: Name of the DataSink class to use
- **write_target.comment**: Description of the sink's purpose
- **write_target.options**: Custom options passed to your sink implementation

**DataSink Interface Requirements:**

Your custom sink class must implement the PySpark DataSink interface:

.. code-block:: python

  # Example custom DataSink implementation
  from pyspark.sql.datasource import DataSink, DataSource, InputPartition
  import requests
  import json
  
  class CustomerAPIDataSource(DataSource):
      """Custom DataSource for streaming to external API."""
      
      @classmethod
      def name(cls):
          """Return the format name for this sink."""
          return "customer_api_sink"
      
      def writer(self, schema, overwrite):
          """Return a DataSink writer instance."""
          return CustomerAPIDataSink(self.options)
  
  
  class CustomerAPIDataSink(DataSink):
      """DataSink implementation for external API."""
      
      def __init__(self, options):
          self.endpoint = options.get("endpoint")
          self.api_key = options.get("apiKey")
          self.batch_size = int(options.get("batchSize", 100))
          self.timeout = int(options.get("timeout", 30))
          self.max_retries = int(options.get("maxRetries", 3))
      
      def write(self, iterator):
          """Write data to external API with batching and retry logic."""
          batch = []
          
          for row in iterator:
              batch.append(row.asDict())
              
              if len(batch) >= self.batch_size:
                  self._send_batch(batch)
                  batch = []
          
          # Send remaining records
          if batch:
              self._send_batch(batch)
      
      def _send_batch(self, batch):
          """Send batch to API with retry logic."""
          headers = {
              "Authorization": f"Bearer {self.api_key}",
              "Content-Type": "application/json"
          }
          
          for attempt in range(self.max_retries):
              try:
                  response = requests.post(
                      self.endpoint,
                      json=batch,
                      headers=headers,
                      timeout=self.timeout
                  )
                  response.raise_for_status()
                  return
              except Exception as e:
                  if attempt == self.max_retries - 1:
                      raise
                  # Exponential backoff
                  time.sleep(2 ** attempt)

**The above YAML translates to the following PySpark code:**

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  
  # Create the custom sink
  dp.create_sink(
      name="customer_crm_api",
      format="customer_api_sink",  # Uses the name() from DataSource
      options={
          "endpoint": "https://crm.example.com/api/customers",
          "apiKey": "secret-api-key",
          "batchSize": "100",
          "timeout": "30",
          "maxRetries": "3",
          "checkpointLocation": "/tmp/checkpoints/crm_api_sink"
      }
  )
  
  # Write to the sink using append flow
  @dp.append_flow(
      name="push_to_crm_api",
      target="customer_crm_api",
      comment="Push customer updates to external CRM API"
  )
  def push_to_crm_api():
      df = spark.readStream.table("v_api_ready_customers")
      return df

**Best Practices for Custom Sinks:**

* **Error Handling**: Implement comprehensive try/catch blocks and logging
* **Retry Logic**: Use exponential backoff for transient failures
* **Dead Letter Queue**: Write failed records to a DLQ for manual review
* **Batch Size Tuning**: Balance throughput vs API rate limits
* **Monitoring**: Log metrics for tracking success/failure rates
* **Authentication**: Use Unity Catalog secrets for API keys and credentials

For more details on implementing custom data sources, see the 
`PySpark Custom Data Sources documentation <https://spark.apache.org/docs/latest/api/python/user_guide/sql/python_data_source.html>`_.

Operational Metadata with Sinks
++++++++++++++++++++++++++++++++

Operational metadata columns can be added to your data before writing to sinks. 
Use a transform action to add metadata columns such as processing timestamps, 
source identifiers, or record hashes.

.. code-block:: yaml

  # Example: Adding operational metadata before sink
  actions:
    - name: add_metadata
      type: transform
      transform_type: sql
      source: v_source_data
      target: v_with_metadata
      sql: |
        SELECT 
          *,
          current_timestamp() as _processing_timestamp,
          'acme_lakehouse' as _source_system,
          md5(concat_ws('|', *)) as _record_hash
        FROM v_source_data
    
    - name: write_to_sink
      type: write
      source: v_with_metadata
      write_target:
        type: sink
        sink_type: kafka
        sink_name: enriched_data_kafka
        # ... sink configuration

Row-Level Security with row_filter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
  - For complete row filter documentation see the `Databricks Row Filters and Column Masks documentation <https://docs.databricks.com/aws/en/ldp/unity-catalog#publish-tables-with-row-filters-and-column-masks>`_.


Test Actions (Data Quality Unit Tests)
---------------------------------------

Test actions let you validate data pipelines using Databricks Lakeflow Declarative Pipelines expectations. They generate lightweight DLT temporary tables that read from existing tables/views and attach expectations that either fail the pipeline or warn on violations.

.. note::
   **CLI Flag Required**: Test actions are skipped by default during code generation for faster builds. Use the ``--include-tests`` flag to generate test code:
   
   .. code-block:: bash
   
      # Skip tests (default) - faster builds
      lhp generate -e dev
      
      # Include tests - for development and testing
      lhp generate -e dev --include-tests

Test Types Overview
~~~~~~~~~~~~~~~~~~~

Test actions come in the following types:

+--------------------------+-------------------------------------------------------------+
| Test Type                | Purpose                                                     |
+==========================+=============================================================+
|| row_count               || Compare row counts between two sources with tolerance.     |
+--------------------------+-------------------------------------------------------------+
|| uniqueness              || Validate unique constraints (with optional filter).        |
+--------------------------+-------------------------------------------------------------+
|| referential_integrity   || Check foreign-key relationships across tables.             |
+--------------------------+-------------------------------------------------------------+
|| completeness            || Ensure specific columns are not null.                      |
+--------------------------+-------------------------------------------------------------+
|| range                   || Validate a column is within min/max bounds.                |
+--------------------------+-------------------------------------------------------------+
|| schema_match            || Compare schemas between two tables via information_schema. |
+--------------------------+-------------------------------------------------------------+
|| all_lookups_found       || Validate dimension lookups succeed.                        |
+--------------------------+-------------------------------------------------------------+
|| custom_sql              || Provide your own SQL plus expectations.                    |
+--------------------------+-------------------------------------------------------------+
|| custom_expectations     || Provide expectations only on a source/table.               |
+--------------------------+-------------------------------------------------------------+

.. note::
   - Default target naming: ``tmp_test_<action_name>`` (temporary tables)
   - Default execution: batch (sufficient for aggregate checks); use streaming only when testing streaming sources explicitly
   - Expectation decorators use aggregated style: ``@dp.expect_all_or_fail``, ``@dp.expect_all`` (warn), ``@dp.expect_all_or_drop``
   - ``on_violation`` supports ``fail`` and ``warn``. Using ``drop`` is possible but generally discouraged for tests


Row Count
~~~~~~~~~

Compare record counts between two sources, with optional tolerance.

.. code-block:: yaml

  actions:
    - name: test_raw_to_bronze_count
      type: test
      test_type: row_count
      source: [raw.orders, bronze.orders]
      tolerance: 0
      on_violation: fail
      description: "Ensure no data loss from raw to bronze"

**Generated PySpark (excerpt):**

.. code-block:: python
  :linenos:

  @dp.expect_all_or_fail({"row_count_match": "abs(source_count - target_count) <= 0"})
  @dp.materialized_view(
      name="tmp_test_test_raw_to_bronze_count", 
      comment="Ensure no data loss from raw to bronze",
      temporary=True
  )
  def tmp_test_test_raw_to_bronze_count():
      return spark.sql("""
          SELECT * FROM
            (SELECT COUNT(*) AS source_count FROM raw.orders),
            (SELECT COUNT(*) AS target_count FROM bronze.orders)
      """)


Uniqueness (with optional filter)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Validate unique constraints on one or more columns. For Type 2 SCD dimensions, use ``filter`` to restrict to active rows.

.. code-block:: yaml

  # Global uniqueness
  - name: test_order_id_unique
    type: test
    test_type: uniqueness
    source: silver.orders
    columns: [order_id]
    on_violation: fail

  # Type 2 SCD: only one active record per natural key
  - name: test_customer_active_unique
    type: test
    test_type: uniqueness
    source: silver.customer_dim
    columns: [customer_id]
    filter: "__END_AT IS NULL"  # Only check active rows
    on_violation: fail

**Generated SQL (with filter):**

.. code-block:: sql

  SELECT customer_id, COUNT(*) as duplicate_count
  FROM silver.customer_dim
  WHERE __END_AT IS NULL
  GROUP BY customer_id
  HAVING COUNT(*) > 1


Referential Integrity
~~~~~~~~~~~~~~~~~~~~~

Ensure that foreign keys in a fact/reference align.

.. code-block:: yaml

  - name: test_orders_customer_fk
    type: test
    test_type: referential_integrity
    source: silver.fact_orders
    reference: silver.dim_customer
    source_columns: [customer_id]
    reference_columns: [customer_id]
    on_violation: fail

**Generated SQL (excerpt):**

.. code-block:: sql

  SELECT s.*, r.customer_id as ref_customer_id
  FROM silver.fact_orders s
  LEFT JOIN silver.dim_customer r ON s.customer_id = r.customer_id

.. code-block:: python

  @dp.expect_all_or_fail({"valid_fk": "ref_customer_id IS NOT NULL"})
  @dp.materialized_view(name="tmp_test_orders_customer_fk", comment="Test description", temporary=True)


Completeness
~~~~~~~~~~~~

Ensure required columns are populated. The generator selects only required columns for efficiency.

.. code-block:: yaml

  - name: test_customer_required_fields
    type: test
    test_type: completeness
    source: silver.dim_customer
    required_columns: [customer_key, customer_id, name, nation_id]
    on_violation: fail

**Generated SQL (optimized):**

.. code-block:: sql

  SELECT customer_key, customer_id, name, nation_id
  FROM silver.dim_customer

.. code-block:: python

  @dp.expect_all_or_fail({
      "required_fields_complete": "customer_key IS NOT NULL AND customer_id IS NOT NULL AND name IS NOT NULL AND nation_id IS NOT NULL"
  })
  @dp.materialized_view(name="tmp_test_customer_required_fields", comment="Test description", temporary=True)


Range
~~~~~

Validate that a column falls within bounds. The generator selects only the tested column.

.. code-block:: yaml

  - name: test_order_date_range
    type: test
    test_type: range
    source: silver.orders
    column: order_date
    min_value: '2020-01-01'
    max_value: 'current_date()'
    on_violation: fail

**Generated expectation:** ``order_date >= '2020-01-01' AND order_date <= 'current_date()'``


All Lookups Found
~~~~~~~~~~~~~~~~~

Validate that dimension lookups succeed (e.g., surrogate keys are present after joins).

.. code-block:: yaml

  - name: test_order_date_lookup
    type: test
    test_type: all_lookups_found
    source: silver.fact_orders
    lookup_table: silver.dim_date
    lookup_columns: [order_date]
    lookup_result_columns: [date_key]
    on_violation: fail

**Generated (excerpt):**

.. code-block:: sql

  SELECT s.*, l.date_key as lookup_date_key
  FROM silver.fact_orders s
  LEFT JOIN silver.dim_date l ON s.order_date = l.order_date

.. code-block:: python

  @dp.expect_all_or_fail({"all_lookups_found": "lookup_date_key IS NOT NULL"})
  @dp.materialized_view(name="tmp_test_order_date_lookup", comment="Test description", temporary=True)


Schema Match
~~~~~~~~~~~~

Compare schemas between two tables using ``information_schema.columns``.

.. code-block:: yaml

  - name: test_orders_schema_match
    type: test
    test_type: schema_match
    source: silver.fact_orders
    reference: gold.fact_orders_expected
    on_violation: fail

**Generated (excerpt):**

.. code-block:: sql

  WITH source_schema AS (
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns WHERE table_name = 'silver.fact_orders'
  ), reference_schema AS (
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns WHERE table_name = 'gold.fact_orders_expected'
  )
  SELECT ... -- schema diff rows

.. code-block:: python

  @dp.expect_all_or_fail({"schemas_match": "diff_count = 0"})
  @dp.materialized_view(name="tmp_test_orders_schema_match", comment="Test description", temporary=True)


Custom SQL
~~~~~~~~~~

Provide a custom SQL statement and attach expectations.

.. code-block:: yaml

  - name: test_revenue_reconciliation
    type: test
    test_type: custom_sql
    source: gold.monthly_revenue
    sql: |
      SELECT month, gold_revenue, silver_revenue,
             (ABS(gold_revenue - silver_revenue) / silver_revenue) * 100 as pct_difference
      FROM ...
    expectations:
      - name: revenue_matches
        expression: "pct_difference < 0.5"
        on_violation: fail


Custom Expectations
~~~~~~~~~~~~~~~~~~~

Attach arbitrary expectations to an existing table/view without custom SQL.

.. code-block:: yaml

  - name: test_orders_business_rules
    type: test
    test_type: custom_expectations
    source: silver.fact_orders
    expectations:
      - name: positive_amount
        expression: "total_price > 0"
        on_violation: fail
      - name: reasonable_discount
        expression: "discount_percent <= 50"
        on_violation: warn


Test Actions Configuration Reference
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Common fields across test actions:

- **name**: Unique name of the action within the FlowGroup
- **type**: Must be ``test``
- **test_type**: One of the supported test types listed above
- **source**: Source table/view; for ``row_count`` use a list of two sources
- **target**: Optional table name; defaults to ``tmp_test_<name>``
- **description**: Optional documentation
- **on_violation**: ``fail`` (default) or ``warn``

Type-specific fields:

- **row_count**: ``source`` (list of two), ``tolerance`` (int)
- **uniqueness**: ``columns`` (list), optional ``filter`` (SQL WHERE clause)
- **referential_integrity**: ``reference``, ``source_columns`` (list), ``reference_columns`` (list)
- **completeness**: ``required_columns`` (list)
- **range**: ``column``, ``min_value`` (optional), ``max_value`` (optional)
- **schema_match**: ``reference``
- **all_lookups_found**: ``lookup_table``, ``lookup_columns`` (list), ``lookup_result_columns`` (list)
- **custom_sql**: ``sql`` (string), optional ``expectations`` (list)
- **custom_expectations**: ``expectations`` (list)


Test Actions Best Practices
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Prefer ``on_violation: fail`` for hard constraints; use ``warn`` for observability
- Scope uniqueness to active/current records in SCD Type 2 dimensions using ``filter``
- Keep SQL minimal – expectations should express the rule; queries should project only required columns
- Group expectations by severity to get consolidated reporting in DLT UI
- Use reference templates in ``Reference_Templates/`` as starting points


Further Reading
----------------

* `Reference templates(Github Repo) <https://github.com/Mmodarre/Lakehouse_Plumber/tree/main/Reference_Templates>`_ fully
  documented YAML files covering every option.
* Databricks Expectations: `DLT expectations <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_ 