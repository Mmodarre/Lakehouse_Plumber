Load Actions
============

.. meta::
   :description: Complete reference for LHP Load action types: CloudFiles, Delta, SQL, JDBC, Python, and custom datasource.


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
-------------------------------------------
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
- **readMode**: is either *batch* or *stream* 
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

.. note::
  **File Path Organization**: Schema files can be organized in subdirectories relative to your project root:
  
  - Root level: ``"customer_schema.yaml"``
  - Single directory: ``"schemas/customer_schema.yaml"``
  - Nested subdirectories: ``"schemas/bronze/dimensions/customer_schema.yaml"``
  
  The framework automatically detects whether the value is an inline DDL string or a file path based on common file indicators (``.yaml``, ``.yml``, ``.ddl``, ``.sql``, or path separators).
  
  **YAML Schema Conversion**: When using YAML schema files (Option 2), the ``nullable`` field is respected during conversion to DDL:
  
  - Columns with ``nullable: false`` are converted to include ``NOT NULL`` constraint
  - Columns with ``nullable: true`` (or omitted, default is true) are converted without constraints
  
  Example: A YAML column defined as ``{name: c_custkey, type: BIGINT, nullable: false}`` will generate ``c_custkey BIGINT NOT NULL`` in the schema hints.
            
.. seealso::
  - For full list of options see the `Databricks Auto Loader documentation <https://docs.databricks.com/en/data/data-sources/cloud-files/auto-loader/index.html>`_.
  - Operational metadata: :doc:`../operational_metadata`

.. Important::
  Lakehouse Plumber uses syntax consistent with Databricks, making it easy to transfer knowledge between the two.
  All options available here mirror those of Databricks Auto Loader.


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

delta
-------------------------------------------
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

**Delta Options**

Delta load actions support the ``options`` field to configure Delta-specific reader options:

.. code-block:: yaml

  actions:
    - name: load_orders_cdc
      type: load
      readMode: stream
      source:
        type: delta
        database: "{catalog}.bronze"
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
| **readChangeFeed**      | string/boolean   | Enable Change Data Feed (requires stream mode)    |
+-------------------------+------------------+---------------------------------------------------+
| **startingVersion**     | string           | Starting version for CDC or time travel           |
+-------------------------+------------------+---------------------------------------------------+
| **startingTimestamp**   | string           | Starting timestamp for CDC (ISO 8601 format)      |
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
  - The ``readChangeFeed`` option requires ``readMode: stream`` and will raise an error if used with batch mode
  - All option values are validated and cannot be ``None`` or empty strings
  - Options work in both streaming and batch modes (except ``readChangeFeed`` which is streaming-only)

**Time Travel Example:**

.. code-block:: yaml

  actions:
    - name: load_customers_snapshot
      type: load
      readMode: batch
      source:
        type: delta
        database: "{catalog}.silver"
        table: customers
        options:
          versionAsOf: "10"
        where_clause: ["status = 'active'"]
        select_columns: ["customer_id", "name", "email"]
      target: v_customers_snapshot
      description: "Load customers at version 10"

.. seealso::
  - For ``stream`` readMode see the Databricks documentation on `Change Data Feed <https://docs.databricks.com/en/data/data-sources/delta/change-data-feed.html>`_
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

kafka
-------------------------------------------
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
-------------------------------------------
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
  - Substitution variables: :doc:`../substitutions`

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
-------------------------------------------
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
  - Secret management: :doc:`../substitutions`

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
-------------------------------------------
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
  - Custom functions: :doc:`../concepts`

.. Important::
  Python functions must accept two parameters: ``spark`` (SparkSession) and ``parameters`` (dict).
  The function must return a PySpark DataFrame that will be used as the view source.

.. note::
  **File Organization**: When using ``module_path``, the path is relative to your YAML file location.
  Common practice is to create an ``extractors/`` or ``functions/`` folder alongside your pipeline YAML files.

.. note::
  **Parameter Substitution**: The ``parameters`` dictionary supports both ``${token}`` 
  (preferred) and ``{token}`` (legacy) substitution for environment-specific values:
  
  .. code-block:: yaml
  
     parameters:
       catalog: "${catalog}"                # Preferred syntax
       table_name: "${schema}.users"        # Preferred syntax
       api_endpoint: "{api_url}"            # Legacy (still works)
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

PySpark Custom DataSource
-------------------------------------------
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
  - Custom integrations: :doc:`../concepts`

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

