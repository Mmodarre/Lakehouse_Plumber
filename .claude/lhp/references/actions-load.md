# Load Actions Reference

Load actions bring data into temporary **views** for downstream processing.

## Common Fields

| Field | Required | Description |
|-------|----------|-------------|
| `name` | **req** | Unique name within the FlowGroup |
| `type` | **req** | Must be `load` |
| `readMode` | opt | `stream` (default) or `batch` |
| `source` | **req** | Source configuration (dict) |
| `target` | **req** | Name of the temporary view created |
| `operational_metadata` | opt | Metadata columns to add |
| `description` | opt | Documentation for the action |

---

## CloudFiles (Auto Loader)

```yaml
- name: load_csv_files
  type: load
  readMode: stream                # opt (default: stream)
  operational_metadata: ["_source_file_path", "_source_file_size", "_source_file_modification_time"]
  source:
    type: cloudfiles              # req
    path: "{landing_volume}/folder/*.csv"  # req — file path pattern
    format: csv                   # req — csv, json, parquet, avro, etc.
    options:                      # opt
      cloudFiles.format: csv
      header: True
      delimiter: "|"
      cloudFiles.maxFilesPerTrigger: 200
      cloudFiles.inferColumnTypes: False
      cloudFiles.schemaEvolutionMode: "addNewColumns"
      cloudFiles.rescuedDataColumn: "_rescued_data"
      cloudFiles.schemaHints: "schemas/table_schema.yaml"
  target: v_table_raw
  description: "Load CSV files"
```

**schemaHints formats** (auto-detected):
- **Inline DDL**: `"customer_id BIGINT, name STRING"`
- **YAML file**: `"schemas/customer_schema.yaml"` (supports `nullable: false` → `NOT NULL`)
- **DDL/SQL file**: `"schemas/customer_schema.ddl"` or `.sql`

**Key points:**
- `readMode: stream` → `spark.readStream.format("cloudFiles")`, `batch` → `spark.read.format("cloudFiles")`
- `_metadata.*` columns (`file_path`, `file_size`, `file_modification_time`) only available in views, not downstream transforms
- All options mirror Databricks Auto Loader options

---

## Delta

```yaml
- name: load_delta
  type: load
  readMode: stream                # opt (default: stream)
  source:
    type: delta                   # req
    database: "{catalog}.{raw_schema}"  # req
    table: customer               # req
    options:                      # opt
      readChangeFeed: "true"      # requires readMode: stream
      startingVersion: "0"
    where_clause: ["status = 'active'"]   # opt — filter rows
    select_columns: ["customer_id", "name"]  # opt — select specific columns
  target: v_customer_raw
```

**Delta options:**

| Option | Description |
|--------|-------------|
| `readChangeFeed` | Enable CDC (stream mode only) |
| `startingVersion` | CDC starting version |
| `startingTimestamp` | CDC starting timestamp (ISO 8601) |
| `versionAsOf` | Time travel: specific version (batch) |
| `timestampAsOf` | Time travel: specific timestamp (batch) |
| `ignoreDeletes` | Ignore delete ops in CDC |
| `skipChangeCommits` | Skip change commits in stream |
| `maxFilesPerTrigger` | Max files per trigger |

**Time travel example:**
```yaml
source:
  type: delta
  database: "{catalog}.silver"
  table: customers
  options:
    versionAsOf: "10"
  where_clause: ["status = 'active'"]
  select_columns: ["customer_id", "name", "email"]
```

---

## SQL

```yaml
# Inline SQL
- name: load_summary
  type: load
  readMode: batch
  source:
    type: sql                     # req
    sql: |                        # req (one of sql or sql_path)
      SELECT * FROM {catalog}.{schema}.table WHERE status = 'active'
  target: v_summary

# External SQL file
- name: load_metrics
  type: load
  readMode: batch
  source:
    type: sql
    sql_path: "sql/metrics_query.sql"  # req (one of sql or sql_path)
  target: v_metrics
```

**Key:** Substitution variables work in both inline SQL and external SQL files (`{token}`, `${secret:scope/key}`).

---

## JDBC

```yaml
# Query-based
- name: load_postgres
  type: load
  readMode: batch
  source:
    type: jdbc                    # req
    url: "jdbc:postgresql://host:5432/db"  # req
    driver: "org.postgresql.Driver"        # req
    user: "${secret:database_secrets/username}"    # req
    password: "${secret:database_secrets/password}"  # req
    query: "SELECT * FROM customers WHERE active = true"  # req (one of query or table)
  target: v_external_customers

# Table-based
- name: load_products
  type: load
  readMode: batch
  source:
    type: jdbc
    url: "jdbc:mysql://host:3306/catalog"
    driver: "com.mysql.cj.jdbc.Driver"
    user: "${secret:mysql/username}"
    password: "${secret:mysql/password}"
    table: "products"             # req (one of query or table)
  target: v_external_products
```

**Key:** `query` and `table` are mutually exclusive — use one or the other. Always use `${secret:scope/key}` for credentials.

---

## Kafka

```yaml
- name: load_kafka_events
  type: load
  readMode: stream                # always streaming
  source:
    type: kafka                   # req
    bootstrap_servers: "broker1:9092,broker2:9092"  # req
    subscribe: "topic1,topic2"    # req (exactly one of subscribe/subscribePattern/assign)
    options:                      # opt
      startingOffsets: "latest"
      failOnDataLoss: false
      kafka.security.protocol: "SASL_SSL"
  target: v_kafka_raw
```

**Subscription methods** (exactly one required):
- `subscribe` — comma-separated topic list
- `subscribePattern` — Java regex for topic names
- `assign` — JSON with specific topic partitions

**Key:** Kafka returns binary `key`/`value` columns. Must deserialize with a transform action.

### AWS MSK IAM Auth

```yaml
options:
  kafka.security.protocol: "SASL_SSL"
  kafka.sasl.mechanism: "AWS_MSK_IAM"
  kafka.sasl.jaas.config: "shadedmskiam.software.amazon.msk.auth.iam.IAMLoginModule required;"
  kafka.sasl.client.callback.handler.class: "shadedmskiam.software.amazon.msk.auth.iam.IAMClientCallbackHandler"
```

Port 9098 for IAM auth. IAM role must have `kafka-cluster:*` permissions. No credentials stored — authentication via IAM.

### Azure Event Hubs OAuth

```yaml
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
      kafka.sasl.jaas.config: >-
        kafkashaded.org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule
        required clientId="${secret:azure_secrets/client_id}"
        clientSecret="${secret:azure_secrets/client_secret}"
        scope="https://${event_hubs_namespace}/.default"
        ssl.protocol="SSL";
      kafka.sasl.oauthbearer.token.endpoint.url: "https://login.microsoft.com/${azure_tenant_id}/oauth2/v2.0/token"
      kafka.sasl.login.callback.handler.class: "kafkashaded.org.apache.kafka.common.security.oauthbearer.secured.OAuthBearerLoginCallbackHandler"
      startingOffsets: "earliest"
  target: v_event_hubs_data_raw
```

Port 9093 for Event Hubs. Namespace format: `<namespace>.servicebus.windows.net`. Service Principal needs "Azure Event Hubs Data Receiver" role.

---

## Python Function

```yaml
- name: load_api_data
  type: load
  readMode: batch
  source:
    type: python                  # req
    module_path: "extractors/api_extractor.py"  # req — path to Python file
    function_name: "extract_data" # opt (default: "get_df")
    parameters:                   # opt — dict passed to function
      api_endpoint: "{api_url}"
      api_key: "${secret:apis/key}"
  target: v_api_data
```

**Function signature:** `def func(spark, parameters: dict) -> DataFrame`

**Key:** `module_path` is relative to project root. Files are copied to `generated/` with substitution processing.

---

## Python Function vs Custom DataSource

| | Python Function (`type: python`) | Custom DataSource (`type: custom_datasource`) |
|---|---|---|
| **Use when** | You need a DataFrame from custom logic (API call, file parse, computation) | You need **streaming with offset tracking** across pipeline runs |
| **Typical readMode** | `batch` | `stream` (or batch) |
| **You write** | A function: `def func(spark, params) -> DataFrame` | A class implementing PySpark `DataSource` + `DataSourceStreamReader` |
| **Offset/checkpoint** | No — each run starts fresh | Yes — `initialOffset`, `latestOffset`, `partitions`, `read` |
| **Complexity** | Low — just return a DataFrame | Higher — must implement DataSource interface with schema, reader methods |

**Rule of thumb:** Start with Python Function. Only move to Custom DataSource when you need Spark-managed streaming offsets or want a reusable connector that behaves like a native Spark data source.

---

## Custom DataSource (PySpark)

```yaml
- name: load_custom
  type: load
  readMode: stream
  source:
    type: custom_datasource       # req
    module_path: "data_sources/my_source.py"  # req — path to DataSource class
    custom_datasource_class: "MyStreamingDataSource"  # req — class name
    options:                      # opt — passed via self.options
      apiKey: "${secret:apis/key}"
  target: v_custom_data
```

**Implementation requirements:**
- Class must implement `DataSource` interface with `name()` classmethod returning the format name
- Framework uses `name()` return value for `.format()`, not the class name
- Source code is automatically copied to generated pipeline, import management handled
- Supports substitution variables in Python files (`{token}`, `${secret:scope/key}`)
