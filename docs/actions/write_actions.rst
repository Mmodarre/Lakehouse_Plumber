Write Actions
=============

.. meta::
   :description: Complete reference for LHP Write action types: streaming tables, materialized views, and sinks (Delta, Kafka, Event Hubs, custom, foreachbatch).


+-------------------+--------------------------------------------------------------------------+
| Sub-type          | Purpose                                                                  |
+===================+==========================================================================+
|| streaming_table  || Create or append to a Delta streaming table in Unity Catalog.           |
||                  || Supports Change Data Feed (CDF), CDC modes, and append flows.           |
+-------------------+--------------------------------------------------------------------------+
|| materialized_view|| Create a Lakeflow *materialized view* for batch-computed analytics.     |
+-------------------+--------------------------------------------------------------------------+

streaming_table
-------------------------------------------
Streaming table write actions create or append to Delta streaming tables. They support three modes: **standard** (append flows), **cdc** (change data capture), and **snapshot_cdc** (snapshot-based CDC).

Append Streaming Table Write
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
      - **table_schema**: DDL schema definition for the table (supports inline DDL or external file - see below)
      - **row_filter**: Row-level security filter using SQL UDF (format: "ROW FILTER function_name ON (column_names)")
      - **comment**: Table comment for documentation
      - **mode**: Streaming mode - "standard" (default), "cdc", or "snapshot_cdc"
- **description**: Optional documentation for the action

**table_schema Format Options**

The ``table_schema`` option supports two formats, automatically detected by the framework:

**Option 1: Inline DDL** (multiline string)

.. code-block:: yaml

  table_schema: |
    customer_id BIGINT NOT NULL,
    name STRING,
    email STRING,
    region STRING,
    registration_date DATE,
    _source_file_path STRING,
    _processing_timestamp TIMESTAMP

**Option 2: External DDL/SQL File**

.. code-block:: yaml

  table_schema: "schemas/customer_table.ddl"
  # or
  table_schema: "schemas/customer_table.sql"
  # or
  table_schema: "schemas/customer_table.yaml"

.. note::
  **External Schema Files**: Schema files can be organized in subdirectories relative to your project root (e.g., ``"schemas/bronze/customer_table.ddl"``). The framework automatically detects file paths based on file extensions (``.ddl``, ``.sql``, ``.yaml``, ``.yml``, ``.json``) or path separators.

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~


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
-------------------------------------------
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
        row_filter: "ROW FILTER catalog.schema.region_access_filter ON (region)"
        comment: "Daily customer summary materialized view"
      description: "Create daily customer summary for analytics"

**Option 2: Inline SQL Query**

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
        row_filter: "ROW FILTER catalog.schema.region_access_filter ON (region)"
      description: "Daily sales summary by region and category"

**Option 3: External SQL File**

.. code-block:: yaml

  actions:
    - name: create_sales_summary_mv
      type: write
      write_target:
        type: materialized_view
        database: "{catalog}.{gold_schema}"
        table: daily_sales_summary
        sql_path: "sql/gold/daily_sales_summary.sql"
        table_properties:
          delta.autoOptimize.optimizeWrite: "true"
          custom.business.domain: "sales_analytics"
        partition_columns: ["sales_date"]
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
      - **sql**: Inline SQL query to define the view (alternative to source or sql_path)
      - **sql_path**: Path to external SQL file to define the view (alternative to source or sql)
      - **table_properties**: Delta table properties for optimization
      - **partition_columns**: Columns to partition the view by
      - **cluster_columns**: Columns to cluster/z-order the view by
      - **table_schema**: DDL schema definition for the view (supports inline DDL or external file - see below)
      - **row_filter**: Row-level security filter using SQL UDF (format: "ROW FILTER function_name ON (column_names)")
      - **comment**: Table comment for documentation
- **description**: Optional documentation for the action

.. note::
  **SQL Query Options**: You can define the materialized view query in three ways:
  
  1. **Source view** (Option 1): Read from an existing view using ``source``
  2. **Inline SQL** (Option 2): Define SQL directly in YAML using ``sql``
  3. **External SQL file** (Option 3): Reference external SQL file using ``sql_path``
  
  External SQL files support substitution variables (``{tokens}`` and ``${secret:scope/key}``) 
  and can be organized in subdirectories (e.g., ``"sql/gold/aggregations/sales_summary.sql"``).

**table_schema Format Options**

The ``table_schema`` option supports two formats, automatically detected by the framework:

**Option 1: Inline DDL**

.. code-block:: yaml

  table_schema: "product_id BIGINT, name STRING, price DECIMAL(10,2), category STRING"

**Option 2: External DDL/SQL File**

.. code-block:: yaml

  table_schema: "schemas/product_view_schema.ddl"
  # or
  table_schema: "schemas/gold/product_view_schema.sql"
  # or
  table_schema: "schemas/product_view_schema.yaml"

.. note::
  **External Schema Files**: Schema files can be organized in subdirectories relative to your project root. The framework automatically detects file paths based on file extensions (``.ddl``, ``.sql``, ``.yaml``, ``.yml``, ``.json``) or path separators.

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
  Materialized views in Databricks refresh automatically based on source data changes.
  Materialized views can either read from source views or execute custom SQL queries.

.. Note::
  The `refresh_schedule` parameter is no longer supported by `@dp.materialized_view`.
  If present in YAML configurations, it will be accepted for backward compatibility but ignored during code generation.

sink
-------------------------------------------
Sink write actions enable streaming data to external destinations beyond traditional 
DLT-managed streaming tables. Sinks provide flexible output for real-time data 
distribution to external systems, Unity Catalog external tables, and event streaming 
services.

**Supported Sink Types:**

+---------------+------------------------------------------------------------+
| Sink Type     | Purpose                                                    |
+===============+============================================================+
| delta         | Write to Unity Catalog external tables or external Delta   |
+---------------+------------------------------------------------------------+
| kafka         | Stream to Apache Kafka or Azure Event Hubs topics          |
+---------------+------------------------------------------------------------+
| custom        | Write to custom destinations via Python DataSink class     |
+---------------+------------------------------------------------------------+

**When to Use Sinks:**

* **Operational use cases** - Fraud detection, real-time analytics, customer recommendations with low-latency requirements
* **External Delta tables** - Write to Unity Catalog external tables or Delta instances managed outside DLT
* **Reverse ETL** - Export processed data to external systems like Kafka for downstream consumption
* **Custom formats** - Use Python custom data sources to write to any destination not directly supported by Databricks

Delta Sink
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

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
-------------------------------------------

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


ForEachBatch Sink
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Process streaming data with custom Python logic for each micro-batch, enabling advanced use cases like 
merging into Delta tables, writing to multiple destinations, or implementing complex upsert logic.

**Use Cases:**

- Merge streaming data into Delta Lake tables with custom logic
- Write each micro-batch to multiple destinations simultaneously
- Implement complex upsert patterns with conditional logic
- Apply custom transformations or validations per batch

.. Important::
  **ForEachBatch sinks are for advanced streaming use cases.** Unlike other sinks that use ``dp.create_sink()``,
  ForEachBatch uses the ``@dp.foreach_batch_sink()`` decorator pattern. You provide the batch processing logic
  (function body), and LHP wraps it with the decorator and generates the append_flow.

**Configuration Options**

ForEachBatch sinks support two modes:

1. **External Python file**: Batch handler code in a separate file (recommended for complex logic)
2. **Inline code**: Batch handler code directly in YAML (suitable for simple cases)

**Option 1: External Python File**

.. code-block:: yaml

  # Example: Merge streaming data into target table
  actions:
    - name: merge_customer_updates
      type: write
      source: v_customer_changes
      write_target:
        type: sink
        sink_type: foreachbatch
        sink_name: customer_merge_sink
        module_path: "batch_handlers/merge_customers.py"
        comment: "Merge customer updates using custom logic"

**User's Python file** (``batch_handlers/merge_customers.py``):

.. code-block:: python

  # Function body only - no decorator, no function signature
  # LHP will wrap this with @dp.foreach_batch_sink decorator
  
  df.createOrReplaceTempView("batch_view")
  df.sparkSession.sql("""
      MERGE INTO ${target_table} AS tgt
      USING batch_view AS src
      ON tgt.customer_id = src.customer_id
      WHEN MATCHED THEN 
          UPDATE SET 
              tgt.email = src.email,
              tgt.phone = src.phone,
              tgt.updated_at = src.updated_at
      WHEN NOT MATCHED THEN 
          INSERT (customer_id, email, phone, created_at, updated_at)
          VALUES (src.customer_id, src.email, src.phone, src.created_at, src.updated_at)
  """)

**Option 2: Inline Batch Handler**

.. code-block:: yaml

  # Example: Simple append to Delta table
  actions:
    - name: append_events
      type: write
      source: v_events
      write_target:
        type: sink
        sink_type: foreachbatch
        sink_name: events_sink
        batch_handler: |
          df.write.format("delta").mode("append").saveAsTable("${events_table}")

**Generated Code Output**

LHP generates the complete ForEachBatch sink code:

.. code-block:: python
  :linenos:

  from pyspark import pipelines as dp
  
  @dp.foreach_batch_sink(name="customer_merge_sink")
  def customer_merge_sink(df, batch_id):
      """ForEachBatch sink: merge_customer_updates"""
      df.createOrReplaceTempView("batch_view")
      df.sparkSession.sql("""
          MERGE INTO catalog.schema.customers AS tgt
          USING batch_view AS src
          ON tgt.customer_id = src.customer_id
          WHEN MATCHED THEN 
              UPDATE SET 
                  tgt.email = src.email,
                  tgt.phone = src.phone,
                  tgt.updated_at = src.updated_at
          WHEN NOT MATCHED THEN 
              INSERT (customer_id, email, phone, created_at, updated_at)
              VALUES (src.customer_id, src.email, src.phone, src.created_at, src.updated_at)
      """)
      return
  
  @dp.append_flow(target="customer_merge_sink", name="f_customer_merge_sink_1")
  def f_customer_merge_sink_1():
      df = spark.readStream.table("v_customer_changes")
      return df

**Anatomy of a ForEachBatch sink write action:**

- **write_target.type**: Must be ``sink``
- **write_target.sink_type**: Must be ``foreachbatch``
- **write_target.sink_name**: Unique identifier for this sink (used in decorator name)
- **write_target.module_path**: Path to Python file with batch handler body (Option 1)
- **write_target.batch_handler**: Inline batch handler code (Option 2)
- **write_target.comment**: Optional description
- **source**: Single source view (string) - ForEachBatch sinks support only one source

.. Important::
  **You must provide EITHER ``module_path`` OR ``batch_handler``, not both.**

**Writing to Multiple Destinations**

ForEachBatch excels at writing each batch to multiple destinations:

.. code-block:: yaml

  actions:
    - name: multi_destination_write
      type: write
      source: v_processed_data
      write_target:
        type: sink
        sink_type: foreachbatch
        sink_name: multi_write_sink
        batch_handler: |
          # Write to Delta table
          df.write.format("delta").mode("append") \
            .option("txnVersion", batch_id) \
            .option("txnAppId", "my-app") \
            .saveAsTable("${primary_table}")
          
          # Also write to backup location
          df.write.format("delta").mode("append") \
            .option("txnVersion", batch_id) \
            .option("txnAppId", "my-app-backup") \
            .save("/mnt/backup/data")
          
          # And write summary to monitoring table
          summary = df.groupBy().count()
          summary.write.format("delta").mode("append") \
            .saveAsTable("${monitoring_table}")

.. Note::
  **Idempotent Writes**: Use ``txnVersion`` and ``txnAppId`` options to make Delta writes idempotent.
  This ensures that if a batch is re-run, duplicate writes are prevented. See 
  `Databricks documentation on idempotent writes <https://docs.databricks.com/structured-streaming/foreach-batch.html#idempotent-table-writes-in-foreachbatch>`_.

**Full Refresh Handling**

ForEachBatch sinks track checkpoints per flow. On **full refresh**, the checkpoint resets and ``batch_id`` 
starts from 0. You are responsible for handling downstream data cleanup:

.. code-block:: python

  # Example: Handle full refresh in your batch handler
  if batch_id == 0:
      # Full refresh - clean up target table
      df.sparkSession.sql("TRUNCATE TABLE ${target_table}")
  
  # Then process the batch normally
  df.write.format("delta").mode("append").saveAsTable("${target_table}")

**Operational Metadata Support**

ForEachBatch sinks support operational metadata columns (like other sinks):

.. code-block:: yaml

  # In lhp.yaml
  operational_metadata:
    columns:
      _ingestion_timestamp:
        expression: "F.current_timestamp()"
      _batch_id:
        expression: "F.lit(batch_id)"
  
  # In flowgroup YAML
  operational_metadata: [_ingestion_timestamp, _batch_id]
  
  actions:
    - name: batch_with_metadata
      type: write
      source: v_data
      write_target:
        type: sink
        sink_type: foreachbatch
        sink_name: my_sink
        batch_handler: |
          # df already has _ingestion_timestamp and _batch_id columns
          df.write.format("delta").mode("append").saveAsTable("target")

**Substitution Token Support**

Use ``${token}`` substitutions in your batch handler code:

.. code-block:: yaml

  # In substitutions.yaml
  dev:
    target_table: "dev_catalog.bronze.customers"
  prod:
    target_table: "prod_catalog.bronze.customers"
  
  # In flowgroup YAML
  actions:
    - name: write_customers
      type: write
      source: v_customers
      write_target:
        type: sink
        sink_type: foreachbatch
        sink_name: customer_sink
        module_path: "batch_handlers/merge.py"  # Uses ${target_table}

**Best Practices**

1. **Keep batch handlers focused**: Each handler should have a single responsibility
2. **Use external files for complex logic**: Easier to test and maintain
3. **Handle errors gracefully**: Consider try-catch blocks for resilience
4. **Make writes idempotent**: Use ``txnVersion`` and ``txnAppId`` for Delta writes
5. **Test with small batches first**: Validate logic before full-scale deployment
6. **Monitor batch processing**: Log batch_id and record counts for observability

**Limitations**

- **Single source only**: ForEachBatch sinks support one source view (not lists)
- **No automatic housekeeping**: You manage downstream data cleanup
- **Serialization requirements**: For Databricks Connect, avoid ``dbutils`` in handlers
- **No parameters**: Use substitution tokens instead of parameter dicts

.. seealso::
  - `Databricks ForEachBatch documentation <https://docs.databricks.com/aws/en/ldp/for-each-batch>`_
  - `Idempotent writes in ForEachBatch <https://docs.databricks.com/structured-streaming/foreach-batch.html#idempotent-table-writes-in-foreachbatch>`_


