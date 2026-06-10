Pipeline Patterns
=======================================

.. meta::
   :description: Practical pipeline patterns for common data engineering scenarios — multi-source ingestion, path filtering, CloudFiles glob patterns, and fan-in architectures.

Reusable patterns for common data engineering scenarios encountered in production
Lakehouse Plumber projects. Each pattern includes the YAML configuration, generated
Python output, and relevant caveats.

.. seealso::
   :doc:`actions/test_reporting` for the data quality test result reporting pattern (publish
   to Azure DevOps, Delta tables, or a custom provider).

Pattern decision table
----------------------

Pick the pattern that matches your task, then jump to its section for the YAML, the
generated Python, and the caveats.

.. list-table::
   :header-rows: 1
   :widths: 30 50 20

   * - Pattern
     - Use this when…
     - Jump to
   * - Multi-Source Ingestion (Fan-In)
     - You consolidate the same schema from multiple sources (regions, buckets,
       accounts) into one :term:`streaming table <Streaming table>` and want each
       source on its own checkpoint.
     - `Multi-Source Ingestion (Fan-In)`_
   * - CloudFiles Path Filtering
     - You exclude specific paths, directories, or file names from a CloudFiles
       Auto Loader load. Covers glob patterns, post-load SQL filters, and
       ``pathGlobFilter``.
     - `CloudFiles Path Filtering`_
   * - ACMI Retail Demo
     - You want a complete reference project showing Bronze, Silver, and Gold
       layers, Change Data Capture (CDC), expectations, templates, presets, and
       environment substitutions end-to-end.
     - `ACMI Retail Demo`_
   * - Multi-Flowgroup Files
     - You repeat similar flowgroups (one per table, one per source system) and
       want to collapse many YAML files into a single template-driven file.
     - `Multi-Flowgroup Files`_
   * - Local Variables
     - You repeat the same value (entity name, table name, file path) across
       actions inside one flowgroup and want a single source of truth.
     - `Local Variables`_
   * - Sink Examples
     - You push data **out** of the lakehouse — Delta to an external catalog,
       Kafka topics, Azure Event Hubs, or a custom REST endpoint.
     - `Sink Examples`_
   * - More Examples (Coming Soon)
     - You are looking for patterns not yet documented (JDBC on-prem ingestion,
       incremental snapshots, Pandas-UDF transforms). Track upcoming additions
       or contribute one.
     - `More Examples (Coming Soon)`_


Multi-Source Ingestion (Fan-In)
-------------------------------

Consolidate data from **multiple sources into a single :term:`streaming table <Streaming table>`** — for example,
the same log schema arriving from S3 buckets in different regions or accounts.

LHP natively supports multiple write actions targeting the same streaming table. It
generates a single ``dp.create_streaming_table()`` call with multiple
``@dp.append_flow()`` functions — one per source. Each :term:`append flow <Append flow>` gets its own
independent checkpoint, so if one source has issues the others continue processing
normally.

The key is the ``create_table`` flag: the **first** write action creates the table
(``create_table: true``, the default), and subsequent writes append to it
(``create_table: false``).

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: pipelines/bronze/logs_multi_region.yaml
   :linenos:

   pipeline: raw_ingestions
   flowgroup: logs_multi_region

   actions:
     # Load from US bucket
     - name: load_logs_us
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket-us-east-1/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.maxFilesPerTrigger: 100
       target: v_logs_us

     # Load from EU bucket
     - name: load_logs_eu
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket-eu-west-1/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.maxFilesPerTrigger: 100
       target: v_logs_eu

     # First write — creates the table
     - name: write_logs_us
       type: write
       source: v_logs_us
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: unified_logs
         create_table: true
       description: "Write US region logs to unified table"

     # Second write — appends to the same table
     - name: write_logs_eu
       type: write
       source: v_logs_eu
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: unified_logs
         create_table: false
       description: "Append EU region logs to unified table"

Generated Output
~~~~~~~~~~~~~~~~

.. code-block:: python
   :caption: Generated logs_multi_region.py
   :linenos:

   from pyspark import pipelines as dp

   @dp.temporary_view()
   def v_logs_us():
       """Write US region logs to unified table"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "parquet") \
           .option("cloudFiles.maxFilesPerTrigger", 100) \
           .load("s3://my-bucket-us-east-1/logs/*.parquet")
       return df

   @dp.temporary_view()
   def v_logs_eu():
       """Append EU region logs to unified table"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "parquet") \
           .option("cloudFiles.maxFilesPerTrigger", 100) \
           .load("s3://my-bucket-eu-west-1/logs/*.parquet")
       return df

   # Single table creation
   dp.create_streaming_table(
       name="catalog.bronze.unified_logs",
       comment="Write US region logs to unified table"
   )

   # One append flow per source
   @dp.append_flow(target="catalog.bronze.unified_logs", name="f_unified_logs_us")
   def f_unified_logs_us():
       """Write US region logs to unified table"""
       df = spark.readStream.table("v_logs_us")
       return df

   @dp.append_flow(target="catalog.bronze.unified_logs", name="f_unified_logs_eu")
   def f_unified_logs_eu():
       """Append EU region logs to unified table"""
       df = spark.readStream.table("v_logs_eu")
       return df

.. important::
   Each streaming table must have exactly one action with ``create_table: true`` across
   the entire pipeline. Additional actions targeting the same table must use
   ``create_table: false``.

Templatising Multi-Source Ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you have many sources following the same pattern, combine this with
:doc:`templates <templates_reference>` to eliminate boilerplate. Define a template for
the load+write pair and invoke it per source, each targeting the same table.

.. seealso::
   - :doc:`best_practices/index` — related best-practice patterns
   - :doc:`actions/write_actions` for full ``streaming_table`` reference


CloudFiles Path Filtering
--------------------------

Three approaches to **exclude specific paths, directories, or files** from a CloudFiles
Auto Loader pipeline — each useful in different scenarios.


Glob Patterns in the Load Path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LHP passes the ``source.path`` string directly to the generated ``.load("...")`` call
with no escaping or transformation. Any glob syntax that Spark and CloudFiles support
works as-is in your YAML.

**Supported glob syntax (Databricks Auto Loader):**

+-------------------------+-------------------------------------------+------------------------------+
| Syntax                  | Meaning                                   | Example                      |
+=========================+===========================================+==============================+
| ``*``                   | Match any sequence of characters          | ``/data/*.parquet``          |
+-------------------------+-------------------------------------------+------------------------------+
| ``?``                   | Match any single character                | ``/data/file_?.csv``         |
+-------------------------+-------------------------------------------+------------------------------+
| ``[abc]``               | Match any character in set                | ``/data/[abc]*.csv``         |
+-------------------------+-------------------------------------------+------------------------------+
| ``[a-z]``               | Match any character in range              | ``/data/part_[0-9].csv``     |
+-------------------------+-------------------------------------------+------------------------------+
| ``[^x]``                | Match any character NOT in set            | ``/data/[^_]*.csv``          |
+-------------------------+-------------------------------------------+------------------------------+
| ``{a,b,c}``             | Match any of the alternatives             | ``/data/{sales,events}/``    |
+-------------------------+-------------------------------------------+------------------------------+

.. code-block:: yaml
   :caption: Exclude a specific day using brace expansion
   :linenos:

   actions:
     - name: load_logs_excluding_day16
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/2026/02/{0[1-9],1[0-5],1[7-9],2[0-9]}/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.useStrictGlobber: "true"
       target: v_logs_raw

You can also put the glob pattern in an environment substitution token if it varies by
environment:

.. code-block:: yaml
   :caption: substitutions/dev.yaml

   dev:
     log_path_pattern: "s3://dev-bucket/data/2026/02/{0[1-9],1[0-5],1[7-9],2[0-9]}/logs/*.parquet"

.. code-block:: yaml
   :caption: flowgroup YAML

   source:
     path: "${log_path_pattern}"

.. warning::
   **Caveats for complex glob patterns:**

   - **Character classes inside brace alternatives** (like ``{0[1-9],1[0-5]}``) should work
     at the Hadoop GlobFilter level, but this specific combination is not shown in Databricks
     documentation. **Test in a dev environment** before relying on it in production.
   - **Use** ``[^x]`` **not** ``[!x]`` for negation — the Unix shell ``[!x]`` syntax is
     not supported by Spark's globber.
   - ``**`` **(globstar)** is not documented for Auto Loader — avoid it.
   - **Notification mode** (``cloudFiles.useNotifications: "true"``) with complex globs is
     undocumented territory. Directory listing mode (the default) is the safe choice for
     glob-heavy paths.
   - Consider adding ``cloudFiles.useStrictGlobber: "true"`` (DBR 12.2+) for predictable,
     Spark-standard globbing behaviour. The default globber is more permissive — for example,
     ``*`` can cross directory boundaries.


Post-Load Filter with SQL Transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CloudFiles natively exposes ``_metadata.file_path`` in the loaded DataFrame. Add a
**SQL transform action** between the load and write to filter out unwanted paths using
full SQL regex power.

.. code-block:: yaml
   :caption: Filter excluded paths via SQL transform
   :linenos:

   actions:
     - name: load_all_files
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/"
         format: parquet
       target: v_raw_data

     - name: filter_excluded_paths
       type: transform
       transform_type: sql
       source: v_raw_data
       target: v_filtered_data
       sql: |
         SELECT * FROM stream(v_raw_data)
         WHERE NOT _metadata.file_path RLIKE '.*/exclude_[^/]+/.*'

     - name: write_bronze
       type: write
       source: v_filtered_data
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: my_table
       description: "Write filtered data to bronze layer"

Auto Loader still **reads** all files before the filter is applied. This approach works
when the excluded files are readable but contain unwanted data. If excluded files cannot
be read at all (e.g. ``AccessDenied``), use glob patterns or ``pathGlobFilter`` instead.


pathGlobFilter Option
~~~~~~~~~~~~~~~~~~~~~

To filter by file **name** (not full path), use the ``pathGlobFilter`` reader option.
This is a Spark-native option that filters on the basename of each file after directory
listing.

.. code-block:: yaml
   :caption: Filter by filename pattern
   :linenos:

   actions:
     - name: load_parquet_only
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/"
         format: parquet
         options:
           pathGlobFilter: "*.parquet"
       target: v_raw_data

``pathGlobFilter`` filters on the **filename only** (basename), not the full path. The
``.load()`` path is a prefix filter; ``pathGlobFilter`` is for suffix or name filtering.


Explicit Include List
~~~~~~~~~~~~~~~~~~~~~

Use brace expansion to explicitly list only the directories to include:

.. code-block:: yaml
   :caption: Include only specific directories
   :linenos:

   actions:
     - name: load_selected_sources
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/{sales,marketing,events}/*.parquet"
         format: parquet
       target: v_selected_data

This generates ``.load("s3://my-bucket/data/{sales,marketing,events}/*.parquet")`` and
Auto Loader will only scan those three directories.

For **truly separate paths** (different buckets or unrelated prefixes), use the
`multi-source ingestion pattern <Multi-Source Ingestion (Fan-In)_>`_ above — multiple
load+write actions targeting the same table with ``create_table: false`` on the
additional writes.


Choosing the Right Approach
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Scenario
     - Approach
     - Notes
   * - Exclude specific date partitions
     - Glob patterns
     - Use brace expansion to enumerate included segments
   * - Exclude paths matching a regex
     - SQL transform
     - Full regex power via ``_metadata.file_path``
   * - Filter by file extension or name
     - ``pathGlobFilter``
     - Set in ``source.options``; filters basename only
   * - Include specific named directories
     - Brace expansion
     - Simple and explicit; documented by Databricks
   * - Multiple separate buckets or prefixes
     - Multiple append flows
     - Independent checkpoints per source


.. seealso::
   - :doc:`actions/load_actions` for full CloudFiles load reference
   - :doc:`actions/write_actions` for ``streaming_table`` and append flow details
   - :doc:`best_practices/index` for related ingestion patterns


ACMI Retail Demo
----------------

Folder: ``Example_Projects/acmi``

Highlights
~~~~~~~~~~

* **Multi-format ingestion** – CSV, JSON, Parquet using *cloudfiles*.
* **Bronze → Silver → Gold** layers encoded as separate pipelines.
* **Change-Data-Feed (CDC)** enabled on streaming tables.
* **Data-Quality Expectations** (`expectations/*.json`).
* **Templates & Presets** to avoid repetition.
* **Environment substitutions** for *dev*, *tst*, *prod*.

Walk-through
~~~~~~~~~~~~

.. code-block:: bash

   # 1. Install prereqs & enter project root
   pip install lakehouse-plumber
   cd Example_Projects/acmi

   # 2. Validate all pipelines for dev environment
   lhp validate --env dev

   # 3. Generate Bronze layer code (raw ingestion)
   lhp generate --env dev --pipeline 01_raw_ingestion

   # Check ./generated/ for Python DLT scripts

   # 4. Generate Silver layer
   lhp generate --env dev --pipeline 03_silver

   # 5. Generate Gold analytics views
   lhp generate --env dev --pipeline 04_gold

Customising the Example
~~~~~~~~~~~~~~~~~~~~~~~

1. Edit ``substitutions/dev.yaml`` to match your catalog and storage paths.
2. Tweak presets under ``presets/`` (e.g., change table properties).
3. Adjust schema hints or expectations JSON to enforce your data contract.

Multi-Flowgroup Files
---------------------

For projects with many similar flowgroups, you can combine multiple flowgroups
into a single YAML file to reduce file proliferation and improve organization.

Example: SAP Master Data (3 files → 1 file)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Instead of:

* ``brand_ingestion.yaml``
* ``category_ingestion.yaml``
* ``carrier_ingestion.yaml``

Use one file ``sap_master_data.yaml``:

.. code-block:: yaml

   pipeline: raw_ingestions_sap
   use_template: TMPL003_parquet_ingestion_template

   flowgroups:
     - flowgroup: sap_brand_ingestion
       template_parameters:
         table_name: raw_sap_brand
         landing_folder: brand

     - flowgroup: sap_cat_ingestion
       template_parameters:
         table_name: raw_sap_cat
         landing_folder: category

     - flowgroup: sap_carrier_ingestion
       template_parameters:
         table_name: raw_sap_carrier
         landing_folder: carrier

**Result:** 67% file reduction with identical functionality.

See :doc:`multi_flowgroup_guide` for complete documentation with inheritance rules,
syntax options, migration guides, and real-world examples.

Local Variables
---------------

Local variables reduce repetition within a single flowgroup by defining reusable values. They use ``%{variable}`` syntax and are resolved before templates and environment substitutions.

Simple Example
~~~~~~~~~~~~~~

Instead of repeating "customer" throughout your flowgroup:

.. code-block:: yaml
   :caption: Without local variables (repetitive)

   pipeline: acmi_edw_bronze
   flowgroup: customer_pipeline

   actions:
     - name: "load_customer_raw"
       source:
         table: "customer_raw"
       target: "v_customer_raw"

     - name: "customer_cleanse"
       source: "v_customer_raw"
       target: "v_customer_cleaned"

     - name: "write_customer_bronze"
       source: "v_customer_cleaned"
       write_target:
         table: "customer"

Use local variables to define it once:

.. code-block:: yaml
   :caption: With local variables (DRY principle)
   :emphasize-lines: 4-7,10,11,14,17,18,21,22

   pipeline: acmi_edw_bronze
   flowgroup: customer_pipeline

   variables:
     entity: customer
     source_table: customer_raw
     target_table: customer

   actions:
     - name: "load_%{entity}_raw"
       source:
         table: "%{source_table}"
       target: "v_%{entity}_raw"

     - name: "%{entity}_cleanse"
       source: "v_%{entity}_raw"
       target: "v_%{entity}_cleaned"

     - name: "write_%{entity}_bronze"
       source: "v_%{entity}_cleaned"
       write_target:
         table: "%{target_table}"

**Benefits:**

- **Single source of truth**: Change "customer" to "order" in one place
- **Reduced errors**: No risk of inconsistent naming across actions
- **Better readability**: Intent is clear from the variables section
- **Works everywhere**: Inline patterns like ``prefix_%{var}_suffix`` supported

Real-World Example
~~~~~~~~~~~~~~~~~~

Here's a production pattern combining local variables with environment substitutions:

.. code-block:: yaml
   :caption: pipelines/bronze/product_ingestion.yaml
   :linenos:
   :emphasize-lines: 4-8,13,16,17,21,22,27,28,31

   pipeline: acmi_edw_bronze
   flowgroup: product_pipeline

   variables:
     entity: product
     source_table: product_raw
     target_table: product
     schema_file: product_schema.yaml

   actions:
     - name: "load_%{entity}_raw"
       type: load
       operational_metadata: ["_processing_timestamp"]
       readMode: stream
       source:
         type: delta
         database: "${catalog}.${raw_schema}"  # Environment token
         table: "%{source_table}"            # Local variable
       target: "v_%{entity}_raw"
       description: "Load %{entity} table from raw schema"

     - name: "%{entity}_quality_check"
       type: transform
       transform_type: sql
       source: "v_%{entity}_raw"
       target: "v_%{entity}_validated"
       sql_path: "sql/quality_checks/%{entity}_check.sql"
       expectations_path: "expectations/%{entity}_expectations.json"

     - name: "write_%{entity}_bronze"
       type: write
       source: "v_%{entity}_validated"
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"  # Environment token
         table: "%{target_table}"               # Local variable
         schema_hints_path: "schemas/%{schema_file}"

**Notice:** Local variables (``%{entity}``) and environment tokens (``${catalog}``) work together seamlessly.

See :doc:`templates_reference` for complete documentation on local variables, including:

- Recursive variable definitions
- Error handling for undefined variables
- Interaction with templates and presets
- Processing order details

Sink Examples
-------------

The ACME Supermarkets project includes comprehensive sink examples demonstrating
data export to external systems. These examples showcase Delta, Kafka, and custom
API sinks for streaming data to destinations beyond traditional DLT-managed tables.

Location: ``Example_Projects/acme_supermarkets_lhp/pipelines/06_sink_examples/``

Delta Sink Example
~~~~~~~~~~~~~~~~~~

Export aggregated sales metrics to external Unity Catalog for cross-workspace analytics.

File: ``01_delta_sink_external_catalog.yaml``

.. code-block:: bash

   cd Example_Projects/acme_supermarkets_lhp
   lhp generate --env dev --pipeline acme_supermarkets_sinks_pipeline
   cat generated/acme_supermarkets_sinks_pipeline/delta_sink_example.py

Key features:

* Aggregates silver layer data
* Writes to external catalog table
* Schema evolution enabled
* Optimized writes for performance

Kafka Sink Example
~~~~~~~~~~~~~~~~~~

Stream order fulfillment events to Kafka for real-time processing by downstream systems.

File: ``02_kafka_sink_order_events.yaml``

Key features:

* Transforms data to Kafka key/value format using ``to_json()``
* JSON serialization of order events
* Kafka headers for event metadata
* Security and performance tuning configuration

.. Important::
   Kafka sinks require explicit ``key`` and ``value`` columns created in a
   transform action before writing.

Azure Event Hubs Example
~~~~~~~~~~~~~~~~~~~~~~~~

Stream inventory alerts to Azure Event Hubs using OAuth authentication.

File: ``03_event_hubs_sink_inventory_alerts.yaml``

Key features:

* OAuth authentication with Azure Event Hubs
* Kafka-compatible interface (``sink_type: kafka``)
* Priority-based alert routing
* Unity Catalog service credentials

Custom API Sink Example
~~~~~~~~~~~~~~~~~~~~~~~

Push customer profile updates to external CRM via REST API.

File: ``04_custom_api_sink_customer_updates.yaml``

Custom implementation: ``sinks/customer_api_sink.py``

Key features:

* HTTP POST with bearer token authentication
* Batch processing with configurable batch size
* Retry logic with exponential backoff
* Dead letter queue for failed records
* Comprehensive error logging

Walk-through
~~~~~~~~~~~~

.. code-block:: bash

   cd Example_Projects/acme_supermarkets_lhp

   # Validate sink configurations
   lhp validate --env dev

   # Generate all sink examples
   lhp generate --env dev --pipeline acme_supermarkets_sinks_pipeline

   # Inspect generated Python code
   cat generated/acme_supermarkets_sinks_pipeline/delta_sink_example.py
   cat generated/acme_supermarkets_sinks_pipeline/kafka_sink_example.py
   cat generated/acme_supermarkets_sinks_pipeline/custom_api_sink_example.py

   # Deploy with Databricks bundles
   databricks bundle deploy -t dev

For more details on sink configuration and options, see :doc:`actions/write_actions`.

More Examples (Coming Soon)
---------------------------

* JDBC ingestion with on-prem Oracle.
* Incremental snapshot tables using *delta* load and *materialized_view* write.
* Python transform with Pandas-UDF cleaning.

Contributions welcome – open a PR adding a folder under ``Example_Projects``!
