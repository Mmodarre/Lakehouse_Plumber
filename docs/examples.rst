Examples
========

This section showcases realistic Lakehouse Plumber configurations.  The first
example – **ACMI** – ships with the repository and demonstrates a full
Bronze → Silver → Gold medallion pipeline based on the TPC-H dataset.

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
   pip install "lakehouse-plumber[cli]"
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
   :emphasize-lines: 4-7,10,11,14,17,18,21,24

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
         database: "{catalog}.{raw_schema}"  # Environment token
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
         database: "{catalog}.{bronze_schema}"  # Environment token
         table: "%{target_table}"               # Local variable
         schema_hints_path: "schemas/%{schema_file}"

**Notice:** Local variables (``%{entity}``) and environment tokens (``{catalog}``) work together seamlessly.

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

For more details on sink configuration and options, see :doc:`actions_reference`.

More Examples (Coming Soon)
---------------------------

* JDBC ingestion with on-prem Oracle.
* Incremental snapshot tables using *delta* load and *materialized_view* write.
* Python transform with Pandas-UDF cleaning.

Contributions welcome – open a PR adding a folder under ``Example_Projects``! 