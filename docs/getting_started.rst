Getting Started
===============

This short tutorial walks you through creating your first **Lakehouse Plumber**
project and generating a Delta Live Tables (DLT) pipeline based on the ACME demo
configuration that ships with the repository.

Prerequisites
-------------

* Python 3.8 + (3.10 recommended)
* Access to a Databricks workspace with DLT enabled (for actual deployment)
* Git installed (optional but recommended)

Installation
------------

.. code-block:: bash

   # Create and activate a virtual environment (optional)
   python -m venv .venv
   source .venv/bin/activate

   # Install Lakehouse Plumber CLI and its extras for docs & examples
   pip install lakehouse-plumber


Option 1: Clone the ACME Example Pipeline
-----------------------------------------
#TODO CREATE SCRIPT TO GET THE DATA READY TO USE IN THE EXAMPLE
There is a companion repository that includes a fully working example (TPC-H retail dataset).
Copy its raw-ingestion flow into your new project:

.. code-block:: bash

   git clone https://github.com/Mmodarre/acme_edw.git
   cd acme_edw
   hlp validate --env dev
   hlp generate --env dev

Option 2: Create a new pipeline configuration
---------------------------------------------

Step 1: Project Initialisation
------------------------------

Use the **`lhp init`** command to scaffold a new repo-ready directory structure:

.. code-block:: bash

   lhp init <my_dlt_project>
   cd <my_dlt_project>

The command creates folders such as ``pipelines/``, ``templates/``,
``substitutions/`` and a starter ``lhp.yaml`` project file. It also includes
example template files with ``.tmpl`` extensions that you can use as starting points.

.. note::
   **VS Code IntelliSense**: If you use VS Code, IntelliSense with autocomplete, 
   validation, and documentation is automatically configured! Open any YAML file 
   to see real-time validation and smart suggestions.

Step 2: Edit the project configuration file
-------------------------------------------

Edit the ``lhp.yaml`` file to configure your project.


.. note::

   The ``lhp.yaml`` file is the main entry point for configuring your project.
   for full documentation on the project configuration file, see :doc:`LHP YAML Configuration`.


Step 3: Create your environment configuration file
--------------------------------------------------

Create a ``substitutions/dev.yaml`` file to match your workspace catalog & storage paths.
You can either:

1. **Rename the example template**: ``mv substitutions/dev.yaml.tmpl substitutions/dev.yaml``
2. **Create a new file** following the same structure as the example template

Edit the file to configure tokens such as ``{catalog}`` or ``${secret:scope/key}`` 
that will be replaced during code generation.

.. tip::
   The ``.tmpl`` files created by ``lhp init`` contain working examples that you can
   use as starting points. Simply rename them or copy their content to create your
   working configuration files.

Step 4: Create your first pipeline configuration
------------------------------------------------

Create a new pipeline configuration in the ``pipelines/`` folder.

.. tip::
   **Understanding Pipeline Configuration Structure:**
   
   **Pipeline:** (line 1) specifies the pipeline name that contains this flowgroup. All YAML files sharing the same pipeline name will be organized together in the same directory during code generation.
   
   **Flowgroup:** (line 2) represents a logical grouping of related actions within the pipeline and serves as an organizational construct without impacting runtime behavior.

   **Actions:** (line 4) define the individual operations in the pipeline. They serve as the fundamental components that execute the data processing workflow:
   
      • **Loads** (lines 5-11) customer data from the Databricks samples catalog using Delta streaming
      • **Transforms** (lines 13-27) the raw data by renaming columns and standardizing field names  
      • **Writes** (lines 29-35) the processed data to a bronze layer streaming table
      • **Leverages substitutions** like ``{catalog}`` and ``{bronze_schema}`` for environment flexibility from ``dev.yaml`` file
      • **Implements medallion architecture** by writing to the bronze schema for downstream processing
      • **Enables streaming** with ``readMode: stream`` for incremental read from Delta Change Data Feed (CDF)

.. code-block:: yaml
   :caption: pipelines/customer_sample.yaml
   :linenos:

   pipeline: tpch_sample_ingestion  # Grouping of generated python files in the same folder
   flowgroup: customer_ingestion   # Logical grouping for generated Python file

   actions:
      - name: customer_sample_load     # Unique action identifier
        type: load                     # Action type: Load
        readMode: stream              # Read using streaming CDF
        source:
           type: delta                # Source format: Delta Lake table
           database: "samples.tpch"   # Source database and schema in Unity Catalog
           table: customer_sample     # Source table name
        target: v_customer_sample_raw # Target view name (temporary in-memory)
        description: "Load customer sample table from Databricks samples catalog"

      - name: transform_customer_sample  # Unique action identifier
        type: transform                  # Action type: Transform
        transform_type: sql             # Transform using SQL query
        source: v_customer_sample_raw   # Input view from previous action
        target: v_customer_sample_cleaned  # Output view name
        sql: |
           SELECT
           c_custkey as customer_id,
           c_name as name,
           c_address as address,
           c_nationkey as nation_id,
           c_phone as phone,
           c_acctbal as account_balance,
           c_mktsegment as market_segment,
           c_comment as comment
           FROM stream(v_customer_sample_raw)
        description: "Transform customer sample table"

      - name: write_customer_sample_bronze  # Unique action identifier
        type: write                         # Action type: Write
        source: v_customer_sample_cleaned   # Input view from previous action
        write_target:
           type: streaming_table            # Output as streaming table
           database: "{catalog}.{bronze_schema}"  # Target database.schema with substitutions
           table: "tpch_sample_customer"    # Final table name
        description: "Write customer sample table to bronze schema"


Validate the Configuration
--------------------------

.. code-block:: shell

   # Check for schema errors, missing secrets, circular dependencies …
   lhp validate --env dev

If everything is green you will see **✅ All configurations are valid**.

Generate DLT Code
-----------------

.. code-block:: shell

   # Create Python files in ./generated/ (default output dir)
   lhp generate --env dev

Inspect the Output
------------------

Navigate to ``generated/tpch_sample_ingestion`` each FlowGroup became a Python
file formatted with `black <https://black.readthedocs.io>`_. These are standard
Lakeflow Declarative Pipeline scripts containing you can run in
Databricks or commit to your repository. (Databricks Assest Bundles integration is coming soon...)

**This is the generated python file from the above YAML configuration:**

.. code-block:: python
   :caption: generated/tpch_sample_ingestion/customer_ingestion.py
   :linenos:

   # Generated by LakehousePlumber
   # Pipeline: tpch_sample_ingestion
   # FlowGroup: customer_ingestion

   import dlt

   # Pipeline Configuration
   PIPELINE_ID = "tpch_sample_ingestion"
   FLOWGROUP_ID = "customer_ingestion"

   # ============================================================================
   # SOURCE VIEWS
   # ============================================================================

   @dlt.view()
   def v_customer_sample_raw():
      """Load customer sample table from Databricks samples catalog"""
      df = spark.readStream \
         .table("samples.tpch.customer_sample")

      return df


   # ============================================================================
   # TRANSFORMATION VIEWS
   # ============================================================================

   @dlt.view(comment="Transform customer sample table")
   def v_customer_sample_cleaned():
      """Transform customer sample table"""
      return spark.sql("""SELECT
   c_custkey as customer_id,
   c_name as name,
   c_address as address,
   c_nationkey as nation_id,
   c_phone as phone,
   c_acctbal as account_balance,
   c_mktsegment as market_segment,
   c_comment as comment
   FROM stream(v_customer_sample_raw)""")


   # ============================================================================
   # TARGET TABLES
   # ============================================================================

   # Create the streaming table
   dlt.create_streaming_table(
      name="acmi_edw_dev.edw_bronze.tpch_sample_customer",
      comment="Streaming table: tpch_sample_customer",
      table_properties={"delta.autoOptimize.optimizeWrite": "true", "delta.enableChangeDataFeed": "true"})


   # Define append flow(s)
   @dlt.append_flow(
      target="acmi_edw_dev.edw_bronze.tpch_sample_customer",
      name="f_customer_sample_bronze",
      comment="Write customer sample table to bronze schema"
   )
   def f_customer_sample_bronze():
      """Write customer sample table to bronze schema"""
      # Streaming flow
      df = spark.readStream.table("v_customer_sample_cleaned")

      return df


Deploy on Databricks
--------------------
**Option 1: Manually create a Lakeflow Declarative Pipeline(ETL)**

1. Create a Lakeflow Declarative Pipeline(ETL) in the Databricks UI.

2. Point the *Notebook/Directory* field to your ``generated/`` folder in the
   workspace (or sync the files via Repos).

**OR** (create new python files and paste the generated code into them.)

3. Configure clusters & permissions, then click **Validate**.

**Option 2: Use Asset Bundles**

:doc:`databricks_bundles`


Working with Example Templates
------------------------------

When you run ``lhp init``, several example template files are created to help you get started:

**Configuration Examples:**
   - ``substitutions/dev.yaml.tmpl`` - Example environment configuration with common substitution variables
   - ``substitutions/prod.yaml.tmpl`` - Production environment example
   - ``substitutions/tst.yaml.tmpl`` - Test environment example

**Pipeline Examples:**
   - ``pipelines/01_raw_ingestion/`` - Complete ingestion pipeline examples for various data formats
   - ``pipelines/02_bronze/`` - Bronze layer transformation examples
   - ``pipelines/03_silver/`` - Silver layer examples with data quality

**Preset Examples:**
   - ``presets/bronze_layer.yaml.tmpl`` - Reusable bronze layer configuration template

**Template Examples:**
   - ``templates/standard_ingestion.yaml.tmpl`` - Standard ingestion pattern template

To use these examples:

1. **Copy and rename** template files: ``cp substitutions/dev.yaml.tmpl substitutions/dev.yaml``
2. **Edit the copied files** to match your environment and requirements
3. **Use them as references** when creating your own configurations
4. **Explore the comprehensive examples** in the ``pipelines/`` directory for different data ingestion patterns

.. note::
   The ``.tmpl`` files are static examples containing LHP template syntax. They are not 
   Jinja2 templates for the init command, but rather complete working examples that you 
   can use as starting points for your own configurations.

Next Steps
----------

* Explore **Presets** and **Templates** to reduce duplication.
* Add **data-quality expectations** to your transforms.
* Add **operational metadata** to your actions.
* Add **Schema Hints** to your Load actions.
* Enable **Change-Data-Feed (CDC)** in bronze ingestions.
* Continue reading the :doc:`concepts` section for deeper architectural details. 