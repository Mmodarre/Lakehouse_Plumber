Dependency Analysis & Job Generation
=====================================

The Dependency Analysis feature automatically analyzes your pipeline structure to understand 
data flow dependencies, execution order, and external data sources. This enables intelligent 
orchestration job generation for Databricks.

.. contents:: Page Outline
   :depth: 2
   :local:

Overview
--------

Lakehouse Plumber analyzes your FlowGroup YAML files to build a comprehensive dependency graph that shows:

- **Pipeline Dependencies**: Which pipelines depend on others
- **Execution Stages**: The optimal order for running pipelines
- **External Sources**: Data dependencies outside your LHP project
- **Parallel Opportunities**: Pipelines that can run simultaneously

This analysis powers orchestration job generation, enabling you to create Databricks jobs 
with proper task dependencies automatically.

**When to Use Dependency Analysis**

==================== ================================================================
Use Case             Description
==================== ================================================================
**Development**      Understand your pipeline architecture and data flow
**Validation**       Validate project structure for consistency
**Job Generation**   Create orchestration jobs with proper dependencies
**CI/CD**            Optimize build and deployment order
==================== ================================================================

Key Concepts
------------

Pipeline Dependencies
~~~~~~~~~~~~~~~~~~~~~

Dependencies are automatically detected by analyzing:

- **Table References**: SQL queries that reference tables from other pipelines
- **Python Functions**: Custom transformations that read from pipeline outputs
- **CDC Snapshots**: Slowly Changing Dimension patterns with source functions

External Sources
~~~~~~~~~~~~~~~~

External sources are data dependencies **outside** your LHP-managed pipelines:

- Source system tables (e.g., ``{catalog}.{migration_schema}.customers``)
- Legacy data sources (e.g., ``{catalog}.{old_schema}.orders``)
- Third-party data feeds

.. note::
   Internal pipeline outputs are **not** considered external sources - they're managed 
   dependencies within your LHP project.

Execution Stages
~~~~~~~~~~~~~~~~

Pipelines are organized into execution stages based on their dependencies:

+----------+---------------------------+----------------------------------------+
| Stage    | Pipelines                 | Dependencies                           |
+==========+===========================+========================================+
| Stage 1  | ``raw_ingestion``         | External sources only                  |
+----------+---------------------------+----------------------------------------+
| Stage 2  | ``bronze_layer``          | Depends on Stage 1                     |
+----------+---------------------------+----------------------------------------+
| Stage 3  | ``silver_layer``          | Depends on Stage 2                     |
+----------+---------------------------+----------------------------------------+
| Stage 4  | ``gold_layer``            | Depends on Stage 3                     |
+----------+---------------------------+----------------------------------------+

Pipelines within the same stage can run in **parallel**.

Using the deps Command
----------------------

The ``lhp deps`` command provides comprehensive dependency analysis with multiple output formats.

**Basic Usage**

.. code-block:: bash

   # Full analysis with all formats
   lhp deps

   # Generate only orchestration job
   lhp deps --format job --job-name my_etl_job

   # Analyze specific pipeline
   lhp deps --pipeline bronze_layer --format json

   # Custom output directory
   lhp deps --output /path/to/analysis --verbose

Command Options
~~~~~~~~~~~~~~~

.. code-block:: text

   lhp deps [OPTIONS]

**Options:**

``--format, -f``
    Output format(s): ``dot``, ``json``, ``text``, ``mermaid``, ``job``, ``all`` (default: ``all``)

    - ``dot``: GraphViz diagram for visualization
    - ``json``: Structured data for programmatic use
    - ``text``: Human-readable analysis report
    - ``mermaid``: Mermaid diagram for documentation
    - ``job``: Databricks orchestration job YAML
    - ``all``: Generate all formats

``--job-name, -j``
    Custom name for generated orchestration job (only with ``job`` format)

``--job-config, -jc``
    Path to job configuration file

``--output, -o``
    Output directory (defaults to ``.lhp/dependencies/``)

``--pipeline, -p``
    Analyze specific pipeline only

``--bundle-output``
    Save job file directly to ``resources/`` directory

``--verbose, -v``
    Enable verbose output with detailed logging

Output Formats
--------------

Text Report
~~~~~~~~~~~

Human-readable analysis showing pipeline details, execution order, and dependency tree:

.. code-block:: text

   ================================================================================
   LAKEHOUSE PLUMBER - PIPELINE DEPENDENCY ANALYSIS
   ================================================================================
   Generated at: 2025-09-25 12:50:59

   SUMMARY
   ----------------------------------------
   Total Pipelines: 7
   Total Execution Stages: 6
   External Sources: 7
   Circular Dependencies: 0

   EXECUTION ORDER
   ----------------------------------------
   Stage 1: unirate_api_ingestion, acmi_edw_raw (can run in parallel)
   Stage 2: acmi_edw_bronze
   Stage 3: acmi_edw_silver
   Stage 4: acmi_edw_gold

JSON Data
~~~~~~~~~

Structured data perfect for integration with other tools:

.. code-block:: json

   {
     "metadata": {
       "total_pipelines": 7,
       "total_external_sources": 7,
       "total_stages": 6,
       "has_circular_dependencies": false
     },
     "pipelines": {
       "acmi_edw_bronze": {
         "depends_on": ["acmi_edw_raw"],
         "flowgroup_count": 14,
         "action_count": 80,
         "external_sources": [
           "{catalog}.{migration_schema}.customers"
         ],
         "stage": 1
       }
     },
     "execution_stages": [
       ["unirate_api_ingestion", "acmi_edw_raw"],
       ["acmi_edw_bronze"],
       ["acmi_edw_silver"]
     ]
   }

GraphViz Diagram
~~~~~~~~~~~~~~~~

DOT format for creating visual dependency diagrams:

.. code-block:: text

   digraph pipeline_dependencies {
     rankdir=LR;
     node [shape=box];
     "acmi_edw_raw" [label="acmi_edw_raw\n(16 flowgroups)"];
     "acmi_edw_bronze" [label="acmi_edw_bronze\n(14 flowgroups)"];
     "acmi_edw_raw" -> "acmi_edw_bronze";
   }

.. tip::
   Use tools like Graphviz or online DOT viewers to visualize your pipeline dependencies as diagrams.

Mermaid Diagram
~~~~~~~~~~~~~~~

Mermaid format for embedding in documentation:

.. code-block:: text

   flowchart TD
       raw_ingestion[raw_ingestion]
       bronze_layer[bronze_layer]
       silver_layer[silver_layer]
       
       raw_ingestion --> bronze_layer
       bronze_layer --> silver_layer

Orchestration Job Generation
----------------------------

The most powerful feature is automatic **orchestration job generation**. This creates a 
Databricks job YAML file with proper task dependencies based on your pipeline analysis.

Generating Jobs
~~~~~~~~~~~~~~~

.. code-block:: bash

   # Generate job with custom name
   lhp deps --format job --job-name data_warehouse_etl

   # Generate job and save directly to resources/
   lhp deps --format job --job-name data_warehouse_etl --bundle-output

   # Generate with custom configuration
   lhp deps --format job --job-config config/job_config.yaml --bundle-output

Generated Job Structure
~~~~~~~~~~~~~~~~~~~~~~~

The generated job YAML follows Databricks Asset Bundle format:

.. code-block:: yaml
   :caption: resources/data_warehouse_etl.job.yml

   resources:
     jobs:
       data_warehouse_etl:
         name: data_warehouse_etl
         max_concurrent_runs: 1
         tasks:
           - task_key: acmi_edw_raw_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.acmi_edw_raw_pipeline.id}
               full_refresh: false

           - task_key: acmi_edw_bronze_pipeline
             depends_on:
               - task_key: acmi_edw_raw_pipeline
             pipeline_task:
               pipeline_id: ${resources.pipelines.acmi_edw_bronze_pipeline.id}
               full_refresh: false

         queue:
           enabled: true
         performance_target: STANDARD

Key Features
~~~~~~~~~~~~

**Automatic Task Dependencies**
    Tasks are linked with ``depends_on`` clauses based on pipeline dependencies

**Pipeline Resource References**
    Uses ``${resources.pipelines.{name}_pipeline.id}`` for proper bundle integration

**Parallel Execution**
    Pipelines in the same stage have no interdependencies and can run in parallel

**Configurable Options**
    Customize with job configuration files (see below)

Customizing Job Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Create a ``job_config.yaml`` file to customize job settings:

.. code-block:: yaml
   :caption: config/job_config.yaml

   max_concurrent_runs: 2
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200
   
   queue:
     enabled: true
   
   tags:
     environment: production
     team: data-platform
   
   email_notifications:
     on_start:
       - admin@example.com
     on_success:
       - team@example.com
     on_failure:
       - oncall@example.com
   
   webhook_notifications:
     on_failure:
       - id: pagerduty-webhook
   
   permissions:
     - level: CAN_MANAGE
       user_name: admin@company.com
     - level: CAN_VIEW
       group_name: data-team
   
   schedule:
     quartz_cron_expression: "0 0 8 * * ?"
     timezone_id: America/New_York
     pause_status: UNPAUSED

**Using Custom Config:**

.. code-block:: bash

   # Use custom config file
   lhp deps --format job --job-config config/job_config.yaml --bundle-output

.. seealso::
   For complete job configuration options, see :doc:`databricks_bundles`.

Integration with Databricks Bundles
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The generated job integrates seamlessly with Databricks Asset Bundles:

.. code-block:: bash

   # Generate job directly to resources/
   lhp deps --format job --job-name my_etl --bundle-output
   
   # Deploy with bundle commands
   databricks bundle deploy --target dev
   
   # Run the job
   databricks bundle run my_etl --target dev

Examples
--------

Simple ETL Pipeline
~~~~~~~~~~~~~~~~~~~

For a basic three-tier architecture:

.. code-block:: bash

   lhp deps --format job --job-name etl_pipeline --bundle-output

**Result**: Creates tasks for Raw → Bronze → Silver → Gold with proper dependencies.

**Generated Task Structure:**

.. code-block:: text

   etl_pipeline
   ├── raw_ingestion_pipeline (Stage 1, no dependencies)
   ├── bronze_layer_pipeline (Stage 2, depends on raw)
   ├── silver_layer_pipeline (Stage 3, depends on bronze)
   └── gold_layer_pipeline (Stage 4, depends on silver)

Complex Multi-Source Pipeline
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For pipelines with multiple data sources and parallel processing:

.. code-block:: bash

   lhp deps --format all --job-name multi_source_etl

**Analysis shows:**

- Multiple Stage 1 pipelines (can run in parallel)
- Convergence in later stages
- Proper orchestration of dependent transformations

**Example Output:**

.. code-block:: text

   EXECUTION ORDER
   ----------------------------------------
   Stage 1: api_ingestion, sftp_ingestion, db_ingestion (parallel)
   Stage 2: bronze_consolidation (waits for all Stage 1)
   Stage 3: silver_transformations
   Stage 4: gold_aggregations

Troubleshooting
---------------

Circular Dependencies
~~~~~~~~~~~~~~~~~~~~~

If circular dependencies are detected:

.. code-block:: text

   ERROR: Circular dependencies detected:
   Pipeline A → Pipeline B → Pipeline C → Pipeline A

**Solution**: Review your FlowGroup SQL queries and break the circular reference by:

- Using temporary views instead of direct table references
- Restructuring data flow to eliminate cycles

Missing Dependencies
~~~~~~~~~~~~~~~~~~~~

If expected dependencies aren't detected:

**Check:**

- SQL table references use correct naming patterns
- Python functions properly reference source tables
- CDC snapshot configurations are correctly structured

External Source Issues
~~~~~~~~~~~~~~~~~~~~~~

If too many external sources are detected:

.. code-block:: text

   WARNING: 50 external sources detected

**Review:**

- CTE names aren't being excluded (should be filtered automatically)
- Internal pipeline references are properly formatted
- Template variables are correctly structured

.. important::
   The dependency analyzer only considers table references in SQL queries and Python functions. 
   Complex dynamic table references may not be detected automatically.

CLI Quick Reference
-------------------

.. code-block:: bash

   # Full analysis with all output formats
   lhp deps
   
   # Generate orchestration job
   lhp deps --format job --job-name my_etl
   
   # Save job directly to bundle resources
   lhp deps --format job --job-name my_etl --bundle-output
   
   # Use custom job configuration
   lhp deps -jc config/job_config.yaml --bundle-output
   
   # Analyze specific pipeline
   lhp deps --pipeline bronze_layer --format json
   
   # Generate Mermaid diagram
   lhp deps --format mermaid
   
   # Custom output directory
   lhp deps --output ./analysis --verbose

Related Documentation
---------------------

* :doc:`databricks_bundles` - Bundle integration and configuration
* :doc:`concepts` - Understanding pipelines and flowgroups
* :doc:`cicd_reference` - CI/CD patterns and deployment workflows
* :doc:`cli` - Complete CLI command reference
