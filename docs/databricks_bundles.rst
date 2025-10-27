Databricks Asset Bundles Integration
====================================

This page covers Lakehouse Plumber's integration with Databricks Asset Bundles (DAB),
enabling seamless deployment and management of generated DLT pipelines as bundle resources.

.. contents:: Page Outline
   :depth: 2
   :local:

Overview
--------

**What are Databricks Asset Bundles?**

Databricks Asset Bundles (DAB) provide a unified way to deploy and manage Databricks 
resources like jobs, pipelines, and notebooks using declarative YAML configuration. 
Bundles enable version control, environment management, and CI/CD integration for 
your entire Databricks workspace.

If you are not familiar with DABs, please refer to the `Databricks Asset Bundles documentation <https://docs.databricks.com/en/dev-tools/bundles/index.html>`_



Prerequisites & Setup
---------------------

**Databricks CLI Requirements**

Install and configure the Databricks CLI:

.. note::
    Follow the steps here to install `Databricks CLI <https://docs.databricks.com/en/dev-tools/cli/index.html>`_

.. code-block:: bash
  
   # Configure authentication
   databricks configure --token
   
   # Verify connection
   databricks workspace list

Getting Started
---------------

**Enabling Bundle Support**

Bundle support is automatically enabled when LHP detects a ``databricks.yml`` file:

.. code-block:: bash

   # Check if bundle support is active
   lhp generate -e dev
   # Look for: "🔄 Syncing bundle resources with generated files..."

**First Bundle Deployment**

1. **Initialize LHP project with bundle support**

.. code-block:: bash


   lhp init --bundle my-data-platform
   cd my-data-platform

.. note::
  With the ``--bundle`` flag, LHP will automatically create a ``databricks.yml`` file
  and the ``resources/lhp`` directory.

3. **Edit the databricks.yml file**

Edit ``databricks.yml`` file to add your Databricks workspace details and your username/email address

.. note::
  * You can edit the ``databricks.yml`` file to add as many targets as you wish.
  * Also feel free to customize the ``databricks.yml`` file to your needs.

.. seealso::
  Refer to Databricks official documentation for more information on how to configure the ``databricks.yml`` file:
  `Databricks Documentation <https://docs.databricks.com/aws/en/dev-tools/bundles/templates#configuration-templates>`_



1. **Create your first pipeline and flowgroup**

Please see :doc:`getting_started` to create your first LHP flowgroup and pipeline.

5. **Generate**

.. code-block:: bash

  lhp generate -e dev 


6. **Verifying Bundle Integration**

After running ``lhp generate``, you should see:

.. code-block:: bash

   🔄 Syncing bundle resources with generated files...
   ✅ Updated 1 bundle resource file(s)

.. important::
  When generating code, LHP looks for a ``databricks.yml`` file in your project root.
  If found, LHP will generate pipeline ``YAML`` files inside the ``resources/lhp/`` directory.
  If not, only the Python files will be generated.

Check the generated resource file:

.. code-block:: bash

   cat resources/lhp/raw_ingestion.pipeline.yml


7. **Validate and deploy the bundle to Databricks**:

.. code-block:: bash

   # Validate bundle configuration
   databricks bundle validate --target dev


.. code-block:: bash
   
   # Deploy bundle to Databricks
   databricks bundle deploy --target dev


.. code-block:: bash
   
   # Verify deployment
   databricks bundle status --target dev




How LHP Integrates with DABs
----------------------------

Lakehouse Plumber does NOT replace Databricks Asset Bundles or Databricks CLI. 
It only generates the pipeline ``YAML`` files for DABs to use.


LHP will:

* **Generate resource YAML files** for each pipeline in the ``resources/`` directory
* **Synchronize resource files** with generated Python notebooks automatically
* **Maintain resource file consistency** by cleaning up obsolete resources

**Benefits of Using Bundles with LHP**

* **Unified Deployment**: Deploy pipelines, jobs, and configurations together 
* **Environment Management**: Separate dev/staging/prod configurations  
* **Version Control**: Track resource changes alongside pipeline code  
* **CI/CD Integration**: Automated deployments through Databricks CLI  
* **Resource Cleanup**: Automatic cleanup of deleted pipelines

**LHP Bundle Integration Flow**

The following diagram illustrates how LHP integrates with Databricks Asset Bundles:

.. mermaid::

   flowchart TD
       A["📁 pipelines/<br/>YAML Configurations"] --> B["🔧 LHP Process"]
       B --> C["📖 Read Pipeline<br/>YAML Files"]
       C --> D["🐍 Generate<br/>Python Code"]
       D --> E["📂 generated/<br/>Python Files"]
       E --> F["🔍 Check Bundle<br/>Support"]
       F --> G["📁 resources/lhp/<br/>Directory"]
       G --> H["📝 Create/Update<br/>Resource YAML"]
       H --> I["📋 resources/lhp/<br/>Pipeline Resources"]
       
       style A fill:#e1f5fe
       style E fill:#f3e5f5
       style I fill:#e8f5e8
       style B fill:#fff3e0

When you run ``lhp generate``, this automated flow ensures your pipeline resources 
stay synchronized with your generated Python code while safely preserving any 
custom bundle resources you've created.



Project Structure
-----------------

Your project should have this structure:

.. code-block:: text
  

   my-data-platform/
   ├── databricks.yml          # Bundle configuration
   ├── lhp.yaml                # LHP project config
   ├── pipelines/              # LHP pipeline definitions
   │   ├── raw_ingestion/
   │   └── bronze_layer/
   ├── resources/              # Bundle resources
   │   ├── lhp/                # LHP-managed resource files (Do NOT modify)
   │   │   ├── raw_ingestion.pipeline.yml
   │   │   └── bronze_layer.pipeline.yml
   │   └── user_custom.pipeline.yml  # User's custom DAB files
   └── generated/              # Generated Python files (Do NOT modify)
       ├── raw_ingestion/
       └── bronze_layer/

.. note::
  **Coexistence with User DAB Files**
  
  LHP manages its resource files in the ``resources/lhp/`` subdirectory, allowing you to 
  safely place your own Databricks Asset Bundle files in the ``resources/`` directory.
  LHP will only manage files it generates under ``resources/lhp/`` directory 
  and those with the ``"Generated by LakehousePlumber"`` header
  and will never modify or delete your custom DAB files.


.. warning::
  * Any DAB ``yml`` files located under the ``resources/lhp`` directory that contain
    the ``"Generated by LakehousePlumber"`` header
    will be automatically overwritten by LHP.

  * To maintain clarity and avoid confusion, we strongly advise against
    making manual changes to files within the ``resources/lhp/`` directory.



Bundle Resource Synchronization
-------------------------------

**How Resource Sync Works**

When bundle support is enabled, LHP automatically:

1. **Generates resource YAML files** using Jinja2 templates for each pipeline
2. **Uses glob patterns** to automatically discover all files in pipeline directories
3. **Removes obsolete resource files** for deleted pipelines
4. **Maintains environment-specific configurations**
  
.. important::
  * LHP will not edit the ``databricks.yml`` file.
  * It will only create the pipeline ``YAML`` files in the ``resources/lhp/`` directory.
  * You can edit the ``databricks.yml`` file to add your Databricks needs.

**Generated Resource YAML Files**

.. code-block:: yaml
  :linenos:
  :caption: resources/lhp/bronze_load.pipeline.yml
  
  # Generated by LakehousePlumber - Bundle Resource for bronze_load
  resources:
    pipelines:
      bronze_load_pipeline:
        name: bronze_load_pipeline
        catalog: main
        schema: lhp_${bundle.target}
        
        libraries:
          - glob:
              include: ../../generated/bronze_load/**
        
        root_path: ${workspace.file_path}/generated/bronze_load/
        
        configuration:
          bundle.sourcePath: ${workspace.file_path}/generated

DABs and Lakeflow Declarative Pipelines limitations
---------------------------------------------------

  **❗ Why does LHP NOT use Notebooks as the source for the pipelines?!**

  * Lakeflow pipelines now use Python files as their source. Using notebooks as pipeline sources is the legacy approach and is now discouraged.
  
  * LHP now uses glob patterns in bundle resource files to automatically discover all Python files in pipeline directories,
    eliminating the need to list individual notebook paths.

  * This approach provides better maintainability and automatically includes new files added to pipeline directories
    without requiring resource file updates.


CLI Commands & Workflows
------------------------

**lhp init --bundle**

Initialize a new LHP project with bundle support:

.. code-block:: bash

   lhp init --bundle my-project
   
   # Creates:
   # ├── databricks.yml                    # Bundle configuration
   # ├── lhp.yaml                          # LHP project configuration  
   # ├── README.md                         # Project documentation
   # ├── .gitignore                        # Git ignore rules
   # ├── .vscode/                          # VS Code integration
   # │   ├── settings.json                 # LHP syntax highlighting
   # │   └── schemas/                      # JSON schemas for IntelliSense
   # ├── pipelines/                        # Comprehensive pipeline examples
   # │   ├── 01_raw_ingestion/            # Data ingestion examples
   # │   ├── 02_bronze/                   # Bronze layer examples  
   # │   └── 03_silver/                   # Silver layer examples
   # ├── resources/                        # Bundle resources
   # │   └── lhp/                         # Generated DLT files location
   # ├── substitutions/                    # Environment configurations
   # │   ├── dev.yaml.tmpl                # Development environment example
   # │   ├── prod.yaml.tmpl               # Production environment example
   # │   └── tst.yaml.tmpl                # Test environment example
   # ├── presets/                          # Reusable configuration presets
   # │   └── bronze_layer.yaml.tmpl       # Bronze layer preset example
   # ├── templates/                        # Custom template examples
   # │   └── standard_ingestion.yaml.tmpl # Standard ingestion template
   # ├── expectations/                     # Data quality examples
   # │   └── customer_quality.json.tmpl   # Data quality expectations
   # └── schemas/                          # Schema definitions
   #     └── customer_schema.yaml.tmpl    # Schema definition example


.. tip::
  
  💡 The ``lhp init`` command creates a set of examples files with .tmpl extensions.
  
  * You can use these examples as starting points for your own configurations.
  * You can also create your own files and use them as starting points for your own configurations.

**lhp generate (with bundle sync)**

Generate Python files and automatically sync bundle resources:

.. code-block:: bash

   # Generate for specific environment
   lhp generate -e dev 
   
   # Force regeneration (ignores state)
   lhp generate -e dev --force
   
   # Disable bundle sync (if needed)
   lhp generate -e dev  --no-bundle



**Environment Targeting**

Bundle targets enable environment-specific deployments:

.. code-block:: bash

   # Deploy to different environments
   databricks bundle deploy --target dev
   databricks bundle deploy --target staging  
   databricks bundle deploy --target prod
   
   # Each target can have different:
   # - Workspace locations
   # - Cluster configurations
   # - Catalog names
   # - Permission settings

Advanced Topics
---------------

**Bundle Sync Behavior**

Bundle synchronization happens automatically after successful generation:

* **Triggers**: After ``lhp generate`` completes successfully
* **Scope**: Processes all generated Python files in output directory
* **Cleanup**: Removes resource files for deleted/excluded pipelines
* **Idempotent**: Safe to run multiple times

**Performance Considerations**

For large projects with many pipelines:

* **Incremental sync**: Only updates changed pipelines
* **Parallel generation**: Use ``lhp generate --parallel`` for faster processing
* **State management**: Leverage ``.lhp_state.json`` for smart regeneration

**Troubleshooting Common Issues**

**Bundle sync not running:**

.. code-block:: bash

   # Verify databricks.yml exists
   ls -la databricks.yml
   
   # Check bundle detection
   lhp generate -e dev --verbose

**Resource files not created:**

.. code-block:: bash

   # Ensure generated directory exists
   ls -la generated/
   
   # Check Python file generation
   lhp generate -e dev --force

**Bundle deployment failures:**

.. code-block:: bash

   # Validate bundle configuration
   databricks bundle validate --target dev
   
   # Check resource file syntax
   yamllint resources/*.yml


**Multi-Environment Setup**

Configure multiple environments with different settings:

.. code-block:: yaml
   :caption: databricks.yml (multi-environment)

   bundle:
     name: acmi-data-platform
   
   targets:
     dev:
       workspace:
         host: https://dev-workspace.cloud.databricks.com
         root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
       variables:
         catalog: "acmi_dev"
         cluster_node_type: "i3.xlarge"
         cluster_workers: 1
     
     staging:
       workspace:
         host: https://staging-workspace.cloud.databricks.com
         root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
       variables:
         catalog: "acmi_staging"
         cluster_node_type: "i3.xlarge" 
         cluster_workers: 2
     
     prod:
       workspace:
         host: https://prod-workspace.cloud.databricks.com
         root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
       variables:
         catalog: "acmi_prod"
         cluster_node_type: "i3.2xlarge"
         cluster_workers: 4

**CI/CD Integration**

GitHub Actions workflow for automated bundle deployment:

.. code-block:: yaml
   :caption: .github/workflows/deploy.yml

   name: Deploy Data Platform
   
   on:
     push:
       branches: [main]
     pull_request:
       branches: [main]
   
   jobs:
     validate:
       runs-on: ubuntu-latest
       strategy:
         matrix:
           environment: [dev, staging, prod]
       
       steps:
         - uses: actions/checkout@v3
         
         - name: Setup Python
           uses: actions/setup-python@v4
           with:
             python-version: '3.10'
         
         - name: Install dependencies
           run: |
             pip install lakehouse-plumber databricks-cli
         
         - name: Generate pipeline code
           run: lhp generate -e ${{ matrix.environment }}
         
         - name: Validate bundle
           run: databricks bundle validate --target ${{ matrix.environment }}
           env:
             DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}
     
     deploy:
       needs: validate
       runs-on: ubuntu-latest
       if: github.ref == 'refs/heads/main'
       
       steps:
         - uses: actions/checkout@v3
         
         - name: Deploy to production
           run: |
             pip install lakehouse-plumber databricks-cli
             lhp generate -e prod
             databricks bundle deploy --target prod
           env:
             DATABRICKS_TOKEN: ${{ secrets.DATABRICKS_TOKEN }}

Reference
---------

**Bundle Sync Options**

================== ==========================================================
Option             Description
================== ==========================================================
``--no-bundle``    Disable bundle support even if databricks.yml exists
``--force``        Force regeneration and bundle sync of all files
``--cleanup``      Clean up obsolete resource files
================== ==========================================================

**Resource File Structure**

Generated resource files follow this pattern:

.. code-block:: text

   resources/
   ├── {pipeline_name}.pipeline.yml    # One file per pipeline
   └── ...

Each resource file contains:

* **Pipeline configuration**: DLT pipeline settings and metadata
* **Cluster settings**: Compute configuration for the pipeline
* **Glob patterns**: Automatic discovery of all Python files in pipeline directories
* **Root path**: Base directory for pipeline execution
* **Environment variables**: Integration with bundle variables

**Customizing Pipeline Configuration**

LHP supports customizing DLT pipeline settings via a YAML configuration file. This allows you to control compute resources, notifications, tags, and other pipeline properties.

*Configuration File Format*

Create a multi-document YAML file with project-level defaults and per-pipeline overrides:

.. code-block:: yaml

   # Project-level defaults (applied to all pipelines)
   project_defaults:
     serverless: true
     edition: ADVANCED
     channel: CURRENT
     continuous: false
   
   ---
   # Pipeline-specific configuration
   pipeline: bronze_load
   serverless: false
   continuous: true
   clusters:
     - label: default
       node_type_id: Standard_D16ds_v5
       autoscale:
         min_workers: 2
         max_workers: 10
   
   ---
   pipeline: silver_load
   serverless: true
   notifications:
     - email_recipients:
         - team@company.com
       alerts:
         - on-update-failure

*Configuration Options*

======================= =================== =====================================================
Option                  Type                Description
======================= =================== =====================================================
``serverless``          boolean             Use serverless compute (default: ``true``)
``edition``             string              DLT edition: ``CORE``, ``PRO``, or ``ADVANCED``
``channel``             string              Runtime channel: ``CURRENT`` or ``PREVIEW``
``continuous``          boolean             Enable continuous processing (streaming)
``photon``              boolean             Enable Photon engine (non-serverless only)
``clusters``            list                Cluster configurations for non-serverless pipelines
``notifications``       list                Email notifications and alert settings
``tags``                dict                Custom tags for the pipeline
``event_log``           dict                Event logging configuration
======================= =================== =====================================================

*Usage*

Specify the config file path when generating pipelines:

.. code-block:: bash

   lhp generate --env dev --pipeline-config templates/bundle/pipeline_config.yaml

Or use the short flag:

.. code-block:: bash

   lhp generate --env dev -pc templates/bundle/pipeline_config.yaml

*Configuration Precedence*

Configurations are merged in the following order (later overrides earlier):

1. **Default values**: Built-in LHP defaults (``serverless: true``, ``edition: ADVANCED``)
2. **Project defaults**: Values from the ``project_defaults`` section
3. **Pipeline-specific**: Values from pipeline-specific sections (highest priority)

*List Merge Behavior*

Lists (like ``notifications`` and ``clusters``) are replaced entirely, not appended:

.. code-block:: yaml

   project_defaults:
     notifications:
       - email_recipients: [admin@company.com]
   
   ---
   pipeline: my_pipeline
   notifications:
     - email_recipients: [team@company.com]  # Replaces, not appends

*Validation*

LHP validates the following configuration values:

* ``edition``: Must be ``CORE``, ``PRO``, or ``ADVANCED``
* ``channel``: Must be ``CURRENT`` or ``PREVIEW``

Other configuration structures (clusters, notifications, etc.) are passed through without validation for flexibility.

*Example: Production Configuration*

.. code-block:: yaml

   # templates/bundle/pipeline_config.yaml
   project_defaults:
     serverless: true
     edition: ADVANCED
     channel: CURRENT
     notifications:
       - email_recipients:
           - data-team@company.com
         alerts:
           - on-update-failure
   
   ---
   pipeline: raw_ingestions
   serverless: false
   continuous: true
   clusters:
     - label: default
       node_type_id: Standard_D16ds_v5
       autoscale:
         min_workers: 2
         max_workers: 10
         mode: ENHANCED
     policy_id: memory-intensive-policy
   
   ---
   pipeline: gold_analytics
   tags:
     layer: gold
     criticality: high
     sla: tier_1
   notifications:
     - email_recipients:
         - analytics@company.com
         - executives@company.com
       alerts:
         - on-update-success
         - on-update-failure

*Best Practices*

* **Use project defaults** for common settings across all pipelines
* **Serverless by default** for cost efficiency and scalability
* **Non-serverless for streaming** when you need dedicated resources or specific node types
* **Environment-specific configs** can be managed by having different config files (e.g., ``pipeline_config_dev.yaml``, ``pipeline_config_prod.yaml``)
* **Version control** your config files alongside your pipeline definitions

**Troubleshooting Guide**

===================================== ================================================================
Issue                                  Solution
===================================== ================================================================
Bundle sync not triggered             Ensure ``databricks.yml`` exists in project root
Resource files not generated          Check generated Python files exist and are valid
Bundle validation fails               Verify YAML syntax in generated resource files  
Deployment permission errors          Check workspace permissions and bundle target paths
Obsolete resources not cleaned up     Run ``lhp generate --force`` to trigger full sync
===================================== ================================================================


Dependency Analysis & Databricks Lakeflow Jobs Generation
---------------------------------------------------------

The **Dependency Analysis** feature in Lakehouse Plumber automatically analyzes your pipeline structure to understand data flow dependencies, execution order, and external data sources. This enables intelligent orchestration job generation and pipeline optimization.

Overview
~~~~~~~~

Lakehouse Plumber analyzes your FlowGroup YAML files to build a comprehensive dependency graph that shows:

- **Pipeline Dependencies**: Which pipelines depend on others
- **Execution Stages**: The optimal order for running pipelines
- **External Sources**: Data dependencies outside your LHP project
- **Parallel Opportunities**: Pipelines that can run simultaneously

This analysis powers orchestration job generation, enabling you to create Lakeflow jobs job with proper task(pipelines) dependencies automatically.

Key Concepts
~~~~~~~~~~~~

Pipeline Dependencies
^^^^^^^^^^^^^^^^^^^^^

Dependencies are automatically detected by analyzing:

- **Table References**: SQL queries that reference tables from other pipelines
- **Python Functions**: Custom transformations that read from pipeline outputs
- **CDC Snapshots**: Slowly Changing Dimension patterns with source functions

External Sources
^^^^^^^^^^^^^^^^

External sources are data dependencies **outside** your LHP-managed pipelines:

- Source system tables (e.g., ``{catalog}.{migration_schema}.customers``)
- Legacy data sources (e.g., ``{catalog}.{old_schema}.orders``)
- Third-party data feeds

.. note::
   Internal pipeline outputs are **not** considered external sources - they're managed dependencies within your LHP project.

Execution Stages
^^^^^^^^^^^^^^^^

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

Pipelines within the same stage can potentially run in **parallel**.

Using the deps Command
~~~~~~~~~~~~~~~~~~~~~~

The ``lhp deps`` command provides comprehensive dependency analysis with multiple output formats:

.. code-block:: bash

   # Basic analysis with all formats
   lhp deps

   # Generate only orchestration job
   lhp deps --format job --job-name my_etl_job

   # Analyze specific pipeline
   lhp deps --pipeline bronze_layer --format json

   # Custom output directory
   lhp deps --output /path/to/analysis --verbose

Command Options
^^^^^^^^^^^^^^^

.. code-block:: bash

   lhp deps [OPTIONS]

**Options:**

``--format, -f``
    Output format(s): ``dot``, ``json``, ``text``, ``job``, ``all`` (default: ``all``)

    - ``dot``: GraphViz diagram for visualization
    - ``json``: Structured data for programmatic use
    - ``text``: Human-readable analysis report
    - ``job``: Databricks orchestration job YAML
    - ``all``: Generate all formats

``--job-name, -j``
    Custom name for generated orchestration job (only used with ``job`` format)

``--output, -o``
    Output directory (defaults to ``.lhp/dependencies/``)

``--pipeline, -p``
    Analyze specific pipeline only

``--verbose, -v``
    Enable verbose output with detailed logging

Output Formats
~~~~~~~~~~~~~~

Text Report
^^^^^^^^^^^

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
   Stage 5: gold_load
   Stage 6: acmi_edw_tests

JSON Data
^^^^^^^^^

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
           "{catalog}.{migration_schema}.customers",
           "{catalog}.{migration_schema}.orders"
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
^^^^^^^^^^^^^^^^

DOT format for creating visual dependency diagrams:

.. code-block:: dot

   digraph pipeline_dependencies {
     rankdir=LR;
     node [shape=box];
     "acmi_edw_raw" [label="acmi_edw_raw\n(16 flowgroups)"];
     "acmi_edw_bronze" [label="acmi_edw_bronze\n(14 flowgroups)"];
     "acmi_edw_raw" -> "acmi_edw_bronze";
   }

.. tip::
   Use tools like Graphviz or online DOT viewers to visualize your pipeline dependencies as diagrams.

Orchestration Job Generation
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The most powerful feature is automatic **orchestration job generation**. This creates a Databricks job YAML file with proper task dependencies based on your pipeline analysis.

Generating Jobs
^^^^^^^^^^^^^^^

.. code-block:: bash

   # Generate job with custom name
   lhp deps --format job --job-name data_warehouse_etl

   # Generate job with default name (project_orchestration)
   lhp deps --format job

Generated Job Structure
^^^^^^^^^^^^^^^^^^^^^^^

The generated job YAML follows Databricks Asset Bundle format:

.. code-block:: yaml
   :caption: data_warehouse_etl.job.yml
   :linenos:

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
^^^^^^^^^^^^

**Automatic Task Dependencies**
    Tasks are linked with ``depends_on`` clauses based on pipeline dependencies

**Pipeline Resource References**
    Uses ``${resources.pipelines.{name}_pipeline.id}`` for proper bundle integration

**Parallel Execution**
    Pipelines in the same stage have no interdependencies and can run in parallel

**Configurable Options**
    Includes commented examples for timeouts, notifications, schedules, and permissions

Customizing Job Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

You can customize job-level configuration (like concurrency limits, notifications, schedules) by providing a custom configuration file.

**Creating a Custom Config File**

Create a YAML file at ``templates/bundle/job_config.yaml`` in your project:

.. code-block:: yaml
   :caption: templates/bundle/job_config.yaml

   max_concurrent_runs: 2
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200
   
   queue:
     enabled: true
   
   tags:
     environment: production
     team: data-platform
     cost_center: analytics
   
   email_notifications:
     on_start:
       - admin@example.com
     on_success:
       - team@example.com
     on_failure:
       - oncall@example.com
       - alerts@example.com
   
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

**Using Custom Config**

.. code-block:: bash

   # Use default config location (templates/bundle/job_config.yaml)
   lhp deps --format job --job-name my_etl

   # Use custom config file path
   lhp deps --format job --job-config custom_job_config.yaml

**Available Configuration Options**

.. list-table::
   :header-rows: 1
   :widths: 30 20 50

   * - Option
     - Default
     - Description
   * - ``max_concurrent_runs``
     - ``1``
     - Maximum number of concurrent job runs
   * - ``performance_target``
     - ``STANDARD``
     - ``STANDARD`` or ``PERFORMANCE_OPTIMIZED``
   * - ``queue.enabled``
     - ``true``
     - Enable job queueing
   * - ``timeout_seconds``
     - None
     - Job-level timeout in seconds
   * - ``tags``
     - None
     - Key-value pairs for job tags
   * - ``email_notifications``
     - None
     - Email alerts (on_start, on_success, on_failure)
   * - ``webhook_notifications``
     - None
     - Webhook alerts (on_start, on_success, on_failure)
   * - ``permissions``
     - None
     - Job access permissions
   * - ``schedule``
     - None
     - Cron schedule configuration

**Merge Behavior**

- User config values **override** defaults
- If a key is not specified, the **default value** is used
- You can add optional fields (like notifications) not in defaults

Integration with Databricks Bundles
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

The generated job works seamlessly with Databricks Asset Bundles:

**Option 1: Manual Placement**

1. **Generate the job** in the default location
2. **Manually copy** to bundle resources directory

   .. code-block:: bash

      lhp deps --format job --job-name my_etl
      # Job created in .lhp/dependencies/my_etl.job.yml
      # Manually copy to resources/

**Option 2: Direct Bundle Output** (Recommended)

Use the ``--bundle-output`` flag to save directly to the ``resources/`` directory:

.. code-block:: bash

   # Save job file directly to resources/ directory
   lhp deps --format job --job-name my_etl --bundle-output

   # With custom config
   lhp deps --format job --job-name my_etl --job-config my_config.yaml --bundle-output

This creates ``resources/my_etl.job.yml`` ready for bundle deployment.

**Deploy with bundle commands**:

.. code-block:: bash

   databricks bundle deploy --target dev
   databricks bundle run my_etl --target dev

3. **Monitor in Databricks UI** - The job appears in your workspace with proper task dependencies

Dependency Analysis Examples
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Simple ETL Pipeline
^^^^^^^^^^^^^^^^^^^

For a basic three-tier architecture:

.. code-block:: bash

   lhp deps --format job --job-name etl_pipeline

**Output**: Creates tasks for Raw → Bronze → Silver → Gold with proper dependencies.

Complex Multi-Source Pipeline
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

For pipelines with multiple data sources and parallel processing:

.. code-block:: bash

   lhp deps --format all --job-name multi_source_etl

**Analysis shows**:
- Multiple Stage 1 pipelines (can run in parallel)
- Convergence in later stages
- Proper orchestration of dependent transformations

Dependency Analysis Troubleshooting
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Circular Dependencies
^^^^^^^^^^^^^^^^^^^^^

If circular dependencies are detected:

.. code-block:: text

   ERROR: Circular dependencies detected:
   Pipeline A → Pipeline B → Pipeline C → Pipeline A

**Solution**: Review your FlowGroup SQL queries and break the circular reference by:
- Using temporary views instead of direct table references
- Restructuring data flow to eliminate cycles

Missing Dependencies
^^^^^^^^^^^^^^^^^^^^

If expected dependencies aren't detected:

**Check**:
- SQL table references use correct naming patterns
- Python functions properly reference source tables
- CDC snapshot configurations are correctly structured

External Source Issues
^^^^^^^^^^^^^^^^^^^^^^

If too many external sources are detected:

.. code-block:: text

   WARNING: 50 external sources detected

**Review**:
- CTE names aren't being excluded (should be filtered automatically)
- Internal pipeline references are properly formatted
- Template variables are correctly structured

.. important::
   The dependency analyzer only considers table references in SQL queries and Python functions. Complex dynamic table references may not be detected automatically.

Integration with Other Features
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The dependency analysis integrates with other Lakehouse Plumber features:

**Code Generation**
    Understanding dependencies helps optimize generated Python code structure

**Validation**
    Dependency analysis validates your project structure for consistency

**Databricks Bundles**
    Generated jobs integrate seamlessly with your bundle configuration

**CI/CD Pipelines**
    Use dependency analysis to optimize build and deployment order


Related Documentation
---------------------

* :doc:`getting_started` – Basic LHP setup and usage
* :doc:`concepts` – Understanding pipelines and flowgroups
* :doc:`cli` – Complete CLI command reference