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

**What LHP Does with Bundles**

Lakehouse Plumber does NOT replace Databricks Asset Bundles or Databricks CLI. 
It generates the pipeline resource YAML files that DABs use for deployment.

Capabilities at a Glance
------------------------

The following table summarizes what you can do with LHP's bundle integration:

.. list-table::
   :header-rows: 1
   :widths: 30 50 20

   * - Capability
     - Description
     - Learn More
   * - **Pipeline Resource Generation**
     - Auto-generate DAB pipeline YAML files from flowgroups
     - `Bundle Resource Synchronization`_
   * - **Pipeline Configuration**
     - Customize compute, runtime, notifications per pipeline
     - `Pipeline Configuration`_
   * - **Job Configuration**
     - Configure orchestration jobs with schedules and alerts
     - `Job Configuration`_
   * - **Multi-Job Orchestration**
     - Split pipelines into separate jobs by layer or domain
     - `Multi-Job Orchestration`_
   * - **Dependency Analysis**
     - Auto-detect pipeline dependencies and execution order
     - :doc:`dependency_analysis`
   * - **Orchestration Job Generation**
     - Generate DAB jobs with proper task dependencies
     - :doc:`dependency_analysis`

**Visual Overview**

.. mermaid::

   flowchart LR
       A["📁 pipelines/<br/>YAML Configs"] --> B["🔧 lhp generate"]
       B --> C["🐍 generated/<br/>Python Files"]
       B --> D["📋 resources/lhp/<br/>Pipeline YAMLs"]
       
       P["⚙️ pipeline_config.yaml<br/>(optional)"] -.-> B
       J["⚙️ job_config.yaml<br/>(optional)"] -.-> E
       
       E["📊 lhp deps"] --> F["📋 resources/<br/>Job YAMLs"]
       D --> G["🚀 databricks bundle deploy"]
       F --> G
       
       style A fill:#e1f5fe
       style C fill:#f3e5f5
       style D fill:#e8f5e8
       style F fill:#fff3e0
       style G fill:#ffebee
       style P fill:#fffde7
       style J fill:#fffde7

Prerequisites & Setup
---------------------

**Requirements**

* Python 3.9+ (3.10 recommended)
* Databricks workspace with Unity Catalog enabled
* Databricks CLI v0.200+ installed and configured
* LakehousePlumber installed: ``pip install lakehouse-plumber``

**Databricks CLI Setup**

Install and configure the Databricks CLI:

.. note::
    Follow the steps here to install `Databricks CLI <https://docs.databricks.com/en/dev-tools/cli/index.html>`_

.. code-block:: bash
  
   # Configure authentication
   databricks configure --token
   
   # Verify connection
   databricks workspace list

How LHP Integrates with DABs
----------------------------

LHP integrates with Databricks Asset Bundles by generating resource files that DABs use for deployment.

**What LHP Does**

* **Generates resource YAML files** for each pipeline in the ``resources/lhp/`` directory
* **Synchronizes resource files** with generated Python notebooks automatically
* **Maintains resource file consistency** by cleaning up obsolete resources
* **Supports customization** through pipeline and job configuration files

**What LHP Does NOT Do**

* Replace or modify your ``databricks.yml`` file
* Deploy resources to Databricks (use ``databricks bundle deploy``)
* Manage non-LHP resources in the ``resources/`` directory

**Benefits of Using Bundles with LHP**

* **Unified Deployment**: Deploy pipelines, jobs, and configurations together 
* **Environment Management**: Separate dev/staging/prod configurations  
* **Version Control**: Track resource changes alongside pipeline code  
* **CI/CD Integration**: Automated deployments through Databricks CLI  
* **Resource Cleanup**: Automatic cleanup of deleted pipelines

Project Structure
-----------------

Your project should have this structure when using bundles:

.. code-block:: text
  

   my-data-platform/
   ├── databricks.yml          # Bundle configuration (you manage this)
   ├── lhp.yaml                # LHP project config
   ├── pipelines/              # LHP pipeline definitions (you create these)
   │   ├── 01_raw_ingestion/
   │   ├── 02_bronze/
   │   └── 03_silver/
   ├── substitutions/          # Environment configs (you create these)
   │   ├── dev.yaml
   │   └── prod.yaml
   ├── config/                 # Optional configuration files
   │   ├── pipeline_config.yaml
   │   └── job_config.yaml
   ├── resources/              # Bundle resources
   │   ├── lhp/                # LHP-managed (auto-generated, do NOT modify)
   │   │   ├── raw_ingestion.pipeline.yml
   │   │   └── bronze_layer.pipeline.yml
   │   └── custom.job.yml      # Your custom DAB files (LHP won't touch)
   └── generated/              # Generated Python files (auto-generated, do NOT modify)
       ├── raw_ingestion/
       └── bronze_layer/

.. note::
  **Coexistence with Your DAB Files**
  
  LHP manages its resource files ONLY in the ``resources/lhp/`` subdirectory. You can 
  safely place your own Databricks Asset Bundle files directly in ``resources/``.
  LHP will never modify or delete files outside ``resources/lhp/``.

.. warning::
  * Files in ``resources/lhp/`` with the ``"Generated by LakehousePlumber"`` header
    will be automatically overwritten by LHP during generation.
  * Do not manually edit files in ``resources/lhp/`` - your changes will be lost.

Getting Started
---------------

Follow these steps to set up bundle integration in your LHP project.

**Step 1: Initialize Project with Bundle Support**

.. code-block:: bash

   lhp init --bundle my-data-platform
   cd my-data-platform

.. note::
  The ``--bundle`` flag creates a ``databricks.yml`` file and the ``resources/lhp`` directory.

**Step 2: Configure databricks.yml**

Edit ``databricks.yml`` to add your Databricks workspace details:

.. code-block:: yaml
   :caption: databricks.yml

   bundle:
     name: my-data-platform
   
   targets:
     dev:
       workspace:
         host: https://your-workspace.cloud.databricks.com
       default: true

.. seealso::
  Refer to Databricks official documentation for more configuration options:
  `Databricks Bundle Configuration <https://docs.databricks.com/aws/en/dev-tools/bundles/templates#configuration-templates>`_

**Step 3: Create Your First Pipeline**

Create a pipeline configuration in the ``pipelines/`` folder. See :doc:`getting_started` for detailed examples.

**Step 4: Generate Code and Resources**

.. code-block:: bash

  lhp generate -e dev 

You should see output like:

.. code-block:: text

   🔄 Syncing bundle resources with generated files...
   ✅ Updated 1 bundle resource file(s)

**Step 5: Verify Generated Resources**

Check the generated resource file:

.. code-block:: bash

   cat resources/lhp/your_pipeline.pipeline.yml

**Step 6: Deploy to Databricks**

.. code-block:: bash

   # Validate bundle configuration
   databricks bundle validate --target dev

   # Deploy bundle to Databricks
   databricks bundle deploy --target dev

   # Verify deployment
   databricks bundle status --target dev

Bundle Resource Synchronization
-------------------------------

**How Resource Sync Works**

When bundle support is enabled (``databricks.yml`` exists), LHP automatically:

1. **Generates resource YAML files** using Jinja2 templates for each pipeline
2. **Uses glob patterns** to automatically discover all files in pipeline directories
3. **Removes obsolete resource files** for deleted pipelines
4. **Maintains environment-specific configurations**
  
.. important::
  * LHP will NOT edit your ``databricks.yml`` file
  * It only creates/updates pipeline YAML files in ``resources/lhp/``
  * You can add custom bundle resources directly in ``resources/``

**Generated Resource File Example**

.. code-block:: yaml
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

**Why Glob Patterns Instead of Notebooks?**

* Lakeflow pipelines now use Python files as their source (notebooks are legacy)
* Glob patterns automatically discover all Python files in pipeline directories
* New files are included automatically without resource file updates

Configuration Management
------------------------

LakehousePlumber provides two configuration files to customize how your pipelines and orchestration jobs are deployed to Databricks:

* **Pipeline Configuration** - Controls DLT pipeline settings (compute, runtime, notifications)
* **Job Configuration** - Controls orchestration job settings (schedules, concurrency, alerts)

Pipeline Configuration
~~~~~~~~~~~~~~~~~~~~~~

**Overview**

Pipeline Configuration controls Delta Live Tables (DLT) pipeline-level settings such as compute resources, runtime environment, processing mode, and monitoring.

**Configuration File Format**

Create a multi-document YAML file with project-level defaults and per-pipeline overrides:

.. code-block:: yaml
   :caption: config/pipeline_config.yaml

   # Project-level defaults (applied to all pipelines)
   project_defaults:
     serverless: true
     edition: ADVANCED
     channel: CURRENT
     continuous: false
   
   ---
   # Pipeline-specific configuration
   pipeline:
     - bronze_load
   serverless: false
   continuous: true
   clusters:
     - label: default
       node_type_id: Standard_D16ds_v5
       autoscale:
         min_workers: 2
         max_workers: 10
   
   ---
   pipeline:
     - silver_load
   serverless: true
   notifications:
     - email_recipients:
         - team@company.com
       alerts:
         - on-update-failure

**Configuration Options**

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Option
     - Type
     - Description
   * - ``catalog``
     - string
     - Unity Catalog name (supports LHP tokens)
   * - ``schema``
     - string
     - Schema/database name (supports LHP tokens)
   * - ``serverless``
     - boolean
     - Use serverless compute (default: ``true``)
   * - ``edition``
     - string
     - DLT edition: ``CORE``, ``PRO``, or ``ADVANCED``
   * - ``channel``
     - string
     - Runtime channel: ``CURRENT`` or ``PREVIEW``
   * - ``continuous``
     - boolean
     - Enable continuous processing (streaming)
   * - ``photon``
     - boolean
     - Enable Photon engine (non-serverless only)
   * - ``clusters``
     - list
     - Cluster configurations for non-serverless pipelines
   * - ``notifications``
     - list
     - Email notifications and alert settings
   * - ``tags``
     - dict
     - Custom tags for the pipeline
   * - ``event_log``
     - dict
     - Event logging configuration

**Usage**

.. code-block:: bash

   # Specify config file when generating
   lhp generate -e dev --pipeline-config config/pipeline_config.yaml
   
   # Short flag version
   lhp generate -e dev -pc config/pipeline_config.yaml

**Configuration Precedence**

Configurations are merged in order (later overrides earlier):

1. **Default values** - Built-in LHP defaults (``serverless: true``, ``edition: ADVANCED``)
2. **Project defaults** - Values from the ``project_defaults`` section
3. **Pipeline-specific** - Values from pipeline-specific sections (highest priority)

.. note::
   Lists (like ``notifications`` and ``clusters``) are **replaced** entirely, not appended.

**Catalog and Schema Configuration**

You can define ``catalog`` and ``schema`` in pipeline config to control where each pipeline writes data:

.. code-block:: yaml

   ---
   pipeline:
     - bronze_load
   catalog: "{catalog}"          # Token from substitutions/dev.yaml
   schema: "{bronze_schema}"     # Token from substitutions/dev.yaml
   
   ---
   pipeline:
     - gold_analytics
   catalog: "analytics_prod"     # Literal value (same across environments)
   schema: "{gold_schema}"       # Token (varies by environment)

.. important::
   Both ``catalog`` AND ``schema`` must be defined together (partial definition raises an error).

**Full Configuration Substitution**

All fields in ``pipeline_config.yaml`` support LHP token substitution, not just catalog/schema:

.. code-block:: yaml

   ---
   pipeline:
     - production_ingestion
   clusters:
     - label: default
       node_type_id: "{pipeline_node_type}"    # Token for sizing
       policy_id: "{pipeline_policy_id}"       # Token for policy
   notifications:
     - email_recipients:
         - "{ops_team_email}"                  # Token for email
   tags:
     environment: "{environment_name}"         # Token for env tag

This enables complete environment-specific configuration from your ``substitutions/{env}.yaml`` files.

Job Configuration
~~~~~~~~~~~~~~~~~

**Overview**

Job Configuration controls Databricks orchestration job settings for dependency-based pipeline execution.

**Configuration File Format**

.. code-block:: yaml
   :caption: config/job_config.yaml

   # Project-level defaults (applied to all jobs)
   project_defaults:
     max_concurrent_runs: 1
     performance_target: STANDARD
     queue:
       enabled: true
     tags:
       managed_by: lakehouse_plumber
   
   ---
   # Job-specific configuration
   job_name:
     - bronze_ingestion_job
   max_concurrent_runs: 2
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200
   email_notifications:
     on_failure:
       - data-engineering@company.com
   schedule:
     quartz_cron_expression: "0 0 2 * * ?"
     timezone_id: "America/New_York"

**Configuration Options**

.. list-table::
   :header-rows: 1
   :widths: 30 15 55

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

**Usage**

.. code-block:: bash

   # Generate orchestration job with config
   lhp deps --job-config config/job_config.yaml --bundle-output
   
   # Short flag version
   lhp deps -jc config/job_config.yaml --bundle-output

**Merge Behavior**

Configs are deep-merged: ``DEFAULT → project_defaults → job-specific``

.. code-block:: yaml

   # Example: Tags are deep-merged
   project_defaults.tags:  {managed_by: "lhp", environment: "dev"}
   job-specific.tags:      {layer: "bronze", environment: "prod"}
   # Result:               {managed_by: "lhp", environment: "prod", layer: "bronze"}

.. note::
   Nested dicts are deep-merged, but lists are **REPLACED** (not appended).

Multi-Job Orchestration
~~~~~~~~~~~~~~~~~~~~~~~

**Overview**

LakehousePlumber supports generating multiple orchestration jobs instead of a single job, enabling better organization for large projects.

**When to Use Multi-Job Mode**

- **Project Segregation**: Separate jobs by data layer (bronze, silver, gold)
- **Resource Optimization**: Different compute requirements per job
- **Team Ownership**: Assign jobs to different teams
- **SLA Management**: Run critical jobs with higher priority/resources
- **Cost Control**: Apply different schedules and timeout policies

**Enabling Multi-Job Mode**

Add the ``job_name`` property to your flowgroup YAMLs:

.. code-block:: yaml
   :caption: pipelines/bronze/customer_ingestion.yaml

   pipeline: data_bronze
   flowgroup: customer_ingestion
   job_name:
     - bronze_ingestion_job   # Assigns this flowgroup to a specific job
   
   actions:
     - name: load_customer
       type: load
       # ... rest of configuration

**Validation Rules**

- **All-or-nothing**: If ANY flowgroup has ``job_name``, ALL must have it
- **Format**: Alphanumeric, underscore, and hyphen only (``^[a-zA-Z0-9_-]+$``)
- **Pipeline filter restriction**: Cannot use ``--pipeline`` flag with multi-job mode

**Generated Files**

When using multi-job mode, LHP generates:

.. code-block:: text

   resources/
   ├── bronze_ingestion_job.job.yml      # Individual job
   ├── silver_transform_job.job.yml      # Individual job
   ├── gold_analytics_job.job.yml        # Individual job
   └── my_project_master.job.yml         # Master orchestration job

The **master job** coordinates all individual jobs using ``job_task`` references with proper dependencies.

Configuration Templates
~~~~~~~~~~~~~~~~~~~~~~~

When you initialize a project, configuration templates are created in ``config/``:

- ``config/job_config.yaml.tmpl`` - Job configuration template with examples
- ``config/pipeline_config.yaml.tmpl`` - Pipeline configuration template with examples

**Getting Started with Templates**

.. code-block:: bash

   # Copy and customize templates
   cp config/job_config.yaml.tmpl config/job_config.yaml
   cp config/pipeline_config.yaml.tmpl config/pipeline_config.yaml
   
   # Edit with your settings, then use
   lhp generate -e dev -pc config/pipeline_config.yaml
   lhp deps -jc config/job_config.yaml --bundle-output

Best Practices
--------------

Environment-Specific Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Different environments typically need different settings. **We recommend maintaining separate configuration files for each environment.**

**Recommended file structure:**

.. code-block:: text

   config/
   ├── pipeline_config-dev.yaml
   ├── pipeline_config-prod.yaml
   ├── job_config-dev.yaml
   └── job_config-prod.yaml

**Common differences by environment:**

==================== ================================================ ================================================
Setting              Development                                      Production
==================== ================================================ ================================================
Cluster size         Smaller nodes (cost efficiency)                  Larger nodes (performance)
Concurrency          Lower (1-2 concurrent runs)                      Higher (3+ concurrent runs)
Notifications        Minimal or none                                  Full alerting to ops teams
Timeouts             Relaxed (for debugging)                          Strict (SLA enforcement)
Performance target   ``STANDARD``                                     ``PERFORMANCE_OPTIMIZED``
==================== ================================================ ================================================

.. seealso::
   For complete CI/CD integration patterns including environment-specific deployment workflows, 
   see :doc:`cicd_reference`.

CLI Quick Reference
-------------------

**Initialize Project with Bundles**

.. code-block:: bash

   lhp init --bundle my-project

**Generate Code and Resources**

.. code-block:: bash

   # Basic generation
   lhp generate -e dev
   
   # With pipeline config
   lhp generate -e dev -pc config/pipeline_config.yaml
   
   # Force regeneration
   lhp generate -e dev --force
   
   # Disable bundle sync
   lhp generate -e dev --no-bundle

**Generate Orchestration Jobs**

.. code-block:: bash

   # Generate job with dependency analysis
   lhp deps --format job --job-name my_etl --bundle-output
   
   # With custom config
   lhp deps -jc config/job_config.yaml --bundle-output

**Deploy to Databricks**

.. code-block:: bash

   # Validate bundle
   databricks bundle validate --target dev
   
   # Deploy
   databricks bundle deploy --target dev
   
   # Run a specific pipeline
   databricks bundle run my_pipeline --target dev

.. seealso::
   For dependency analysis commands and output formats, see :doc:`dependency_analysis`.

Advanced Topics
---------------

**Bundle Sync Behavior**

Bundle synchronization happens automatically after successful generation:

* **Triggers**: After ``lhp generate`` completes successfully
* **Scope**: Processes all generated Python files in output directory
* **Cleanup**: Removes resource files for deleted/excluded pipelines
* **Idempotent**: Safe to run multiple times

**Multi-Environment Setup**

Configure multiple environments with different settings in ``databricks.yml``:

.. code-block:: yaml
   :caption: databricks.yml

   bundle:
     name: acmi-data-platform
   
   targets:
     dev:
       workspace:
         host: https://dev-workspace.cloud.databricks.com
         root_path: /Users/${workspace.current_user.userName}/.bundle/${bundle.name}/${bundle.target}
     
     prod:
       workspace:
         host: https://prod-workspace.cloud.databricks.com
         root_path: /Shared/.bundle/${bundle.name}/${bundle.target}

**Troubleshooting**

===================================== ================================================================
Issue                                  Solution
===================================== ================================================================
Bundle sync not triggered             Ensure ``databricks.yml`` exists in project root
Resource files not generated          Check generated Python files exist and are valid
Bundle validation fails               Verify YAML syntax in generated resource files  
Deployment permission errors          Check workspace permissions and bundle target paths
Obsolete resources not cleaned up     Run ``lhp generate --force`` to trigger full sync
===================================== ================================================================

Related Documentation
---------------------

* :doc:`getting_started` - Basic LHP setup and usage
* :doc:`concepts` - Understanding pipelines and flowgroups
* :doc:`dependency_analysis` - Pipeline dependency analysis and job generation
* :doc:`cicd_reference` - CI/CD patterns and deployment workflows
* :doc:`cli` - Complete CLI command reference
