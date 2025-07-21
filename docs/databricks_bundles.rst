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

**How LHP Integrates with DABs**

Lakehouse Plumber automatically integrates with Databricks Asset Bundles when a 
``databricks.yml`` file is present in your project. LHP will:

* **Generate resource YAML files** for each pipeline in the ``resources/`` directory
* **Synchronize resource files** with generated Python notebooks automatically
* **Enable environment-specific deployments** through bundle targeting
* **Maintain resource file consistency** by cleaning up obsolete resources

**Benefits of Using Bundles with LHP**

✅ **Unified Deployment**: Deploy pipelines, jobs, and configurations together  
✅ **Environment Management**: Separate dev/staging/prod configurations  
✅ **Version Control**: Track resource changes alongside pipeline code  
✅ **CI/CD Integration**: Automated deployments through Databricks CLI  
✅ **Resource Cleanup**: Automatic cleanup of deleted pipelines  

Prerequisites & Setup
---------------------

**Databricks CLI Requirements**

Install and configure the Databricks CLI:

.. code-block:: bash

   # Install Databricks CLI
   pip install databricks-cli
   
   # Configure authentication
   databricks configure --token
   
   # Verify connection
   databricks workspace list

**Bundle Initialization**

If you don't have an existing bundle, initialize one:

.. code-block:: bash

   # In your LHP project directory
   databricks bundle init
   
   # Or manually create databricks.yml
   touch databricks.yml

**Project Structure Requirements**

Your project should have this structure:

.. code-block:: text

   my-data-platform/
   ├── databricks.yml          # Bundle configuration
   ├── lhp.yaml                # LHP project config
   ├── pipelines/              # LHP pipeline definitions
   │   ├── raw_ingestion/
   │   └── bronze_layer/
   ├── resources/              # Generated bundle resources
   │   ├── raw_ingestion.pipeline.yml
   │   └── bronze_layer.pipeline.yml
   └── generated/              # Generated Python files
       ├── raw_ingestion/
       └── bronze_layer/

Getting Started
---------------

**Enabling Bundle Support**

Bundle support is automatically enabled when LHP detects a ``databricks.yml`` file:

.. code-block:: bash

   # Check if bundle support is active
   lhp generate -e dev
   # Look for: "🔄 Syncing bundle resources with generated files..."

**First Bundle Deployment**

1. **Initialize LHP project with bundle support**:

.. code-block:: bash

   lhp init --bundle my-data-platform
   cd my-data-platform

2. **Configure your databricks.yml**:

.. code-block:: yaml

   bundle:
     name: my-data-platform
   
   targets:
     dev:
       workspace:
         host: https://your-workspace.cloud.databricks.com
   
   resources:
     # LHP will automatically populate this section

3. **Create your first pipeline**:

.. code-block:: yaml
   :caption: pipelines/raw_ingestion/customer.yaml

   pipeline: raw_ingestion
   flowgroup: customer
   actions:
     - name: load_customer
       type: load
       source:
         type: cloudfiles
         path: "/mnt/data/customers/"
         format: parquet
       target: customer_raw
     
     - name: write_customer
       type: write
       source: customer_raw
       write_target:
         type: streaming_table
         database: "{catalog}.{raw_schema}"
         table: "customer"

4. **Generate and deploy**:

.. code-block:: bash

   # Generate Python files and sync bundle resources
   lhp generate -e dev
   
   # Deploy bundle to Databricks
   databricks bundle deploy --target dev
   
   # Verify deployment
   databricks bundle status --target dev

**Verifying Bundle Integration**

After running ``lhp generate``, you should see:

.. code-block:: bash

   🔄 Syncing bundle resources with generated files...
   ✅ Updated 1 bundle resource file(s)

Check the generated resource file:

.. code-block:: bash

   cat resources/raw_ingestion.pipeline.yml

Bundle Resource Synchronization
-------------------------------

**How Resource Sync Works**

When bundle support is enabled, LHP automatically:

1. **Scans generated Python files** in the output directory
2. **Creates/updates resource YAML files** for each pipeline
3. **Removes obsolete resource files** for deleted pipelines
4. **Maintains environment-specific configurations**

**Generated Resource YAML Files**

LHP generates bundle resource files with this structure:

.. code-block:: yaml
   :caption: resources/raw_ingestion.pipeline.yml

   resources:
     jobs:
       raw_ingestion:
         name: "raw_ingestion_${bundle.target}"
         job_clusters:
           - job_cluster_key: "main"
             new_cluster:
               spark_version: "13.3.x-scala2.12"
               node_type_id: "i3.xlarge"
               num_workers: 2
         tasks:
           - task_key: "customer"
             job_cluster_key: "main"
             notebook_task:
               notebook_path: "../generated/raw_ingestion/customer.py"
               source: WORKSPACE
           # Additional tasks for other flowgroups...

**Environment-Specific Configurations**

Resource files support environment-specific values through bundle variables:

.. code-block:: yaml
   :caption: databricks.yml

   targets:
     dev:
       variables:
         catalog: "dev_catalog"
         cluster_size: "small"
     
     prod:
       variables:
         catalog: "prod_catalog"
         cluster_size: "large"

CLI Commands & Workflows
------------------------

**lhp init --bundle**

Initialize a new LHP project with bundle support:

.. code-block:: bash

   lhp init --bundle my-project
   
   # Creates:
   # ├── databricks.yml
   # ├── lhp.yaml
   # ├── pipelines/
   # ├── resources/
   # └── substitutions/

**lhp generate (with bundle sync)**

Generate Python files and automatically sync bundle resources:

.. code-block:: bash

   # Generate for specific environment
   lhp generate -e dev
   
   # Force regeneration (ignores state)
   lhp generate -e dev --force
   
   # Disable bundle sync (if needed)
   lhp generate -e dev --no-bundle

**Bundle Deployment Workflow**

Complete workflow for deploying changes:

.. code-block:: bash

   # 1. Update pipeline definitions
   vim pipelines/raw_ingestion/customer.yaml
   
   # 2. Generate Python files and sync resources
   lhp generate -e dev
   
   # 3. Validate bundle configuration
   databricks bundle validate --target dev
   
   # 4. Deploy to Databricks
   databricks bundle deploy --target dev
   
   # 5. Monitor deployment
   databricks bundle status --target dev

Bundle Configuration
--------------------

**databricks.yml Structure**

Basic bundle configuration for LHP projects:

.. code-block:: yaml
   :caption: databricks.yml

   bundle:
     name: my-data-platform
     
   variables:
     catalog:
       description: "Unity Catalog name"
     
   targets:
     dev:
       workspace:
         host: https://your-workspace.cloud.databricks.com
         root_path: /Users/your-email@company.com/.bundle/${bundle.name}/${bundle.target}
       variables:
         catalog: "dev_catalog"
     
     prod:
       workspace:
         host: https://your-workspace.cloud.databricks.com
         root_path: /Shared/.bundle/${bundle.name}/${bundle.target}
       variables:
         catalog: "prod_catalog"
   
   resources:
     # LHP automatically populates this section
     # Do not edit manually - will be overwritten

**Resource File Templates**

LHP uses internal templates to generate resource files. The structure includes:

* **Pipeline configurations**: DLT pipeline settings and cluster configs
* **Notebook paths**: References to generated Python files
* **Environment variables**: Integration with bundle variables
* **Dependencies**: Automatic dependency resolution between pipelines

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

Examples
--------

**Complete Workflow Example**

Here's a complete example using the ACMI demo project:

.. code-block:: bash

   # 1. Clone the example project
   git clone https://github.com/your-org/acmi-data-platform.git
   cd acmi-data-platform
   
   # 2. Initialize bundle support
   lhp init --bundle .
   
   # 3. Configure databricks.yml
   cat > databricks.yml << EOF
   bundle:
     name: acmi-data-platform
   
   targets:
     dev:
       workspace:
         host: https://your-workspace.cloud.databricks.com
       variables:
         catalog: "acmi_dev"
   EOF
   
   # 4. Generate and deploy
   lhp generate -e dev
   databricks bundle deploy --target dev

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
================== ==========================================================

**Resource File Structure**

Generated resource files follow this pattern:

.. code-block:: text

   resources/
   ├── {pipeline_name}.pipeline.yml    # One file per pipeline
   └── ...

Each resource file contains:

* **Jobs configuration**: DLT pipeline as Databricks job
* **Cluster settings**: Compute configuration for the pipeline
* **Notebook references**: Paths to generated Python files
* **Environment variables**: Integration with bundle variables

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

**Related Documentation**

* :doc:`getting_started` – Basic LHP setup and usage
* :doc:`concepts` – Understanding pipelines and flowgroups  
* :doc:`cli` – Complete CLI command reference
* `Databricks Asset Bundles Documentation <https://docs.databricks.com/dev-tools/bundles/index.html>`_ 