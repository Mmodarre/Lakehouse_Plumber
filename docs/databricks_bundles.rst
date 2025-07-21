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

.. seealso::
    `Databricks Asset Bundles documentation <https://docs.databricks.com/en/dev-tools/bundles/index.html>`

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
  `Databricks Asset Bundles documentation <https://docs.databricks.com/aws/en/dev-tools/bundles/templates>`_



1. **Create your first pipeline and flowgroup**

Please see :doc:`getting_started` to create your first LHP flowgroup and pipeline.

5. **Generate**

.. code-block:: bash

  lhp generate -e dev --cleanup


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
       D --> F["🔍 Check Bundle<br/>Support"]
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
  * Any DAB ``yml`` files under ``resources/lhp`` directory with 
    the ``"Generated by LakehousePlumber"`` header
    will be overwritten by LHP.

  * For transparency and organization, we recommend to refrain from 
    modifying the content of the ``resources/lhp/`` directory.



Bundle Resource Synchronization
-------------------------------

**How Resource Sync Works**

When bundle support is enabled, LHP automatically:

1. **Scans generated Python files** in the output directory
2. **Creates/updates resource YAML files** for each pipeline
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
          - notebook:
              path: ../../generated/bronze_load/customer_bronze.py
          - notebook:
              path: ../../generated/bronze_load/lineitem_bronze.py
          - notebook:
              path: ../../generated/bronze_load/nation_bronze.py
          - notebook:
              path: ../../generated/bronze_load/orders_bronze.py
          - notebook:
              path: ../../generated/bronze_load/part_bronze.py
          - notebook:
              path: ../../generated/bronze_load/partsupp_bronze.py
          - notebook:
              path: ../../generated/bronze_load/region_bronze.py
          - notebook:
              path: ../../generated/bronze_load/supplier_bronze.py
        configuration:
          bundle.sourcePath: ${workspace.file_path}/generated


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
   # │   └── lhp/
   # └── substitutions/

**lhp generate (with bundle sync)**

Generate Python files and automatically sync bundle resources:

.. code-block:: bash

   # Generate for specific environment
   lhp generate -e dev --cleanup
   
   # Force regeneration (ignores state)
   lhp generate -e dev --force
   
   # Disable bundle sync (if needed)
   lhp generate -e dev --cleanup --no-bundle



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