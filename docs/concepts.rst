Concepts & Architecture
=======================

At its core Lakehouse Plumber converts **declarative YAML** into regular
Databricks Lakeflow Declarative Pipelines (ETL) Python code.  The YAML files are intentionally
simple and the heavy-lifting happens inside the Plumber engine at generation time.
This page explains the key building blocks you will interact with.

.. contents:: Page outline
   :depth: 2
   :local:

FlowGroups
----------
A **FlowGroup** represents a logical slice of your pipeline often a single
source table or business entity. YAML files can contain one or multiple
FlowGroups (see :doc:`multi_flowgroup_guide` for details on multi-flowgroup files).

Required keys in a FlowGroup YAML file

.. code-block:: yaml

   pipeline:  bronze_raw                 # pipeline name (logical)
   flowgroup: customer_bronze_ingestion  # unique name for the flowgroup (logical)
   actions:                              # list of steps in the flowgroup

Optional keys in a FlowGroup YAML file

.. code-block:: yaml

   job_name: NCR  # Optional: Assign flowgroup to a specific orchestration job

The ``job_name`` property enables **multi-job orchestration**, allowing you to split your flowgroups into separate Databricks jobs rather than a single monolithic orchestration job. This is useful for:

* **Separate scheduling** - Different jobs can run on different schedules (e.g., hourly POS data, daily ERP data)
* **Isolated execution** - Jobs run independently with separate concurrency and resource settings
* **Modular organization** - Group related flowgroups by source system, business domain, or data criticality
* **Flexible configuration** - Each job can have its own tags, notifications, timeouts, and performance targets

.. important::
   **All-or-Nothing Rule**: If ``job_name`` is defined for **any** flowgroup in your project, it **must** be defined for **all** flowgroups. This ensures consistent orchestration behavior and prevents configuration errors.

**Example with multi-job orchestration:**

.. code-block:: yaml
   :caption: pipelines/ncr/pos_transactions.yaml

   pipeline: bronze_ncr
   flowgroup: pos_transaction_bronze
   job_name: NCR  # Assigns this flowgroup to the "NCR" orchestration job
   
   actions:
     - name: load_pos_data
       type: load
       source:
         type: cloudfiles
         path: "/mnt/landing/ncr/pos/*.parquet"
       target: v_pos_raw

When ``job_name`` is used:

* Each unique ``job_name`` generates a separate Databricks job file (e.g., ``NCR.job.yml``, ``SAP_SFCC.job.yml``)
* A **master orchestration job** is generated that coordinates execution across all jobs
* Dependencies between jobs are automatically detected and handled in the master job
* Per-job configuration is managed through multi-document ``job_config.yaml`` files

.. seealso::
   For complete details on multi-job orchestration, job configuration, and the master orchestration job, see :doc:`databricks_bundles`.

.. note::
   **FlowGroup vs Pipeline:**
   - A **FlowGroup** represents a logical slice of your pipeline often a single source table or business entity.

   - A **Pipeline** is a logical grouping of FlowGroups. It is used to group the generated python files in the same folder.

   - Lakeflow Declarative Pipelines are **declarative** (as the name suggests) hence the order of the actions is determind at runtime by the Lakeflow engine based on the dependencies between the tables/views.

   - **YAML files** can contain one flowgroup (traditional) or multiple flowgroups (see :doc:`multi_flowgroup_guide`).

Actions
-------
Every FlowGroup lists one or more **Actions** 
Actions come in three top-level types:

+----------------+----------------------------------------------------------+
| Type           | Purpose                                                  |
+================+==========================================================+
|| **Load**      || Bring data into a temporary **view** (e.g. CloudFiles,  |
||               || Delta, JDBC, SQL, Python, custom_datasource).           |
+----------------+----------------------------------------------------------+
|| **Transform** || Manipulate data in one or more steps (SQL, Python,      |
||               || schema adjustments, data-quality checks, temp tables…). |
+----------------+----------------------------------------------------------+
|| **Write**     || Persist the final dataset to a *streaming_table*,      |
||               || *materialized_view*, or external *sink* (Kafka,        |
||               || Delta, custom API).                                     |
+----------------+----------------------------------------------------------+


.. note::
   - You may chain **zero or many Transform actions** between a Load and a Write.

.. important::
   - the order of the actions is determind at runtime by the Lakeflow engine based on the dependencies between the tables/views, Not the order in the YAML file or the generated Python file.


For a complete catalogue of Action sub-types and their options see
:doc:`actions_reference`.

Presets
-------
A **Preset** is a YAML file that provides default configuration snippets you can
reuse across FlowGroups. Presets inject default values that are merged with
explicit configurations in templates and flowgroups.

Common use cases:

* Standardised table properties for all Bronze streaming tables
* CloudFiles ingestion options (error handling, schema evolution)
* Spark configuration tuning

Example preset file:

.. code-block:: yaml
   :caption: presets/cloudfiles_defaults.yaml

   name: cloudfiles_defaults
   version: "1.0"
   description: "Standard CloudFiles options"
   
   defaults:
     load_actions:
       cloudfiles:
         options:
           cloudFiles.rescuedDataColumn: "_rescued_data"
           ignoreCorruptFiles: "true"
           ignoreMissingFiles: "true"
           cloudFiles.maxFilesPerTrigger: 200

Usage in a FlowGroup:

.. code-block:: yaml
   
   presets:
     - cloudfiles_defaults
   
   actions:
     - name: load_data
       type: load
       source:
         type: cloudfiles
         options:
           cloudFiles.format: csv  # Merged with preset options

For complete preset documentation see :doc:`presets_reference`.

Templates
---------
While presets inject reusable **values**, **Templates** inject reusable **action
patterns** think of them as parametrised macros.

In a template file you define parameters and a list of actions that reference
those parameters.  Inside a FlowGroup you apply the template and provide actual
arguments

**Example of a template file:**

.. code-block:: yaml
   :caption: templates/csv_ingestion_template.yaml
   :linenos:

   # This is a template for ingesting CSV files with schema enforcement
   # It is used to generate the actions for the pipeline
   # within the pipeline all it need to defined are the parameters for the table name and landing folder
   # the template will generate the actions for the pipeline

   name: csv_ingestion_template
   version: "1.0"
   description: "Standard template for ingesting CSV files with schema enforcement"

   presets:
   - bronze_layer

   parameters:
   - name: table_name
      required: true
      description: "Name of the table to ingest"
   - name: landing_folder
      required: true
      description: "Name of the landing folder"

   actions:
   - name: load_{{ table_name }}_csv
      type: load
      readMode : "stream"
      operational_metadata: ["_source_file_path","_source_file_size","_source_file_modification_time","_record_hash"]
      source:
         type: cloudfiles
         path: "{landing_volume}/{{ landing_folder }}/*.csv"
         format: csv
         options:
         cloudFiles.format: csv
         header: True
         delimiter: "|"
         cloudFiles.maxFilesPerTrigger: 11
         cloudFiles.inferColumnTypes: False
         cloudFiles.schemaEvolutionMode: "addNewColumns"
         cloudFiles.rescuedDataColumn: "_rescued_data"
         cloudFiles.schemaHints: "schemas/{{ table_name }}_schema.yaml"

      target: v_{{ table_name }}_cloudfiles
      description: "Load {{ table_name }} CSV files from landing volume"

   - name: write_{{ table_name }}_cloudfiles
      type: write
      source: v_{{ table_name }}_cloudfiles
      write_target:
         type: streaming_table
         database: "{catalog}.{raw_schema}"
         table: "{{ table_name }}"
         description: "Write {{ table_name }} to raw layer" 

**Example of a flowgroup using the template:**

.. code-block:: yaml
   :caption: pipelines/01_raw_ingestion/csv_ingestions/customer_ingestion.yaml
   :linenos:
   :emphasize-lines: 11-14

   # This pipeline is used to ingest the customer table from the csv files into the raw schema
   # Pipeline variable puts the generate files in the same folder for the pipeline to pick up
   pipeline: raw_ingestions
   # Flowgroup are conceptual artifacts and has no functional purpose
   # there are used to group actions together in the generated files
   flowgroup: customer_ingestion

   # Use the template to generate the actions for the pipeline
   # Template parameters are used to pass in the table name and landing folder
   # The template will generate the actions for the pipeline
   use_template: csv_ingestion_template
   template_parameters:
   table_name: customer
   landing_folder: customer


Configuration Management
------------------------

LakehousePlumber provides two types of configuration files to customize how your pipelines and orchestration jobs are deployed to Databricks: **Pipeline Configuration** for DLT pipeline settings and **Job Configuration** for orchestration job settings.

Pipeline Configuration
~~~~~~~~~~~~~~~~~~~~~~

**Pipeline Configuration** controls Delta Live Tables (DLT) pipeline-level settings such as compute resources, runtime environment, processing mode, and monitoring. These settings are applied when generating Databricks Asset Bundle resource files.

**Key configuration options:**

- **Compute**: Serverless vs. classic clusters with custom sizing
- **DLT Edition**: CORE, PRO, or ADVANCED feature sets
- **Runtime Channel**: CURRENT (stable) or PREVIEW (latest features)
- **Processing Mode**: Continuous streaming vs. triggered batch
- **Monitoring**: Email notifications, tags, and event logging

**Configuration file structure:**

Pipeline configuration uses multi-document YAML with three levels of precedence:

1. **System defaults** - Built into LakehousePlumber
2. **Project defaults** - Apply to all pipelines in your project
3. **Pipeline-specific** - Override defaults for individual pipelines

.. code-block:: yaml
   :caption: templates/bundle/pipeline_config.yaml
   :linenos:

   # Project-level defaults (applies to all pipelines)
   project_defaults:
     serverless: true
     edition: ADVANCED
     channel: CURRENT
     continuous: false
   
   ---
   # Pipeline-specific override
   pipeline: bronze_ingestion
   serverless: false
   continuous: true
   clusters:
     - label: default
       node_type_id: Standard_D16ds_v5
       autoscale:
         min_workers: 2
         max_workers: 10

**Usage patterns:**

.. code-block:: bash

   # Auto-loaded from default location
   lhp generate -e dev
   
   # Explicit configuration file
   lhp generate -e dev --pipeline-config config/pipeline_config.yaml

.. seealso::
   For complete pipeline configuration options and validation rules, see :doc:`databricks_bundles`.

Job Configuration
~~~~~~~~~~~~~~~~~

**Job Configuration** controls Databricks orchestration job settings for dependency-based pipeline execution. These settings are applied when generating job resource files with the ``lhp deps`` command.

**Key configuration options:**

- **Execution Control**: Concurrent runs, timeouts, performance targets
- **Queue Management**: Job queuing behavior when at capacity
- **Notifications**: Email and webhook alerts for job events
- **Scheduling**: Quartz cron expressions for automated execution
- **Access Control**: Permissions and ownership settings

**Configuration file structure:**

Job configuration uses a single YAML document with flat key-value structure:

.. code-block:: yaml
   :caption: config/job_config.yaml
   :linenos:

   # Core job settings
   max_concurrent_runs: 2
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200
   
   # Queue configuration
   queue:
     enabled: true
   
   # Notifications
   email_notifications:
     on_failure:
       - data-engineering@company.com
       - data-ops@company.com
   
   # Scheduling
   schedule:
     quartz_cron_expression: "0 0 2 * * ?"
     timezone_id: "America/New_York"
   
   # Tags for cost tracking
   tags:
     environment: production
     cost_center: analytics
     team: data-engineering

**Usage patterns:**

.. code-block:: bash

   # Generate job file with custom configuration
   lhp deps --job-config config/job_config.yaml
   
   # Output directly to bundle resources directory
   lhp deps --job-config config/job_config.yaml --bundle-output

.. seealso::
   For complete job configuration options and dependency analysis features, see :doc:`databricks_bundles`.

Configuration Templates
~~~~~~~~~~~~~~~~~~~~~~~

When you initialize a new LakehousePlumber project, configuration template files are automatically created in the ``config/`` directory:

- ``config/job_config.yaml.tmpl`` - Job configuration template
- ``config/pipeline_config.yaml.tmpl`` - Pipeline configuration template

These ``.tmpl`` files serve as comprehensive references with:

- Detailed comments explaining each option
- Example configurations for common scenarios
- Validation rules and allowed values
- Links to relevant documentation

**Getting started:**

.. code-block:: bash

   # 1. Initialize project with templates
   lhp init my_project --bundle
   
   # 2. Copy and customize templates
   cd my_project
   cp config/job_config.yaml.tmpl config/job_config.yaml
   cp config/pipeline_config.yaml.tmpl templates/bundle/pipeline_config.yaml
   
   # 3. Edit configuration files with your settings
   # (Remove .tmpl extension to activate)
   
   # 4. Use in generation commands
   lhp generate -e dev --pipeline-config templates/bundle/pipeline_config.yaml
   lhp deps --job-config config/job_config.yaml --bundle-output

Best Practices
~~~~~~~~~~~~~~

Environment-Specific Configuration
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Different environments (dev, test, prod) typically have different requirements for compute resources, alerting, permissions, and operational settings. **It is strongly recommended to maintain separate configuration files for each environment** rather than using a single configuration for all environments.

**Recommended file structure:**

.. code-block:: text

   my_project/
   ├── config/
   │   ├── job_config-dev.yaml      # Development job settings
   │   ├── job_config-test.yaml     # Test/staging job settings
   │   ├── job_config-prod.yaml     # Production job settings
   │   ├── pipeline_config-dev.yaml # Development pipeline settings
   │   ├── pipeline_config-test.yaml
   │   └── pipeline_config-prod.yaml
   └── ...

**Common environment-specific differences:**

Development (dev):
  - Smaller cluster sizes for cost efficiency
  - Fewer concurrent runs
  - Minimal or no email notifications
  - Relaxed timeouts for debugging
  - Lower performance targets (STANDARD)

Test/Staging:
  - Medium cluster sizes
  - Moderate concurrency limits
  - Notifications to QA/testing teams
  - Realistic production-like settings
  - Standard performance targets

Production:
  - Production-grade cluster sizes
  - Higher concurrency for throughput
  - Critical alerting to ops teams
  - Strict timeouts and SLAs
  - Performance-optimized targets (PERFORMANCE_OPTIMIZED)
  - Comprehensive tags for cost tracking
  - Formal permissions and access controls

**Example: Environment-specific pipeline configuration**

.. code-block:: yaml
   :caption: config/pipeline_config-dev.yaml (Development)
   :linenos:

   project_defaults:
     serverless: true
     edition: ADVANCED
     continuous: false
     development: true  # Enable development mode features
   
   ---
   pipeline: bronze_ingestion
   # Dev: Use smaller serverless for faster iteration
   serverless: true

.. code-block:: yaml
   :caption: config/pipeline_config-prod.yaml (Production)
   :linenos:

   project_defaults:
     serverless: false  # Production uses dedicated clusters
     edition: ADVANCED
     continuous: true   # 24/7 streaming
     clusters:
       - label: default
         node_type_id: Standard_D32ds_v5  # Larger nodes
         autoscale:
           min_workers: 5
           max_workers: 20
     notifications:
       email_recipients:
         - data-ops@company.com
         - platform-alerts@company.com
     tags:
       environment: production
       cost_center: data-platform
       sla: critical
   
   ---
   pipeline: bronze_ingestion
   # Production-specific overrides
   clusters:
     - label: default
       node_type_id: Standard_D64ds_v5  # Critical pipeline needs more power
       autoscale:
         min_workers: 10
         max_workers: 50

**Example: Environment-specific job configuration**

.. code-block:: yaml
   :caption: config/job_config-dev.yaml (Development)
   :linenos:

   max_concurrent_runs: 1
   performance_target: STANDARD
   timeout_seconds: 14400  # 4 hours for debugging
   
   queue:
     enabled: true
   
   tags:
     environment: dev
     cost_center: engineering

.. code-block:: yaml
   :caption: config/job_config-prod.yaml (Production)
   :linenos:

   max_concurrent_runs: 3  # Higher throughput
   performance_target: PERFORMANCE_OPTIMIZED
   timeout_seconds: 7200   # Strict 2-hour SLA
   
   queue:
     enabled: true
   
   email_notifications:
     on_failure:
       - data-ops@company.com
       - platform-oncall@company.com
     on_success:
       - data-metrics@company.com
   
   webhook_notifications:
     on_failure:
       - id: pagerduty-webhook
         url: "https://events.pagerduty.com/v2/enqueue"
   
   schedule:
     quartz_cron_expression: "0 0 2 * * ?"  # 2 AM daily
     timezone_id: "America/New_York"
   
   tags:
     environment: production
     cost_center: data-platform
     sla: critical
     on_call_team: data-ops

CI/CD Integration
^^^^^^^^^^^^^^^^^

Use environment-specific configuration files in your CI/CD pipelines by dynamically selecting the config file based on the target environment.

**GitHub Actions example:**

.. code-block:: yaml
   :caption: .github/workflows/deploy.yml
   :linenos:
   :emphasize-lines: 16-17, 25-26

   name: Deploy LHP Pipelines
   
   on:
     push:
       branches:
         - main
         - develop
   
   jobs:
     deploy:
       runs-on: ubuntu-latest
       steps:
         - uses: actions/checkout@v3
         
         - name: Set environment
           id: set-env
           run: |
             if [[ "${{ github.ref }}" == "refs/heads/main" ]]; then
               echo "ENV=prod" >> $GITHUB_OUTPUT
             else
               echo "ENV=dev" >> $GITHUB_OUTPUT
             fi
         
         - name: Generate pipelines
           run: |
             lhp generate -e ${{ steps.set-env.outputs.ENV }} \
               --pipeline-config config/pipeline_config-${{ steps.set-env.outputs.ENV }}.yaml \
               --force
         
         - name: Generate orchestration job
           run: |
             lhp deps \
               --job-config config/job_config-${{ steps.set-env.outputs.ENV }}.yaml \
               --bundle-output

**Azure DevOps example:**

.. code-block:: yaml
   :caption: azure-pipelines.yml
   :linenos:
   :emphasize-lines: 16-20

   trigger:
     branches:
       include:
         - main
         - develop
   
   pool:
     vmImage: 'ubuntu-latest'
   
   variables:
     - name: lhp_env
       ${{ if eq(variables['Build.SourceBranch'], 'refs/heads/main') }}:
         value: 'prod'
       ${{ else }}:
         value: 'dev'
   
   steps:
     - script: |
         lhp generate -e $(lhp_env) \
           --pipeline-config config/pipeline_config-$(lhp_env).yaml \
           --force
       displayName: 'Generate LHP pipelines'
     
     - script: |
         lhp deps \
           --job-config config/job_config-$(lhp_env).yaml \
           --bundle-output
       displayName: 'Generate orchestration job'

**Makefile example:**

.. code-block:: makefile
   :caption: Makefile
   :linenos:

   .PHONY: deploy-dev deploy-test deploy-prod
   
   deploy-dev:
   	lhp generate -e dev \
   		--pipeline-config config/pipeline_config-dev.yaml \
   		--force
   	lhp deps \
   		--job-config config/job_config-dev.yaml \
   		--bundle-output
   
   deploy-test:
   	lhp generate -e test \
   		--pipeline-config config/pipeline_config-test.yaml \
   		--force
   	lhp deps \
   		--job-config config/job_config-test.yaml \
   		--bundle-output
   
   deploy-prod:
   	lhp generate -e prod \
   		--pipeline-config config/pipeline_config-prod.yaml \
   		--force
   	lhp deps \
   		--job-config config/job_config-prod.yaml \
   		--bundle-output

**Shell script example:**

.. code-block:: bash
   :caption: scripts/deploy.sh
   :linenos:

   #!/bin/bash
   set -euo pipefail
   
   ENV=${1:-dev}  # Default to dev if not specified
   
   echo "Deploying to environment: $ENV"
   
   # Validate environment
   if [[ ! "$ENV" =~ ^(dev|test|prod)$ ]]; then
       echo "Error: Invalid environment. Must be dev, test, or prod"
       exit 1
   fi
   
   # Generate pipelines with environment-specific config
   lhp generate -e "$ENV" \
       --pipeline-config "config/pipeline_config-${ENV}.yaml" \
       --force
   
   # Generate orchestration job with environment-specific config
   lhp deps \
       --job-config "config/job_config-${ENV}.yaml" \
       --bundle-output
   
   echo "Deployment to $ENV completed successfully"

**Usage:**

.. code-block:: bash

   # Deploy to development
   ./scripts/deploy.sh dev
   
   # Deploy to production
   ./scripts/deploy.sh prod

.. tip::
   **Version Control Best Practices**:
   
   - Commit all environment-specific configuration files to version control
   - Use code review for production configuration changes
   - Document environment-specific settings in comments
   - Keep sensitive values (credentials, API keys) in Databricks secrets, not in config files
   - Use tags consistently across environments for cost tracking and resource management

.. warning::
   Never hardcode environment-specific secrets or credentials in configuration files. Always use Databricks secret references (``${secret:scope/key}``) in your substitution files and reference them through substitution tokens in configuration files when needed.

.. note::
   Configuration files are **optional**. LakehousePlumber uses sensible defaults for all settings. Use configuration files when you need to customize deployment behavior beyond the defaults.

.. seealso::
   For project initialization and directory structure details, see :doc:`cli`.

Substitutions & Secrets
-----------------------

Environment Configuration
~~~~~~~~~~~~~~~~~~~~~~~~~

Tokens wrapped in ``{token}`` or ``${token}`` are replaced at generation time
using files under ``substitutions/<env>.yaml``. This enables environment-specific
configurations while keeping pipeline definitions portable.

**Example substitution file:**

.. code-block:: yaml
   :caption: substitutions/dev.yaml
   :linenos:
   :emphasize-lines: 10-15

   # Environment-specific tokens
   dev:
     catalog: dev_catalog
     bronze_schema: bronze
     silver_schema: silver
     landing_path: /mnt/dev/landing
     checkpoint_path: /mnt/dev/checkpoints

   # Secret configuration
   secrets:
     default_scope: dev_secrets
     scopes:
       database_secrets: dev_db_secrets
       storage_secrets: dev_azure_secrets
       api_secrets: dev_external_apis


Secret Management
~~~~~~~~~~~~~~~~~

**Secret references** use the ``${secret:scope/key}`` syntax and are converted to
secure ``dbutils.secrets.get()`` calls in generated Python code. LHP validates
scope aliases and collects every secret used by the pipeline, making security
reviews and approvals easier.

**Secret reference formats:**

- ``${secret:scope_alias/key}`` - Uses specific scope alias (resolved to actual Databricks scope)
- ``${secret:key}`` - Uses default_scope if configured

.. note::
   Scope aliases (like ``database_secrets``) are mapped to actual Databricks secret scope 
   names (like ``dev_db_secrets``) in the substitution file. This provides flexibility 
   to use different scope names across environments while keeping pipeline definitions portable.


File Substitution Support
~~~~~~~~~~~~~~~~~~~~~~~~~

.. versionadded:: Latest

LakehousePlumber now supports substitutions in external files, providing the same environment-specific flexibility for Python functions and SQL files that you have in YAML configurations.

**Supported File Types:**

================== ==================================================
File Type          Where Used
================== ==================================================
**Python Files**   • Snapshot CDC ``source_function`` files
                   • Python transform ``module_path`` files
                   • Custom datasource ``module_path`` files
**SQL Files**      • SQL load actions with ``sql_path``
                   • SQL transform actions with ``sql_path``
================== ==================================================

**Example Python Function with Substitutions:**

.. code-block:: python
   :caption: py_functions/customer_snapshot.py
   :linenos:
   :emphasize-lines: 4-5,10

   from typing import Optional, Tuple
   from pyspark.sql import DataFrame

   catalog = "{catalog}"
   schema = "{bronze_schema}"

   def next_customer_snapshot(latest_version: Optional[int]) -> Optional[Tuple[DataFrame, int]]:
       if latest_version is None:
           df = spark.sql(f"""
               SELECT * FROM {catalog}.{schema}.customers 
               WHERE snapshot_id = 1
           """)
           return (df, 1)
       return None

**Example SQL File with Substitutions:**

.. code-block:: text
   :caption: sql/customer_metrics.sql
   :linenos:
   :emphasize-lines: 4-6

   SELECT 
       customer_id,
       customer_name,
       '{environment}' as source_env
   FROM {catalog}.{bronze_schema}.customers
   WHERE created_date >= '{cutoff_date}'

**Secret Support in Files:**

Both Python and SQL files support secret substitutions with the same syntax as YAML:

.. code-block:: python
   :caption: Example with secrets

   # Environment token
   api_endpoint = "{api_base_url}"
   
   # Secret reference  
   api_key = "${secret:api_keys/service_key}"
   db_password = "${secret:database/password}"

**Processing Behavior:**

- **Tokens and secrets** are processed before the file content is used
- **Python files** have substitutions applied before import management
- **SQL files** have substitutions applied before query execution
- **Backward compatible** - files without substitution variables work unchanged
- **Same syntax** as YAML substitutions for consistency

**Example pipeline with secrets:**

.. code-block:: yaml
   :caption: pipelines/customer_ingestion/external_load.yaml
   :linenos:
   :emphasize-lines: 9-12

   pipeline: customer_ingestion
   flowgroup: external_load

   actions:
     - name: load_from_postgres
       type: load
       source:
         type: jdbc
         url: "jdbc:postgresql://${secret:database_secrets/host}:5432/customers"
         user: "${secret:database_secrets/username}"
         password: "${secret:database_secrets/password}"
         driver: "org.postgresql.Driver"
         table: "customers"
       target: v_customers_raw

**Generated Python code:**

.. code-block:: python
   :caption: Generated DLT code with secure secret handling
   :linenos:
   :emphasize-lines: 6-8

   @dp.temporary_view()
   def v_customers_raw():
       """Load from external database"""
       df = spark.read \
           .format("jdbc") \
           .option("url", f"jdbc:postgresql://{dbutils.secrets.get(scope='dev_db_secrets', key='host')}:5432/customers") \
           .option("user", f"{dbutils.secrets.get(scope='dev_db_secrets', key='username')}") \
           .option("password", f"{dbutils.secrets.get(scope='dev_db_secrets', key='password')}") \
           .option("driver", "org.postgresql.Driver") \
           .option("dbtable", "customers") \
           .load()
       
       return df


Operational Metadata
---------------------

Column Definitions
~~~~~~~~~~~~~~~~~~

Operational metadata are automatically generated columns that provide lineage, data
provenance, and processing context. These columns are added to your tables without
requiring manual SQL modifications.

.. note::
   Operational metadata columns are defined in the project level configuration file. under the ``operational_metadata`` key.

**Project-level configuration:**

.. code-block:: yaml
   :caption: lhp.yaml - Project operational metadata configuration
   :linenos:

   # LakehousePlumber Project Configuration
   name: my_lakehouse_project
   version: "1.0"

   operational_metadata:
     columns:
       _processing_timestamp:
         expression: "F.current_timestamp()"
         description: "When the record was processed by the pipeline"
         applies_to: ["streaming_table", "materialized_view", "view"]
       
       _source_file_path:
         expression: "F.col('_metadata.file_path')"
         description: "Source file path for lineage tracking"
         applies_to: ["view"]
       
       _record_hash:
         expression: "F.xxhash64(*[F.col(c) for c in df.columns])"
         description: "Hash of all record fields for change detection"
         applies_to: ["streaming_table", "materialized_view", "view"]
         additional_imports:
           - "from pyspark.sql.functions import xxhash64"
       
       _pipeline_name:
         expression: "F.lit('${pipeline_name}')"
         description: "Name of the processing pipeline"
         applies_to: ["streaming_table", "materialized_view", "view"]

Version Requirements
~~~~~~~~~~~~~~~~~~~~

LakehousePlumber supports version enforcement to ensure consistent code generation across development and CI environments. This prevents "works on my machine" issues and ensures reproducible builds.

**Basic configuration:**

.. code-block:: yaml
   :caption: lhp.yaml - Version enforcement examples
   :linenos:

   # LakehousePlumber Project Configuration
   name: my_lakehouse_project
   version: "1.0"
   
   # Enforce version requirements (optional)
   required_lhp_version: ">=0.4.1,<0.5.0"  # Allow patch updates within 0.4.x

**Version specification formats:**

.. code-block:: yaml
   :caption: Version requirement examples

   # Exact version pin (strict)
   required_lhp_version: "==0.4.1"
   
   # Allow patch updates only
   required_lhp_version: "~=0.4.1"          # Equivalent to >=0.4.1,<0.5.0
   
   # Range with exclusions
   required_lhp_version: ">=0.4.1,<0.5.0,!=0.4.3"  # Exclude known bad version
   
   # Allow minor updates
   required_lhp_version: ">=0.4.0,<1.0.0"

**Behavior:**

- When ``required_lhp_version`` is set, ``lhp validate`` and ``lhp generate`` will fail if the installed version doesn't satisfy the requirement
- Informational commands like ``lhp show`` skip version checking to allow inspection even with mismatches
- Version checking uses `PEP 440 <https://peps.python.org/pep-0440/>`_ version specifiers

**Emergency bypass:**

.. code-block:: bash
   :caption: Bypass version checking in emergencies

   # Temporarily bypass version checking
   export LHP_IGNORE_VERSION=1
   lhp generate -e dev
   
   # Or inline
   LHP_IGNORE_VERSION=1 lhp validate -e prod

**CI/CD integration:**

.. code-block:: bash
   :caption: CI pipeline with version enforcement

   # Install exact version matching project requirements
   pip install "lakehouse-plumber$(yq -r .required_lhp_version lhp.yaml | sed 's/^//')"
   
   # Or use range-compatible version
   pip install "lakehouse-plumber>=0.4.1,<0.5.0"
   
   # Validate and generate (will fail if version mismatch)
   lhp validate -e prod
   lhp generate -e prod

.. note::
   Version enforcement is **optional**. Projects without ``required_lhp_version`` work normally with any installed LakehousePlumber version.

.. warning::
   Use the bypass environment variable (``LHP_IGNORE_VERSION=1``) only in emergencies. It's not recommended for production environments as it defeats the purpose of version consistency.

Target Type Compatibility
~~~~~~~~~~~~~~~~~~~~~~~~~

The ``applies_to`` field controls which DLT table types can use each operational metadata column.
LHP automatically filters columns based on the target type to prevent runtime errors.

**Purpose of target type restrictions:**

When defining operational metadata columns at the project level, the ``applies_to`` field serves as a 
**safeguard mechanism** to protect end users from accidentally using incompatible columns in their 
pipeline configurations. This is a defensive design pattern that prevents common mistakes.

**Best practice for project administrators:**

- Set restrictive ``applies_to`` values for source-specific columns (e.g., CloudFiles metadata)
- Use broader ``applies_to`` values for universal columns (e.g., timestamps, pipeline names)
- This protects pipeline developers from runtime failures and provides clear usage guidance

**Target types:**

- **``view``** - Source views created by load actions (``@dp.temporary_view()``)
- **``streaming_table``** - Live tables with streaming updates (``@dp.materialized_view()``)  
- **``materialized_view``** - Batch-computed views for analytics (``@dp.temporary_view()``)

**Source-specific metadata limitations:**

.. warning::
   - Metadata columns that depend on CloudFiles features (like ``_metadata.file_path``) are **only available in views** that load data from CloudFiles sources. These columns will cause runtime errors if used with JDBC, SQL, Delta, or custom_datasource sources.
   - Custom data sources may provide their own metadata columns depending on their implementation, but CloudFiles-specific metadata will not be available.

.. seealso::
   For complete details on file metadata columns available in Databricks CloudFiles, refer to the Databricks documentation:
   `File Metadata Columns <https://docs.databricks.com/aws/en/ingestion/file-metadata-column>`_


**Examples of source-restricted columns:**

.. code-block:: yaml
   :caption: CloudFiles-only operational metadata
   :linenos:
   :emphasize-lines: 6

   operational_metadata:
     columns:
       _source_file_name:
         expression: "F.col('_metadata.file_name')"
         description: "Original file name with extension"
         applies_to: ["view"]  # Only views, and only CloudFiles sources
       
       _file_modification_time:
         expression: "F.col('_metadata.file_modification_time')"
         description: "When the source file was last modified"
         applies_to: ["view"]  # Only views, and only CloudFiles sources
       
       _processing_timestamp:
         expression: "F.current_timestamp()"
         description: "When record was processed (works everywhere)"
         applies_to: ["streaming_table", "materialized_view", "view"]

**Safe usage patterns:**

.. code-block:: yaml
   :caption: Source-aware metadata configuration
   :linenos:

   # CloudFiles load action - can use file metadata
   - name: load_files
     type: load
     source:
       type: cloudfiles
       path: "/mnt/data/*.json"
     operational_metadata:
       - "_source_file_name"        # ✓ Available in CloudFiles
       - "_file_modification_time"  # ✓ Available in CloudFiles
       - "_processing_timestamp"    # ✓ Available everywhere
     target: v_file_data

   # JDBC load action - file metadata not available  
   - name: load_database
     type: load
     source:
       type: jdbc
       table: "customers"
     operational_metadata:
       - "_processing_timestamp"    # ✓ Available everywhere
       # DO NOT USE: "_source_file_name" would cause runtime error
     target: v_database_data

   # Custom data source - metadata depends on implementation
   - name: load_api_data
     type: load
     module_path: "data_sources/api_source.py"
     custom_datasource_class: "APIDataSource"
     options:
       api_endpoint: "https://api.example.com/data"
     operational_metadata:
       - "_processing_timestamp"    # ✓ Available everywhere
       # Custom metadata depends on DataSource implementation
     target: v_api_data

Usage in YAML Files
~~~~~~~~~~~~~~~~~~~

Operational metadata can be configured at multiple levels with **additive behavior** - columns from all levels are combined together:

.. important::
   **Additive Behavior**: Operational metadata columns are **never overridden** between levels. 
   Instead, columns from preset + flowgroup + action levels are **combined together**. 
   The only exception is ``operational_metadata: false`` at action level, which disables **all** metadata.

**Preset level**

.. code-block:: yaml
   :caption: presets/bronze_layer.yaml
   :linenos:

   name: bronze_layer
   version: "1.0"
   
   defaults:
     operational_metadata: ["_processing_timestamp", "_source_file_path"]

**FlowGroup level**

.. code-block:: yaml
   :caption: pipelines/customer_ingestion/load_customers.yaml
   :linenos:
   :emphasize-lines: 4

   pipeline: customer_ingestion
   flowgroup: load_customers
   presets: ["bronze_layer"]
   operational_metadata: ["_record_hash"]  # Adds to preset columns

   actions:
     - name: load_customer_files
       type: load
       source:
         type: cloudfiles
         path: "/mnt/landing/customers/*.json"
         format: json
       target: v_customers_raw

**Action level**

.. code-block:: yaml
   :caption: Action-specific metadata configuration
   :linenos:
   :emphasize-lines: 8-11

   actions:
     - name: load_with_custom_metadata
       type: load
       source:
         type: cloudfiles
         path: "/mnt/data/*.parquet"
         format: parquet
       operational_metadata:  # Adds to flowgroup + preset columns
         - "_pipeline_name"
         - "_custom_business_logic"
       target: v_enriched_data
     
     - name: load_without_metadata
       type: load
       source:
         type: sql
         sql: "SELECT * FROM source_table"
               operational_metadata: false  # Disables all metadata
        target: v_clean_data

**Additive behavior example:**

.. code-block:: yaml
   :caption: Complete example showing additive behavior
   :linenos:
   :emphasize-lines: 4, 9, 18-20

   # Preset defines base columns
   # presets/bronze_layer.yaml
   defaults:
     operational_metadata: ["_processing_timestamp"]

   # FlowGroup adds more columns  
   pipeline: customer_ingestion
   flowgroup: load_customers
   operational_metadata: ["_source_file_path", "_record_hash"]

   actions:
     - name: load_customer_files
       type: load
       source:
         type: cloudfiles
         path: "/mnt/data/*.json"
       # Action adds even more columns
       operational_metadata:
         - "_pipeline_name"
         - "_custom_business_logic"
       target: v_customers_raw

   # Final result: ALL columns combined
   # ✓ _processing_timestamp      (from preset)
   # ✓ _source_file_path          (from flowgroup)  
   # ✓ _record_hash               (from flowgroup)
   # ✓ _pipeline_name             (from action)
   # ✓ _custom_business_logic     (from action)

Usage Patterns
~~~~~~~~~~~~~~

**Enable all available columns:**

.. code-block:: yaml

   operational_metadata: true

**Select specific columns:**

.. code-block:: yaml

   operational_metadata:
     - "_processing_timestamp"
     - "_source_file_path"
     - "_record_hash"

**Disable metadata:**

.. code-block:: yaml

   operational_metadata: false

**Generated Python code:**

.. code-block:: python
   :caption: Generated DLT code with operational metadata
   :linenos:
   :emphasize-lines: 8-11

   @dp.temporary_view()
   def v_customers_raw():
       """Load customer files from landing zone"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "json") \
           .load("/mnt/landing/customers/*.json")
       
       # Add operational metadata columns
       df = df.withColumn('_processing_timestamp', F.current_timestamp())
       df = df.withColumn('_source_file_path', F.col('_metadata.file_path'))
       df = df.withColumn('_record_hash', F.xxhash64(*[F.col(c) for c in df.columns]))
       
       return df


.. danger::
   - When you add operational metadata columns to an upstream action,
     if your downstream action is a transformation, for example SQL transform,
     you need to make sure they are included in the SQL query.

Internal Implementation Note
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

The codebase maintains strict semantic separation between single and multi-document YAML files:

- ``load_yaml_file()`` - For single-document files (configs, templates, presets)
  
  * Validates exactly one document exists
  * Raises ``MultiDocumentError`` (LHP-IO-003) for empty files or files with multiple documents
  * Used for templates, presets, configs, and other single-document files

- ``load_yaml_documents_all()`` - For multi-document files (flowgroup files only)
  
  * Returns list of all documents
  * Used exclusively for flowgroup YAML files that may contain multiple flowgroups

This strict validation prevents accidental misuse and catches bugs early. If you encounter a
``MultiDocumentError``, the error message will guide you to the correct loading method.

What's Next?
------------

Now that you understand the core building blocks of Lakehouse Plumber, explore these advanced features:

* **Dependency Analysis** - Understand how your pipelines depend on each other and generate orchestration jobs automatically. See :doc:`databricks_bundles`.
* **Templates & Presets** - Reuse common patterns across your pipelines. See :doc:`templates_reference`.
* **Databricks Bundles** - Deploy and manage your pipelines as code. See :doc:`databricks_bundles`.

For hands-on examples and complete workflows, check out :doc:`getting_started`.
