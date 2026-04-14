Concepts & Architecture
=======================

.. meta::
   :description: Core concepts: Pipelines, FlowGroups, Actions, substitutions, presets, and templates. Understand the LHP architecture.

At its core Lakehouse Plumber converts **declarative YAML** into regular
Databricks Lakeflow Declarative Pipelines (ETL) Python code.  The YAML files are intentionally
simple and the heavy-lifting happens inside the Plumber engine at generation time.
This page explains the key building blocks you will interact with.


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

   job_name:
     - NCR            # Optional: Assign flowgroup to a specific orchestration job
   variables:         # Optional: Define local variables for this flowgroup
     entity: customer
     table: customer_raw

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
   job_name:
     - NCR  # Assigns this flowgroup to the "NCR" orchestration job
   
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

   - Lakeflow Declarative Pipelines are **declarative** (as the name suggests) hence the order of the actions is determined at runtime by the Lakeflow engine based on the dependencies between the tables/views.

   - **YAML files** can contain one flowgroup (traditional) or multiple flowgroups (see :doc:`multi_flowgroup_guide`).

Actions
-------
Every FlowGroup lists one or more **Actions** 
Actions come in three top-level types:

+----------------+----------------------------------------------------------+
| Type           | Purpose                                                  |
+================+==========================================================+
| **Load**       | Bring data into a temporary **view** (e.g. CloudFiles,   |
|                | Delta, JDBC, SQL, Python, custom_datasource).            |
+----------------+----------------------------------------------------------+
| **Transform**  | Manipulate data in one or more steps (SQL, Python,       |
|                | schema adjustments, data-quality checks, temp tables…).  |
+----------------+----------------------------------------------------------+
| **Write**      | Persist the final dataset to a *streaming_table*,        |
|                | *materialized_view*, or external *sink* (Kafka,          |
|                | Delta, custom API).                                      |
+----------------+----------------------------------------------------------+


.. note::
   - You may chain **zero or many Transform actions** between a Load and a Write.

.. important::
   - the order of the actions is determined at runtime by the Lakeflow engine based on the dependencies between the tables/views, Not the order in the YAML file or the generated Python file.


For a complete catalogue of Action sub-types and their options see
:doc:`actions/index`.

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
         path: "${landing_volume}/{{ landing_folder }}/*.csv"
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
         database: "${catalog}.${raw_schema}"
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

LakehousePlumber provides two configuration files to customize how your pipelines and 
orchestration jobs are deployed to Databricks:

- **Pipeline Configuration** (``pipeline_config.yaml``) - Controls SDP pipeline settings like compute, runtime, notifications
- **Job Configuration** (``job_config.yaml``) - Controls orchestration job settings like concurrency, schedules, permissions

.. seealso::
   For complete configuration options, examples, and best practices, see the Configuration Management section in :doc:`databricks_bundles`.

Substitutions & Secrets
-----------------------

LakehousePlumber supports environment-aware tokens, local variables, secret references, and
file substitutions that make your pipeline definitions portable across environments.

.. seealso::
   For the full reference on all substitution syntaxes, processing order, secret management,
   and file substitution support, see :doc:`substitutions`.

Operational Metadata
---------------------

Operational metadata columns provide lineage, data provenance, and processing context.
They are defined at the project level in ``lhp.yaml`` and can be selectively enabled
at the preset, flowgroup, or action level with additive behavior.

.. seealso::
   For the full reference on column definitions, target type compatibility, usage patterns,
   version requirements, and event log configuration, see :doc:`operational_metadata`.

State Management & Smart Generation
------------------------------------

Lakehouse Plumber keeps a small **state file** under ``.lhp_state.json`` that
maps generated Python files to their source YAML.  It records checksums and
dependency links so that future `lhp generate` runs can:

* re-process only *new* or *stale* FlowGroups.
* skip files whose inputs did not change.
* optionally clean up orphaned files when you delete YAML.

This behaviour is similar to Gradle's incremental build or Terraform's state
management.

**How state management works:**

.. code-block:: json
   :caption: .lhp_state.json example
   :linenos:

   {
     "version": "1.0",
     "generated_files": {
       "customer_ingestion.py": {
         "source_yaml": "pipelines/bronze/customer_ingestion.yaml",
         "checksum": "a1b2c3d4e5f6",
         "environment": "dev",
         "dependencies": ["presets/bronze_layer.yaml"]
       }
     }
   }

**Benefits:**

- **Faster regeneration** - Only changed files are processed
- **Dependency tracking** - Upstream changes trigger downstream regeneration
- **Cleanup support** - Detect and remove orphaned generated files
- **CI/CD optimization** - Skip unchanged pipeline generation in builds

Dependency Resolver
-------------------

Transforms may reference earlier views or tables via the ``source`` field. LHP builds a
directed acyclic graph (DAG) of these references, detects cycles, and ensures downstream
FlowGroups regenerate when upstream definitions change.

.. seealso::
   :doc:`dependency_analysis` for the full 5-step resolution process, dependency chain
   examples, and the ``lhp deps`` command.

Pipeline Generation Workflow
----------------------------

The complete pipeline generation process follows this workflow:

.. mermaid::

   graph TD
       subgraph "Discovery Phase"
           A[Scan YAML Files] --> B[Apply Include Patterns]
           B --> C[Parse FlowGroups]
       end

       subgraph "Resolution Phase"
           C --> D[Apply Presets]
           D --> E[Expand Templates]
           E --> F[Apply Substitutions]
           F --> G[Validate Configuration]
       end

       subgraph "Generation Phase"
           G --> H[Resolve Dependencies]
           H --> I[Check State]
           I --> J{Changed?}
           J -->|Yes| K[Generate Code]
           J -->|No| L[Skip Generation]
           K --> M[Update State]
           L --> M
       end

       subgraph "Output"
           M --> N[Python DLT Files]
       end

**Key optimization points:**

- **Smart discovery** - Include patterns reduce files to process
- **Incremental generation** - State tracking skips unchanged files
- **Dependency awareness** - Changes propagate to affected downstream files
- **Validation early** - Catch errors before code generation
- **Parallel processing** - Independent FlowGroups can be processed simultaneously

Troubleshooting
---------------

Common issues include state management problems (stale ```.lhp_state.json```), dependency
resolution failures, and performance with large projects.

.. seealso::
   :doc:`errors_reference` for error codes, resolution steps, and general troubleshooting
   tips (state debugging, dependency debugging, performance optimization).

What's Next?
------------

Now that you understand the core building blocks of Lakehouse Plumber, explore these topics:

* :doc:`substitutions` - Environment tokens, local variables, secrets, and file substitution support.
* :doc:`operational_metadata` - Audit columns, version requirements, and event log configuration.
* :doc:`templates_reference` - Reuse common patterns across your pipelines.
* :doc:`databricks_bundles` - Deploy and manage your pipelines as code.
* :doc:`dependency_analysis` - Pipeline dependency analysis and orchestration job generation.

For hands-on examples and complete workflows, check out :doc:`getting_started`.
