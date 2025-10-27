CLI Reference
=============

The **Lakehouse Plumber** command-line interface provides project creation,
validation, code generation, state inspection and more.  All commands are
implemented with `click <https://click.palletsprojects.com>`_ so you can use the
usual ``--help`` flags.

.. click:: lhp.cli.main:cli
   :prog: lhp
   :nested: full

.. seealso::

   For detailed information about dependency analysis and orchestration job generation, see :doc:`databricks_bundles`.

Project Initialization
======================

The ``lhp init`` command creates a new LakehousePlumber project with the complete directory structure and configuration templates.

Basic Usage
-----------

.. code-block:: bash

   # Create a standard project
   lhp init my_project

   # Create a Databricks Asset Bundle project
   lhp init my_project --bundle

**Created Directory Structure:**

.. code-block:: text

   my_project/
   ├── config/                    # Configuration templates
   │   ├── job_config.yaml.tmpl
   │   └── pipeline_config.yaml.tmpl
   ├── pipelines/                 # Pipeline flowgroups
   ├── templates/                 # Reusable action templates
   ├── presets/                   # Configuration presets
   ├── substitutions/            # Environment-specific values
   ├── schemas/                  # Table schema definitions
   ├── expectations/             # Data quality expectations
   ├── generated/                # Generated Python code (gitignored)
   └── resources/                # Bundle resources (--bundle only)

Configuration Templates
-----------------------

The ``config/`` directory contains template files for customizing Databricks job and pipeline settings:

**job_config.yaml.tmpl**

Template for customizing orchestration job configuration used with ``lhp deps`` command:

- Job execution settings (max_concurrent_runs, timeout, performance_target)
- Queue configuration
- Email and webhook notifications
- Scheduling (Quartz cron expressions)
- Tags and permissions

**Usage:**

.. code-block:: bash

   # 1. Copy and customize the template
   cd my_project
   cp config/job_config.yaml.tmpl config/job_config.yaml
   # Edit config/job_config.yaml with your settings

   # 2. Use with lhp deps command
   lhp deps --job-config config/job_config.yaml --bundle-output

See :doc:`databricks_bundles` for detailed job configuration options.

**pipeline_config.yaml.tmpl**

Template for customizing Delta Live Tables (DLT) pipeline settings used with ``lhp generate`` command:

- Compute configuration (serverless vs. classic clusters)
- DLT edition and runtime channel
- Processing mode (continuous vs. triggered)
- Notifications and tags
- Event logging

**Usage:**

.. code-block:: bash

   # 1. Copy and customize the template
   cp config/pipeline_config.yaml.tmpl templates/bundle/pipeline_config.yaml
   # Edit templates/bundle/pipeline_config.yaml with your settings

   # 2. Auto-loaded when generating (if at default location)
   lhp generate -e dev

   # 3. Or specify custom path
   lhp generate -e dev --pipeline-config config/my_pipeline_config.yaml

See :doc:`databricks_bundles` for detailed pipeline configuration options.

.. note::
   Template files (\*.tmpl) are provided as starting points. Copy and remove the
   ``.tmpl`` extension to activate them. This allows you to keep the original
   templates as reference while customizing your own versions.

Test Generation Workflow
========================

By default, Lakehouse Plumber skips test actions during code generation for faster builds and cleaner production pipelines. Use the ``--include-tests`` flag to include data quality tests when needed.

**Common Usage Patterns:**

.. code-block:: bash

   # Production builds (default) - fast, clean pipelines
   lhp generate -e prod
   lhp generate -e dev

   # Development with data quality testing
   lhp generate -e dev --include-tests

   # Validation always checks tests (regardless of generation)
   lhp validate -e dev

**Flag Behavior:**

- **Without flag**: Test actions are validated but not generated, test-only flowgroups are skipped entirely
- **With flag**: All test actions are included in generated code as temporary DLT tables
- **Validation**: Always processes test actions to catch configuration errors early

**Examples:**

.. code-block:: bash

   # Skip tests for faster CI/CD builds
   lhp generate -e prod --force --dry-run

   # Include tests for comprehensive validation
   lhp generate -e dev --include-tests --force

   # Preview test generation without writing files
   lhp generate -e dev --include-tests --dry-run

.. note::
   Test actions generate temporary DLT tables that persist test results for inspection and debugging while being automatically cleaned up when the pipeline completes.