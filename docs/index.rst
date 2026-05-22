.. Lakehouse Plumber documentation master file

====================================
Lakehouse Plumber
====================================

.. meta::
   :description: YAML-driven framework for generating Databricks Lakeflow Declarative Pipelines. Eliminate boilerplate with reusable templates and presets.

Managing dozens of Lakeflow/DLT pipelines means thousands of lines of repetitive Python —
inconsistent patterns, boilerplate sprawl, and painful maintenance across environments.

Lakehouse Plumber turns concise YAML **actions** into fully-featured
Databricks Lakeflow Declarative Pipelines (formerly Delta Live Tables) — without hiding the Databricks
platform you already know and love.

How LHP Solves It
-----------------

- **Eliminates boilerplate** — a template + 5-line config replaces 86 lines of Python per table.
- **Zero runtime overhead** — pure code generation, not a runtime framework.
- **Transparent output** — readable Python files, version-controlled and debuggable in the Databricks IDE.
- **Fits DataOps workflows** — CI/CD, automated testing, multi-environment substitutions.
- **No lock-in** — the output is plain Python & SQL you own and control.
- **Data democratization** — power users create artifacts within platform standards.

**Real-World Example**

Instead of repeating 86 lines of Python per table, write a **5-line configuration**:

.. code-block:: yaml
   :caption: customer_ingestion.yaml (5 lines per table)

   pipeline: raw_ingestions
   flowgroup: customer_ingestion

   use_template: csv_ingestion_template
   template_parameters:
     table_name: customer
     landing_folder: customer

**Result:** 4,300 lines of repetitive Python → 250 lines total (1 template + 50 simple configs).
See :doc:`quickstart` for the full template and generated output.

Quick Start
-----------

Get started in minutes:

.. code-block:: bash

   pip install lakehouse-plumber
   lhp init my_project
   cd my_project

   # Edit your YAML flowgroups (IntelliSense auto-configured)
   lhp validate --env dev
   lhp generate --env dev

   # Inspect the generated/ directory — readable Python ready for Databricks

.. note::
   **New to LHP?** Follow the :doc:`quickstart` to build your first pipeline in 10 minutes.

Core Workflow
-------------

The execution model is deliberately simple:

.. mermaid::

   graph LR
       A[Load] --> B{0..N Transform}
       B --> C[Write]

1. **Load**    Ingest raw data from CloudFiles, Delta, JDBC, SQL, or custom Python.
2. **Transform**   Apply *zero or many* transforms (SQL, Python, schema, data-quality, temp-tables…).
3. **Write**   Persist results as Streaming Tables, Materialized Views, or Snapshots.

Where to next
-------------

The sidebar groups documentation by purpose, following the
`Diátaxis <https://diataxis.fr/>`_ framework:

* **Get Started** — install LHP, set up your editor, and ship your first pipeline.
* **How-to** — task-shaped recipes for common data-engineering problems.
* **Explanation** — the *why* behind LHP's design and patterns.
* **Reference** — exhaustive lookup tables for CLI flags, YAML keys, error codes, and the public API.

If you have a specific problem, jump straight to :doc:`how_to_index`. If you
want to understand the execution model first, read :doc:`architecture`.

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Get Started

   quickstart
   editor_setup
   requirements

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: How-to

   how_to_index
   decisions
   ingest_with_autoloader
   pipeline_patterns
   multi_flowgroup_guide
   dynamic_templates_guide
   configure_bundles
   configure_catalog_schema
   enable_monitoring
   quarantine_records
   migrate_from_dlt
   troubleshooting
   cicd
   actions/test_reporting

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Explanation

   architecture
   skills_concept
   best_practices/index

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Reference

   cli
   actions/index
   substitutions
   operational_metadata
   templates_reference
   presets_reference
   blueprints
   bundle_config_reference
   monitoring_reference
   dependency_analysis
   quarantine
   errors_reference
   glossary
   api
   changelog
