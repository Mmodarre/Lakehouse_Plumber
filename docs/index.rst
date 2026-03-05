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
=================

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
See :doc:`getting_started` for the full template and generated output.

Quick Start
===========

Get started in minutes:

.. code-block:: bash

   pip install lakehouse-plumber
   lhp init my_project --bundle
   cd my_project

   # Edit your YAML flowgroups (IntelliSense auto-configured)
   lhp validate --env dev
   lhp generate --env dev

   # Inspect the generated/ directory — readable Python ready for Databricks

.. note::
   **New to LHP?** Follow the :doc:`getting_started` tutorial to build your first pipeline in 10 minutes.

Core Workflow
=============

The execution model is deliberately simple:

.. mermaid::

   graph LR
       A[Load] --> B{0..N Transform}
       B --> C[Write]

1. **Load**    Ingest raw data from CloudFiles, Delta, JDBC, SQL, or custom Python.
2. **Transform**   Apply *zero or many* transforms (SQL, Python, schema, data-quality, temp-tables…).
3. **Write**   Persist results as Streaming Tables, Materialized Views, or Snapshots.

Features at a Glance
====================

**Pipeline Definition**

* **Actions** — Load | Transform | Write with many sub-types (see :doc:`actions/index`).
* **Sinks** — Stream to external destinations: Delta tables, Kafka, Event Hubs, custom APIs.
* **CDC & SCD** — change-data capture SCD type 1 and 2, and snapshot ingestion.
* **Append Flows** — multi-source writes to a single streaming table.
* **Data-Quality** — declarative expectations integrated into transforms.
* **Seeding** — seed data from existing tables using Lakeflow native features.

**Reusability**

* **Presets & Templates** — reuse patterns without copy-paste.
* **Local Variables** — flowgroup-scoped variables (``%{var}``) reduce repetition.
* **Substitutions** — environment-aware tokens & secret references.

**Operations**

* **Operational Metadata** — custom audit columns and metadata.
* **Pipeline Monitoring** — centralized event log aggregation and analysis (see :doc:`monitoring`).
* **Dependency Analysis** — automatic dependency detection and orchestration job generation (see :doc:`dependency_analysis`).
* **Smart State Management** — regenerate only what changed; cleanup orphaned code.

**Developer Experience**

* **IntelliSense** — VS Code schema hints & YAML completion (automatically configured).

Next Steps
==========

**Getting Started**

* :doc:`getting_started` – a hands-on walk-through using the ACMI demo project.
* :doc:`examples` – real-world examples and sink configurations.

**Configuration Guides**

* :doc:`concepts` – deep-dive into FlowGroups, Actions, presets, templates and more.
* :doc:`substitutions` – environment tokens, local variables, and secret management.
* :doc:`operational_metadata` – audit columns, version requirements, and event log configuration.
* :doc:`multi_flowgroup_guide` – reduce file proliferation with multiple flowgroups per YAML file.
* :doc:`actions/index` – complete reference for all action types and sub-types.
* :doc:`templates_reference` – comprehensive guide to creating and using templates.
* :doc:`dynamic_templates_guide` – conditionals, loops, and advanced Jinja2 features.
* :doc:`presets_reference` – reusable default configurations.

**Deployment & Operations**

* :doc:`databricks_bundles` – integrate with Databricks Asset Bundles for production deployments.
* :doc:`monitoring` – centralized event log monitoring and analysis across all pipelines.
* :doc:`dependency_analysis` – pipeline dependency analysis and orchestration job generation.
* :doc:`cicd_reference` – CI/CD patterns, deployment strategies, and DataOps best practices.

**Reference**

* :doc:`cli` – command-line reference.
* :doc:`errors_reference` – error codes, causes, and resolution steps.
* :doc:`api` – REST API reference.

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Getting Started

   getting_started
   examples

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Configuration Guides

   concepts
   substitutions
   operational_metadata
   multi_flowgroup_guide
   actions/index
   templates_reference
   dynamic_templates_guide
   presets_reference

.. toctree::
   :maxdepth: 2
   :hidden:
   :caption: Deployment & Operations

   databricks_bundles
   monitoring
   dependency_analysis
   cicd_reference

.. toctree::
   :maxdepth: 1
   :hidden:
   :caption: Reference

   cli
   errors_reference
   api