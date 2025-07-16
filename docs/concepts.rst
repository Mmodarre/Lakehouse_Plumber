Concepts & Architecture
=======================

At its core Lakehouse Plumber converts **declarative YAML** into regular
Databricks Delta Live Tables (DLT) Python code.  The YAML files are intentionally
simple – the heavy-lifting happens inside the Plumber engine at generation time.
This page explains the key building blocks you will interact with.

.. contents:: Page outline
   :depth: 2
   :local:

FlowGroups
----------
A **FlowGroup** represents a logical slice of your pipeline – often a single
source table or business entity.  Each YAML file contains exactly one
FlowGroup.

Required keys::

   flowgroup: customer_bronze_ingestion  # unique name
   pipeline:  bronze_raw                 # group into a pipeline build
   actions:                              # ordered list of steps
     - …

Splitting large pipelines into FlowGroups keeps configurations small, enables
smart incremental regeneration, and mirrors the way DLT executes tables
independently.

Actions
-------
Every FlowGroup lists one or more **Actions** in the order they should run.
Actions come in three top-level types:

+---------+-------------------------------------------------------------+
| Type    | Purpose                                                     |
+=========+=============================================================+
| *Load*  | Bring data into a temporary **view** (e.g. CloudFiles,      |
|         | Delta, JDBC, SQL, Python).                                  |
+---------+-------------------------------------------------------------+
| *Transform* | Manipulate data in one or more steps (SQL, Python,      |
|             | schema adjustments, data-quality checks, temp tables…). |
+---------+-------------------------------------------------------------+
| *Write* | Persist the final dataset – *streaming_table* or            |
|         | *materialized_view*.                                        |
+---------+-------------------------------------------------------------+

You may chain **zero or many Transform actions** between a Load and a Write.

For a complete catalogue of Action sub-types and their options see
:doc:`actions_reference`.

Presets
-------
A **Preset** is a YAML file that provides default configuration snippets you can
reuse across FlowGroups.  Typical examples:

* Standardised table properties for all Bronze streaming tables.
* Company-wide data-quality policy for Personally Identifiable Information.

Usage inside a FlowGroup::

   presets:
     - bronze_layer

Templates
---------
While presets inject reusable **values**, **Templates** inject reusable **action
patterns** – think of them as parametrised macros.

In a template file you define parameters and a list of actions that reference
those parameters.  Inside a FlowGroup you apply the template and provide actual
arguments::

   use_template: standard_ingestion
   template_parameters:
     source_path: /mnt/landing/customers/*.json
     table_name:  customers

Substitutions & Secrets
-----------------------
Tokens wrapped in ``{token}`` or ``${token}`` are replaced at generation time
using files under ``substitutions/<env>.yaml``.

Example ``substitutions/dev.yaml``::

   catalog: dev_catalog
   bronze_schema: bronze

**Secret references** use the ``${secret:scope/key}`` syntax.  Plumber validates
scope aliases and collects every secret used by the pipeline, making reviews and
approvals easier.

Operational Metadata
--------------------
Add lineage columns without writing code.  At project, preset, FlowGroup or
Action level specify::

   operational_metadata: true          # all default columns

…or provide an explicit list (order preserved)::

   operational_metadata:
     - _ingestion_timestamp
     - _source_file

The utility class ensures the column list is valid for the chosen target type
(view, streaming_table, materialized_view).

State Management & Smart Generation
-----------------------------------
Lakehouse Plumber keeps a small **state file** under ``.lhp_state.json`` that
maps generated Python files to their source YAML.  It records checksums and
dependency links so that future `lhp generate` runs can:

* re-process only *new* or *stale* FlowGroups.
* skip files whose inputs did not change.
* optionally clean up orphaned files when you delete YAML.

This behaviour is similar to Gradle’s incremental build or Terraform’s state
management.

Dependency Resolver
-------------------
Transforms may reference earlier views (or tables) via the ``source`` field.
Plumber’s resolver builds a DAG, checks for cycles, and ensures downstream
FlowGroups regenerate when upstream definitions change.

Putting It Together
-------------------
The figure below shows a high-level data flow for one FlowGroup.

.. mermaid::

   graph TD
       subgraph Generate Time
           A[FlowGroup YAML] -->|Parser| B(Config Objects)
           B -->|Templates & Presets| C[Resolved Config]
           C -->|Substitutions| D[Enriched Config]
           D -->|Validation & DAG| E[Action Orchestrator]
           E -->|Jinja2| F[Python DLT Script]
       end
       F -->|Commit / Upload| G[DLT Pipeline] 