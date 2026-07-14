======
Guides
======

.. meta::
   :description: Task-oriented guides for Lakehouse Plumber — ingest from any source, choose a write mode, reuse and scale patterns, and operate your pipelines.

The Get Started course walks the whole path once. These guides go deep on one
task at a time — a specific source to ingest, a write mode to choose, a reuse
tool to reach for. Each is self-contained and **fixture-backed**: the YAML, the
command output, and the generated Lakeflow are real and regenerated on every
build, so nothing here can drift from what Lakehouse Plumber actually emits.

Ingest
======

Read data into a pipeline from a specific source.

.. toctree::
   :maxdepth: 1

   ingest/auto-loader
   ingest/delta
   ingest/sql
   ingest/jdbc
   ingest/kafka
   ingest/custom-datasource

Transform
=========

Reshape, enrich, and validate rows between a load and a write.

.. toctree::
   :maxdepth: 1

   transform/sql
   transform/python
   transform/schema
   transform/temp-table
   transform/data-quality

Write
=====

Persist results in the right kind of target.

.. toctree::
   :maxdepth: 1

   write/streaming-table-standard
   write/streaming-table-cdc
   write/streaming-table-snapshot-cdc
   write/materialized-view
   write/sinks

Reuse and scale
===============

Remove duplication, then multiply your pipelines.

.. toctree::
   :maxdepth: 1

   reuse-and-scale/substitutions-and-secrets
   reuse-and-scale/templates
   reuse-and-scale/blueprints
   reuse-and-scale/multi-flowgroup

Quality and ops
===============

Guard data quality and see what your pipelines are doing.

.. toctree::
   :maxdepth: 1

   quality-and-ops/quarantine
   quality-and-ops/monitoring
   quality-and-ops/test-reporting
   quality-and-ops/dependency-analysis

Develop
=======

Author, run, and iterate on pipelines locally.

.. toctree::
   :maxdepth: 1

   develop/web-ide
   develop/ai-assistant
   develop/sandbox
   develop/editor-setup
   develop/coding-agents

Ship
====

Test, package, and deploy — and migrate what you already have.

.. toctree::
   :maxdepth: 1

   ship/ci-cd
   ship/package-as-wheels
   ship/migrate-from-dlt
