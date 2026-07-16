======
Guides
======

.. meta::
   :description: Task-oriented guides for Lakehouse Plumber, grouped by the four action kinds plus reuse, operations, development, and shipping. Ingest from any source, choose a write mode, test your data, and operate your pipelines.

The Get Started course walks the whole path once. These guides go deep on one
task at a time. They're grouped by what you're doing — the four **action kinds**
(ingest, transform, write, test), then reuse, operations, development, and
shipping. Each guide is self-contained and **fixture-backed**: the YAML, the
command output, and the generated Lakeflow are real and regenerated on every
build, so nothing here can drift from what Lakehouse Plumber actually emits.

Start at the category that matches your task — each opens with a chooser or a
short map of its guides.

.. grid:: 1 2 2 2
   :gutter: 3

   .. grid-item-card:: Ingest →
      :link: ingest/index
      :link-type: doc

      Read data in — Auto Loader, Delta, SQL, JDBC, Kafka, or a custom source.

   .. grid-item-card:: Transform →
      :link: transform/index
      :link-type: doc

      Reshape between load and write — SQL, Python, schema, quality, quarantine.

   .. grid-item-card:: Write →
      :link: write/index
      :link-type: doc

      Choose a target — streaming table, CDC, snapshot CDC, materialized view, sink.

   .. grid-item-card:: Test →
      :link: test/index
      :link-type: doc

      Unit-test your data with test actions, and report the results.

   .. grid-item-card:: Reuse and scale →
      :link: reuse-and-scale/index
      :link-type: doc

      Remove duplication, then multiply — substitutions, templates, blueprints.

   .. grid-item-card:: Operate →
      :link: ops/index
      :link-type: doc

      Monitoring, logging, and dependency analysis for running pipelines.

   .. grid-item-card:: Develop →
      :link: develop/index
      :link-type: doc

      Author locally — web IDE, AI assistant, sandbox, editor, coding agents.

   .. grid-item-card:: Deploy →
      :link: deploy/index
      :link-type: doc

      Package pipeline code for a workspace — bundles and Python wheels.

   .. grid-item-card:: Ship →
      :link: ship/index
      :link-type: doc

      Validate and ship in CI, and migrate an existing DLT pipeline.

.. toctree::
   :maxdepth: 2
   :hidden:

   ingest/index
   transform/index
   write/index
   test/index
   reuse-and-scale/index
   ops/index
   develop/index
   deploy/index
   ship/index
