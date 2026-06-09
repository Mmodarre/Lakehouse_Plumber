Actions Reference
=================

.. meta::
   :description: Complete reference for all LHP action types: Load, Transform, Write, and Test. Includes the action execution model, a minimum FlowGroup example, and pointers to each sub-type reference.

Concept
-------

What is an Action
~~~~~~~~~~~~~~~~~

An Action is the atomic unit of work in a Lakehouse Plumber (LHP) FlowGroup. A
Pipeline contains one or more FlowGroups, and a FlowGroup contains an ordered
list of Actions. Each Action declares one step — read a source, transform a
view, persist a table, or assert a quality rule — and LHP compiles it into the
corresponding Lakeflow Declarative Pipelines Python construct.

Action flow
~~~~~~~~~~~

.. mermaid::

   graph LR
       A[Load] --> B{0..N Transform}
       B --> C[Write]

Every FlowGroup starts with one Load, applies zero or more Transforms, and
ends with one Write. Test actions attach data-quality expectations to any
view along the way.

Minimum FlowGroup
~~~~~~~~~~~~~~~~~

This FlowGroup ingests a Delta source, renames a column, and writes the
result to a streaming table:

.. code-block:: yaml
   :caption: pipelines/bronze/customer.yaml

   pipeline: bronze_ingestion
   flowgroup: customer

   actions:
     - name: load_customer
       type: load
       readMode: stream
       source:
         type: delta
         database: samples.tpch
         table: customer_sample
       target: v_customer_raw

     - name: rename_customer_key
       type: transform
       transform_type: sql
       source: v_customer_raw
       target: v_customer
       sql: "SELECT c_custkey AS customer_id, c_name AS name FROM stream(v_customer_raw)"

     - name: write_customer_bronze
       type: write
       source: v_customer
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: customer

Action types reference
----------------------

Actions come in four top-level types:

+----------------+----------------------------------------------------------+
| Type           | Purpose                                                  |
+================+==========================================================+
|| **Load**      || Bring data into a temporary **view** (e.g. CloudFiles,  |
||               || Delta, JDBC, SQL, Python).                              |
+----------------+----------------------------------------------------------+
|| **Transform** || Manipulate data in one or more steps (SQL, Python,      |
||               || schema adjustments, data-quality checks, temp tables…). |
+----------------+----------------------------------------------------------+
|| **Write**     || Persist the final dataset to a *streaming_table* or     |
||               || *materialized_view*.                                    |
+----------------+----------------------------------------------------------+
|| **Test**      || Validate data quality using DLT expectations in         |
||               || temporary tables (uniqueness, referential integrity…).  |
+----------------+----------------------------------------------------------+

Cross-cutting fields
--------------------

A few fields are valid on **any** action type, regardless of its sub-type:

+--------------------------+--------------------------------------------------+
| Field                    | Purpose                                          |
+==========================+==================================================+
|| ``name``                || Unique action name within the FlowGroup.        |
+--------------------------+--------------------------------------------------+
|| ``description``         || Optional documentation for the action.          |
+--------------------------+--------------------------------------------------+
|| ``depends_on``          || Optional list of upstream table references      |
||                         || (``catalog.schema.table`` or ``schema.table``)  |
||                         || that explicitly declare dependency-graph edges  |
||                         || the analyzer cannot parse from the action's     |
||                         || source. Entries are **additive** — they add     |
||                         || edges on top of whatever is parsed. See         |
||                         || :doc:`/dependency_analysis`.                     |
+--------------------------+--------------------------------------------------+

Where to start
--------------

* :doc:`/quickstart` — build your first pipeline end-to-end in about ten minutes,
  including the load and write Actions used above.
* :doc:`/decisions` — decision matrices for picking a load source, write
  target, streaming vs. batch mode, and write mode.

Further Reading
---------------

* `Reference templates (Github Repo) <https://github.com/Mmodarre/Lakehouse_Plumber/tree/main/Reference_Templates>`_ fully
  documented YAML files covering every option.
* Databricks Expectations: `DLT expectations <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_

.. toctree::
   :maxdepth: 1
   :titlesonly:

   load_actions
   transform_actions
   write_actions
   test_actions
