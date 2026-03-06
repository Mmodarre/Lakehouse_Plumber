Actions Reference
=================

.. meta::
   :description: Complete reference for all LHP action types: Load, Transform, Write, Test, and Sink with configuration examples.

Actions are the building block of Lakehouse Plumber flowgroups.

Actions Overview
----------------

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

.. toctree::
   :maxdepth: 2

   load_actions
   transform_actions
   write_actions
   test_actions

Further Reading
---------------

* `Reference templates (Github Repo) <https://github.com/Mmodarre/Lakehouse_Plumber/tree/main/Reference_Templates>`_ fully
  documented YAML files covering every option.
* Databricks Expectations: `DLT expectations <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_
