Actions Reference
=================

This page lists every **Action sub-type** supported by Lakehouse Plumber – what
it does, key configuration fields, and a minimal YAML example.  Use it as a
quick lookup when writing FlowGroups.

.. contents:: Table of Contents
   :depth: 1
   :local:

Load Actions
------------

+--------------+------------------------------------------------------------------+
| Sub-type     | Purpose & Source                                                 |
+==============+==================================================================+
| cloudfiles   | Databricks *Auto Loader* (CloudFiles) – stream files from      |
|              | object storage (CSV, JSON, Parquet, …).                         |
+--------------+------------------------------------------------------------------+
| delta        | Read from an existing Delta table / Change Data Feed (CDC).     |
+--------------+------------------------------------------------------------------+
| sql          | Execute an arbitrary SQL query and load the result as a view.   |
+--------------+------------------------------------------------------------------+
| jdbc         | Ingest from an external RDBMS via JDBC with secret handling.    |
+--------------+------------------------------------------------------------------+
| python       | Call custom Python code (path or inline), returning a           |
|              | DataFrame.                                                       |
+--------------+------------------------------------------------------------------+

Example – CloudFiles::

   - name: load_customer_raw
     type: load
     sub_type: cloudfiles
     source:
       type: cloudfiles
       path: /mnt/landing/customers/*.json
       format: json
       schema_hints: true
       schema_evolution_mode: addNewColumns
     target: v_customer_raw

Transform Actions
-----------------

+--------------+------------------------------------------------------------------+
| Sub-type     | Purpose                                                          |
+==============+==================================================================+
| sql          | Run an inline SQL statement or external ``.sql`` file.          |
+--------------+------------------------------------------------------------------+
| python       | Apply arbitrary PySpark code; useful for complex logic.         |
+--------------+------------------------------------------------------------------+
| schema       | Add / drop / rename columns or change data types.               |
+--------------+------------------------------------------------------------------+
| data_quality | Attach *expectations* (fail, warn, drop) to validate data.      |
+--------------+------------------------------------------------------------------+
| temp_table   | Create an intermediate temp table / view for re-use.            |
+--------------+------------------------------------------------------------------+

Example – SQL Transform::

   - name: transform_customer_clean
     type: transform
     sub_type: sql
     source: v_customer_raw
     target: v_customer_clean
     sql: |
       SELECT *
       FROM $source
       WHERE customer_id IS NOT NULL

Write Actions
-------------

+-------------------+-------------------------------------------------------------+
| Sub-type          | Purpose                                                     |
+===================+=============================================================+
| streaming_table   | Create/append to a Delta streaming table in Unity          |
|                   | Catalog (supports CDF, table properties).                  |
+-------------------+-------------------------------------------------------------+
| materialized_view | Create a DLT *LIVE VIEW* that refreshes on a schedule.     |
+-------------------+-------------------------------------------------------------+

Example – Streaming Table::

   - name: write_customer_bronze
     type: write
     sub_type: streaming_table
     source: v_customer_clean
     write_target:
       type: streaming_table
       catalog: dev_catalog
       database: bronze
       table: customer
       table_properties:
         delta.enableChangeDataFeed: "true"
         quality: bronze

Further Reading
---------------
* :doc:`concepts` – deeper dive into how Actions fit inside FlowGroups.
* `Reference templates <https://github.com/.../Reference_Templates>`_ – fully-
  documented YAML files covering every option. 