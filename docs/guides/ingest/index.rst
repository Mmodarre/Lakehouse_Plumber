======
Ingest
======

.. meta::
   :description: Choose how to read data into a Lakehouse Plumber pipeline — Auto Loader for files, Delta for tables, JDBC for databases, Kafka for streams, SQL for queries, or a custom Python data source.

A ``load`` action reads data into a pipeline. Which one you reach for depends on
**where the data lives**.

.. list-table::
   :header-rows: 1
   :widths: 26 40 34

   * - Your data is…
     - Reach for
     - Guide
   * - Files landing in cloud storage (JSON, CSV, Parquet)
     - **Auto Loader** — incremental, exactly-once file ingestion
     - :doc:`auto-loader`
   * - An existing Delta table (any catalog)
     - **Delta** load — batch or streaming, cross-catalog
     - :doc:`delta`
   * - The result of a SQL query over existing tables
     - **SQL** load
     - :doc:`sql`
   * - In an operational database (Postgres, MySQL, …)
     - **JDBC** load
     - :doc:`jdbc`
   * - A streaming Kafka topic
     - **Kafka** load
     - :doc:`kafka`
   * - Behind an API or protocol with no built-in loader
     - A **custom Python data source**
     - :doc:`custom-datasource`

.. toctree::
   :maxdepth: 1
   :hidden:

   Auto Loader (files) <auto-loader>
   Delta table <delta>
   SQL query <sql>
   JDBC database <jdbc>
   Kafka topic <kafka>
   Custom data source <custom-datasource>
