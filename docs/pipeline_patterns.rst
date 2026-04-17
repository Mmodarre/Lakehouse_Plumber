=======================================
Pipeline Patterns
=======================================

.. meta::
   :description: Practical pipeline patterns for common data engineering scenarios — multi-source ingestion, path filtering, CloudFiles glob patterns, and fan-in architectures.

Reusable patterns for common data engineering scenarios encountered in production
Lakehouse Plumber projects. Each pattern includes the YAML configuration, generated
Python output, and relevant caveats.


Multi-Source Ingestion (Fan-In)
-------------------------------

Consolidate data from **multiple sources into a single streaming table** — for example,
the same log schema arriving from S3 buckets in different regions or accounts.

LHP natively supports multiple write actions targeting the same streaming table. It
generates a single ``dp.create_streaming_table()`` call with multiple
``@dp.append_flow()`` functions — one per source. Each append flow gets its own
independent checkpoint, so if one source has issues the others continue processing
normally.

The key is the ``create_table`` flag: the **first** write action creates the table
(``create_table: true``, the default), and subsequent writes append to it
(``create_table: false``).

Configuration
~~~~~~~~~~~~~

.. code-block:: yaml
   :caption: pipelines/bronze/logs_multi_region.yaml
   :linenos:

   pipeline: raw_ingestions
   flowgroup: logs_multi_region

   actions:
     # Load from US bucket
     - name: load_logs_us
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket-us-east-1/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.maxFilesPerTrigger: 100
       target: v_logs_us

     # Load from EU bucket
     - name: load_logs_eu
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket-eu-west-1/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.maxFilesPerTrigger: 100
       target: v_logs_eu

     # First write — creates the table
     - name: write_logs_us
       type: write
       source: v_logs_us
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: unified_logs
         create_table: true
       description: "Write US region logs to unified table"

     # Second write — appends to the same table
     - name: write_logs_eu
       type: write
       source: v_logs_eu
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: unified_logs
         create_table: false
       description: "Append EU region logs to unified table"

Generated Output
~~~~~~~~~~~~~~~~

.. code-block:: python
   :caption: Generated logs_multi_region.py
   :linenos:

   from pyspark import pipelines as dp

   @dp.temporary_view()
   def v_logs_us():
       """Write US region logs to unified table"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "parquet") \
           .option("cloudFiles.maxFilesPerTrigger", 100) \
           .load("s3://my-bucket-us-east-1/logs/*.parquet")
       return df

   @dp.temporary_view()
   def v_logs_eu():
       """Append EU region logs to unified table"""
       df = spark.readStream \
           .format("cloudFiles") \
           .option("cloudFiles.format", "parquet") \
           .option("cloudFiles.maxFilesPerTrigger", 100) \
           .load("s3://my-bucket-eu-west-1/logs/*.parquet")
       return df

   # Single table creation
   dp.create_streaming_table(
       name="catalog.bronze.unified_logs",
       comment="Write US region logs to unified table"
   )

   # One append flow per source
   @dp.append_flow(target="catalog.bronze.unified_logs", name="f_unified_logs_us")
   def f_unified_logs_us():
       """Write US region logs to unified table"""
       df = spark.readStream.table("v_logs_us")
       return df

   @dp.append_flow(target="catalog.bronze.unified_logs", name="f_unified_logs_eu")
   def f_unified_logs_eu():
       """Append EU region logs to unified table"""
       df = spark.readStream.table("v_logs_eu")
       return df

.. important::
   Each streaming table must have exactly one action with ``create_table: true`` across
   the entire pipeline. Additional actions targeting the same table must use
   ``create_table: false``.

Templatising Multi-Source Ingestion
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

When you have many sources following the same pattern, combine this with
:doc:`templates <templates_reference>` to eliminate boilerplate. Define a template for
the load+write pair and invoke it per source, each targeting the same table.

.. seealso::
   - :ref:`BP-11.9 <bp-11-9>` and :ref:`BP-18.4 <bp-18-4>` in :doc:`best_practices`
   - :doc:`actions/write_actions` for full ``streaming_table`` reference


CloudFiles Path Filtering
--------------------------

Three approaches to **exclude specific paths, directories, or files** from a CloudFiles
Auto Loader pipeline — each useful in different scenarios.


Glob Patterns in the Load Path
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

LHP passes the ``source.path`` string directly to the generated ``.load("...")`` call
with no escaping or transformation. Any glob syntax that Spark and CloudFiles support
works as-is in your YAML.

**Supported glob syntax (Databricks Auto Loader):**

+-------------------------+-------------------------------------------+------------------------------+
| Syntax                  | Meaning                                   | Example                      |
+=========================+===========================================+==============================+
| ``*``                   | Match any sequence of characters          | ``/data/*.parquet``          |
+-------------------------+-------------------------------------------+------------------------------+
| ``?``                   | Match any single character                | ``/data/file_?.csv``         |
+-------------------------+-------------------------------------------+------------------------------+
| ``[abc]``               | Match any character in set                | ``/data/[abc]*.csv``         |
+-------------------------+-------------------------------------------+------------------------------+
| ``[a-z]``               | Match any character in range              | ``/data/part_[0-9].csv``     |
+-------------------------+-------------------------------------------+------------------------------+
| ``[^x]``                | Match any character NOT in set            | ``/data/[^_]*.csv``          |
+-------------------------+-------------------------------------------+------------------------------+
| ``{a,b,c}``             | Match any of the alternatives             | ``/data/{sales,events}/``    |
+-------------------------+-------------------------------------------+------------------------------+

.. code-block:: yaml
   :caption: Exclude a specific day using brace expansion
   :linenos:

   actions:
     - name: load_logs_excluding_day16
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/2026/02/{0[1-9],1[0-5],1[7-9],2[0-9]}/logs/*.parquet"
         format: parquet
         options:
           cloudFiles.useStrictGlobber: "true"
       target: v_logs_raw

You can also put the glob pattern in an environment substitution token if it varies by
environment:

.. code-block:: yaml
   :caption: substitutions/dev.yaml

   dev:
     log_path_pattern: "s3://dev-bucket/data/2026/02/{0[1-9],1[0-5],1[7-9],2[0-9]}/logs/*.parquet"

.. code-block:: yaml
   :caption: flowgroup YAML

   source:
     path: "${log_path_pattern}"

.. warning::
   **Caveats for complex glob patterns:**

   - **Character classes inside brace alternatives** (like ``{0[1-9],1[0-5]}``) should work
     at the Hadoop GlobFilter level, but this specific combination is not shown in Databricks
     documentation. **Test in a dev environment** before relying on it in production.
   - **Use** ``[^x]`` **not** ``[!x]`` for negation — the Unix shell ``[!x]`` syntax is
     not supported by Spark's globber.
   - ``**`` **(globstar)** is not documented for Auto Loader — avoid it.
   - **Notification mode** (``cloudFiles.useNotifications: "true"``) with complex globs is
     undocumented territory. Directory listing mode (the default) is the safe choice for
     glob-heavy paths.
   - Consider adding ``cloudFiles.useStrictGlobber: "true"`` (DBR 12.2+) for predictable,
     Spark-standard globbing behaviour. The default globber is more permissive — for example,
     ``*`` can cross directory boundaries.


Post-Load Filter with SQL Transform
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

CloudFiles natively exposes ``_metadata.file_path`` in the loaded DataFrame. Add a
**SQL transform action** between the load and write to filter out unwanted paths using
full SQL regex power.

.. code-block:: yaml
   :caption: Filter excluded paths via SQL transform
   :linenos:

   actions:
     - name: load_all_files
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/"
         format: parquet
       target: v_raw_data

     - name: filter_excluded_paths
       type: transform
       transform_type: sql
       source: v_raw_data
       target: v_filtered_data
       sql: |
         SELECT * FROM stream(v_raw_data)
         WHERE NOT _metadata.file_path RLIKE '.*/exclude_[^/]+/.*'

     - name: write_bronze
       type: write
       source: v_filtered_data
       write_target:
         type: streaming_table
         database: "${catalog}.${bronze_schema}"
         table: my_table
       description: "Write filtered data to bronze layer"

.. note::
   Auto Loader still **reads** all files before the filter is applied. This approach
   works when the excluded files are readable but contain unwanted data. If excluded files
   cannot be read at all (e.g. ``AccessDenied``), use glob patterns or ``pathGlobFilter``
   instead.


pathGlobFilter Option
~~~~~~~~~~~~~~~~~~~~~

To filter by file **name** (not full path), use the ``pathGlobFilter`` reader option.
This is a Spark-native option that filters on the basename of each file after directory
listing.

.. code-block:: yaml
   :caption: Filter by filename pattern
   :linenos:

   actions:
     - name: load_parquet_only
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/"
         format: parquet
         options:
           pathGlobFilter: "*.parquet"
       target: v_raw_data

.. important::
   ``pathGlobFilter`` filters on the **filename only** (basename), not the full path. The
   ``.load()`` path is a prefix filter; ``pathGlobFilter`` is for suffix or name filtering.


Explicit Include List
~~~~~~~~~~~~~~~~~~~~~

Use brace expansion to explicitly list only the directories to include:

.. code-block:: yaml
   :caption: Include only specific directories
   :linenos:

   actions:
     - name: load_selected_sources
       type: load
       readMode: stream
       source:
         type: cloudfiles
         path: "s3://my-bucket/data/{sales,marketing,events}/*.parquet"
         format: parquet
       target: v_selected_data

This generates ``.load("s3://my-bucket/data/{sales,marketing,events}/*.parquet")`` and
Auto Loader will only scan those three directories.

For **truly separate paths** (different buckets or unrelated prefixes), use the
`multi-source ingestion pattern <Multi-Source Ingestion (Fan-In)_>`_ above — multiple
load+write actions targeting the same table with ``create_table: false`` on the
additional writes.


Choosing the Right Approach
~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 35 15 50

   * - Scenario
     - Approach
     - Notes
   * - Exclude specific date partitions
     - Glob patterns
     - Use brace expansion to enumerate included segments
   * - Exclude paths matching a regex
     - SQL transform
     - Full regex power via ``_metadata.file_path``
   * - Filter by file extension or name
     - ``pathGlobFilter``
     - Set in ``source.options``; filters basename only
   * - Include specific named directories
     - Brace expansion
     - Simple and explicit; documented by Databricks
   * - Multiple separate buckets or prefixes
     - Multiple append flows
     - Independent checkpoints per source


.. seealso::
   - :doc:`actions/load_actions` for full CloudFiles load reference
   - :doc:`actions/write_actions` for ``streaming_table`` and append flow details
   - :doc:`best_practices` for enterprise ingestion patterns
