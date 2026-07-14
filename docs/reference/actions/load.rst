Load actions
============

.. meta::
   :description: Reference for LHP load actions — the cloudfiles, delta, sql, python, jdbc, kafka, and custom_datasource source types, their YAML fields, defaults, and minimal examples.

A load action ingests data into a temporary view. Every load action sets
``type: load``, a ``source:`` mapping whose ``source.type`` selects the source
sub-type, and a ``target:`` naming the view it creates::

   - name: <action_name>
     type: load
     source:
       type: <cloudfiles|delta|sql|python|jdbc|kafka|custom_datasource>
       ...
     target: <view_name>

Action-level fields
-------------------

These fields apply to every load sub-type. All except ``source``/``target``
are optional.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Unique action name within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Always ``load``.
   * - ``source``
     - mapping
     - Yes
     - —
     - Source configuration. ``source.type`` selects the sub-type. For ``sql`` this may instead be an inline SQL string.
   * - ``target``
     - string
     - Yes
     - —
     - Name of the temporary view this action creates.
   * - ``readMode``
     - string
     - No
     - sub-type
     - ``batch`` or ``stream``; selects ``spark.read`` vs ``spark.readStream``. Default and constraints vary by sub-type (below). Ignored by ``sql``, ``python``, and ``jdbc``.
   * - ``description``
     - string
     - No
     - auto
     - Docstring for the generated view function.
   * - ``operational_metadata``
     - bool or list[string]
     - No
     - —
     - ``true``/``false`` to toggle metadata columns, or a list of metadata column names to add.

cloudfiles
----------

Streams files into a temporary view via Databricks Auto Loader. Fields live
under ``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``cloudfiles``.
   * - ``path``
     - string
     - Yes
     - —
     - File path or glob to ingest.
   * - ``format``
     - string
     - Yes
     - —
     - File format; feeds ``cloudFiles.format`` (e.g. ``json``, ``csv``, ``parquet``).
   * - ``options``
     - mapping
     - No
     - —
     - ``cloudFiles.*`` and reader options passed to Auto Loader.
   * - ``reader_options``
     - mapping
     - No
     - —
     - Options merged verbatim into the reader.
   * - ``format_options``
     - mapping
     - No
     - —
     - Options prefixed with ``<format>.``.
   * - ``schema``
     - string or mapping
     - No
     - —
     - Schema-file path, or ``{file: <path>}``. Mutually exclusive with ``schema_file`` and ``options.cloudFiles.schemaHints``.
   * - ``schema_file``
     - string
     - No
     - —
     - Back-compat schema-file path.

``readMode`` must be ``stream`` (``batch`` is rejected). Legacy scalar options
(``schema_location``, ``schema_infer_column_types``, ``max_files_per_trigger``,
``schema_evolution_mode``, ``rescue_data_column``) are accepted for
back-compat but are superseded by ``options``.

.. code-block:: yaml

   - name: load_orders
     type: load
     source:
       type: cloudfiles
       path: "${landing_volume}/orders/*.json"
       format: json
     target: v_orders_raw

delta
-----

Reads a Delta table into a temporary view. Fields live under ``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``delta``.
   * - ``catalog``
     - string
     - Yes
     - —
     - Catalog of the source table.
   * - ``schema``
     - string
     - Yes
     - —
     - Schema of the source table.
   * - ``table``
     - string
     - Yes
     - —
     - Source table name.
   * - ``options``
     - mapping
     - No
     - —
     - Delta reader options (values must be non-empty). CDC and time-travel keys go here.
   * - ``where_clause``
     - list[string]
     - No
     - ``[]``
     - Filter expressions applied with ``.where(...)``.
   * - ``select_columns``
     - list[string]
     - No
     - —
     - Column projection applied with ``.select(...)``.

``readMode`` defaults to ``batch`` and accepts ``batch`` or ``stream``.
``options.readChangeFeed: "true"`` requires ``readMode: stream``, or a
``startingVersion``/``startingTimestamp`` bound in batch mode.

.. code-block:: yaml

   - name: load_customers
     type: load
     source:
       type: delta
       catalog: "${catalog}"
       schema: "${bronze_schema}"
       table: customers
       readMode: batch
     target: v_customers

sql
---

Materializes a SQL query into a temporary view. ``source:`` may be an inline
SQL string, or a mapping carrying ``sql`` or ``sql_path``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``sql`` (when ``source`` is a mapping).
   * - ``sql``
     - string
     - One of
     - —
     - Inline SQL query. Provide exactly one of ``sql`` / ``sql_path``.
   * - ``sql_path``
     - string
     - One of
     - —
     - Path to an external ``.sql`` file. Provide exactly one of ``sql`` / ``sql_path``.

``readMode`` is ignored for SQL loads (always a ``spark.sql`` call).
Substitution tokens (``${token}``, ``${secret:scope/key}``) resolve in both
inline SQL and external files.

.. code-block:: yaml

   - name: load_summary
     type: load
     source:
       type: sql
       sql: "SELECT * FROM ${catalog}.${bronze_schema}.orders"
     target: v_orders

python
------

Calls a Python function that returns a DataFrame. Fields live under
``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``python``.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to a ``.py`` file (relative to project root); must end in ``.py``.
   * - ``function_name``
     - string
     - No
     - ``get_df``
     - Entry function, called as ``fn(spark, parameters) -> DataFrame``.
   * - ``parameters``
     - mapping
     - No
     - ``{}``
     - Passed as the function's second argument.

.. code-block:: yaml

   - name: load_external
     type: load
     source:
       type: python
       module_path: "loaders/external.py"
       function_name: get_df
       parameters:
         region: "${region}"
     target: v_external

jdbc
----

Reads from an external database over JDBC. Fields live under ``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``jdbc``.
   * - ``url``
     - string
     - Yes
     - —
     - JDBC URL.
   * - ``user``
     - string
     - Yes
     - —
     - Username; supply via ``${secret:scope/key}``.
   * - ``password``
     - string
     - Yes
     - —
     - Password; supply via ``${secret:scope/key}``.
   * - ``driver``
     - string
     - Yes
     - —
     - JDBC driver class.
   * - ``table``
     - string
     - One of
     - —
     - Table name. Provide exactly one of ``table`` / ``query`` (``query`` wins if both are set).
   * - ``query``
     - string
     - One of
     - —
     - SQL query. Provide exactly one of ``table`` / ``query``.

.. code-block:: yaml

   - name: load_jdbc
     type: load
     source:
       type: jdbc
       url: "jdbc:postgresql://host:5432/db"
       user: "${secret:db/user}"
       password: "${secret:db/password}"
       driver: org.postgresql.Driver
       table: public.orders
     target: v_orders

kafka
-----

Streams from Apache Kafka (or Kafka-protocol-compatible endpoints) into a
temporary view. Fields live under ``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``kafka``.
   * - ``bootstrap_servers``
     - string
     - Yes
     - —
     - Maps to ``kafka.bootstrap.servers``.
   * - ``subscribe``
     - string
     - One of
     - —
     - Topic(s). Provide exactly one of ``subscribe`` / ``subscribePattern`` / ``assign``.
   * - ``subscribePattern``
     - string
     - One of
     - —
     - Topic regex. One of the three subscription methods.
   * - ``assign``
     - string (JSON)
     - One of
     - —
     - Partition assignment. One of the three subscription methods.
   * - ``options``
     - mapping
     - No
     - —
     - Extra ``kafka.*`` options.

``readMode`` must be ``stream`` (``batch`` is rejected). Kafka returns binary
``key``/``value`` columns — deserialize them in a downstream transform.

.. code-block:: yaml

   - name: load_events
     type: load
     source:
       type: kafka
       bootstrap_servers: "broker1:9092,broker2:9092"
       subscribe: orders
     target: v_events

custom_datasource
-----------------

Reads through a user-supplied PySpark ``DataSource`` class. Fields live under
``source:``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``custom_datasource``.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to the ``.py`` DataSource module.
   * - ``custom_datasource_class``
     - string
     - Yes
     - —
     - Name of the ``DataSource`` class to register and use.
   * - ``options``
     - mapping
     - No
     - ``{}``
     - Passed to the reader as ``.option(...)`` calls.

``readMode`` defaults to ``stream``; ``batch`` is also supported. The class's
``name()`` classmethod return value is used as the ``.format(...)`` name.

.. code-block:: yaml

   - name: load_custom
     type: load
     source:
       type: custom_datasource
       module_path: "sources/my_source.py"
       custom_datasource_class: MyDataSource
       options:
         endpoint: "${api_endpoint}"
     target: v_custom
