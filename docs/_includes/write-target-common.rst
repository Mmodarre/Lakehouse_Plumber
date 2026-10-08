Shared table and view fields
----------------------------


These ``write_target`` fields apply to ``streaming_table`` and
``materialized_view``. Sinks use their own :doc:`field set </reference/actions/write/sink>`.

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``type``
     - string
     - required
     - ``streaming_table``, ``materialized_view``, or ``sink``.
   * - ``catalog``
     - string
     - —
     - UC catalog.
   * - ``schema``
     - string
     - —
     - UC namespace schema (not DDL — use ``table_schema`` for DDL).
   * - ``table``
     - string
     - —
     - Target table/view name.
   * - ``create_table``
     - bool
     - ``true``
     - Streaming table only; ``snapshot_cdc`` and ``replace`` force ``true``.
   * - ``temporary``
     - bool
     - ``false``
     - Emitted as ``temporary=``.
   * - ``private``
     - bool
     - ``false``
     - Emitted as ``private=``. Creates the table for the pipeline's lifetime without publishing it to the metastore; the dataset is visible only inside the pipeline.
   * - ``comment``
     - string
     - derived
     - Defaults to a description derived from the target table name.
   * - ``table_properties``
     - dict
     - —
     - Delta table properties.
   * - ``tags``
     - dict
     - —
     - UC tags ``{key: value}``; applied by a generated tagging hook (REST API), not table DDL. See :doc:`/reference/config/uc-tagging`.
   * - ``tags_file``
     - string
     - —
     - Path to a unified schema/tags file (convention ``schemas/<table>.yaml``) whose table-level ``tags`` and per-column ``tags`` supply UC tags. May be the same file as ``table_schema``. Mutually exclusive with an inline ``tags`` mapping. See :doc:`/reference/config/schema-files`.
   * - ``partition_columns``
     - list
     - —
     - Emitted as ``partition_cols=``.
   * - ``cluster_columns``
     - list
     - —
     - Liquid clustering; emitted as ``cluster_by=``. With ``cluster_by_auto``, these become the initial clustering keys.
   * - ``cluster_by_auto``
     - bool
     - —
     - Auto liquid clustering. Can be combined with ``cluster_columns``, which become the initial clustering keys; Databricks may later change the keys based on the workload.
   * - ``spark_conf``
     - dict
     - —
     - Spark configuration.
   * - ``table_schema``
     - string
     - —
     - Inline DDL, or a ``.ddl``/``.sql``/``.yaml``/``.json`` file path (auto-detected). A ``.yaml``/``.json`` file is the unified schema/tags file; its per-column ``type``/``nullable``/``comment`` are read here. Any UC ``tags`` it carries are ignored unless the same file is also set as ``tags_file`` — otherwise ``lhp generate`` warns ``LHP-CFG-069``.
   * - ``row_filter``
     - string
     - —
     - Row-filter clause, emitted as ``row_filter=``.
   * - ``path``
     - string
     - —
     - Storage location, emitted as ``path=``.
   * - ``database``
     - string
     - —
     - Deprecated (removed at 1.0.0); use ``catalog`` + ``schema``.

.. versionadded:: 0.9.2
   The ``private`` field, for SDP private datasets that persist for the
   pipeline's lifetime without being published to the metastore.
