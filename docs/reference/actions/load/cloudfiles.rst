Auto Loader (cloudfiles)
========================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: cloudfiles``.

Streams files into a temporary view via Databricks Auto Loader. Fields live
under ``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_orders
     type: load
     source:
       type: cloudfiles
       path: "${landing_volume}/orders/*.json"
       format: json
     target: v_orders_raw

Source fields
-------------

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

Read mode placement
-------------------

Prefer action-level ``readMode`` alongside ``source`` and ``target``.
The generator also accepts ``source.readMode`` as a fallback. An action-level
value takes precedence when both are supplied.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/auto-loader>` or
:doc:`choose another load source </reference/actions/load>`.

Reader options are open-ended mappings. For the full Databricks option set,
see `Spark API options <https://docs.databricks.com/aws/en/spark/api-options>`_.

Option precedence
-----------------

LHP processes ``options`` first, then merges ``reader_options``, then
``format_options`` (prefixing keys with the file format when needed). Later
mappings overwrite the same emitted option. Legacy scalar option fields fill
only options that have not already been supplied. ``cloudFiles.format`` defaults
to ``source.format`` when it is absent from the options.
