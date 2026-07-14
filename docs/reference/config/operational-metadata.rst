Operational metadata
=====================

.. meta::
   :description: Reference for the operational_metadata config block in lhp.yaml — column definitions (expression, applies_to, imports), column-group presets, the built-in columns, and the operational_metadata selection field on flowgroups and actions.

Operational metadata columns are Spark expressions appended to generated
tables as extra columns. Define them under the optional ``operational_metadata``
key in ``lhp.yaml``, then add them to a flowgroup or action with the
``operational_metadata`` selection field::

   operational_metadata:
     columns:
       <name>:
         expression: <spark_expression>
     presets:
       <group_name>:
         columns: [<name>, ...]

Project block
-------------

Top-level keys under ``operational_metadata`` in ``lhp.yaml``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``columns``
     - mapping
     - Yes
     - —
     - Named metadata columns (``<name>`` → column definition).
   * - ``presets``
     - mapping
     - No
     - —
     - Named column groups (``<name>`` → preset definition).

Column definition
-----------------

Each entry under ``columns.<name>``. A bare string value is shorthand for
``expression``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``expression``
     - string
     - Yes
     - —
     - Spark expression evaluated per row (e.g. ``F.current_timestamp()``). ``${pipeline_name}`` and ``${flowgroup_name}`` are substituted.
   * - ``description``
     - string
     - No
     - —
     - Free-text description of the column.
   * - ``applies_to``
     - list[string]
     - No
     - ``["streaming_table", "materialized_view"]``
     - Target types the column is emitted for. Values: ``view``, ``streaming_table``, ``materialized_view``.
   * - ``additional_imports``
     - list[string]
     - No
     - —
     - Extra import statements the expression needs (e.g. ``from pyspark.sql.functions import xxhash64``).
   * - ``enabled``
     - bool
     - No
     - ``true``
     - Whether the column is active.

Preset definition
-----------------

Each entry under ``presets.<name>`` names a group of columns. A bare list
value is shorthand for ``columns``.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``columns``
     - list[string]
     - Yes
     - —
     - Column names in the group. Each must be defined under ``columns``.
   * - ``description``
     - string
     - No
     - —
     - Free-text description of the group.

Built-in columns
----------------

Available only when the project defines no ``operational_metadata.columns``.
Defining any project column replaces this set.

.. list-table::
   :header-rows: 1
   :widths: 22 40 24 24

   * - Name
     - Expression
     - Applies to
     - Description
   * - ``_ingestion_timestamp``
     - ``F.current_timestamp()``
     - view, streaming_table, materialized_view
     - When the record was ingested.
   * - ``_source_file``
     - ``F.input_file_name()``
     - view
     - Source file path.
   * - ``_pipeline_run_id``
     - ``F.lit(spark.conf.get("pipelines.id", "unknown"))``
     - view, streaming_table, materialized_view
     - Pipeline run identifier.
   * - ``_pipeline_name``
     - ``F.lit("${pipeline_name}")``
     - view, streaming_table, materialized_view
     - Pipeline name.
   * - ``_flowgroup_name``
     - ``F.lit("${flowgroup_name}")``
     - view, streaming_table, materialized_view
     - FlowGroup name.

Selection field
---------------

The ``operational_metadata`` field on a flowgroup or action selects which
columns to add. Preset, flowgroup, and action selections are additive.

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``operational_metadata``
     - bool or list[string]
     - No
     - —
     - Column names to add, or ``false`` to disable.

Value semantics:

- ``list[string]`` — the metadata column names to add. Combined with the names selected at preset and flowgroup levels.
- ``false`` — at action level, disables all operational metadata for the action; overrides preset and flowgroup selections.
- ``true`` — accepted, but adds no columns. List the column names explicitly to add them.

Example
-------

Define two columns in ``lhp.yaml`` and enable them on a load action.

.. code-block:: yaml

   # lhp.yaml
   operational_metadata:
     columns:
       _ingested_at:
         expression: "F.current_timestamp()"
         applies_to: ["view"]
       _source_file_path:
         expression: "F.col('_metadata.file_path')"
         applies_to: ["view"]

.. code-block:: yaml

   # pipelines/orders/load_orders.yaml
   pipeline: orders
   flowgroup: load_orders

   actions:
     - name: load_orders_raw
       type: load
       source:
         type: cloudfiles
         path: "${landing_volume}/orders/*.json"
         format: json
       operational_metadata:
         - _ingested_at
         - _source_file_path
       target: v_orders_raw
