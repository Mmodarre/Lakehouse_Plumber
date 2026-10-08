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

.. seealso::

   Concept: :doc:`/concepts/how-lhp-works`.

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

The project block also accepts a ``defaults`` mapping. The current selector
uses explicit lists from the action, flowgroup and action preset; it does not
activate columns from this project ``defaults`` mapping or expand the named
metadata ``presets``. Select column names explicitly. These parsed fields must
not be treated as automatic activation switches.

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
     - Spark expression evaluated per row (e.g. ``F.current_timestamp()``). ``${pipeline_name}``, ``${flowgroup_name}`` and environment ``${token}`` values are substituted; ``${secret:...}`` is rejected. See `Substitution in expressions`_.
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
     - Parsed, but not consulted by the current column selector. Control emission through explicit selection and ``applies_to``.

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

Substitution in expressions
---------------------------

Each selected column's ``expression`` is resolved per flowgroup for the
``--env`` environment, in this order:

1. Context tokens are replaced first and win over a substitutions key of the
   same name: ``${pipeline_name}``, ``${flowgroup_name}``, and, on delta and
   JDBC loads, ``${source_table}`` (the table the load reads).
2. A ``${secret:...}`` reference is rejected with ``LHP-CFG-070``, including
   one reached through a token value. The expression's result is written into
   every row of the table, so a secret would be stored as table data. Put
   non-secret per-environment values in ``substitutions/<env>.yaml``, and
   per-table logic in a transform action.
3. Environment ``${token}`` values are substituted from the ``global`` and
   ``<env>`` blocks of ``substitutions/<env>.yaml``.
4. Any ``${...}``, ``%{...}`` or ``{{ ... }}`` still present fails with
   ``LHP-CFG-010``, naming the column, the token and the environment. Local
   variables and template parameters are not resolved in ``lhp.yaml``.

The bare ``{token}`` form is not substituted in expressions, so a regex
quantifier such as ``F.col("id").rlike("\\d{8}")`` is emitted unchanged.
Only expressions that are rendered into generated code are resolved: a column
selected on an action that does not emit operational metadata, such as a
streaming table or materialized view write, a schema transform or a test
action, is not checked.
``lhp validate --env <env>`` reports the same errors as
``lhp generate --env <env>``.

.. code-block:: yaml

   # lhp.yaml
   operational_metadata:
     columns:
       _source_system:
         expression: "F.lit('${source_system}')"
         applies_to: ["view"]

.. code-block:: yaml

   # substitutions/dev.yaml
   dev:
     source_system: crm_dev

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
