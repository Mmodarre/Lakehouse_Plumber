==================
Transform actions
==================

.. meta::
   :description: Reference for Lakehouse Plumber (LHP) transform actions — the sql, python, data_quality, temp_table, and schema transform types, their YAML fields, defaults, and generated Lakeflow constructs.

A transform action reads one or more upstream views and produces a ``target``
view (or, for ``temp_table``, a temporary table). Every transform sets
``type: transform`` and a ``transform_type`` of ``sql``, ``python``,
``data_quality``, ``temp_table``, or ``schema``. Type-specific fields are flat
on the action.

.. seealso::

   How-to guides: :doc:`/guides/transform/sql`, :doc:`/guides/transform/python`, :doc:`/guides/transform/schema`, :doc:`/guides/transform/temp-table`, :doc:`/guides/transform/data-quality`.

Common fields
=============

Fields accepted by every ``transform_type``. Type-specific fields are listed in
each section below.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Action name, unique within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Always ``transform``.
   * - ``transform_type``
     - string
     - Yes
     - —
     - One of ``sql``, ``python``, ``data_quality``, ``temp_table``, ``schema``.
   * - ``target``
     - string
     - Yes
     - —
     - Name of the produced view (or temporary table). Required for all transforms.
   * - ``source``
     - string / list
     - Yes
     - —
     - Upstream view name(s). Accepted shape and requirement vary per type — see each section.
   * - ``description``
     - string
     - No
     - auto
     - Comment on the generated view/table. Defaults to an auto-generated string.
   * - ``operational_metadata``
     - bool / list
     - No
     - —
     - Add operational metadata columns; a list selects column names. Honored by ``sql``, ``python``, ``data_quality``, ``temp_table``; ignored by ``schema``.
   * - ``depends_on``
     - list
     - No
     - —
     - Extra upstream table references (``catalog.schema.table`` or ``schema.table``) added to the dependency graph. Additive; malformed entries raise ``LHP-VAL-063``.

sql
===

Runs a SQL query over the source view(s) and emits a ``@dp.temporary_view``.
Requires exactly one of ``sql`` or ``sql_path``.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string / list
     - Yes
     - —
     - Input view name(s).
   * - ``sql``
     - string
     - Cond.
     - —
     - Inline SQL query. Exactly one of ``sql`` / ``sql_path``.
   * - ``sql_path``
     - string
     - Cond.
     - —
     - Path to an external ``.sql`` file, relative to project root. Exactly one of ``sql`` / ``sql_path``.

Substitution tokens (``${token}``, ``${secret:scope/key}``) are resolved in both
inline SQL and external files. Wrap a source in ``stream(view_name)`` inside the
query to read it as a stream.

.. code-block:: yaml

   - name: transform_orders
     type: transform
     transform_type: sql
     source: v_orders_raw
     target: v_orders
     sql: "SELECT * FROM v_orders_raw WHERE amount > 0"

python
======

Calls a function in a project Python module and emits a ``@dp.temporary_view``.
The whole module file is copied to ``generated/<pipeline>/custom_python_functions/``
and imported as ``from custom_python_functions.<module> import <function>``.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string / list
     - Yes
     - —
     - Input view name(s); ``None`` is rejected. A list passes a ``List[DataFrame]`` to the function.
   * - ``module_path``
     - string
     - Yes
     - —
     - Path to a ``.py`` file, relative to project root.
   * - ``function_name``
     - string
     - Yes
     - —
     - Name of the function to call.
   * - ``parameters``
     - dict
     - No
     - ``{}``
     - Passed to the function as its ``parameters`` argument.
   * - ``readMode``
     - string
     - No
     - ``batch``
     - ``batch`` or ``stream``; controls ``spark.read`` vs ``spark.readStream`` on each source.

Function signature: single source ``def fn(df, spark, parameters)``; list source
``def fn(dataframes, spark, parameters)``; no source ``def fn(spark, parameters)``.

.. code-block:: yaml

   - name: transform_enrich
     type: transform
     transform_type: python
     source: v_orders
     target: v_orders_enriched
     module_path: "transforms/enrich.py"
     function_name: enrich
     parameters:
       lookup: regions

data_quality
============

Applies expectation rules to the source view. ``mode: dqe`` emits a
``@dp.temporary_view`` decorated with expectation decorators; ``mode: quarantine``
emits a Delta dead-letter (DLQ) subsystem that routes violating rows to a
quarantine table. ``readMode`` must be ``stream``.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Input view name.
   * - ``expectations_file``
     - string
     - Yes
     - —
     - Path to a YAML expectations file, relative to project root.
   * - ``mode``
     - string
     - No
     - ``dqe``
     - ``dqe`` or ``quarantine``.
   * - ``quarantine``
     - object
     - Cond.
     - —
     - Required when ``mode: quarantine``. Block with ``dlq_table`` and ``source_table`` (both required, both fully-qualified ``catalog.schema.table``).
   * - ``readMode``
     - string
     - No
     - ``stream``
     - Must be ``stream``.

The expectations file is a YAML mapping. Each key is a boolean SQL expression
the row must satisfy; each value sets a per-rule ``action`` and optional ``name``.

.. list-table::
   :header-rows: 1
   :widths: 22 16 16 46

   * - Key
     - Type
     - Default
     - Description
   * - ``<expression>``
     - mapping key
     - —
     - A boolean SQL expression, e.g. ``amount >= 0``. Used as the constraint.
   * - ``action``
     - string
     - ``warn``
     - ``warn``, ``drop``, or ``fail``. Selects the emitted decorator.
   * - ``name``
     - string
     - the expression
     - Rule name used as the key in the emitted expectation dict.

Each ``action`` maps to a Lakeflow expectation decorator (``dqe`` mode):

.. list-table::
   :header-rows: 1
   :widths: 16 42 42

   * - ``action``
     - Decorator
     - Behavior
   * - ``warn``
     - ``@dp.expect_all``
     - Logs a warning; keeps violating rows. Default.
   * - ``drop``
     - ``@dp.expect_all_or_drop``
     - Drops violating rows.
   * - ``fail``
     - ``@dp.expect_all_or_fail``
     - Fails the pipeline on any violation.

In ``quarantine`` mode every rule is coerced to ``drop`` regardless of its
configured ``action``; ``warn`` / ``fail`` rules trigger a validation warning. The
DLQ outbox table is derived as ``<dlq_table>_outbox``.

.. code-block:: yaml

   - name: dq_orders
     type: transform
     transform_type: data_quality
     source: v_orders
     target: v_orders_validated
     expectations_file: "expectations/orders.yaml"
     mode: dqe
     readMode: stream

.. code-block:: yaml

   # expectations/orders.yaml
   order_id IS NOT NULL:
     action: drop
     name: valid_order_id
   amount >= 0:
     action: warn
     name: non_negative_amount

Quarantine mode replaces the inline decorators with the DLQ subsystem:

.. code-block:: yaml

   - name: dq_orders
     type: transform
     transform_type: data_quality
     source: v_orders
     target: v_orders_validated
     expectations_file: "expectations/orders.yaml"
     mode: quarantine
     readMode: stream
     quarantine:
       dlq_table: main.ops.orders_dlq
       source_table: main.bronze.orders

temp_table
==========

Materializes an intermediate temporary table via ``@dp.table(temporary=True)``,
cleaned up when the pipeline run completes.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string / dict
     - Yes
     - —
     - Input view name (string), or a dict with a ``view`` / ``source`` key.
   * - ``sql``
     - string
     - No
     - —
     - Optional query. The literal ``{source}`` placeholder is replaced with the source view name. Without ``sql`` the action is a passthrough materialization of ``source``.
   * - ``readMode``
     - string
     - No
     - ``batch``
     - ``batch`` or ``stream``.

.. code-block:: yaml

   - name: stage_orders
     type: transform
     transform_type: temp_table
     source: v_orders
     target: tmp_orders
     sql: "SELECT * FROM {source} WHERE status = 'open'"

schema
======

Renames, casts, and (in ``strict`` mode) filters columns of the source view via a
schema mapping, emitting a ``@dp.temporary_view``. Requires exactly one of
``schema_inline`` or ``schema_file``.

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Input view name. Must be a plain string (the nested ``source.view`` form is not supported).
   * - ``schema_inline``
     - string
     - Cond.
     - —
     - Inline schema definition (arrow or structured YAML). Exactly one of ``schema_inline`` / ``schema_file``.
   * - ``schema_file``
     - string
     - Cond.
     - —
     - Path to an external schema file. Exactly one of ``schema_inline`` / ``schema_file``.
   * - ``enforcement``
     - string
     - No
     - ``permissive``
     - ``strict`` keeps only defined columns; ``permissive`` transforms defined columns and passes the rest through unchanged.
   * - ``readMode``
     - string
     - No
     - ``stream``
     - ``batch`` or ``stream``.

Schema definitions use arrow syntax, one entry per column:

- ``old_col -> new_col: BIGINT`` — rename and cast.
- ``old_col -> new_col`` — rename only.
- ``col: DECIMAL(18,2)`` — cast in place.
- ``col`` — pass through / explicit keep (``strict`` mode).

A ``$`` is allowed in a source-column name (left of ``->``, a cast-only ``col``,
or a pass-through ``col``). A rename target (right of ``->``) must be a clean
identifier of letters, digits, and underscores; anything else raises
``LHP-VAL-011``.

.. code-block:: yaml

   - name: enforce_schema
     type: transform
     transform_type: schema
     source: v_orders
     target: v_orders_typed
     schema_file: "schemas/orders.yaml"
     enforcement: strict
