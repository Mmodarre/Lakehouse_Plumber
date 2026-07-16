============
Test actions
============

.. meta::
   :description: Field reference for every LHP test action type — row_count, uniqueness, referential_integrity, completeness, range, schema_match, all_lookups_found, custom_sql, custom_expectations.

A test action has ``type: test`` and a ``test_type``; all fields are flat on the
action. Test actions are skipped by default — they are validated and generated
only when ``lhp generate`` / ``lhp validate`` is run with ``--include-tests``.
Each test action emits one temporary Lakeflow table (``@dp.table(..., temporary=True)``)
whose query returns violation rows, with expectation decorators attached.

.. seealso::

   How-to guides: :doc:`/guides/test/data-tests`, :doc:`/guides/test/test-reporting`.

Base fields
-----------

Common to every test action, regardless of ``test_type``.

.. list-table::
   :header-rows: 1
   :widths: 20 12 10 22 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``name``
     - string
     - Yes
     - —
     - Action name; unique within the flowgroup.
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``test``.
   * - ``test_type``
     - string
     - Yes
     - —
     - One of the nine types below. Any other value is rejected at validation.
   * - ``target``
     - string
     - No
     - ``tmp_test_<name>``
     - Name of the generated temporary table.
   * - ``description``
     - string
     - No
     - ``Test: <test_type>``
     - Table comment and function docstring.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``; invalid values coerced to ``fail``. Selects the decorator for the seven built-in types; for ``custom_sql`` / ``custom_expectations`` the decorator comes from each entry in ``expectations``.
   * - ``test_id``
     - string
     - No
     - —
     - Correlates this test with a test-reporting hook.

row_count
---------

Compares record counts between exactly two sources within a tolerance.

.. list-table::
   :header-rows: 1
   :widths: 22 12 12 14 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - list
     - Yes
     - —
     - Exactly two tables/views whose counts are compared.
   * - ``tolerance``
     - integer
     - No
     - ``0``
     - Allowed absolute difference in row counts.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_row_count
     type: test
     test_type: row_count
     source: [v_orders_raw, v_orders_clean]
     tolerance: 0

uniqueness
----------

Asserts a unique constraint over one or more columns.

.. list-table::
   :header-rows: 1
   :widths: 22 12 12 14 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view to check.
   * - ``columns``
     - list
     - Yes
     - —
     - Columns whose combined value must be unique.
   * - ``filter``
     - string
     - No
     - —
     - WHERE-clause predicate restricting the rows checked.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_unique
     type: test
     test_type: uniqueness
     source: v_orders_clean
     columns: [order_id]

referential_integrity
----------------------

Checks that source foreign-key values resolve in a reference table.

.. list-table::
   :header-rows: 1
   :widths: 24 12 12 12 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view holding the foreign-key values.
   * - ``reference``
     - string
     - Yes
     - —
     - Three-part name (``catalog.schema.table``) of the referenced table.
   * - ``source_columns``
     - list
     - Yes
     - —
     - Foreign-key columns in ``source``; equal length to ``reference_columns``.
   * - ``reference_columns``
     - list
     - Yes
     - —
     - Key columns in ``reference``; equal length to ``source_columns``.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_customer_fk
     type: test
     test_type: referential_integrity
     source: v_orders_clean
     reference: main.sales.customers
     source_columns: [customer_id]
     reference_columns: [id]

completeness
------------

Asserts that required columns are non-null.

.. list-table::
   :header-rows: 1
   :widths: 24 12 12 12 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view to check.
   * - ``required_columns``
     - list
     - Yes
     - —
     - Columns that must be non-null.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_complete
     type: test
     test_type: completeness
     source: v_orders_clean
     required_columns: [order_id, customer_id, order_date]

range
-----

Asserts a column's values fall within min/max bounds.

.. list-table::
   :header-rows: 1
   :widths: 20 12 12 14 42

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view to check.
   * - ``column``
     - string
     - Yes
     - —
     - Column whose values are range-checked.
   * - ``min_value``
     - number
     - No
     - —
     - Inclusive lower bound. At least one of ``min_value`` / ``max_value`` is required.
   * - ``max_value``
     - number
     - No
     - —
     - Inclusive upper bound. At least one of ``min_value`` / ``max_value`` is required.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_amount_range
     type: test
     test_type: range
     source: v_orders_clean
     column: amount
     min_value: 0

schema_match
------------

Diffs two tables' column schemas via ``information_schema.columns``.

.. list-table::
   :header-rows: 1
   :widths: 22 12 12 14 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Three-part name (``catalog.schema.table``) of the table to check.
   * - ``reference``
     - string
     - Yes
     - —
     - Three-part name (``catalog.schema.table``) of the expected-schema table.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_schema_match
     type: test
     test_type: schema_match
     source: main.sales.orders
     reference: main.sales.orders_expected

all_lookups_found
-----------------

Asserts that every source row resolves against a lookup table.

.. list-table::
   :header-rows: 1
   :widths: 26 12 10 12 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view holding the lookup keys.
   * - ``lookup_table``
     - string
     - Yes
     - —
     - Three-part name (``catalog.schema.table``) of the lookup table.
   * - ``lookup_columns``
     - list
     - Yes
     - —
     - Key columns in ``source``; equal length to ``lookup_result_columns``.
   * - ``lookup_result_columns``
     - list
     - Yes
     - —
     - Matching columns in ``lookup_table``; equal length to ``lookup_columns``.
   * - ``on_violation``
     - string
     - No
     - ``fail``
     - ``fail``, ``warn``, or ``drop``.

.. code-block:: yaml

   - name: orders_product_lookup
     type: test
     test_type: all_lookups_found
     source: v_orders_clean
     lookup_table: main.sales.products
     lookup_columns: [product_id]
     lookup_result_columns: [id]

custom_sql
----------

Runs a SQL query whose returned rows are treated as violations.

.. list-table::
   :header-rows: 1
   :widths: 20 12 12 14 42

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``sql``
     - string
     - Yes
     - —
     - Query whose returned rows represent violations.
   * - ``source``
     - string
     - No
     - —
     - Table or view the query reads from.
   * - ``expectations``
     - list
     - No
     - —
     - Named expectations attached to the query; each carries its own ``expression`` and ``on_violation``.

.. code-block:: yaml

   - name: orders_negative_amount
     type: test
     test_type: custom_sql
     source: v_orders_clean
     sql: "SELECT * FROM v_orders_clean WHERE amount < 0"
     expectations:
       - name: no_negative_rows
         expression: "amount >= 0"
         on_violation: fail

custom_expectations
-------------------

Attaches arbitrary named expectations to a source table/view.

.. list-table::
   :header-rows: 1
   :widths: 22 12 12 14 40

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Table or view to attach expectations to.
   * - ``expectations``
     - list
     - Yes
     - —
     - List of expectation dicts; each carries its own ``expression`` and ``on_violation``.

.. code-block:: yaml

   - name: orders_custom_checks
     type: test
     test_type: custom_expectations
     source: v_orders_clean
     expectations:
       - name: positive_amount
         expression: "amount > 0"
         on_violation: fail

Generated output
----------------

Every test action generates a function decorated with
``@dp.table(name="<target>", comment="<description>", temporary=True)`` that
returns the violation query, plus one expectation decorator per violation
bucket. The ``on_violation`` value maps to the decorator as follows:

.. list-table::
   :header-rows: 1
   :widths: 18 42 40

   * - on_violation
     - Decorator
     - Behavior
   * - ``fail``
     - ``@dp.expect_all_or_fail``
     - Pipeline update fails when any row violates.
   * - ``warn``
     - ``@dp.expect_all``
     - Violations recorded as metrics; rows retained.
   * - ``drop``
     - ``@dp.expect_all_or_drop``
     - Violating rows dropped from the output.

For the seven built-in types the action-level ``on_violation`` drives the
decorator. For ``custom_sql`` and ``custom_expectations`` the decorators are
built from each entry in ``expectations`` using that entry's own
``on_violation``.
