Test Actions (Data Quality Unit Tests)
=======================================

Test actions let you validate data pipelines using Databricks Lakeflow Declarative Pipelines expectations. They generate lightweight DLT temporary tables that read from existing tables/views and attach expectations that either fail the pipeline or warn on violations.

.. note::
   **CLI Flag Required**: Test actions are skipped by default during code generation for faster builds. Use the ``--include-tests`` flag to generate test code:
   
   .. code-block:: bash
   
      # Skip tests (default) - faster builds
      lhp generate -e dev
      
      # Include tests - for development and testing
      lhp generate -e dev --include-tests

.. contents:: Table of Contents
   :depth: 2
   :local:


Overview
--------

Test actions come in the following types:

+--------------------------+-------------------------------------------------------------+
| Test Type                | Purpose                                                     |
+==========================+=============================================================+
|| row_count               || Compare row counts between two sources with tolerance.     |
+--------------------------+-------------------------------------------------------------+
|| uniqueness              || Validate unique constraints (with optional filter).        |
+--------------------------+-------------------------------------------------------------+
|| referential_integrity   || Check foreign-key relationships across tables.             |
+--------------------------+-------------------------------------------------------------+
|| completeness            || Ensure specific columns are not null.                      |
+--------------------------+-------------------------------------------------------------+
|| range                   || Validate a column is within min/max bounds.                |
+--------------------------+-------------------------------------------------------------+
|| schema_match            || Compare schemas between two tables via information_schema. |
+--------------------------+-------------------------------------------------------------+
|| all_lookups_found       || Validate dimension lookups succeed.                        |
+--------------------------+-------------------------------------------------------------+
|| custom_sql              || Provide your own SQL plus expectations.                    |
+--------------------------+-------------------------------------------------------------+
|| custom_expectations     || Provide expectations only on a source/table.               |
+--------------------------+-------------------------------------------------------------+

.. note::
   - Default target naming: ``tmp_test_<action_name>`` (temporary tables)
   - Default execution: batch (sufficient for aggregate checks); use streaming only when testing streaming sources explicitly
   - Expectation decorators use aggregated style: ``@dlt.expect_all_or_fail``, ``@dlt.expect_all`` (warn), ``@dlt.expect_all_or_drop``
   - ``on_violation`` supports ``fail`` and ``warn``. Using ``drop`` is possible but generally discouraged for tests


Row Count
---------

Compare record counts between two sources, with optional tolerance.

.. code-block:: yaml

  actions:
    - name: test_raw_to_bronze_count
      type: test
      test_type: row_count
      source: [raw.orders, bronze.orders]
      tolerance: 0
      on_violation: fail
      description: "Ensure no data loss from raw to bronze"

**Generated PySpark (excerpt):**

.. code-block:: python
  :linenos:

  @dlt.expect_all_or_fail({"row_count_match": "abs(source_count - target_count) <= 0"})
  @dlt.table(
      name="tmp_test_test_raw_to_bronze_count", 
      comment="Ensure no data loss from raw to bronze",
      temporary=True
  )
  def tmp_test_test_raw_to_bronze_count():
      return spark.sql("""
          SELECT * FROM
            (SELECT COUNT(*) AS source_count FROM raw.orders),
            (SELECT COUNT(*) AS target_count FROM bronze.orders)
      """)


Uniqueness (with optional filter)
---------------------------------

Validate unique constraints on one or more columns. For Type 2 SCD dimensions, use ``filter`` to restrict to active rows.

.. code-block:: yaml

  # Global uniqueness
  - name: test_order_id_unique
    type: test
    test_type: uniqueness
    source: silver.orders
    columns: [order_id]
    on_violation: fail

  # Type 2 SCD: only one active record per natural key
  - name: test_customer_active_unique
    type: test
    test_type: uniqueness
    source: silver.customer_dim
    columns: [customer_id]
    filter: "__END_AT IS NULL"  # Only check active rows
    on_violation: fail

**Generated SQL (with filter):**

.. code-block:: sql

  SELECT customer_id, COUNT(*) as duplicate_count
  FROM silver.customer_dim
  WHERE __END_AT IS NULL
  GROUP BY customer_id
  HAVING COUNT(*) > 1


Referential Integrity
---------------------

Ensure that foreign keys in a fact/reference align.

.. code-block:: yaml

  - name: test_orders_customer_fk
    type: test
    test_type: referential_integrity
    source: silver.fact_orders
    reference: silver.dim_customer
    source_columns: [customer_id]
    reference_columns: [customer_id]
    on_violation: fail

**Generated SQL (excerpt):**

.. code-block:: sql

  SELECT s.*, r.customer_id as ref_customer_id
  FROM silver.fact_orders s
  LEFT JOIN silver.dim_customer r ON s.customer_id = r.customer_id

.. code-block:: python

  @dlt.expect_all_or_fail({"valid_fk": "ref_customer_id IS NOT NULL"})
  @dlt.table(name="tmp_test_orders_customer_fk", comment="Test description", temporary=True)


Completeness
------------

Ensure required columns are populated. The generator selects only required columns for efficiency.

.. code-block:: yaml

  - name: test_customer_required_fields
    type: test
    test_type: completeness
    source: silver.dim_customer
    required_columns: [customer_key, customer_id, name, nation_id]
    on_violation: fail

**Generated SQL (optimized):**

.. code-block:: sql

  SELECT customer_key, customer_id, name, nation_id
  FROM silver.dim_customer

.. code-block:: python

  @dlt.expect_all_or_fail({
      "required_fields_complete": "customer_key IS NOT NULL AND customer_id IS NOT NULL AND name IS NOT NULL AND nation_id IS NOT NULL"
  })
  @dlt.table(name="tmp_test_customer_required_fields", comment="Test description", temporary=True)


Range
-----

Validate that a column falls within bounds. The generator selects only the tested column.

.. code-block:: yaml

  - name: test_order_date_range
    type: test
    test_type: range
    source: silver.orders
    column: order_date
    min_value: '2020-01-01'
    max_value: 'current_date()'
    on_violation: fail

**Generated expectation:** ``order_date >= '2020-01-01' AND order_date <= 'current_date()'``


All Lookups Found
-----------------

Validate that dimension lookups succeed (e.g., surrogate keys are present after joins).

.. code-block:: yaml

  - name: test_order_date_lookup
    type: test
    test_type: all_lookups_found
    source: silver.fact_orders
    lookup_table: silver.dim_date
    lookup_columns: [order_date]
    lookup_result_columns: [date_key]
    on_violation: fail

**Generated (excerpt):**

.. code-block:: sql

  SELECT s.*, l.date_key as lookup_date_key
  FROM silver.fact_orders s
  LEFT JOIN silver.dim_date l ON s.order_date = l.order_date

.. code-block:: python

  @dlt.expect_all_or_fail({"all_lookups_found": "lookup_date_key IS NOT NULL"})
  @dlt.table(name="tmp_test_order_date_lookup", comment="Test description", temporary=True)


Schema Match
------------

Compare schemas between two tables using ``information_schema.columns``.

.. code-block:: yaml

  - name: test_orders_schema_match
    type: test
    test_type: schema_match
    source: silver.fact_orders
    reference: gold.fact_orders_expected
    on_violation: fail

**Generated (excerpt):**

.. code-block:: sql

  WITH source_schema AS (
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns WHERE table_name = 'silver.fact_orders'
  ), reference_schema AS (
    SELECT column_name, data_type, ordinal_position
    FROM information_schema.columns WHERE table_name = 'gold.fact_orders_expected'
  )
  SELECT ... -- schema diff rows

.. code-block:: python

  @dlt.expect_all_or_fail({"schemas_match": "diff_count = 0"})
  @dlt.table(name="tmp_test_orders_schema_match", comment="Test description", temporary=True)


Custom SQL
----------

Provide a custom SQL statement and attach expectations.

.. code-block:: yaml

  - name: test_revenue_reconciliation
    type: test
    test_type: custom_sql
    source: gold.monthly_revenue
    sql: |
      SELECT month, gold_revenue, silver_revenue,
             (ABS(gold_revenue - silver_revenue) / silver_revenue) * 100 as pct_difference
      FROM ...
    expectations:
      - name: revenue_matches
        expression: "pct_difference < 0.5"
        on_violation: fail


Custom Expectations
-------------------

Attach arbitrary expectations to an existing table/view without custom SQL.

.. code-block:: yaml

  - name: test_orders_business_rules
    type: test
    test_type: custom_expectations
    source: silver.fact_orders
    expectations:
      - name: positive_amount
        expression: "total_price > 0"
        on_violation: fail
      - name: reasonable_discount
        expression: "discount_percent <= 50"
        on_violation: warn


Configuration Reference
-----------------------

Common fields across test actions:

- **name**: Unique name of the action within the FlowGroup
- **type**: Must be ``test``
- **test_type**: One of the supported test types listed above
- **source**: Source table/view; for ``row_count`` use a list of two sources
- **target**: Optional table name; defaults to ``tmp_test_<name>``
- **description**: Optional documentation
- **on_violation**: ``fail`` (default) or ``warn``

Type-specific fields:

- **row_count**: ``source`` (list of two), ``tolerance`` (int)
- **uniqueness**: ``columns`` (list), optional ``filter`` (SQL WHERE clause)
- **referential_integrity**: ``reference``, ``source_columns`` (list), ``reference_columns`` (list)
- **completeness**: ``required_columns`` (list)
- **range**: ``column``, ``min_value`` (optional), ``max_value`` (optional)
- **schema_match**: ``reference``
- **all_lookups_found**: ``lookup_table``, ``lookup_columns`` (list), ``lookup_result_columns`` (list)
- **custom_sql**: ``sql`` (string), optional ``expectations`` (list)
- **custom_expectations**: ``expectations`` (list)


Best Practices
--------------

- Prefer ``on_violation: fail`` for hard constraints; use ``warn`` for observability
- Scope uniqueness to active/current records in SCD Type 2 dimensions using ``filter``
- Keep SQL minimal – expectations should express the rule; queries should project only required columns
- Group expectations by severity to get consolidated reporting in DLT UI
- Use reference templates in ``Reference_Templates/`` as starting points


Further Reading
---------------

* Reference Templates: ``Reference_Templates/test_*.yaml``
* Databricks Expectations: `DLT expectations <https://docs.databricks.com/en/delta-live-tables/expectations.html>`_


