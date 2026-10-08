SQL checks (custom_sql)
=======================

Runs a SQL query whose returned rows are treated as violations.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

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

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

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

Expectation entries
-------------------

Each item under ``expectations`` has these fields:

.. list-table::
   :header-rows: 1
   :widths: 24 18 58

   * - Field
     - Default
     - Meaning
   * - ``name``
     - required
     - Name used in the generated expectation dictionary.
   * - ``expression``
     - required
     - SQL boolean expression evaluated for each query row.
   * - ``on_violation``
     - ``fail``
     - ``fail``, ``warn`` or ``drop``. Set a supported value; unrecognised values produce no decorator for the entry.

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
