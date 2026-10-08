Required values (completeness)
==============================

Asserts that required columns are non-null.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_complete
     type: test
     test_type: completeness
     source: v_orders_clean
     required_columns: [order_id, customer_id, order_date]

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
