Value range (range)
===================

Asserts a column's values fall within min/max bounds.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_amount_range
     type: test
     test_type: range
     source: v_orders_clean
     column: amount
     min_value: 0

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
