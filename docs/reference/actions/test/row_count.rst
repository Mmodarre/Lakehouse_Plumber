Row counts (row_count)
======================

Compares record counts between exactly two sources within a tolerance.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_row_count
     type: test
     test_type: row_count
     source: [v_orders_raw, v_orders_clean]
     tolerance: 0

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
