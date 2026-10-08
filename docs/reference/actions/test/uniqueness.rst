Unique keys (uniqueness)
========================

Asserts a unique constraint over one or more columns.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_unique
     type: test
     test_type: uniqueness
     source: v_orders_clean
     columns: [order_id]

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
