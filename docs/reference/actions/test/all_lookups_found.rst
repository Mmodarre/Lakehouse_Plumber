Lookup matches (all_lookups_found)
==================================

Asserts that every source row resolves against a lookup table.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_product_lookup
     type: test
     test_type: all_lookups_found
     source: v_orders_clean
     lookup_table: main.sales.products
     lookup_columns: [product_id]
     lookup_result_columns: [id]

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
