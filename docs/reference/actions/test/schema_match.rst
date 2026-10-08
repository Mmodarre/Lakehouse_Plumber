Schema comparison (schema_match)
================================

Diffs two tables' column schemas via ``information_schema.columns``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_schema_match
     type: test
     test_type: schema_match
     source: main.sales.orders
     reference: main.sales.orders_expected

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
