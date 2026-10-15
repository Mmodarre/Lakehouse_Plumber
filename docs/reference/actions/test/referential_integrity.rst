Referential integrity (referential_integrity)
=============================================

Checks that source foreign-key values resolve in a reference table.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_customer_fk
     type: test
     test_type: referential_integrity
     source: v_orders_clean
     reference: main.sales.customers
     source_columns: [customer_id]
     reference_columns: [id]

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

.. include:: /_includes/test-common.rst

.. include:: /_includes/test-output.rst

Related guides
--------------

- :doc:`/guides/test/data-tests`
- :doc:`All test actions </reference/actions/test>`
