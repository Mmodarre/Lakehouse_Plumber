Custom expectations (custom_expectations)
=========================================

Attaches arbitrary named expectations to a source table/view.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: orders_custom_checks
     type: test
     test_type: custom_expectations
     source: v_orders_clean
     expectations:
       - name: positive_amount
         expression: "amount > 0"
         on_violation: fail

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
     - Table or view to attach expectations to.
   * - ``expectations``
     - list
     - Yes
     - —
     - List of expectation dicts; each carries its own ``expression`` and ``on_violation``.

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
