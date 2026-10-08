Temporary table (temp_table)
============================

Materializes an intermediate temporary table via ``@dp.table(temporary=True)``,
cleaned up when the pipeline run completes.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: stage_orders
     type: transform
     transform_type: temp_table
     source: v_orders
     target: tmp_orders
     sql: "SELECT * FROM v_orders WHERE status = 'open'"

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string / dict
     - Yes
     - —
     - Input view name (string), or a dict with a ``view`` / ``source`` key.
   * - ``sql``
     - string
     - No
     - —
     - Optional query. Use the input view name in the query. Without ``sql`` the action is a passthrough materialization of ``source``.
   * - ``readMode``
     - string
     - No
     - ``batch``
     - ``batch`` or ``stream``.

The normal CLI resolves substitution tokens before this generator runs. A bare
``{source}`` in SQL is treated as a substitution token and fails validation if
no such token is defined; use the view name or a defined ``${token}`` instead.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/temp-table`
- :doc:`All transform actions </reference/actions/transform>`
