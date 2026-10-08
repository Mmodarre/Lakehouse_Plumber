SQL transform (sql)
===================

Runs a SQL query over the source view(s) and emits a ``@dp.temporary_view``.
Requires exactly one of ``sql`` or ``sql_path``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: transform_orders
     type: transform
     transform_type: sql
     source: v_orders_raw
     target: v_orders
     sql: "SELECT * FROM v_orders_raw WHERE amount > 0"

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
     - string / list
     - Yes
     - —
     - Input view name(s).
   * - ``sql``
     - string
     - Cond.
     - —
     - Inline SQL query. Exactly one of ``sql`` / ``sql_path``.
   * - ``sql_path``
     - string
     - Cond.
     - —
     - Path to an external ``.sql`` file, relative to project root. Exactly one of ``sql`` / ``sql_path``.

Substitution tokens (``${token}``, ``${secret:scope/key}``) are resolved in both
inline SQL and external files. Wrap a source in ``stream(view_name)`` inside the
query to read it as a stream.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/sql`
- :doc:`All transform actions </reference/actions/transform>`
