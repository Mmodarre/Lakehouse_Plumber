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
inline SQL and external files. A plain ``${name}`` that is still in the
``sql_path`` file after substitution fails ``lhp validate`` and ``lhp generate``
with ``LHP-CFG-010``. Dotted or prefixed Spark variables such as
``${mypipeline.start}`` or ``${hiveconf:x}`` are left as written.
To read a pipeline ``configuration:`` parameter at runtime, give it a dotted
key (for example ``mypipeline.start_date``) and reference it as
``${mypipeline.start_date}``; a plain ``${start_date}`` fails even when the
configuration defines it.
Wrap a source in ``stream(view_name)`` inside the query to read it as a stream.

Set the action-level mapping ``sql_parameters`` to render the ``sql_path`` file
as a Jinja template (``{{ name }}``) before substitution; a template with
``render_sql_files: true`` sets it from its parameters. See
:ref:`template-sql-files`.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/sql`
- :doc:`All transform actions </reference/actions/transform>`
