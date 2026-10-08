SQL query (sql)
===============

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: sql``.

Materialize a SQL query into a temporary view. Fields live under
``source:``; provide ``type: sql`` and exactly one of ``sql`` or ``sql_path``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_summary
     type: load
     source:
       type: sql
       sql: "SELECT * FROM ${catalog}.${bronze_schema}.orders"
     target: v_orders

Source fields
-------------

.. list-table::
   :header-rows: 1
   :widths: 20 18 12 14 36

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``type``
     - string
     - Yes
     - —
     - Must be ``sql``.
   * - ``sql``
     - string
     - One of
     - —
     - Inline SQL query. Provide exactly one of ``sql`` / ``sql_path``.
   * - ``sql_path``
     - string
     - One of
     - —
     - Path to an external ``.sql`` file. Provide exactly one of ``sql`` / ``sql_path``.

``readMode`` is ignored for SQL loads (always a ``spark.sql`` call).
Substitution tokens (``${token}``, ``${secret:scope/key}``) resolve in both
inline SQL and external files.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/sql>` or
:doc:`choose another load source </reference/actions/load>`.
