JDBC database (jdbc)
====================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: jdbc``.

Reads from an external database over JDBC. Fields live under ``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_jdbc
     type: load
     source:
       type: jdbc
       url: "jdbc:postgresql://host:5432/db"
       user: "${secret:db/user}"
       password: "${secret:db/password}"
       driver: org.postgresql.Driver
       table: public.orders
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
     - Must be ``jdbc``.
   * - ``url``
     - string
     - Yes
     - —
     - JDBC URL.
   * - ``user``
     - string
     - Yes
     - —
     - Username; supply via ``${secret:scope/key}``.
   * - ``password``
     - string
     - Yes
     - —
     - Password; supply via ``${secret:scope/key}``.
   * - ``driver``
     - string
     - Yes
     - —
     - JDBC driver class.
   * - ``table``
     - string
     - One of
     - —
     - Table name. Supply ``table`` or ``query``; when both are set, ``query`` wins.
   * - ``query``
     - string
     - One of
     - —
     - SQL query. Supply ``query`` or ``table``.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/jdbc>` or
:doc:`choose another load source </reference/actions/load>`.

The JDBC generator uses the fields listed above. It does not forward an
arbitrary ``source.options`` mapping; use a Python loader when the connection
requires reader options beyond this interface.
