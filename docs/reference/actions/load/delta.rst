Delta table (delta)
===================

**YAML location:** an entry in a flowgroup's ``actions`` list.
**Source selector:** ``source.type: delta``.

Reads a Delta table into a temporary view. Fields live under ``source:``.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: load_customers
     type: load
     source:
       type: delta
       catalog: "${catalog}"
       schema: "${bronze_schema}"
       table: customers
     readMode: batch
     target: v_customers

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
     - Must be ``delta``.
   * - ``catalog``
     - string
     - Yes
     - —
     - Catalog of the source table.
   * - ``schema``
     - string
     - Yes
     - —
     - Schema of the source table.
   * - ``table``
     - string
     - Yes
     - —
     - Source table name.
   * - ``options``
     - mapping
     - No
     - —
     - Delta reader options (values must be non-empty). CDC and time-travel keys go here.
   * - ``where_clause``
     - list[string]
     - No
     - ``[]``
     - Filter expressions applied with ``.where(...)``.
   * - ``select_columns``
     - list[string]
     - No
     - —
     - Column projection applied with ``.select(...)``.

``readMode`` defaults to ``batch`` and accepts ``batch`` or ``stream``.
``options.readChangeFeed: "true"`` requires ``readMode: stream``, or a
``startingVersion``/``startingTimestamp`` bound in batch mode.

Read mode placement
-------------------

Prefer action-level ``readMode`` alongside ``source`` and ``target``.
The generator also accepts ``source.readMode`` as a fallback. An action-level
value takes precedence when both are supplied.

.. include:: /_includes/load-common.rst

See :doc:`the worked guide </guides/ingest/delta>` or
:doc:`choose another load source </reference/actions/load>`.
