Materialized view
=================

``write_target.type: materialized_view``. Emits a ``@dp.materialized_view(...)``
decorated function. Target-specific fields are listed below.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: write_customer_summary
     type: write
     write_target:
       type: materialized_view
       catalog: "${catalog}"
       schema: "${gold_schema}"
       table: customer_summary
       sql: "SELECT customer_id, COUNT(*) AS orders FROM v_orders GROUP BY customer_id"

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

.. list-table::
   :header-rows: 1
   :widths: 22 14 12 52

   * - Key
     - Type
     - Default
     - Notes
   * - ``sql``
     - string
     - —
     - Inline query; one of ``sql`` / ``sql_path`` / action ``source``.
   * - ``sql_path``
     - string
     - —
     - External ``.sql`` query file.
   * - ``refresh_schedule``
     - string
     - —
     - Cron/schedule; emitted as ``refresh_schedule=``.
   * - ``refresh_policy``
     - string
     - —
     - One of ``auto``, ``incremental``, ``incremental_strict``, ``full``.

Define the view by exactly one of: an action-level ``source`` view, inline
``sql``, or ``sql_path``. When ``sql``/``sql_path`` is provided, no
action-level ``source`` is needed.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/materialized-view`
- :doc:`All write actions </reference/actions/write>`
