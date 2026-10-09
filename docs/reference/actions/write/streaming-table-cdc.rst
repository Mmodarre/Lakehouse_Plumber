CDC streaming table
===================

``mode: cdc`` requires a ``cdc_config`` block. Fields under
``write_target.cdc_config``:

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: write_customer_silver
     type: write
     source: v_customer_bronze
     write_target:
       type: streaming_table
       mode: cdc
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim
       cdc_config:
         keys: ["customer_id"]
         sequence_by: "last_modified_dt"
         scd_type: 2

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

.. list-table::
   :header-rows: 1
   :widths: 28 18 10 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``keys``
     - list[string]
     - required
     - Non-empty business keys.
   * - ``sequence_by``
     - string / list[string]
     - —
     - Ordering column(s); a list emits ``sequence_by=struct(...)``.
   * - ``scd_type``
     - int
     - ``1``
     - ``1`` or ``2``; emitted as ``stored_as_scd_type=``.
   * - ``ignore_null_updates``
     - bool
     - —
     - —
   * - ``apply_as_deletes``
     - string
     - —
     - SQL expression.
   * - ``apply_as_truncates``
     - string
     - —
     - SQL expression; not allowed with ``scd_type: 2``.
   * - ``track_history_column_list``
     - list[string]
     - —
     - ``scd_type: 2``; mutually exclusive with ``track_history_except_column_list``.
   * - ``track_history_except_column_list``
     - list[string]
     - —
     - ``scd_type: 2``; mutually exclusive with ``track_history_column_list``.
   * - ``column_list``
     - list[string]
     - —
     - Mutually exclusive with ``except_column_list``.
   * - ``except_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``column_list``.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/streaming-table-cdc`
- :doc:`All write actions </reference/actions/write>`
