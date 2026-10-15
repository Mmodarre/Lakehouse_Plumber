Append to a streaming table
===========================

``write_target.type: streaming_table``. The ``mode`` field selects the flow shape.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: write_customer_silver
     type: write
     source: v_customer_bronze
     write_target:
       type: streaming_table
       mode: standard
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim

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
   * - ``mode``
     - string
     - ``standard``
     - One of ``standard``, ``cdc``, ``snapshot_cdc``, ``replace``.

``source`` may be a single view or a list of views (multi-source append flow
into one table).

Generates ``dp.create_streaming_table(...)`` when ``create_table`` is true,
plus one ``@dp.append_flow(...)`` per source view.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/streaming-table-standard`
- :doc:`All write actions </reference/actions/write>`
