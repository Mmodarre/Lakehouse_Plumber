Replace rows in a streaming table
=================================

``mode: replace`` generates a Databricks REPLACE USING flow
(``@dp.replace_flow``). For each ``replace_using`` key value present in a batch
it *replaces* that key's target rows with the batch's rows for that key —
a replace, **not** a merge. ``replace_using`` is a **grouping key, not a unique
primary key**: one key may own many rows, and every row the source sends for it
is kept (appended, never collapsed into one). This is the opposite of
``mode: cdc``, which merges change events onto a unique key into a single row.
``sequence_by`` orders competing batches for a key (highest wins). It requires a
``replace_config`` block, forces ``create_table: true``, and reads its ``source``
as a **streaming** query (``readMode: batch`` is rejected). A REPLACE USING
target must be served by this single flow — it cannot share its table with any
other write action. Fields under ``write_target.replace_config``:

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: write_orders_current
     type: write
     source: v_order_updates
     write_target:
       type: streaming_table
       mode: replace
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: orders_current
       replace_config:
         replace_using: ["order_id"]
         sequence_by: "updated_at"

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
   * - ``replace_using``
     - list[string]
     - required
     - Non-empty grouping-key columns whose rows a batch replaces; need not be
       unique (many rows per key are kept). Emitted as ``replace_using=``.
   * - ``sequence_by``
     - string
     - required
     - Exactly one ordering column (highest value wins); emitted as
       ``sequence_by=``. Unlike ``cdc_config``, a list is **not** allowed.

.. note::

   A batch-read sibling ``mode: replace_where`` (a ``replace_where_config`` block
   for Databricks' FLOW REPLACE WHERE) is a planned future extension — mirroring
   ``cdc`` → ``snapshot_cdc``, split by streaming vs batch source read. It is not
   yet implemented.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/streaming-table-replace`
- :doc:`All write actions </reference/actions/write>`
