Snapshot CDC streaming table
============================

``mode: snapshot_cdc`` requires a ``snapshot_cdc_config`` block and forces
``create_table: true``. Fields under ``write_target.snapshot_cdc_config``:

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: write_customer_snapshot
     type: write
     write_target:
       type: streaming_table
       mode: snapshot_cdc
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: customer_dim
       snapshot_cdc_config:
         source: "${catalog}.${bronze_schema}.customer_snapshot"
         keys: ["customer_id"]
         stored_as_scd_type: 2

With the direct ``source`` form shown here, the containing flowgroup must
also have a load action under the current relationship validator. A snapshot
write using ``source_function`` can be the only action; the
:doc:`worked guide </guides/write/streaming-table-snapshot-cdc>` shows that form.

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
   * - ``source``
     - string
     - one-of
     - External table/path; exactly one of ``source`` / ``source_function``.
   * - ``source_function``
     - dict
     - one-of
     - ``{file, function, parameters?}``; ``file`` and ``function`` required.
   * - ``keys``
     - list[string]
     - required
     - Non-empty.
   * - ``stored_as_scd_type``
     - int
     - ``1``
     - ``1`` or ``2``.
   * - ``track_history_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``track_history_except_column_list``.
   * - ``track_history_except_column_list``
     - list[string]
     - —
     - Mutually exclusive with ``track_history_column_list``.

With ``source_function``, each ``parameters`` entry is bound as a keyword
argument via ``functools.partial`` (the function must declare them as
keyword-only args after ``*``); ``source_function.file`` is copied alongside
the generated pipeline and resolved relative to project root.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/streaming-table-snapshot-cdc`
- :doc:`All write actions </reference/actions/write>`
