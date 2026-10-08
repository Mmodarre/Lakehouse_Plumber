Write actions
=============

Every write sets ``type: write`` and a ``write_target`` mapping. Choose a
target and mode for a complete example, target settings and action fields.
For worked examples and help choosing a target, see :doc:`/guides/write/index`.

.. toctree::
   :maxdepth: 1

   Append to a streaming table <write/streaming-table>
   CDC streaming table <write/streaming-table-cdc>
   Snapshot CDC streaming table <write/streaming-table-snapshot-cdc>
   Replace rows in a streaming table <write/streaming-table-replace>
   Materialized view <write/materialized-view>
   Sink <write/sink>

.. include:: /_includes/write-common.rst

Shared fields
-------------

Table and materialized-view pages include their shared target options alongside their mode-specific settings.

Streaming table
---------------

:doc:`Append to a streaming table: syntax and options <write/streaming-table>`.

mode: cdc
---------

:doc:`CDC streaming table: syntax and options <write/streaming-table-cdc>`.

mode: snapshot_cdc
------------------

:doc:`Snapshot CDC streaming table: syntax and options <write/streaming-table-snapshot-cdc>`.

mode: replace
-------------

:doc:`Replace rows in a streaming table: syntax and options <write/streaming-table-replace>`.

Materialized view
-----------------

:doc:`Materialized view: syntax and options <write/materialized-view>`.

Sink
----

:doc:`Sink: syntax and options <write/sink>`.

Emitted constructs
------------------


- ``standard``: ``dp.create_streaming_table(...)`` (when ``create_table`` is
  true) plus one ``@dp.append_flow(target=, name=, comment=)`` decorator per
  source view.
- ``cdc``: ``dp.create_streaming_table(...)`` (when ``create_table`` is true)
  plus ``dp.create_auto_cdc_flow(...)``.
- ``snapshot_cdc``: ``dp.create_streaming_table(...)`` (always) plus
  ``dp.create_auto_cdc_from_snapshot_flow(...)``.
- ``replace``: ``dp.create_streaming_table(...)`` (always) plus a single
  ``@dp.replace_flow(target=, name=, replace_using=, sequence_by=, comment=)``
  decorator.

sink_type-specific fields
-------------------------

See :doc:`write/sink` for Delta, Kafka, Event Hubs, foreachbatch and custom sinks.

Unity Catalog tags
------------------


Table-level ``tags`` and ``tags_file`` belong on ``write_target``. See
:doc:`/reference/config/uc-tagging` for project settings and runtime behaviour.

uc_tagging config block
-----------------------

The project block is documented in :doc:`/reference/config/uc-tagging`.

How the hook applies tags
-------------------------

See :doc:`/reference/config/uc-tagging` for hook behaviour and permissions.

Schema & tags file
------------------

.. raw:: html

   <span id="uc-tags-file"></span>

See :doc:`/reference/config/schema-files` for the shared file format.

Tagging error handling
----------------------

See :doc:`/reference/config/uc-tagging` for warnings and event-log diagnosis.
