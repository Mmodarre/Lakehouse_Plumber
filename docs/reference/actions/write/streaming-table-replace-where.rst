Replace rows matching a predicate
=================================

``mode: replace_where`` generates a Databricks REPLACE WHERE flow: a single
``@dp.table(..., replace_where=...)`` statement that defines the streaming
table and its flow together, over a **batch** read of the action's ``source``
view. On every refresh the flow deletes the target rows that match the
predicate and recomputes them by re-running the source query for that range.
Rows that don't match are left untouched. The pipeline applies the predicate
when reading the source, so it need not be repeated there. It requires a
``replace_where_config`` block and always creates its table, and the target
must be served by this single flow. It is the batch-read sibling of
``mode: replace`` (REPLACE USING), which reads a stream.

.. versionadded:: 0.9.4

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.
The flowgroup also needs a load that produces ``v_orders`` as a batch view, such
as a ``delta`` load (batch is its default read).

.. code-block:: yaml

   - name: write_orders_recent
     type: write
     source: v_orders
     write_target:
       type: streaming_table
       mode: replace_where
       catalog: "${catalog}"
       schema: "${silver_schema}"
       table: orders_recent
       replace_where_config:
         replace_where: "order_date >= date_add(current_date(), -7)"

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

Fields under ``write_target.replace_where_config``:

.. list-table::
   :header-rows: 1
   :widths: 28 18 10 44

   * - Key
     - Type
     - Default
     - Notes
   * - ``replace_where``
     - string
     - required
     - A non-blank SQL predicate, emitted verbatim as ``replace_where="..."``.
       ``${token}`` substitutions are resolved first; a ``${secret:...}``
       reference is rejected, because the predicate is written into the
       generated code. No other key is allowed in the block.

Generated code
--------------

The minimal example generates:

.. code-block:: python

   @dp.table(
       name="dev_catalog.silver.orders_recent",
       comment="Streaming table: orders_recent",
       table_properties={"pipelines.reset.allowed": "false"},
       replace_where="order_date >= date_add(current_date(), -7)",
   )
   def orders_recent():
       """Replace-where flow to dev_catalog.silver.orders_recent"""
       df = spark.read.table("v_orders")

       return df

There is no ``dp.create_streaming_table`` and no flow decorator. Databricks
defines a REPLACE WHERE table and its flow in one statement, and the flow takes
the table's name: in the event log the flow is named after the table's full
``catalog.schema.table`` name.

- ``table_properties`` is always emitted, because LHP adds the reset guard (see
  `Full refresh and the reset guard`_).
- ``spark_conf``, ``partition_columns`` (as ``partition_cols=``),
  ``cluster_columns`` (as ``cluster_by=``), ``cluster_by_auto``, ``path``,
  ``table_schema`` (as ``schema=``), ``row_filter`` and ``private`` are emitted
  only when set. ``replace_where=`` is always the last argument.
- No ``temporary=``, no ``@dp.expect*`` decorator and no operational-metadata
  column is emitted on the write.

Rules
-----

``lhp validate`` and ``lhp generate`` enforce these rules. Errors in the
``replace_where_config`` block and the write's own settings are listed inside
``LHP-VAL-007``, and so are the ``LHP-VAL-006`` and ``LHP-VAL-070`` errors
below. Only ``LHP-CFG-004`` and ``LHP-VAL-010`` are reported on their own.

- **One batch source.** ``source`` is a single view: a string, or a list with
  one view. A list of two or more views is rejected.
- **Batch read.** The flow reads its source with ``spark.read``. ``readMode:
  stream`` on the write is rejected; Databricks would fail the flow with
  ``REPLACE_WHERE_WITH_STREAMING_SOURCE``. The view it reads must be a batch
  view: a streaming view, such as a ``cloudfiles`` or ``kafka`` load or a
  ``delta`` load with ``readMode: stream``, fails ``LHP-VAL-070``.
- **The action creates its table.** ``create_table: false`` is rejected.
- **No other flow on the table.** A second write action in the same pipeline
  that targets the table fails: ``LHP-CFG-004`` when it also creates the table,
  and otherwise ``LHP-VAL-010``, with a message that names the REPLACE WHERE
  flow. Databricks allows a single flow on a REPLACE WHERE target
  (``UNSUPPORTED_FEATURE.MULTI_FLOW_REPLACE_WHERE_TARGET``).
- **No temporary table.** ``temporary: true`` is rejected; ``temporary: false``
  passes. Use ``private: true`` for a table that only the pipeline can see.
- **No expectations.** Action-level ``expectations`` are rejected. Databricks
  does not support expectations on a REPLACE WHERE target
  (``UNSUPPORTED_FEATURE.REPLACE_WHERE_FLOW_WITH_EXPECTATIONS``).
- An unknown mode such as ``replace-where`` fails ``LHP-VAL-006`` with the
  suggestion ``Did you mean 'replace_where'?``.

.. _replace-where-reset:

Full refresh and the reset guard
--------------------------------

A full refresh of a REPLACE WHERE flow re-runs the source query for the current
predicate only. Without protection, every row outside the predicate is deleted:
a table that has kept a year of history behind a 7-day predicate is left with 7
days.

LHP therefore sets ``table_properties["pipelines.reset.allowed"]`` to
``"false"`` unless the YAML or a preset already sets it. An explicit value
always wins. The key is matched without regard to case, so a value under a
differently cased key is kept and no second key is added.

With the guard in place, Databricks behaves as follows:

- **A full refresh of the whole pipeline** succeeds. The guarded table is not
  reset: it keeps its rows outside the predicate, and only the rows inside the
  predicate are recomputed. The event log has no RESET ``dataset_life_cycle``
  event for the table. That missing event is the only sign that the table was
  protected.
- **A full refresh that selects the table** fails the whole update while the
  pipeline is analysed, with:

  .. code-block:: text

     Table <catalog.schema.table> is not resettable, so it cannot be contained in fullRefreshTables in the refresh selection.

To reset a guarded table on purpose, do one of these:

- Set ``pipelines.reset.allowed: "true"`` in the write's ``table_properties``,
  then generate, deploy and run the full refresh. Remove the setting afterwards,
  so that LHP adds the ``"false"`` default again, and deploy once more.
- Drop the table.

.. important::

   After a reset, the table holds only the rows that match the current
   predicate. Every older row is deleted.

.. _replace-where-incremental:

Incremental refresh
-------------------

Databricks plans every refresh of a REPLACE WHERE flow as one technique. These
are the techniques observed; the list is not exhaustive, and Databricks has
others, such as ``PARTITION_OVERWRITE``:

- ``NO_OP``, ``APPEND_ONLY`` or ``ROW_BASED`` are incremental. Which one is
  chosen depends on what changed in the source: nothing, only inserts, or
  updates and deletes.
- ``COMPLETE_REPLACE_WHERE`` deletes and re-inserts every row that matches the
  predicate. The first refresh of a flow always works this way.

Databricks lists the conditions for an incremental refresh. When one of them
does not hold, that refresh recomputes every row inside the predicate:

- The pipeline runs on **serverless** compute. Incremental refresh is only
  supported there.
- The query has a **supported shape**: a deterministic query whose operators
  Databricks can refresh incrementally.
- The predicate is **deterministic**. Temporal functions such as
  ``current_date()`` are allowed; ``rand()`` is not.
- The predicate references **base columns of a source table**. A predicate on a
  derived value, such as an aggregate or a window-function output, cannot be
  applied to the source.
- No DML run outside the pipeline has changed rows inside the predicate.
- The predicate does not cover rows that the previous predicate excluded.

Writing the predicate:

- **Use a moving lower bound**, such as ``order_date >=
  date_add(current_date(), -7)``. It stays eligible for incremental refresh.
- **A moving upper bound**, such as ``order_date BETWEEN
  date_add(current_date(), -7) AND current_date()``, can move the predicate over
  rows it excluded before. That refresh falls back to a full recomputation once.
- **Widening the predicate** has the same one-time cost: the first refresh after
  the change recomputes in full, and later refreshes can be incremental again.
- **Narrowing the predicate** keeps the rows that no longer match. They stay in
  the table as they were.

Operational metadata
~~~~~~~~~~~~~~~~~~~~

LHP adds :doc:`operational-metadata </reference/config/operational-metadata>`
columns on the load, or on a transform that supports them, never on the write.
They are therefore part of the query the REPLACE WHERE flow runs. A
non-deterministic column, such as one computed with ``F.current_timestamp()``,
makes **every** refresh a ``COMPLETE_REPLACE_WHERE``: the data stays correct,
but each refresh deletes and re-inserts every row inside the predicate.
Deterministic columns, such as a record hash computed with ``xxhash64``, do not
affect incremental refresh. LHP warns about this case with ``LHP-VAL-073``
(below).

Expectations on a view upstream of the write, for example a batch
``data_quality`` transform in ``dqe`` mode, are accepted and their ``drop``
rules work. They stop incremental refresh, though: every refresh recomputes in
full, and the flow's event-log metrics do not count the dropped rows.

Reading the event log
~~~~~~~~~~~~~~~~~~~~~

Each refresh records a ``planning_information`` event for the flow, whose
``origin.flow_name`` is the table's full name. Its message names the chosen
technique:

.. code-block:: text

   Flow 'dev_catalog.silver.orders_recent' has been planned to be executed as COMPLETE_REPLACE_WHERE.

``details.planning_information.technique_information`` lists the candidate
techniques; the one with ``"is_chosen": true`` ran. When something blocks
incremental refresh, an entry lists ``incrementalization_issues``. A
non-deterministic metadata column shows up as:

.. code-block:: json

   {
     "issue_type": "PLAN_NOT_DETERMINISTIC",
     "prevent_incrementalization": true,
     "operator_name": "Project",
     "expression_name": "current_timestamp",
     "plan_not_deterministic_sub_type": "TIME_FUNCTION"
   }

Don't use the row counts in ``flow_progress`` (``num_output_rows``,
``num_deleted_rows``) to check this. They report the rows inside the predicate
whether the refresh was incremental or not.

LHP-VAL-073: non-deterministic metadata
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For every ``mode: replace_where`` write, ``lhp validate`` and ``lhp generate``
walk upstream through the ``source`` of the actions in the same flowgroup. They
resolve the operational-metadata columns of each action that adds them. A
column is reported when its expression calls ``current_timestamp``,
``current_date``, ``localtimestamp``, ``now``, ``curdate``, ``rand``, ``randn``,
``random``, ``uuid``, ``shuffle``, ``monotonically_increasing_id``,
``input_file_name``, ``input_file_block_start``, ``input_file_block_length`` or
``spark_partition_id``; uses the keyword ``current_timestamp``,
``current_date`` or ``localtimestamp``; or calls ``unix_timestamp()`` with no
argument. Case is ignored.

``LHP-VAL-073`` is a **warning**: the run succeeds unless you pass
``--strict``. Each flowgroup gets one warning that lists every write, upstream
action, column and expression found. ``--show-details`` prints it in full; for
a load that selects the built-in ``_ingestion_timestamp`` column it reads:

.. code-block:: text

   Non-deterministic operational metadata is computed upstream of a REPLACE WHERE (mode: replace_where) table. Incremental refresh is disabled: every refresh rewrites the whole replace_where window (COMPLETE_REPLACE_WHERE).
     - write 'write_orders_recent' <- action 'load_orders': column '_ingestion_timestamp' = F.current_timestamp()
   Deselect these columns on the listed actions (for example 'operational_metadata: false') or make their expressions deterministic. See https://lakehouse-plumber.readthedocs.io/en/latest/reference/actions/write/streaming-table-replace-where.html

To resolve it, deselect the columns on the listed actions, with
``operational_metadata: false`` or a selection of deterministic columns only,
or make their expressions deterministic.

The check has limits:

- It stays inside one flowgroup and follows declared ``source`` edges only. It
  does not follow a view that is read only inside SQL text, such as a ``JOIN``
  the action does not list as a source, or a view that another flowgroup of the
  same pipeline produces.
- It is advisory. A missed case never produces wrong data; it only loses
  incremental refresh.
- Warnings are de-duplicated per code and YAML file. When two flowgroups in
  one file both trigger it, only the warning of the flowgroup whose name sorts
  first is shown; ``--strict`` still fails the run.

Downstream readers
------------------

Every refresh rewrites the rows inside the predicate, so a table written by
``mode: replace_where`` is not an append-only source. A streaming read of it
fails on Databricks with ``DELTA_SOURCE_TABLE_IGNORE_CHANGES`` once a refresh
has rewritten a row. With ``skipChangeCommits`` the stream keeps running, but it
silently skips every commit that rewrote rows, including the new rows in that
commit.

Read the table in batch (``readMode: batch``, a materialized view, or SQL
without ``STREAM()``), or follow it through its change data feed, as shown in
`Mirror the table through its change feed`_.

LHP-VAL-072: streaming reads
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``lhp validate`` and ``lhp generate`` check every pipeline in the run for a
streaming read of a table that a ``mode: replace_where`` write produces, in the
same pipeline or another one. A pipeline with such a read fails with one
``LHP-VAL-072`` error that lists each read, and ``lhp generate`` writes no
files. These reads are checked:

- A ``delta`` load that reads a stream (``readMode: stream``), with or without
  ``skipChangeCommits``.
- A ``STREAM(<table>)`` read in the SQL of a SQL load or a SQL transform, inline
  or in a ``sql_path`` file, after substitution.
- A ``STREAM(<table>)`` read in a ``custom_sql`` test. Test actions are checked
  only when the run includes them (``--include-tests``).

A change-feed read is exempt: a ``delta`` load whose ``options.readChangeFeed``
is the boolean ``true`` or the string ``"true"`` in any case. Batch reads are not
checked.

A read names the table as ``catalog.schema.table`` or as ``schema.table``. It
must name the same table as the write, compared without regard to case or
backticks. A ``schema.table`` read matches only when a single catalog in the
run produces that table, and only outside a filtered run (see the limits
below). A table of the same name in another catalog is a different table.

For SQL that LHP cannot parse, such as a ``WATERMARK`` clause or
``STREAM(t) WITH (...)`` options, LHP reads the argument of each ``STREAM``
lexically:

- A two- or three-part name is checked like any other read.
- ``$source``, a ``${token}`` placeholder and a one-part name (a view) are not
  table reads. Neither is a table-valued function, such as
  ``STREAM read_files(...)``.
- Anything else, such as a subquery, is a read LHP cannot identify. When the run
  writes a REPLACE WHERE table, it is reported as ``LHP-VAL-072`` with a second
  message.

SQL that LHP cannot read is reported the same way, with the second message.

The two messages read:

.. code-block:: text

   <pipeline>.<flowgroup>.<action> streams from '<table>', a REPLACE WHERE table written by <pipeline>.<flowgroup>.<action>. Each refresh rewrites rows, which a streaming read cannot follow (skipChangeCommits silently drops data). Use readMode: batch, or stream its change feed with options.readChangeFeed: "true" (see docs).

   <pipeline>.<flowgroup>.<action> may stream from a REPLACE WHERE table: LHP cannot tell which table a STREAM read in its SQL names (the SQL does not parse and the STREAM argument is not a plain table name, or the SQL cannot be read), and the project writes REPLACE WHERE tables (<tables>). Each refresh rewrites rows, which a streaming read cannot follow (skipChangeCommits silently drops data). Name the streamed table with its full catalog.schema.table, as STREAM(catalog.schema.table), or read it through a delta load action (see docs).

To resolve the first, read the table in batch or through its change feed. To
resolve the second, write the streamed table as ``STREAM(catalog.schema.table)``
so that LHP can check it, or read it through a ``delta`` load action.

``LHP-VAL-072`` belongs to the reading pipeline, not to a flowgroup. In
``lhp validate`` output it appears on the pipeline's row:

.. code-block:: text

   ✗ gold_orders  LHP-VAL-072  Streaming read of a REPLACE WHERE table

In the ``--show-details`` panel, the context line names only the pipeline. The
message names each read, and the write behind it, as
``pipeline.flowgroup.action``.

The check has limits:

- **Python code is not inspected.** A ``spark.readStream`` in a Python load or
  transform is not detected.
- **Filtered runs check less.** With ``-p/--pipeline``, or a sandbox profile
  that limits the pipelines, only the selected pipelines are checked. A read
  whose REPLACE WHERE table is written by a pipeline outside the selection is
  not reported, and ``schema.table`` reads are not checked at all, because they
  may name a table in another catalog. Run a full ``lhp validate`` before you
  deploy.
- Only tables that LHP generates with ``mode: replace_where`` are known.
- A table named through ``IDENTIFIER(...)`` inside ``STREAM()`` is not
  detected.

Mirror the table through its change feed
----------------------------------------

To keep a streaming copy of a REPLACE WHERE table, stream its change data feed
into an AUTO CDC write. Databricks records each rewrite as ``delete`` and
``insert`` change events, which AUTO CDC applies to the copy.

First turn the change data feed on explicitly in the REPLACE WHERE write.
Adding the property to an existing REPLACE WHERE table does not reset it:

.. code-block:: yaml

   write_target:
     type: streaming_table
     mode: replace_where
     # ...
     table_properties:
       delta.enableChangeDataFeed: "true"

Then add the mirror, typically in a downstream pipeline:

.. literalinclude:: ../../../_fixtures/guide_write_st_replace_where/pipelines/orders_recent_mirror.yaml
   :language: yaml
   :caption: pipelines/orders_recent_mirror.yaml

.. important::

   ``sequence_by`` must list ``_change_type`` after ``_commit_version``. Within
   one commit, a rewritten key has both a ``delete`` and an ``insert`` event
   with the same ``_commit_version``. With ``sequence_by: "_commit_version"``
   alone, AUTO CDC kept the ``delete`` in every such tie observed, so rewritten
   keys disappeared from the mirror without any error. With
   ``["_commit_version", "_change_type"]`` the ``insert`` wins inside a commit,
   because ``"insert"`` sorts after ``"delete"``, while ``_commit_version``
   still orders the commits.

The other settings:

- ``where_clause`` drops update pre-images. No REPLACE WHERE refresh produced
  update events when this recipe was tested, so the filter is insurance:
  ``"update_preimage"`` sorts after ``"update_postimage"``, so a pre-image would
  otherwise win the tie.
- ``apply_as_deletes`` turns ``delete`` events into deletes.
- ``except_column_list`` keeps the three change-feed columns out of the
  mirror.

Expect a large change volume when the REPLACE WHERE table is refreshed with
``COMPLETE_REPLACE_WHERE``: every refresh emits a ``delete`` and an ``insert``
for every row inside the predicate. The mirror stays correct.

The same change-feed load can feed a plain append streaming table, to keep a log
of the change events instead of a copy. Databricks then warns that it cannot
turn on the change data feed for that log table, because it contains the
reserved ``_change_type``, ``_commit_version`` and ``_commit_timestamp``
columns.

.. note::

   The recipe was tested on Databricks over several refreshes, which rewrote,
   deleted and inserted rows and narrowed the predicate. The producers were
   hand-written REPLACE WHERE tables and the consumers were generated by LHP;
   the mirror matched the table after every refresh. With an LHP-generated
   producer it was run for one update. Resets, schema changes, change-feed
   retention gaps and readers that start from a ``startingVersion`` were not
   tested.

Backfills
---------

A REPLACE WHERE table takes a single flow, so a backfill cannot be a second
flow. To recompute a different range for one update, Databricks takes a
predicate override when you start the update: ``replace_where_overrides``, with
one entry per flow giving its ``flow_name`` and a ``predicate_override``. The
override applies to that update only; later updates use the predicate in the
generated code. LHP does not generate overrides, and the override was not
exercised when this feature was tested on Databricks.

The ``flow_name`` is the table's full name, for example
``dev_catalog.silver.orders_recent``, because the flow takes the table's name.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/streaming-table-replace-where`
- :doc:`/reference/actions/write/streaming-table-replace` (REPLACE USING, the
  streaming sibling)
- :doc:`All write actions </reference/actions/write>`
