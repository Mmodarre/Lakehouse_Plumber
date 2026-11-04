Row expectations (data_quality)
===============================

Applies expectation rules to the source view. ``mode: dqe`` emits a
``@dp.temporary_view`` decorated with expectation decorators; ``mode: quarantine``
emits a Delta dead-letter (DLQ) subsystem that routes violating rows to a
quarantine table. ``dqe`` reads a stream or a batch; ``quarantine`` reads only a
stream.

Minimal example
---------------

Add this entry to the ``actions`` list in your :doc:`flowgroup </reference/config/flowgroups>`.

.. code-block:: yaml

   - name: dq_orders
     type: transform
     transform_type: data_quality
     source: v_orders
     target: v_orders_validated
     expectations_file: "expectations/orders.yaml"
     mode: dqe

.. contents:: On this page
   :local:
   :depth: 2

Options
-------

.. list-table::
   :header-rows: 1
   :widths: 20 14 10 12 44

   * - Field
     - Type
     - Required
     - Default
     - Description
   * - ``source``
     - string
     - Yes
     - —
     - Input view name.
   * - ``expectations_file``
     - string
     - Yes
     - —
     - Path to a YAML expectations file, relative to project root.
   * - ``mode``
     - string
     - No
     - ``dqe``
     - ``dqe`` or ``quarantine``.
   * - ``quarantine``
     - object
     - Cond.
     - —
     - Required when ``mode: quarantine``. Block with ``dlq_table`` and ``source_table`` (both required, both fully-qualified ``catalog.schema.table``).
   * - ``readMode``
     - string
     - No
     - inherited
     - ``stream`` or ``batch``. Unset in ``dqe`` mode, the action reads the way the
       view it reads was made; see :ref:`dq-read-mode`. ``mode: quarantine``
       accepts only ``stream`` (``LHP-VAL-068`` otherwise).

The expectations file holds the rules. The mapping format is shown here: each key
is a boolean SQL expression the row must satisfy, and each value sets a per-rule
``action`` and optional ``name``. :doc:`/reference/config/expectations`
covers the list format and every check on a rule.

.. list-table::
   :header-rows: 1
   :widths: 22 16 16 46

   * - Key
     - Type
     - Default
     - Description
   * - ``<expression>``
     - mapping key
     - —
     - A boolean SQL expression, e.g. ``amount >= 0``. Used as the constraint.
   * - ``action``
     - string
     - ``warn``
     - ``warn``, ``drop``, or ``fail``. Selects the emitted decorator.
   * - ``name``
     - string
     - the expression
     - Rule name used as the key in the emitted expectation dict.

Each ``action`` maps to a Lakeflow expectation decorator (``dqe`` mode):

.. list-table::
   :header-rows: 1
   :widths: 16 42 42

   * - ``action``
     - Decorator
     - Behavior
   * - ``warn``
     - ``@dp.expect_all``
     - Logs a warning; keeps violating rows. Default.
   * - ``drop``
     - ``@dp.expect_all_or_drop``
     - Drops violating rows.
   * - ``fail``
     - ``@dp.expect_all_or_fail``
     - Fails the pipeline on any violation.

In ``quarantine`` mode every rule is coerced to ``drop`` regardless of its
configured ``action``; ``warn`` / ``fail`` rules trigger a validation warning. The
DLQ outbox table is derived as ``<dlq_table>_outbox``. Create both tables with the
DDL in :doc:`/reference/config/quarantine-dlq`, which clusters them by
``_dlq_source_table`` and enables deletion vectors.

.. code-block:: yaml

   # expectations/orders.yaml
   order_id IS NOT NULL:
     action: drop
     name: valid_order_id
   amount >= 0:
     action: warn
     name: non_negative_amount

Quarantine mode replaces the inline decorators with the DLQ subsystem:

.. code-block:: yaml

   - name: dq_orders
     type: transform
     transform_type: data_quality
     source: v_orders
     target: v_orders_validated
     expectations_file: "expectations/orders.yaml"
     mode: quarantine
     quarantine:
       dlq_table: main.ops.orders_dlq
       source_table: main.bronze.orders

.. _dq-read-mode:

Read mode
---------

A view is either streaming or batch, and Databricks requires a reader to read it
the same way (``LHP-VAL-070``, below). A ``dqe`` action with no ``readMode``
takes the kind of the view it reads when another load or transform in the same
flowgroup produces that view:

- a load or transform produces a view of the kind it reads: a ``delta`` load is
  batch unless it sets ``readMode: stream``, a ``jdbc`` load is batch, and
  ``cloudfiles`` and ``kafka`` loads are streams;
- a ``sql`` load or transform is a stream exactly when its SQL reads
  ``STREAM(...)``;
- a ``schema``, ``python`` or ``data_quality`` transform takes the kind of its
  own read, so a chain carries one kind through.

When the source is external to the flowgroup, or its kind is not known (a
``python`` load, or a ``temp_table``), the action defaults to ``stream``. A
``python`` load that returns a batch DataFrame therefore needs ``readMode: batch``
on the ``data_quality`` action. An explicit ``readMode`` always wins.

A batch ``data_quality`` action that feeds a pass-through materialized view is
fused into the view; see :ref:`mv-expectations`.

A generated pipeline fails on Databricks when a view is read the other way, so
``lhp validate`` checks every read of a view that a load or transform of the same
flowgroup produces, wherever both the view's kind and the read are known. A
mismatch is ``LHP-VAL-070``:

- a stream read (``readStream``, or ``STREAM(view)`` in SQL) of a batch view fails;
- a batch read (``spark.read``, or SQL without ``STREAM()``) of a streaming view
  fails;
- a standard streaming-table append flow that reads a batch view with
  ``readMode: batch`` fails too, unless the write sets ``once: true`` (a one-time
  backfill).

The message names both actions and suggests the ``readMode`` that fixes it.
Reads of external tables and of views from other flowgroups are not checked.
Test actions always read their source with batch SQL, so point them at tables or
batch views, not streaming views.

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/data-quality`
- :doc:`All transform actions </reference/actions/transform>`
