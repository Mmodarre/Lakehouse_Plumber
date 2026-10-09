Row expectations (data_quality)
===============================

Applies expectation rules to the source view. ``mode: dqe`` emits a
``@dp.temporary_view`` decorated with expectation decorators; ``mode: quarantine``
emits a Delta dead-letter (DLQ) subsystem that routes violating rows to a
quarantine table. ``readMode`` must be ``stream``.

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
     readMode: stream

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
     - ``stream``
     - Must be ``stream``.

The expectations file is a YAML mapping. Each key is a boolean SQL expression
the row must satisfy; each value sets a per-rule ``action`` and optional ``name``.

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
DLQ outbox table is derived as ``<dlq_table>_outbox``.

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
     readMode: stream
     quarantine:
       dlq_table: main.ops.orders_dlq
       source_table: main.bronze.orders

.. include:: /_includes/transform-common.rst

Related guides
--------------

- :doc:`/guides/transform/data-quality`
- :doc:`All transform actions </reference/actions/transform>`
