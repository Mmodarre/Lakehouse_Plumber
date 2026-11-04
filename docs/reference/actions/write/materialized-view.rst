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
   * - ``expectations_file``
     - string
     - —
     - Path to a data-quality expectations file, relative to the project root. Each rule becomes a ``@dp.expect_all``, ``@dp.expect_all_or_drop`` or ``@dp.expect_all_or_fail`` decorator on the view. See :doc:`/reference/config/expectations` for the file formats and :ref:`mv-expectations` below. Valid only on a ``materialized_view`` write target.

Define the view by exactly one of: an action-level ``source`` view, inline
``sql``, or ``sql_path``. When ``sql``/``sql_path`` is provided, no
action-level ``source`` is needed.

Substitution tokens (``${token}``, ``${secret:scope/key}``) resolve in both
inline SQL and the ``sql_path`` file. A plain ``${name}`` that is still in the
``sql_path`` file after substitution fails ``lhp validate`` and ``lhp generate``
with ``LHP-CFG-010``. Dotted or prefixed Spark variables such as
``${mypipeline.start}`` or ``${hiveconf:x}`` are left as written.
To read a pipeline ``configuration:`` parameter at runtime, give it a dotted
key (for example ``mypipeline.start_date``) and reference it as
``${mypipeline.start_date}``; a plain ``${start_date}`` fails even when the
configuration defines it.

In a template action, a ``sql_path`` file that contains ``{{``, ``{%`` or
``{#`` is rendered with the template parameters before substitution, like the
template's inline SQL. See :ref:`template-sql-files`.

.. _mv-expectations:

Expectations
------------

A materialized view takes data-quality expectations in two ways. Both put the
decorators on the view itself, which is what lets Databricks keep refreshing the
view incrementally. Expectations on a view *upstream* of a materialized view
force it to recompute in full on every refresh.

**Shape 1: ``write_target.expectations_file``.** Name an expectations file on the
write target. This works for every way of defining the view (``sql``, ``sql_path``
or a ``source`` view). The file uses the same formats as a ``data_quality``
transform (:doc:`/reference/config/expectations`).

.. code-block:: yaml

   - name: write_mv_expect_sql_gate
     type: write
     write_target:
       type: materialized_view
       catalog: "${catalog}"
       schema: "${gold_schema}"
       table: "mv_expect_sql_gate"
       sql_path: "sql/gold/mv_expect_sql_gate.sql"
       expectations_file: "expectations/mv_write_expectations.json"

**Shape 2: a fused ``data_quality`` action.** When a ``data_quality`` action
feeds a pass-through materialized view, code generation moves its expectations
onto the view. The ``data_quality`` action is written as usual:

.. code-block:: yaml

   - name: check_mv_expect_orders
     type: transform
     transform_type: data_quality
     source: v_mv_expect_orders
     target: v_mv_expect_orders_checked
     expectations_file: "expectations/mv_write_expectations_dq.json"

   - name: write_mv_expect_fused
     type: write
     source: v_mv_expect_orders_checked
     write_target:
       type: materialized_view
       catalog: "${catalog}"
       schema: "${gold_schema}"
       table: "mv_expect_fused"

The materialized view then carries the action's expectations and its
operational-metadata columns, and reads the action's *source* view
(``v_mv_expect_orders``). The ``data_quality`` view is still emitted, but nothing
reads it, so it never runs. The ``data_quality`` action fuses when all of these
hold:

- it is in ``dqe`` mode (``quarantine`` mode never fuses);
- it reads in batch. An unset ``readMode`` inherits its producer's kind (see
  :doc:`/reference/actions/transform/data_quality`);
- exactly one action in the flowgroup reads its view;
- that action is a ``materialized_view`` write whose ``source`` is the
  ``data_quality`` view alone, with no ``sql`` or ``sql_path``, and that is the
  only write to its table.

Both shapes can meet on one view. Expectation names must be unique on the view,
across the file and the fused action: a repeat fails with ``LHP-VAL-071``.

.. note::

   A materialized view whose own ``sql`` or ``sql_path`` reads a ``data_quality``
   view is **not** fused. It works, but Databricks recomputes it in full on every
   refresh. To keep it incremental, put the rules on the view with Shape 1, or make
   the view a pass-through (``source`` only).

Setting ``expectations_file`` on the *action* rather than in ``write_target``
raises ``LHP-CFG-073``. Setting it in the ``write_target`` of a streaming table
or sink raises ``LHP-CFG-001``.

.. include:: /_includes/write-target-common.rst

.. include:: /_includes/write-common.rst

Related guides
--------------

- :doc:`/guides/write/materialized-view`
- :doc:`All write actions </reference/actions/write>`
