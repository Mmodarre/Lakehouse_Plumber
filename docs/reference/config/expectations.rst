Expectation formats
===================

Choose the contract by the action consuming the expectations.

.. list-table::
   :header-rows: 1
   :widths: 35 35 30

   * - Use
     - Where configured
     - Format and behaviour
   * - Row-level data-quality rules
     - ``expectations_file`` on a ``data_quality`` transform
     - :doc:`Data-quality transform </reference/actions/transform/data_quality>` and :doc:`worked file example </guides/transform/data-quality>`
   * - Materialized-view expectations
     - ``write_target.expectations_file`` on a ``materialized_view`` write
     - :ref:`Materialized view <mv-expectations>` and :doc:`guide </guides/write/materialized-view>`
   * - Quarantine rules and recycling
     - ``expectations_file`` with ``mode: quarantine``
     - :doc:`quarantine-dlq` and :doc:`quarantine guide </guides/transform/quarantine>`
   * - Standalone custom assertions
     - ``expectations`` on a ``custom_expectations`` test action
     - :doc:`Custom expectations </reference/actions/test/custom_expectations>`

An expectations file, a table schema and a schema-transform definition have
different grammars. See :doc:`schema-files` when defining types or casts.

Rule files
----------

A ``data_quality`` transform and a materialized-view write read the same file
formats. Both formats are accepted in YAML or JSON. Each rule picks one of three
reactions to a row that fails its constraint.

.. list-table::
   :header-rows: 1
   :widths: 14 30 56

   * - Action
     - Decorator
     - Behaviour
   * - ``warn``
     - ``@dp.expect_all``
     - Logs the violation and keeps the row. The default when a rule sets no action.
   * - ``drop``
     - ``@dp.expect_all_or_drop``
     - Drops violating rows.
   * - ``fail``
     - ``@dp.expect_all_or_fail``
     - Fails the update on any violation. On a materialized view this makes the
       rule a gate.

**Mapping format.** Each key is the constraint, a boolean SQL expression. Each
value sets ``action`` (``warn``, ``drop`` or ``fail``) and an optional ``name``
(default: the constraint).

.. code-block:: yaml

   order_key IS NOT NULL:
     action: fail
     name: order_key_present
   total_price >= 0:
     action: drop
     name: price_not_negative

**List format.** Each entry sets ``constraint`` (or ``expression``) and ``name``
(or ``message``). The reaction is ``failureAction`` (``warn``, ``drop`` or
``fail``), or else ``type`` (``expect``, ``expect_or_drop`` or
``expect_or_fail``). An entry with neither is a ``warn``. A list entry that
carries ``action`` is rejected: that key belongs to the mapping format.

.. code-block:: yaml

   - name: price_not_negative
     constraint: "total_price >= 0"
     failureAction: warn
   - name: amount_reconciled
     constraint: "amount_reconciled"
     failureAction: fail

A file may also wrap either format under a top-level ``expectations:`` key. A
document that is neither a mapping nor a list, or an ``expectations:`` key that
holds something else (including ``null``), fails with ``LHP-VAL-008``.

Checks at ``lhp validate``
~~~~~~~~~~~~~~~~~~~~~~~~~~

Rules are checked when the flowgroup is validated, and the same checks apply to
expectations written inline on a ``data_quality`` action.

.. list-table::
   :header-rows: 1
   :widths: 18 82

   * - Code
     - Raised when
   * - ``LHP-VAL-001``
     - A rule has no constraint.
   * - ``LHP-VAL-006``
     - A rule names an unknown ``action``, ``failureAction`` or ``type``, or a
       list entry uses the mapping key ``action`` (the message suggests
       ``failureAction``).
   * - ``LHP-VAL-008``
     - A rule is not a mapping, or a ``name``, ``message``, ``constraint``,
       ``expression`` or constraint key is not text (for example ``name: 2024``
       or ``constraint: true``). Quote the value.
   * - ``LHP-VAL-071``
     - Two rules on a materialized view share a name: within the view's
       ``write_target.expectations_file``, or within the rules of a fused
       ``data_quality`` action.

A ``null`` ``type`` or ``failureAction`` means ``expect`` (a warning).
