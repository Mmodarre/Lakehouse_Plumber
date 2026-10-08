Generated output
----------------


Every test action generates a function decorated with
``@dp.table(name="<target>", comment="<description>", temporary=True)`` that
returns the violation query, plus one expectation decorator per violation
bucket. The ``on_violation`` value maps to the decorator as follows:

.. list-table::
   :header-rows: 1
   :widths: 18 42 40

   * - on_violation
     - Decorator
     - Behavior
   * - ``fail``
     - ``@dp.expect_all_or_fail``
     - Pipeline update fails when any row violates.
   * - ``warn``
     - ``@dp.expect_all``
     - Violations recorded as metrics; rows retained.
   * - ``drop``
     - ``@dp.expect_all_or_drop``
     - Violating rows dropped from the output.

For the seven built-in types the action-level ``on_violation`` drives the
decorator. For ``custom_sql`` and ``custom_expectations`` the decorators are
built from each entry in ``expectations`` using that entry's own
``on_violation``.
