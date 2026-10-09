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
   * - Quarantine rules and recycling
     - ``expectations_file`` with ``mode: quarantine``
     - :doc:`quarantine-dlq` and :doc:`quarantine guide </guides/transform/quarantine>`
   * - Standalone custom assertions
     - ``expectations`` on a ``custom_expectations`` test action
     - :doc:`Custom expectations </reference/actions/test/custom_expectations>`

An expectations file, a table schema and a schema-transform definition have
different grammars. See :doc:`schema-files` when defining types or casts.
