Check data quality
==================

.. raw:: html

   <span id="test"></span>


Choose a check by what should happen to the data.

.. list-table::
   :header-rows: 1

   * - I want to
     - Use
   * - Record, drop or fail rows based on rules inside a flow
     - :doc:`Row expectations </guides/transform/data-quality>`
   * - Keep invalid rows for inspection and recycling
     - :doc:`Quarantine </guides/transform/quarantine>`
   * - Assert row counts, uniqueness, relationships or other dataset properties
     - :doc:`Test actions <data-tests>`
   * - Publish test outcomes to another system
     - :doc:`Test reporting <test-reporting>`

.. toctree::
   :maxdepth: 1
   :hidden:

   Row expectations </guides/transform/data-quality>
   Quarantine </guides/transform/quarantine>
   Data tests <data-tests>
   Test reporting <test-reporting>
