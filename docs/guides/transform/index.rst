=========
Transform
=========

.. meta::
   :description: Reshape, enrich, and validate rows between a load and a write in Lakehouse Plumber — SQL and Python transforms, schema enforcement, temp tables, data-quality expectations, and quarantine.

A ``transform`` action sits between a load and a write, reshaping the rows that
flow through. Pick the transform type by what you're doing to the data.

.. list-table::
   :header-rows: 1
   :widths: 30 40 30

   * - You want to…
     - Reach for
     - Guide
   * - Filter, join, or reshape with a query
     - A **SQL** transform
     - :doc:`sql`
   * - Do something SQL can't express cleanly (loop over columns, call a library)
     - A **Python** transform
     - :doc:`python`
   * - Rename and cast columns to a target schema
     - A **schema** transform
     - :doc:`schema`
   * - Stage an intermediate result the rest of the flow reads
     - A **temp table**
     - :doc:`temp-table`
   * - Drop rows that fail expectations
     - A **data-quality** transform (``mode: dqe``)
     - :doc:`data-quality`
   * - Keep failed rows for inspection and recycling instead of dropping them
     - A **data-quality** transform in **quarantine** mode
     - :doc:`quarantine`

.. note::

   Data quality has two homes. A ``data_quality`` transform gates rows *inside* a
   flow (drop or quarantine). To assert a standalone property — uniqueness, a row
   count, referential integrity — as a **unit test for your data**, use a
   :doc:`test action </guides/test/index>` instead.

.. toctree::
   :maxdepth: 1
   :hidden:

   SQL transform <sql>
   Python transform <python>
   Schema enforcement <schema>
   Temp tables <temp-table>
   Data-quality expectations <data-quality>
   Quarantine bad rows <quarantine>
