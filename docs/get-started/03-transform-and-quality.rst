========================================
Add a transform and data quality
========================================

.. meta::
   :description: Extend your first Lakehouse Plumber pipeline with a SQL transform that reshapes the data and a data-quality gate that drops invalid rows.

Your :doc:`first pipeline <02-your-first-pipeline>` landed raw CSV straight into a
table. Raw is rarely table-ready — the types are strings, the column names are
whatever the file had, and some rows are junk. Let's fix both: add a **transform**
to reshape the data, and a **data-quality** check to drop the bad rows before they
reach the table.

Same flowgroup, two new actions. You still don't write PySpark.

The updated flowgroup
=====================

Insert two actions between the load and the write — a ``transform`` and a
``data_quality`` check:

.. literalinclude:: ../_fixtures/transform_quality/pipelines/bronze_ingest.yaml
   :language: yaml
   :caption: pipelines/bronze_ingest.yaml
   :emphasize-lines: 17-41

Actions chain by view name: each ``target`` becomes the next action's
``source``. So ``load`` produces ``v_orders_raw``, ``clean_orders`` reshapes it
into ``v_orders_clean``, ``validate_orders`` gates that into
``v_orders_validated``, and the ``write`` reads the validated view.

The ``clean_orders`` transform runs plain SQL: strings become
``BIGINT`` / ``DOUBLE`` / ``TIMESTAMP``, ``cust_id`` becomes ``customer_id``, and
rows with no ``order_id`` are filtered out.

The expectations
================

The ``data_quality`` action applies **expectations** — rules every row must
pass. The rules live in their own file:

.. literalinclude:: ../_fixtures/transform_quality/expectations/orders_quality.yaml
   :language: yaml
   :caption: expectations/orders_quality.yaml

Each rule is a SQL boolean expression with an ``action``. Both use ``drop``, so
rows that fail are discarded and only valid records flow on. The column names are
the *post-transform* ones — the checks run after ``clean_orders``, on
``customer_id`` and ``amount``, not the raw CSV headers.

Generate and read the output
============================

.. code-block:: console

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ bronze_ingest  1 file
   ✓ generate (0.36s)
   ✓ format (0.05s)
   1 pipeline generated · 1 file · 0.4s

The pipeline now has three views — raw, clean, validated — feeding the table:

.. literalinclude:: ../_fixtures/transform_quality/generated/dev/bronze_ingest/orders_ingest.py
   :language: python
   :caption: generated/dev/bronze_ingest/orders_ingest.py
   :emphasize-lines: 58-67

The quality gate is that one decorator — ``@dp.expect_all_or_drop`` — that LHP
wrote from your two rules. You described the *rules*; LHP wrote the
*enforcement*. Change a rule's ``action`` and LHP generates different
enforcement — but you never touch the Python.

What you just did
=================

Two more actions of intent — a transform and a quality gate — and LHP wrote about
forty more lines of Lakeflow: a typed SQL view and an expectation-enforced view,
correctly chained into the table.

Dropping bad rows is the simple case. When you'd rather **keep** them — route
failures to a dead-letter table, fix them, and recycle them back into the
pipeline — that's *quarantine* mode, covered in the data-quality guide.

What's next
===========

- **Reuse with a preset** — you've now written the same ``cloudFiles`` +
  ``streaming_table`` shape by hand. Next you'll factor the shared configuration
  out so you don't repeat it for every table you ingest.
