=================================
Test data quality in the pipeline
=================================

.. meta::
   :description: Declare a data-quality test as a Lakehouse Plumber test action — a uniqueness, row-count, or referential check that runs inside the pipeline and reports pass/fail — instead of hand-writing an assertion query and expectation decorator.

Let's assert something you actually care about: every order in the bronze
``orders`` table has a unique ``order_id``. Not in a notebook you run by hand
after the fact, but as a check that lives in the pipeline, runs on every update,
and reports its result alongside the data it guards.

You could hand-write this. You'd author a ``@dp.table(temporary=True)`` function
that runs a ``GROUP BY ... HAVING COUNT(*) > 1`` duplicate query, then decorate
it with ``@dp.expect_all_or_fail`` carrying the exact expression
``duplicate_count == 0`` — and remember the ``temporary=True`` flag so the check
table never persists. Or you declare ``type: test`` with ``test_type:
uniqueness``, name the columns, and let Lakehouse Plumber write the query, the
temporary table, and the expectation. That is the whole idea: **declare your
data tests, don't hand-write the assertion code.**

Before you start
================

You need a Lakehouse Plumber project (see the Get Started course for
``lhp init``) and a table to test — here, a bronze ``orders`` table another
flowgroup produces. A test action references its target by name, so the tested
table does not have to live in the same flowgroup; keep your data tests in a
dedicated flowgroup and point them at the tables they guard.

Declare the test
================

A test is one ``test`` action. Give it a ``test_type``, point ``source`` at the
table to check, list the ``columns`` whose combined value must be unique, and
choose what happens on a violation. Create ``pipelines/orders_dq.yaml``:

.. literalinclude:: ../../_fixtures/guide_qo_test_reporting/pipelines/orders_dq.yaml
   :language: yaml
   :caption: pipelines/orders_dq.yaml
   :emphasize-lines: 10-14

The ``${catalog}`` and ``${bronze_schema}`` tokens resolve per environment from
``substitutions/dev.yaml``, so the same test ships unchanged from ``dev`` to
prod. ``on_violation: fail`` makes a duplicate stop the pipeline; ``warn`` records
the result and lets the run continue. This guide stays on the single
``uniqueness`` case — the eight other test types (``row_count``,
``referential_integrity``, ``completeness``, ``range``, ``schema_match``,
``all_lookups_found``, ``custom_sql``, ``custom_expectations``) take the same
shape and are covered in the test action reference.

Generate with tests included
============================

Test actions are opt-in. Both ``lhp validate`` and ``lhp generate`` skip them by
default, so a plain generate on this flowgroup writes nothing. Pass
``--include-tests`` to emit the test code:

.. code-block:: console

   $ lhp validate --env dev --include-tests
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_dq  0 files
   ✓ validate (0.33s)
   1 validated · 0.3s

   $ lhp generate --env dev
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_dq  0 files
   ✓ generate (0.39s)
   ✓ format (0.05s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 0 files · 0.4s

   $ lhp generate --env dev --include-tests
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_dq  1 file
   ✓ generate (0.35s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

The plain generate reports ``0 files`` — the test is there but held back. With
``--include-tests`` the same flowgroup writes ``1 file``.

Read what Lakehouse Plumber wrote
=================================

Open ``generated/dev/bronze_dq/orders_dq.py``. This is the entire output —
nothing is hidden behind a runtime:

.. literalinclude:: ../../_fixtures/guide_qo_test_reporting/generated/dev/bronze_dq/orders_dq.py
   :language: python
   :caption: generated/dev/bronze_dq/orders_dq.py
   :emphasize-lines: 18-23

Lakehouse Plumber turned the check into a temporary Lakeflow table. It wrote the
duplicate-finding query — ``SELECT order_id, COUNT(*) AS duplicate_count ...
GROUP BY order_id HAVING COUNT(*) > 1`` — with your ``${...}`` tokens resolved to
``dev_catalog.bronze.orders``, wrapped it in ``@dp.table(...)`` marked
``temporary=True`` so the check never persists, and attached the expectation
``@dp.expect_all_or_fail({"no_duplicates": "duplicate_count == 0"})``. When the
pipeline runs, Databricks evaluates that expectation and records the pass/fail
count in the pipeline's event log and data-quality metrics — that is where the
result is reported.

.. note::

   ``on_violation`` decides the decorator: ``fail`` emits
   ``@dp.expect_all_or_fail`` (a violation stops the flow), ``warn`` emits
   ``@dp.expect_all`` (the result is recorded, the run continues), and ``drop``
   emits ``@dp.expect_all_or_drop``. Choose ``fail`` for a hard gate. Choose
   ``warn`` when you forward results to an external system, because a ``fail``
   aborts the flow before its metrics are recorded and the reporting hook never
   sees them.

What you just did
=================

You wrote a seven-line test action and Lakehouse Plumber generated the 31-line
Lakeflow test table: **zero lines of the assertion came from you** — not the
``GROUP BY ... HAVING`` duplicate query, not the ``@dp.table(temporary=True)``
wrapper, not the ``@dp.expect_all_or_fail`` expression. The rule you own —
``order_id`` is unique — is the one thing you typed. Change ``on_violation`` or
add a ``filter`` and regenerate, and Lakehouse Plumber rewrites the table to
match; you never touch the Python.

What's next
===========

- **Reach for another test type** — compare counts across two tables with
  ``row_count``, check foreign keys with ``referential_integrity``, or bound a
  column with ``range``. The full type list and every field lives in the test
  action reference.
- **Publish results to an external system** — add a ``test_reporting`` provider
  to ``lhp.yaml`` and a ``test_id`` to each reported action. Lakehouse Plumber
  then generates one per-pipeline event hook that collects the DQ results and
  calls your provider function, forwarding them to a Delta audit table, Azure
  DevOps Test Plans, or your own endpoint. Use ``on_violation: warn`` on those
  actions so every outcome reaches the provider. See the test reporting
  reference.
