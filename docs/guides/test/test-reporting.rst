==========================================
Report test results to an external system
==========================================

.. meta::
   :description: Forward Lakehouse Plumber test-action results to a Delta audit table, Azure DevOps Test Plans, or any endpoint — declare a test_reporting provider in lhp.yaml and LHP generates the per-pipeline event hook that collects and publishes every result.

You've :doc:`written a test action <data-tests>`. Its pass/fail result lands in
the pipeline's event log — but that's inside Databricks. When your team tracks
test outcomes somewhere else — a Delta audit table, Azure DevOps Test Plans, a
CI dashboard — you want each result *published* there automatically, on every
run.

You could hand-write that: an ``@dp.on_event_hook`` that parses
``flow_progress`` events, extracts the data-quality metrics, correlates each one
to the right test, waits for the pipeline's terminal state, and calls your
publishing code exactly once. Or you declare a ``test_reporting`` provider in
``lhp.yaml``, tag each test with a ``test_id``, and let Lakehouse Plumber
generate the whole hook. That is the idea on every page: **declare your ETL,
don't hand-write it.**

Before you start
================

You need a project with at least one :doc:`test action <data-tests>` and a
Python function that knows how to publish results to your system. This guide
publishes to a Delta audit table; the same shape works for any sink.

.. important::

   Use ``on_violation: warn`` on any test whose result you report. A ``fail``
   aborts the flow *before* its metrics are recorded, so the reporting hook never
   sees them — ``warn`` records the outcome and lets the run continue to terminal
   state, where the hook publishes.

Write the provider function
===========================

The provider is an ordinary Python module exporting one function with a fixed
signature: ``publish_results(results, config, context, spark)``. Lakehouse
Plumber calls it once, at pipeline terminal state, with the accumulated results.

.. literalinclude:: ../../_fixtures/guide_test_reporting/providers/audit_delta.py
   :language: python
   :caption: providers/audit_delta.py

Swap the body for an Azure DevOps API call or a webhook — the contract is the
same. Return a ``{"published": N, "failed": M}`` dict so LHP can log the outcome.

Declare the provider in ``lhp.yaml``
====================================

Point ``test_reporting`` at the module, the function, and (optionally) a config
file passed to the provider verbatim:

.. literalinclude:: ../../_fixtures/guide_test_reporting/lhp.yaml
   :language: yaml
   :caption: lhp.yaml
   :lines: 6-13

.. literalinclude:: ../../_fixtures/guide_test_reporting/providers/audit_config.yaml
   :language: yaml
   :caption: providers/audit_config.yaml

Tag the test with a ``test_id``
===============================

The ``test_id`` correlates a test action with the entry your provider publishes.
Add it to every test you report:

.. literalinclude:: ../../_fixtures/guide_test_reporting/pipelines/orders_dq.yaml
   :language: yaml
   :caption: pipelines/orders_dq.yaml
   :emphasize-lines: 12,13

Generate with tests included
============================

Test actions are opt-in, so pass ``--include-tests``:

.. code-block:: console

   $ lhp generate --env dev --include-tests
   ✓ discover (0.01s)
   ✓ preflight (0.00s)
   ✓ bronze_dq  1 file
   ✓ generate (0.35s)
   ✓ format (0.02s)
   ✓ monitoring (0.00s)
   1 pipeline generated · 1 file · 0.4s

Read what Lakehouse Plumber wrote
=================================

Alongside the test table, LHP wrote one ``_test_reporting_hook.py`` per pipeline
and copied your provider into ``test_reporting_providers/``:

.. code-block:: text

   generated/dev/bronze_dq/
   ├── orders_dq.py                        # the test table
   ├── _test_reporting_hook.py             # the generated event hook
   └── test_reporting_providers/
       ├── __init__.py
       └── audit_delta.py                  # your provider, copied in

The hook imports your function, maps each test table to its ``test_id``, embeds
the provider config, and accumulates results until the pipeline terminates:

.. literalinclude:: ../../_fixtures/guide_test_reporting/generated/dev/bronze_dq/_test_reporting_hook.py
   :language: python
   :caption: generated/dev/bronze_dq/_test_reporting_hook.py
   :lines: 1-21

You wrote a provider function and two lines of config. Lakehouse Plumber wrote
the event plumbing — the ``flow_progress`` parsing, the terminal-state guard, the
single-publish latch — that is tedious to get right by hand.

.. note::

   ``${...}`` tokens in the config file (like ``audit_table`` above) are passed
   through to the provider **unresolved** — the hook receives the literal
   ``${catalog}.${bronze_schema}.test_audit_log``. Resolve them in your provider,
   or hard-code fully qualified names in the config file.

What's next
===========

- **Report from many pipelines** — the ``test_reporting`` block is project-wide,
  so every pipeline with a tagged test gets its own hook. One provider, all
  pipelines.
- **Reference** — every ``test_reporting`` field is in the
  :doc:`test action reference </reference/actions/test>`.
