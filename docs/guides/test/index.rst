====
Test
====

.. meta::
   :description: Test your data with Lakehouse Plumber test actions — the fourth action kind — and publish the results to an external system.

A **test action** is the fourth Lakehouse Plumber action kind, alongside load,
transform, and write. It's a **unit test for your data**: a standalone assertion
— uniqueness, a row count, referential integrity — that runs inside the pipeline
on every update and reports pass or fail.

- :doc:`data-tests` — declare a test action and let LHP generate the assertion.
- :doc:`test-reporting` — forward every result to a Delta audit table, Azure
  DevOps Test Plans, or your own endpoint.

.. note::

   A test action is different from a ``data_quality`` :doc:`transform
   </guides/transform/data-quality>`: the transform gates rows *inside* a flow;
   a test action asserts a property *about* a table and reports the outcome.

.. toctree::
   :maxdepth: 1
   :hidden:

   Data tests <data-tests>
   Test reporting <test-reporting>
