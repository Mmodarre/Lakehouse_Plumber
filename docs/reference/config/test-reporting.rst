Test reporting configuration
============================

**File:** ``lhp.yaml``. **Path:** ``test_reporting``.
Omit the block to generate without a reporting provider.
See :doc:`/guides/test/test-reporting` for a complete provider example.

.. code-block:: yaml

   test_reporting:
     module_path: providers/audit_delta.py
     function_name: publish_results
     config_file: providers/audit_config.yaml

Fields
------

.. list-table::
   :header-rows: 1
   :widths: 25 15 20 40

   * - Field
     - Type
     - Required / default
     - Meaning
   * - ``module_path``
     - string
     - Required
     - Project-relative provider Python file, copied into the generated pipeline.
   * - ``function_name``
     - string
     - Required
     - Callable exported by the module.
   * - ``config_file``
     - string or null
     - unset
     - Optional provider configuration file. The resulting configuration is passed to the provider; without it, the provider receives an empty mapping.

The optional provider configuration is copied into the generated hook. Tokens
inside that file are passed through unresolved; resolve them in your provider
or supply concrete values.

Provider contract
-----------------

The hook calls the function with keyword arguments:

.. code-block:: python

   def publish_results(results, config, context, spark):
       # Publish the collected results using your chosen destination.
       return {"published": len(results), "failed": 0}

``results`` contains the collected test results. ``config`` is the provider's
configuration mapping. ``context`` includes ``pipeline_id``, ``update_id``,
``pipeline_name`` and ``terminal_state``. ``spark`` is the active Spark session.
The returned mapping's ``published`` and ``failed`` counts are logged.
Provider exceptions are caught and reported by the hook.

Generate with ``--include-tests``. Assign ``test_id`` on individual
:doc:`test actions </reference/actions/test>` to correlate results. Use
``on_violation: warn`` for tests whose metrics must reach the reporting hook;
a failing flow can stop before its metrics are recorded.

A non-mapping block or missing provider fields produces ``LHP-CFG-009`` with
a test-reporting-specific title. Diagnose using the title and details as well
as the code.
