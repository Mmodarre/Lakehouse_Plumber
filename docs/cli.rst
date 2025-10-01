CLI Reference
=============

The **Lakehouse Plumber** command-line interface provides project creation,
validation, code generation, state inspection and more.  All commands are
implemented with `click <https://click.palletsprojects.com>`_ so you can use the
usual ``--help`` flags.

.. click:: lhp.cli.main:cli
   :prog: lhp
   :nested: full

.. seealso::

   For detailed information about dependency analysis and orchestration job generation, see :doc:`dependency_analysis`.

Test Generation Workflow
========================

By default, Lakehouse Plumber skips test actions during code generation for faster builds and cleaner production pipelines. Use the ``--include-tests`` flag to include data quality tests when needed.

**Common Usage Patterns:**

.. code-block:: bash

   # Production builds (default) - fast, clean pipelines
   lhp generate -e prod
   lhp generate -e dev

   # Development with data quality testing
   lhp generate -e dev --include-tests

   # Validation always checks tests (regardless of generation)
   lhp validate -e dev

**Flag Behavior:**

- **Without flag**: Test actions are validated but not generated, test-only flowgroups are skipped entirely
- **With flag**: All test actions are included in generated code as temporary DLT tables
- **Validation**: Always processes test actions to catch configuration errors early

**Examples:**

.. code-block:: bash

   # Skip tests for faster CI/CD builds
   lhp generate -e prod --force --dry-run

   # Include tests for comprehensive validation
   lhp generate -e dev --include-tests --force

   # Preview test generation without writing files
   lhp generate -e dev --include-tests --dry-run

.. note::
   Test actions generate temporary DLT tables that persist test results for inspection and debugging while being automatically cleaned up when the pipeline completes.