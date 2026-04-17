Test Result Reporting (Publishing)
====================================

.. meta::
   :description: Publish DQ test results from Lakehouse Plumber pipelines to external systems like Azure DevOps, Delta audit tables, or custom providers.

Test result reporting extends :doc:`test_actions` by **publishing DQ expectation
results to external systems** after each pipeline run. Lakehouse Plumber generates
a ``@dp.on_event_hook()`` per pipeline that listens for DQ metrics in the Databricks
event stream, accumulates pass/fail results, and calls a user-supplied **provider
function** at pipeline completion.

The design is pluggable: LHP generates the hook and wiring; you supply the provider
module that decides *where* results go — Azure DevOps Test Plans, a Delta audit table,
a REST API, or anything reachable from the Databricks driver.

.. note::
   **Prerequisites:**

   * ``test_reporting`` section configured in ``lhp.yaml``
   * ``test_id`` set on each test action you want to report
   * ``--include-tests`` flag passed to ``lhp generate``


Architecture
--------------------------------------------

The generated hook runs inside the Databricks pipeline process on the driver node:

.. mermaid::

   flowchart LR
       A["Pipeline Execution"] --> B["flow_progress events<br/>(DQ expectations)"]
       B --> C["Hook accumulates<br/>results per test_id"]
       C --> D{"Terminal state?<br/>COMPLETED / FAILED<br/>STOPPING / CANCELED"}
       D -->|Yes| E["Provider function<br/>publish_results()"]
       E --> F["External System<br/>(ADO / Delta / Custom)"]

1. As each test action's temporary table materializes, Databricks emits
   ``flow_progress`` events containing expectation pass/fail counts.
2. The generated hook filters events to only those tables mapped via ``test_id``,
   building an in-memory results list.
3. When the pipeline reaches a terminal state, the hook calls your provider function
   exactly once with all accumulated results.


Quick Start
--------------------------------------------

**Step 1 — Add** ``test_reporting`` **to** ``lhp.yaml``

.. code-block:: yaml

   # lhp.yaml
   test_reporting:
     module_path: py_functions/test_reporting_publisher.py
     function_name: publish_results

**Step 2 — Add** ``test_id`` **to test actions**

.. code-block:: yaml

   # pipelines/02_bronze/tst_customer_dq.yaml
   actions:
     - name: tst_customer_pk_uniqueness
       type: test
       test_type: uniqueness
       source: v_customer_bronze_DQE
       columns: [customer_id]
       on_violation: warn
       test_id: "SIT-G01"

**Step 3 — Create a provider module**

.. code-block:: python

   # py_functions/test_reporting_publisher.py
   def publish_results(results, config, context, spark):
       for r in results:
           print(f"[{r['test_id']}] {r['status']}: {r['expectation_name']}")
       return {"published": len(results), "failed": 0}

**Step 4 — Generate with** ``--include-tests``

.. code-block:: bash

   lhp generate --env dev --include-tests

**Step 5 — Inspect the generated output**

.. code-block:: text

   generated/dev/my_pipeline/
   ├── _test_reporting_hook.py          # Event hook (generated)
   ├── test_reporting_providers/
   │   ├── __init__.py
   │   └── test_reporting_publisher.py  # Your provider module (copied)
   └── tst_customer_dq.py              # Test action code


Configuration Reference
--------------------------------------------

``lhp.yaml`` test_reporting Section
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

.. list-table::
   :header-rows: 1
   :widths: 25 15 60

   * - Field
     - Required
     - Description
   * - ``module_path``
     - Yes
     - Path to the provider Python module, relative to project root.
   * - ``function_name``
     - Yes
     - Name of the callable inside the module (e.g., ``publish_results``).
   * - ``config_file``
     - No
     - Path to a YAML file loaded as a dict and passed to the provider as ``config``.
       Useful for connection strings, API endpoints, or test-case mappings.

``test_id`` Field on Test Actions
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

``test_id`` is an **opt-in** field on any test action. Only actions with ``test_id``
set are included in reporting — this lets you control exactly which tests publish
results while keeping other tests as internal validation.

.. code-block:: yaml
   :caption: Actions with and without test_id

   actions:
     - name: tst_customer_pk_uniqueness
       type: test
       test_type: uniqueness
       source: v_customer_bronze_DQE
       columns: [customer_id]
       on_violation: warn
       test_id: "SIT-G01"          # ← reported to external system

     - name: tst_customer_balance_range
       type: test
       test_type: range
       source: v_customer_bronze_DQE
       column: account_balance
       min_value: -1000
       max_value: 10000
       on_violation: warn
       # No test_id — runs normally but is NOT reported

**Table name mapping rules:**

The hook maps each test action's materialized table name to its ``test_id``. The table
name used depends on whether the action has an explicit ``target``:

* **Explicit target** — uses the ``target`` value directly (e.g., ``target: my_custom_name``).
* **No target** — defaults to ``tmp_test_<action_name>``
  (e.g., action ``tst_customer_completeness`` → ``tmp_test_tst_customer_completeness``).


.. _provider-interface:

Provider Interface
--------------------------------------------

Function Contract
~~~~~~~~~~~~~~~~~~

The provider function is called once per pipeline run with this signature:

.. code-block:: python

   def publish_results(results, config, context, spark):
       """
       Args:
           results: list[dict]  — accumulated DQ results
           config:  dict        — from config_file (or empty dict)
           context: dict        — pipeline execution context
           spark:   SparkSession

       Returns:
           dict with "published" (int) and "failed" (int) counts
       """

**results** — Each entry is a dict with these keys:

.. code-block:: python

   {
       "test_id": "SIT-G01",                         # From test action YAML
       "flow_name": "tst_customer_pk_uniqueness",     # Unqualified table name
       "expectation_name": "uniqueness_check",        # DQ expectation name
       "passed_records": 1000,                        # Records passing
       "failed_records": 0,                           # Records failing
       "status": "PASS",                              # "PASS" if failed == 0, else "FAIL"
       "collected_at": "2026-01-15T10:30:00+00:00",   # ISO 8601 UTC timestamp
   }

**config** — The parsed content of ``config_file`` if specified, otherwise ``{}``.

**context** — Pipeline execution metadata:

.. code-block:: python

   {
       "pipeline_id": "abc123-...",
       "update_id": "def456-...",
       "pipeline_name": "acmi_edw_bronze",
       "terminal_state": "COMPLETED",   # or FAILED, STOPPING, CANCELED
   }

**Return value** — A dict with counts:

.. code-block:: python

   {"published": 5, "failed": 0}

See `Writing a Custom Provider`_ below for a skeleton, error handling rules, and
how substitution tokens work in provider modules.


Generated Output
--------------------------------------------

When ``test_reporting`` is configured and at least one test action has ``test_id``,
``lhp generate --include-tests`` produces these additional files per pipeline:

.. code-block:: text

   generated/<env>/<pipeline>/
   ├── _test_reporting_hook.py            # Generated event hook
   └── test_reporting_providers/
       ├── __init__.py                    # Package init
       └── <provider_module>.py           # Copy of your provider module

The hook file is fully generated from a template — **do not edit it**. Key sections:

.. code-block:: python
   :caption: _test_reporting_hook.py (annotated excerpt)

   from pyspark import pipelines as dp

   # Maps unqualified table names to external test IDs
   _TEST_ID_MAP = {
       "tst_customer_pk_uniqueness": "SIT-G01",
       "tmp_test_tst_customer_completeness": "SIT-G02",
   }

   _PROVIDER_CONFIG = {}          # Populated from config_file if set
   _collected_results = []        # Accumulates results across events
   _TERMINAL_STATES = frozenset({"STOPPING", "FAILED", "CANCELED", "COMPLETED"})

   @dp.on_event_hook(max_allowable_consecutive_failures=5)
   def test_reporting_hook(event):
       """Listens to pipeline events, collects DQ results, publishes at end."""
       ...

The hook only processes events for tables present in ``_TEST_ID_MAP`` — other test
actions and pipeline tables are ignored.


Validation
--------------------------------------------

``lhp validate`` performs test reporting checks when the ``test_reporting`` section
exists in ``lhp.yaml``:

**Always checked:**

* ``module_path`` — the provider module file must exist at the specified path.
* ``config_file`` — if specified, the file must exist.

**With** ``--include-tests``:

* At least one test action across the validated pipelines must have ``test_id`` set.
  A configuration with no ``test_id`` on any action triggers a validation error.

.. code-block:: bash

   # Basic validation (checks file existence)
   lhp validate --env dev

   # Extended validation (also checks test_id presence)
   lhp validate --env dev --include-tests


Built-in Providers
--------------------------------------------

LHP ships three ready-to-use provider modules in the ``providers/`` directory of the
repository. To use one, **copy it into your LHP project** (e.g., into ``py_functions/``)
and reference it in ``lhp.yaml``.

.. list-table::
   :header-rows: 1
   :widths: 30 70

   * - Provider
     - Use Case
   * - ``delta_test_reporter.py``
     - Write results to a Delta audit table. Recommended default — always have a
       queryable history of test outcomes.
   * - ``ado_test_reporter.py``
     - Publish to ADO Test Plans using a **config mapping** file that translates
       friendly ``test_id`` labels (e.g., ``"SIT-G01"``) to ADO Test Case IDs.
   * - ``ado_test_reporter_inline.py``
     - Publish to ADO Test Plans where ``test_id`` values in the YAML **are the ADO
       Test Case IDs directly** (e.g., ``test_id: "2272983"``). No mapping file needed.


Delta Audit Table (Recommended Default)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For teams that want a persistent record of all DQ results without an external test
management system. This is the **recommended starting point**.

**1. Create the audit table** (run once in your catalog):

.. code-block:: sql
   :force:

   CREATE TABLE IF NOT EXISTS <catalog>.<schema>.lhp_test_results (
       pipeline_name     STRING      NOT NULL,
       pipeline_id       STRING      NOT NULL,
       update_id         STRING      NOT NULL,
       test_id           STRING      NOT NULL,
       flow_name         STRING      NOT NULL,
       expectation_name  STRING      NOT NULL,
       passed_records    BIGINT      NOT NULL,
       failed_records    BIGINT      NOT NULL,
       status            STRING      NOT NULL,
       terminal_state    STRING      NOT NULL,
       collected_at      STRING      NOT NULL,
       published_at      STRING      NOT NULL
   );

**2. Copy the provider and configure** ``lhp.yaml``:

.. code-block:: bash

   cp providers/delta_test_reporter.py py_functions/

.. code-block:: yaml

   # lhp.yaml
   test_reporting:
     module_path: py_functions/delta_test_reporter.py
     function_name: publish_results

The provider uses ``${catalog}.${audit_schema}.lhp_test_results`` as the default table
name. The ``${catalog}`` and ``${audit_schema}`` tokens are resolved by LHP's
:doc:`/substitutions` system at generate time, so the same module works across
environments.

The provider also supports ``dry_run`` and ``log_level`` options via ``config_file``,
and verifies the target table exists before writing.


Azure DevOps — Config Mapping (``ado_test_reporter.py``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For enterprise teams using ADO Test Plans where ``test_id`` values in flowgroup YAML
are **friendly labels** (e.g., ``"SIT-G01"``) translated to ADO Test Case IDs via a
config mapping file.

**1. Copy the provider:**

.. code-block:: bash

   cp providers/ado_test_reporter.py py_functions/

**2. Create a config file** (``config/ado_config.yaml``):

.. code-block:: yaml

   ado:
     organization: my-org
     project: my-project
     pat_secret_scope: my-scope        # Databricks secret scope
     pat_secret_key: ado-pat           # Key within that scope
     api_version: "7.1"                # Optional, defaults to 7.1

   test_plan:
     plan_id: 12345
     suite_id: 67890

   # Translates friendly test_id → ADO Test Case ID
   test_case_mapping:
     SIT-G01: 2272983
     SIT-G02: 2272984
     SIT-G03: 2272985

**3. Configure** ``lhp.yaml``:

.. code-block:: yaml

   test_reporting:
     module_path: py_functions/ado_test_reporter.py
     function_name: publish_results
     config_file: config/ado_config.yaml

**4. Use friendly labels in flowgroup YAML:**

.. code-block:: yaml

   actions:
     - name: tst_billing_pk_check
       type: test
       test_type: uniqueness
       source: v_billing_bronze
       columns: [transaction_id]
       on_violation: warn
       test_id: "SIT-G01"          # Mapped to ADO Test Case 2272983

The provider creates one ADO Test Run per pipeline execution, batches all results, and
completes the run (4 API calls total). PAT authentication uses Databricks secrets —
no credentials in config files.


Azure DevOps — Inline (``ado_test_reporter_inline.py``)
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

For teams that prefer to put the **ADO Test Case ID directly** as the ``test_id``
value — no mapping file needed. Simpler setup, but less readable YAML.

**1. Copy the provider:**

.. code-block:: bash

   cp providers/ado_test_reporter_inline.py py_functions/

**2. Create a config file** (``config/ado_config.yaml``) — same as above but
**without** ``test_case_mapping``:

.. code-block:: yaml

   ado:
     organization: my-org
     project: my-project
     pat_secret_scope: my-scope
     pat_secret_key: ado-pat

   test_plan:
     plan_id: 12345
     suite_id: 67890

**3. Configure** ``lhp.yaml``:

.. code-block:: yaml

   test_reporting:
     module_path: py_functions/ado_test_reporter_inline.py
     function_name: publish_results
     config_file: config/ado_config.yaml

**4. Use ADO Test Case IDs directly in flowgroup YAML:**

.. code-block:: yaml

   actions:
     - name: tst_billing_pk_check
       type: test
       test_type: uniqueness
       source: v_billing_bronze
       columns: [transaction_id]
       on_violation: warn
       test_id: "2272983"           # This IS the ADO Test Case ID

.. tip::
   **Which ADO variant to choose?**

   * Use **config mapping** (``ado_test_reporter.py``) when you want readable,
     project-specific test IDs in your YAML and a centralized mapping file.
   * Use **inline** (``ado_test_reporter_inline.py``) when you want the simplest
     setup and don't mind numeric ADO IDs in your YAML.


Writing a Custom Provider
~~~~~~~~~~~~~~~~~~~~~~~~~~

If the built-in providers don't fit your needs, write your own. Your module must
define a function matching the :ref:`provider interface contract <provider-interface>`:

.. code-block:: python

   def publish_results(results, config, context, spark):
       """
       Args:
           results: list[dict] — one dict per DQ expectation, with keys:
               test_id, flow_name, expectation_name,
               passed_records, failed_records, status, collected_at
           config:  dict — parsed from config_file (empty dict if not set)
           context: dict — pipeline_id, update_id, pipeline_name, terminal_state
           spark:   SparkSession

       Returns:
           dict with "published" (int) and "failed" (int) counts
       """
       published, failed = 0, 0
       for result in results:
           try:
               # Your logic here — API call, Delta write, webhook, etc.
               published += 1
           except Exception:
               failed += 1
       return {"published": published, "failed": failed}

Key rules:

* The function name must match ``function_name`` in ``lhp.yaml``.
* **Return** ``{"published": N, "failed": M}`` — the hook logs these counts.
* **Fatal errors** — raise an exception. The hook catches it and prints a diagnostic;
  the pipeline itself is not affected.
* **Partial failures** — return the counts (e.g., ``{"published": 3, "failed": 2}``).
* The module is **copied** into ``test_reporting_providers/`` in the generated output.
  :doc:`/substitutions` tokens (``${catalog}``, ``${env_token}``, etc.) in the source
  are resolved at generate time.

The built-in providers in ``providers/`` are good reference implementations showing
error handling, dry-run support, table existence checks, and logging patterns.


Important Considerations
--------------------------------------------

.. warning::
   **on_violation: fail and expect_or_fail** — When a test action uses
   ``on_violation: fail``, the generated expectation uses ``@dp.expect_all_or_fail``.
   If the expectation fails, the pipeline aborts the flow *before* recording metrics
   in the event log. This means the hook **will not receive** DQ results for failed
   ``fail``-mode expectations. Use ``on_violation: warn`` for test actions whose results
   you want to report.

.. note::
   **Event hooks are Public Preview** — The ``@dp.on_event_hook()`` decorator was
   introduced in Databricks in January 2026 and is currently in Public Preview.
   Refer to Databricks documentation for the latest status.

.. note::
   **Best-effort delivery** — Databricks documentation notes approximately a 10% event
   miss rate for event hooks. The hook is best-effort, not guaranteed delivery. For
   critical audit requirements, consider supplementing with a post-run query against the
   pipeline event log.

* **Substitution tokens in provider modules** are resolved at generate time, not at
  runtime. Tokens like ``${catalog}`` or ``${secret:scope/key}`` in your provider source
  code are replaced when ``lhp generate`` runs.

* **One hook per pipeline** — LHP generates a single ``_test_reporting_hook.py`` per
  pipeline. If a pipeline has test actions across multiple flowgroups, all ``test_id``
  mappings are merged into one hook.


Related Documentation
--------------------------------------------

* :doc:`test_actions` — test action types, configuration, and best practices
* :doc:`/monitoring` — centralized pipeline event log monitoring
* :doc:`/cli` — command-line reference (``--include-tests`` flag)
* :doc:`/substitutions` — environment tokens and secret references in provider modules
* :doc:`/errors_reference` — error codes and resolution steps
