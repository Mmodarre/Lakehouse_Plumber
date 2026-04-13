# Test Reporting Providers

Pluggable Python modules called by the LHP-generated `_test_reporting_hook.py` to
publish DQ (data quality) expectation results to external systems.

## What Are Providers?

When `test_reporting` is configured in a pipeline's `lhp.yaml`, LHP generates a
`_test_reporting_hook.py` file that:

1. Listens for `flow_progress` events during pipeline execution
2. Accumulates DQ expectation pass/fail results
3. At pipeline terminal state (`COMPLETED`, `FAILED`, `STOPPING`, `CANCELED`),
   calls the provider function with all collected results

The provider function is responsible for publishing those results to an external
system — a Delta table, Azure DevOps Test Plans, or any custom destination.

## Provider Function Contract

Every provider must expose a function with this signature:

```python
def publish_results(results, config, context, spark):
    """
    Args:
        results: list[dict] — collected DQ results, each with:
            {
                "test_id": "SIT-G01",           # or ADO Test Case ID in inline mode
                "flow_name": "customers",        # unqualified table name
                "expectation_name": "pk_not_null",
                "passed_records": 1000,
                "failed_records": 0,
                "status": "PASS",                # "PASS" or "FAIL"
                "collected_at": "2025-01-15T10:30:00+00:00"
            }

        config: dict — provider configuration (from config_file YAML or inline config)

        context: dict — pipeline execution context:
            {
                "pipeline_name": "raw_customers",
                "pipeline_id": "abc-123",
                "update_id": "def-456",
                "terminal_state": "COMPLETED"
            }

        spark: SparkSession — active Spark session from the pipeline driver

    Returns:
        dict with "published" and "failed" counts:
            {"published": 5, "failed": 1}
    """
```

## Configuration in lhp.yaml

Add a `test_reporting` section to your pipeline or flowgroup YAML:

```yaml
test_reporting:
  module_path: "providers/delta_test_reporter.py"
  function_name: "publish_results"
  config_file: "config/ado_config.yaml"   # optional — loaded and passed as config dict
  config:                                  # optional — inline config (merged with config_file)
    result_table: "my_catalog.audit.lhp_test_results"
```

## Available Providers

### Delta Provider (`delta_test_reporter.py`)

Simplest provider — appends each result as a row to a pre-existing Delta table.

**Setup:**

1. Create the target table using the DDL below
2. Configure `test_reporting` in your `lhp.yaml`
3. Run `lhp generate --env <env>`

**Configuration:**

```yaml
test_reporting:
  module_path: "providers/delta_test_reporter.py"
  function_name: "publish_results"
  config:
    result_table: "my_catalog.audit_schema.lhp_test_results"
```

| Config Key     | Default                                          | Description                         |
|----------------|--------------------------------------------------|-------------------------------------|
| `result_table` | `{catalog}.{audit_schema}.lhp_test_results`     | Fully qualified Delta table name    |
| `dry_run`      | `false`                                          | Log without writing                 |
| `log_level`    | `INFO`                                           | Logging level                       |

**Delta Table DDL:**

```sql
CREATE TABLE IF NOT EXISTS {catalog}.{audit_schema}.lhp_test_results (
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
  collected_at      TIMESTAMP   NOT NULL,
  published_at      TIMESTAMP   NOT NULL
)
USING DELTA
COMMENT 'LHP DQ test results — published by test_reporting hook'
TBLPROPERTIES (
  'delta.columnMapping.mode' = 'name',
  'delta.minReaderVersion' = '2',
  'delta.minWriterVersion' = '5'
);
```

Natural key: `(pipeline_name, update_id, test_id, expectation_name)`.
`update_id` is unique per pipeline execution — append mode gives full history.

**Important:** The table must exist before the provider runs. The provider verifies
existence with `spark.catalog.tableExists()` and fails with an actionable error if
the table is missing.

---

### ADO Provider — Config Mapping (`ado_test_reporter.py`)

Publishes results to Azure DevOps Test Plans. Uses a `test_case_mapping` in the
config YAML to translate pipeline `test_id` values (e.g., `"SIT-G01"`) into ADO
Test Case IDs (e.g., `2272983`).

**API Flow (3-4 HTTP calls total):**

1. **GET test points** for the configured suite — builds `test_case_id → point_id` map
2. **POST create Test Run** linked to the plan, with resolved `pointIds`
3. **PATCH add results** — batch array with outcome, comment, completedDate
4. **PATCH complete run** — sets state to `Completed` (auto-updates test point outcomes)

**Configuration:**

```yaml
test_reporting:
  module_path: "providers/ado_test_reporter.py"
  function_name: "publish_results"
  config_file: "config/ado_config.yaml"
```

See `sample_configs/ado_config.yaml` for the full config structure. Key sections:

| Config Section       | Purpose                                               |
|----------------------|-------------------------------------------------------|
| `ado.organization`   | ADO organization name                                 |
| `ado.project`        | ADO project name                                      |
| `ado.pat_secret_scope` / `pat_secret_key` | Databricks secret scope/key for the PAT |
| `ado.api_version`    | ADO REST API version (default: `7.1`)                 |
| `test_plan.plan_id`  | ADO Test Plan ID                                      |
| `test_plan.suite_id` | ADO Test Suite ID                                     |
| `test_case_mapping`  | `{test_id: ado_test_case_id}` mapping                 |

**Authentication:** PAT is retrieved from Databricks secrets via
`dbutils.secrets.get(scope=..., key=...)`. Never hardcode the PAT in config files.

**Unmapped results:** Results whose `test_id` has no mapping (or maps to `0`/`0000000`)
are logged as warnings and counted as `failed` in the return value.

---

### ADO Provider — Inline (`ado_test_reporter_inline.py`)

Same as the config mapping provider, but `test_id` values in the pipeline YAML
**ARE** the ADO Test Case IDs directly:

```yaml
- name: tst_pk_unique
  type: test
  test_id: "2272983"   # This IS the ADO Test Case ID
```

No `test_case_mapping` section is needed in the config. Each `result["test_id"]` is
validated as a numeric string and used directly as the ADO test case ID.

**Configuration:**

```yaml
test_reporting:
  module_path: "providers/ado_test_reporter_inline.py"
  function_name: "publish_results"
  config_file: "config/ado_config.yaml"
```

The config file is the same as the config mapping provider, minus the
`test_case_mapping` section.

**When to use which ADO provider:**

| Scenario | Provider |
|----------|----------|
| `test_id` is a logical name (`SIT-G01`) and config maps it to ADO | `ado_test_reporter.py` |
| `test_id` is the ADO Test Case ID directly (`2272983`) | `ado_test_reporter_inline.py` |

---

## Substitution Support

Provider files and config YAML files support LHP substitution tokens:

- `{catalog}`, `{audit_schema}`, `{ado_org}`, etc.
- These resolve at `lhp generate` time

**Note:** Substitution support in provider files is a separate framework enhancement.
Until that fix ships, hardcode environment-specific values or use config overrides.
The provider files are written with substitution placeholders so they will work
automatically once the framework fix lands.

## dry_run

All providers support `dry_run: true` in their config. When enabled, the provider
logs what it would publish without making any external calls (no Delta writes, no
ADO API calls). The return value is `{"published": 0, "failed": 0}`.

## Logging

All providers use Python's `logging` module. Set `log_level` in the config to
control verbosity:

```yaml
config:
  log_level: "DEBUG"   # DEBUG, INFO, WARNING, ERROR
```

Logs appear in the Databricks driver logs. Search for the provider name
(`delta_test_reporter`, `ado_test_reporter`, `ado_test_reporter_inline`) to filter.
