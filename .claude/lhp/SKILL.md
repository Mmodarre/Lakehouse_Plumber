---
name: lhp
description: "Lakehouse Plumber (LHP) project configuration and development assistant. Converts declarative YAML into Databricks Lakeflow Declarative Pipelines (formerly DLT) Python code. Use when: (1) Setting up new LHP projects (lhp init, lhp.yaml, substitutions, presets, templates), (2) Writing or editing pipeline YAML flowgroup configurations, (3) Configuring load/transform/write/test actions, (4) Creating or modifying templates and presets, (5) Setting up Databricks Asset Bundle integration, (6) Running dependency analysis or orchestration job generation, (7) Troubleshooting LHP validation or generation errors, (8) Any task involving LHP pipeline YAML files, lhp.yaml, substitutions/, templates/, presets/, or the lhp CLI."
---

# Lakehouse Plumber (LHP)

YAML-to-Python code generator for Databricks Lakeflow Declarative Pipelines.

## Before Starting

### Environment Check (run once per session)

Before any LHP work, verify the environment is ready:

1. **Check for active virtual environment**: Run `echo $VIRTUAL_ENV` (or `which python3`). If no venv is active, warn the user and suggest activating one.
2. **Check LHP version**: Run `lhp --version`. Compare against `required_lhp_version` in `lhp.yaml` if present.

If either check fails, stop and help the user fix their environment before proceeding.

### Read Project Context

Read the user's existing project files before generating new configurations:
1. `lhp.yaml` — project config, operational metadata definitions
2. `substitutions/` — environment tokens and secret scopes
3. `presets/` — reusable defaults
4. `templates/` — reusable action patterns
5. Existing `pipelines/` YAML files — match naming/structure conventions

## Core Architecture

```
Three main actions: Load, Transform, Write.
- Load: Load data from a source into a view.
- Transform: Transform data in a view into a new view.
- Write: Write data from a view into a table or sink.

One auxiliary action: Test.
- Test: For testing only in test environment using expectations.
```

- **Pipeline**: Logical grouping; generated files organized by pipeline name. All files in a pipeline run in a single Spark Declarative Pipeline.
- **FlowGroup**: One source entity; becomes one Python file. A flowgroup is a logical grouping of actions.
- **Action**: Individual operation (load, transform, write, test)

### Minimal FlowGroup

```yaml
pipeline: <pipeline_name>
flowgroup: <flowgroup_name>

actions:
  - name: <action_name>
    type: load
    readMode: stream          # stream or batch
    source:
      type: cloudfiles        # cloudfiles|delta|sql|jdbc|python|kafka|custom_datasource
      path: "{landing_volume}/folder/*.csv"
      format: csv
    target: v_raw_data

  - name: transform_data
    type: transform
    transform_type: sql       # sql|python|schema|data_quality|temp_table
    source: v_raw_data
    target: v_cleaned
    sql: |
      SELECT * FROM stream(v_raw_data)

  - name: write_table
    type: write
    source: v_cleaned
    write_target:
      type: streaming_table   # streaming_table|materialized_view
      database: "{catalog}.{schema}"
      table: "my_table"
```

### Substitution Syntax (Processing Order)

| Order | Syntax | Type |
|-------|--------|------|
| 1st | `%{var}` | Local variable (flowgroup-scoped) |
| 2nd | `{{ param }}` | Template parameter (Jinja2) |
| 3rd | `${token}` / `{token}` | Environment substitution |
| 4th | `${secret:scope/key}` | Secret -> dbutils.secrets.get() |

## Template and Flowgroup Naming Conventions

When creating reusable templates:

**Template Files:** `TMPL<number>_<source_type>_<function>.yaml`
- `<number>`: Sequential identifier (001, 002, etc.)
- `<source_type>`: Source type from the load action (delta, cloudfiles, jdbc, kafka, sql, etc.)
- `<function>`: What the template does (scd2, bronze, incremental, etc.)
- Examples:
  - `TMPL001_delta_scd2.yaml` - SCD Type 2 from Delta source
  - `TMPL002_cloudfiles_bronze.yaml` - Bronze ingestion from CloudFiles
  - `TMPL003_jdbc_incremental.yaml` - Incremental load from JDBC

**Flowgroups Using Templates:** `<domain>_<final_table>_TMPL<number>`
- `<domain>`: Business domain or subject area (billing, orders, customers, etc.)
- `<final_table>`: The final target table name
- `TMPL<number>`: Template number being used (must match template file)
- Examples:
  - `billing_invoice_TMPL001` - Uses TMPL001 for invoice table
  - `orders_customer_TMPL002` - Uses TMPL002 for customer table
  - `analytics_fact_sales_TMPL003` - Uses TMPL003 for fact_sales table

## Quick Reference: Action Types

| Action | Sub-types | Key |
|--------|-----------|-----|
| **Load** | cloudfiles, delta, sql, jdbc, python, kafka, custom_datasource | See [actions-load.md](references/actions-load.md) |
| **Transform** | sql, python, schema, data_quality, temp_table | `stream(view)` required for streaming SQL |
| **Write** | streaming_table, materialized_view, sink (delta/kafka/custom) | CDC/SCD via `mode: cdc` + `cdc_config` |
| **Test** | row_count, uniqueness, referential_integrity, completeness, range, schema_match, all_lookups_found, custom_sql, custom_expectations | `--include-tests` flag needed |
| **Monitoring** | event_log, monitoring in lhp.yaml | See [monitoring.md](references/monitoring.md) |

## Key Rules

1. **`stream(view_name)`** required in SQL transforms reading from streaming sources
2. **CloudFiles `_metadata.*` columns** only available in views, not downstream transforms
3. **Preset lists are replaced**, not merged; nested dicts are deep-merged
4. **All-or-nothing job_name**: if any flowgroup has `job_name`, all must have it
5. **Never put secrets in YAML values** — always use `${secret:scope/key}`
6. **Validate before generating**: `lhp validate --env <env>`
7. **`readMode: stream`** -> `spark.readStream`, **`batch`** -> `spark.read`
8. **Monitoring requires event_log** — `monitoring: {}` won't work without `event_log` section

## CLI Quick Reference

```bash
lhp init <project> [--bundle]           # Scaffold project
lhp validate --env <env>                # Validate configs
lhp generate --env <env>                # Generate Python code if config change detected
lhp generate --env <env> --force        # Force regeneration of all flowgroups
lhp generate --env <env> --include-tests --force  # With tests, forced
lhp state --env <env>                  # Show current state of the project
lhp deps --format job --job-name <name> --bundle-output  # Orchestration job
```

## Reference Files

Load these based on the user's task:

- **[actions-load.md](references/actions-load.md)** — Load action types (cloudfiles, delta, sql, jdbc, kafka, python, custom_datasource) with all fields. Load when writing/debugging load configurations.
- **[actions-transform.md](references/actions-transform.md)** — Transform types (sql, python, data_quality, schema, temp_table). Load when writing/debugging transforms.
- **[actions-write.md](references/actions-write.md)** — Write targets (streaming_table, materialized_view), CDC modes, sinks (delta, kafka, custom, foreachbatch). Load when configuring writes or sinks.
- **[actions-test.md](references/actions-test.md)** — All 9 test types with field references. Load when adding data quality tests.
- **[cdc-patterns.md](references/cdc-patterns.md)** — CDC and SCD2 patterns for Delta CDF, PostgreSQL WAL, and snapshot CDC. Load when implementing any CDC/SCD2 pattern.
- **[templates-presets.md](references/templates-presets.md)** — Template structure, naming conventions, parameter types, preset matching/merge behavior. Load when creating or editing templates or presets.
- **[project-config.md](references/project-config.md)** — lhp.yaml, substitutions, local variables, operational metadata, CLI commands, multi-flowgroup syntax. Load for project setup or config questions.
- **[advanced.md](references/advanced.md)** — Databricks bundles, pipeline/job configuration, dependency analysis, multi-job orchestration, CI/CD patterns. Load for deployment or orchestration tasks.
- **[monitoring.md](references/monitoring.md)** — Event log injection, monitoring pipeline, materialized views, `__eventlog_monitoring` alias. Load when configuring event_log or monitoring in lhp.yaml.
- **[errors.md](references/errors.md)** — All LHP error codes (LHP-CFG/VAL/IO/ACT/DEP) with causes and fixes. Load when troubleshooting any LHP error.

## Instructions

1. **Read project files first** — match existing patterns and conventions
2. **Validate substitution tokens** — ensure `${token}` has corresponding entry in substitution files
3. **Use templates when available** — check if a template already exists for the pattern
4. **Apply presets** — reference project presets where appropriate
5. **Generate valid YAML** — proper indentation, correct field nesting
6. **Explain behavior** — describe what the generated YAML will produce in Python/Spark Declarative Pipelines
7. **Suggest validation** — recommend `lhp validate --env dev` after changes
8. **For CDC/SCD2 patterns**:
   - Exclude CDC metadata columns (`__START_AT`, `__END_AT`) using `* except` in transforms
   - Use business timestamps (modified_at, created_at) for `sequence_by`
   - Add technical columns to `except_column_list` in cdc_config
   - Apply `operational_metadata` at action level, not write level
   - Use `* except` pattern for future-proof column selection and schema evolution
9. **For Kafka sources** — remind that key/value are binary, need deserialization transform
10. **For operational_metadata**:
    - Apply at action level (load, transform), not write level
    - For file sources: include file metadata (`_source_file_path`, `_source_file_name`, `_processing_timestamp`)
    - For non-file sources: For example `_processing_timestamp`
11. **For creating templates**:
    - Follow naming convention: `TMPL<number>_<source_type>_<function>.yaml`
    - Define clear, required parameters with descriptions
    - Use Jinja2 syntax for parameter substitution: `{{ param_name }}`
    - Quote array and string parameters in YAML: `keys: "{{ natural_keys }}"`
    - Provide defaults for optional parameters
    - Document the template purpose, parameters, and usage examples in templates/README.md
    - Test templates by creating example flowgroups before finalizing
12. **For using templates in flowgroups**:
    - Name flowgroup: `<domain>_<final_table>_TMPL<number>`
    - Reference template with `use_template: TMPL<number>_<source_type>_<function>`
    - Provide all required parameters under `template_parameters:`
    - Use natural YAML for objects and arrays (not JSON strings)
    - Keep optional parameters only if overriding defaults
    - If multiple flowgroups use the same template. use multi-flowgroup syntax.
13. **For error troubleshooting**: Load errors.md, find the error code, follow resolution. Always suggest `lhp validate --env <env> --verbose`.
14. **For monitoring/event log setup**: Load monitoring.md. Require `event_log` before `monitoring`. Use `__eventlog_monitoring` alias in pipeline_config.yaml.
15. **Not all fields are required**: When showing YAML examples, annotate fields as required/optional. Only required fields must be present; optional fields have sensible defaults.
