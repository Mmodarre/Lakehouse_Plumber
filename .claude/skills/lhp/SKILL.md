---
name: lhp
description: "Lakehouse Plumber (LHP): the tool that compiles declarative YAML into Databricks Lakeflow/DLT Python. Use whenever the user is working inside an LHP project. Dead-giveaway signals: the word `flowgroup`; the `lhp` CLI (`init`/`validate`/`generate`/`dag`/`diff`); or LHP files (`lhp.yaml`, `substitutions/`, `presets/`, `templates/`, `blueprints/`, `pipelines/*.yaml`). Covers scaffolding a new LHP project; authoring or fixing flowgroup load/transform/write/test actions; regenerating Python after editing YAML and checking the diff; reusing one pattern across regions/tenants/sites via templates or blueprints; setting up substitutions or Asset Bundle integration; and debugging any LHP validate/generate failure (unresolved `${token}`, non-streaming source) including the SQL or YAML behind the generated Python. Fire even when the user only says `flowgroup` or `lhp` with little else. Skip for hand-written DLT/Spark notebooks, generic SQL, Genie, or CI/CD with no LHP project involved."
---

# Lakehouse Plumber (LHP)

YAML-to-Python code generator for Databricks Lakeflow Declarative Pipelines.

## Before You Start (read this first)

- **Reuse before authoring.** On any "create a flowgroup / pipeline / ingestion" request, scan `templates/` and `blueprints/` **first**. If an existing template (one parameterised flowgroup) or blueprint (a whole-flowgroup pattern repeated per site/region/tenant) already fits, propose reusing it via `use_template:` / `use_blueprint:` and confirm with the user *before* hand-writing new actions. Only author fresh YAML when nothing fits.
- **Write-target rule of thumb:** incremental/append/streaming ingest → `streaming_table`; full recompute, joins, aggregations, or dimensional rebuilds → `materialized_view`.
- **New project from scratch?** Load [quickstart.md](references/quickstart.md) for the `init → lhp.yaml/substitutions/pipeline_config → validate → generate` path.
- **Common mistakes to avoid:**
  - Deprecated `{token}` braces — always `${token}` (only `%{local_var}` uses non-`$` braces).
  - Missing `stream(view)` wrapper in a SQL transform that reads a streaming source.
  - Secrets inline in YAML — always `${secret:scope/key}`.
  - `operational_metadata` at the write level — apply it at the load/transform (action) level.
  - Suggesting `lhp show` (removed) or `lhp deps` / `--force` (deprecated). Use `lhp dag`, `lhp diff`; every `generate` is a full regenerate.

### Read Project Context

Read the user's existing project files before generating new configurations:
1. `lhp.yaml` — project config, operational metadata definitions
2. `substitutions/` — environment tokens and secret scopes
3. `presets/` — reusable defaults (action-type config)
4. `templates/` — reusable action patterns (one parameterised flowgroup)
5. `blueprints/` — reusable whole-flowgroup patterns, instantiated per site/region/tenant
6. Existing `pipelines/` YAML files — match naming/structure conventions

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
      path: "${landing_volume}/folder/*.csv"
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
      database: "${catalog}.${schema}"
      table: "my_table"
```

### Substitution Syntax (Processing Order)

| Order | Syntax | Type |
|-------|--------|------|
| 1st | `%{var}` | Local variable (flowgroup-scoped) |
| 2nd | `{{ param }}` | Template parameter (Jinja2) |
| 3rd | `${token}` | Environment substitution (bare `{token}` is **deprecated**, `LHP-DEPR-001`) |
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

Each sub-type has its own leaf reference file. Load only the one(s) you need.

| Action | Sub-type | Reference |
|--------|----------|-----------|
| **Load** | cloudfiles | [actions-load-cloudfiles.md](references/actions-load-cloudfiles.md) |
| **Load** | delta | [actions-load-delta.md](references/actions-load-delta.md) |
| **Load** | sql | [actions-load-sql.md](references/actions-load-sql.md) |
| **Load** | python | [actions-load-python.md](references/actions-load-python.md) |
| **Load** | jdbc | [actions-load-jdbc.md](references/actions-load-jdbc.md) |
| **Load** | custom_datasource | [actions-load-custom-datasource.md](references/actions-load-custom-datasource.md) |
| **Load** | kafka | [actions-load-kafka.md](references/actions-load-kafka.md) |
| **Transform** | sql (`stream(view)` for streaming) | [actions-transform-sql.md](references/actions-transform-sql.md) |
| **Transform** | python | [actions-transform-python.md](references/actions-transform-python.md) |
| **Transform** | data_quality | [actions-transform-data-quality.md](references/actions-transform-data-quality.md) |
| **Transform** | temp_table | [actions-transform-temp-table.md](references/actions-transform-temp-table.md) |
| **Transform** | schema | [actions-transform-schema.md](references/actions-transform-schema.md) |
| **Write** | streaming_table (standard) | [actions-write-streaming-table-standard.md](references/actions-write-streaming-table-standard.md) |
| **Write** | streaming_table `mode: cdc` (`cdc_config`) | [actions-write-streaming-table-cdc.md](references/actions-write-streaming-table-cdc.md) |
| **Write** | streaming_table `mode: snapshot_cdc` | [actions-write-streaming-table-snapshot-cdc.md](references/actions-write-streaming-table-snapshot-cdc.md) |
| **Write** | materialized_view | [actions-write-materialized-view.md](references/actions-write-materialized-view.md) |
| **Write** | sink (delta) | [actions-write-sink-delta.md](references/actions-write-sink-delta.md) |
| **Write** | sink (kafka) | [actions-write-sink-kafka.md](references/actions-write-sink-kafka.md) |
| **Write** | sink (Azure Event Hubs, via Kafka) | [actions-write-sink-eventhubs.md](references/actions-write-sink-eventhubs.md) |
| **Write** | sink (custom) | [actions-write-sink-custom.md](references/actions-write-sink-custom.md) |
| **Write** | sink (foreachbatch) | [actions-write-sink-foreachbatch.md](references/actions-write-sink-foreachbatch.md) |
| **Test** | row_count (`--include-tests` needed) | [actions-test-row-count.md](references/actions-test-row-count.md) |
| **Test** | uniqueness | [actions-test-uniqueness.md](references/actions-test-uniqueness.md) |
| **Test** | referential_integrity | [actions-test-referential-integrity.md](references/actions-test-referential-integrity.md) |
| **Test** | completeness | [actions-test-completeness.md](references/actions-test-completeness.md) |
| **Test** | range | [actions-test-range.md](references/actions-test-range.md) |
| **Test** | schema_match | [actions-test-schema-match.md](references/actions-test-schema-match.md) |
| **Test** | all_lookups_found | [actions-test-all-lookups-found.md](references/actions-test-all-lookups-found.md) |
| **Test** | custom_sql | [actions-test-custom-sql.md](references/actions-test-custom-sql.md) |
| **Test** | custom_expectations | [actions-test-custom-expectations.md](references/actions-test-custom-expectations.md) |
| **Monitoring** | event_log, monitoring in lhp.yaml | [monitoring.md](references/monitoring.md) |

## Key Rules

1. **`stream(view_name)`** required in SQL transforms reading from streaming sources
2. **CloudFiles `_metadata.*` columns** only available in views, not downstream transforms
3. **Preset lists are replaced**, not merged; nested dicts are deep-merged
4. **All-or-nothing job_name**: if any flowgroup has `job_name`, all must have it
5. **Never put secrets in YAML values** — always use `${secret:scope/key}`
6. **Validate before generating**: `lhp validate --env <env>`
7. **`readMode: stream`** -> `spark.readStream`, **`batch`** -> `spark.read`
8. **Monitoring requires event_log** — `monitoring: {}` won't work without `event_log` section
9. **`catalog` and `schema` are REQUIRED in `pipeline_config.yaml`** — set them per-pipeline or in a top-level `project_defaults` block. Missing either fails `lhp generate` with `BundleResourceError`. See [project-config.md](references/project-config.md) and `docs/how-to/configure-catalog-and-schema.rst`.
10. **`resources/lhp/` is exclusively managed by LHP** — every `lhp generate` wipes it and rewrites it. Place custom resource YAMLs (hand-written jobs, dashboards, secret scopes) under `resources/` at the top level or any non-`lhp` subdirectory.
11. **Every `lhp generate` is a full regenerate** — there is no incremental mode and no `--force` flag (it was removed and is a no-op). Never suggest `--force`.

## Best Practice Defaults (apply unless the user overrides)

These defaults reflect LHP's published enterprise best practices. Load [best-practices.md](references/best-practices.md) for full rules and rationale.

1. **Medallion defaults by layer:**
   - Bronze → `streaming_table` + DQE `warn` + file metadata
   - Silver → `materialized_view` + DQE `drop` + `updated_at`
   - Gold → `materialized_view` + DQE `fail` on critical invariants
2. **CloudFiles bronze must set** `cloudFiles.schemaEvolutionMode: rescue` and `cloudFiles.rescuedDataColumn: _rescued_data`. Silent data loss otherwise.
3. **Default transforms to SQL** for silver/gold. Reserve Python for UDFs/ML/procedural logic only. Externalize SQL > ~5 lines into `sql/<system>/<layer>/<name>.sql`.
4. **Prefer `cluster_columns`** (liquid clustering) over `partition_columns` on write targets.
5. **Every write target needs a `comment`** (Unity Catalog description) and a `description` on every action (generated-code comment).
6. **Keep each YAML file 50–200 lines**, one pipeline per file, grouped by business domain (`pipelines/<system>/<layer>/`).
7. **Extract a template only after 3+ flowgroups share the pattern.** Write concrete flowgroups first.
8. **Cap presets at ~15–20 files.** Use `extends` for hierarchy (`global_defaults` → `<layer>_standard` → domain-specific).
9. **`%{var}` is flowgroup-local; `${TOKEN}` is environment.** Never put environment values in `variables:`.
10. **Treat preset edits as high-blast-radius.** Run full-project `lhp validate` before merging preset changes.
11. **Templates/presets are flat** — no subdirectory discovery. Use prefix naming (`TMPLxxx_<layer>_<action>_<type>`, `<scope>_<layer>_<purpose>`).

## CLI Quick Reference

```bash
lhp init <project> [--no-bundle]        # Scaffold project (Asset Bundle ON by default)
lhp validate --env <env>                # Validate configs
lhp generate --env <env>                # Generate Python code (always a FULL regenerate)
lhp generate --env <env> --include-tests  # With test actions included
lhp generate --env <env> --sandbox   # Personal namespaced sandbox (scope from .lhp/profile.yaml)
lhp diff --env <env>                    # Show what generate would change on disk
lhp dag --format job --job-name <name> --bundle-output  # Orchestration job
lhp list templates | presets | blueprints   # List reusable artifacts
```

## Reference Files

Load these based on the user's task:

Action references are split per sub-type — one leaf file per action sub-type. Load only the leaf for the sub-type you are writing or debugging. The full set is enumerated in the **Quick Reference: Action Types** table above. By category:

- **Load** — [cloudfiles](references/actions-load-cloudfiles.md), [delta](references/actions-load-delta.md), [sql](references/actions-load-sql.md), [python](references/actions-load-python.md), [jdbc](references/actions-load-jdbc.md), [custom_datasource](references/actions-load-custom-datasource.md), [kafka](references/actions-load-kafka.md).
- **Transform** — [sql](references/actions-transform-sql.md), [python](references/actions-transform-python.md), [data_quality](references/actions-transform-data-quality.md), [temp_table](references/actions-transform-temp-table.md), [schema](references/actions-transform-schema.md).
- **Write** — [streaming_table standard](references/actions-write-streaming-table-standard.md), [streaming_table cdc](references/actions-write-streaming-table-cdc.md), [streaming_table snapshot_cdc](references/actions-write-streaming-table-snapshot-cdc.md), [materialized_view](references/actions-write-materialized-view.md), [sink delta](references/actions-write-sink-delta.md), [sink kafka](references/actions-write-sink-kafka.md), [sink eventhubs](references/actions-write-sink-eventhubs.md), [sink custom](references/actions-write-sink-custom.md), [sink foreachbatch](references/actions-write-sink-foreachbatch.md).
- **Test** — [row_count](references/actions-test-row-count.md), [uniqueness](references/actions-test-uniqueness.md), [referential_integrity](references/actions-test-referential-integrity.md), [completeness](references/actions-test-completeness.md), [range](references/actions-test-range.md), [schema_match](references/actions-test-schema-match.md), [all_lookups_found](references/actions-test-all-lookups-found.md), [custom_sql](references/actions-test-custom-sql.md), [custom_expectations](references/actions-test-custom-expectations.md). All 9 require the `--include-tests` flag.
- **[cdc-patterns.md](references/cdc-patterns.md)** — CDC and SCD2 patterns for Delta CDF, PostgreSQL WAL, and snapshot CDC. Load when implementing any CDC/SCD2 pattern.
- **[templates-presets.md](references/templates-presets.md)** — Template structure, naming conventions, parameter types (incl. inline SQL parametrized with Jinja), preset matching/merge behavior. Load when creating or editing templates or presets.
- **[blueprints.md](references/blueprints.md)** — Blueprints: whole-flowgroup patterns expanded per instance (sites/regions/tenants), `use_blueprint:` syntax, `%{var}` resolution, `lhp list blueprints` / `lhp dag --blueprint <name>`. Load when the same flowgroup repeats across deployments, or to decide blueprint vs template vs preset.
- **[quickstart.md](references/quickstart.md)** — First-project setup: `lhp init`, `lhp.yaml`, `databricks.yml`, `substitutions/`, `config/pipeline_config.yaml`, first flowgroup, validate + generate. Load when scaffolding a new project from scratch.
- **[project-config.md](references/project-config.md)** — lhp.yaml, substitutions, local variables, operational metadata, CLI commands, multi-flowgroup syntax. Load for project setup or config questions.
- **[advanced.md](references/advanced.md)** — Databricks bundles, pipeline/job configuration, dependency analysis, multi-job orchestration, CI/CD patterns. Load for deployment or orchestration tasks.
- **[monitoring.md](references/monitoring.md)** — Event log injection, monitoring pipeline, materialized views, `__eventlog_monitoring` alias. Load when configuring event_log or monitoring in lhp.yaml.
- **[sandbox.md](references/sandbox.md)** — Developer sandbox mode (`--sandbox`): personal namespaced copies of in-scope pipelines, profile + team policy config, rename semantics, sandbox error codes. Load when the user mentions `--sandbox`, `.lhp/profile.yaml`, developer isolation, or parallel development on a shared environment.
- **[errors.md](references/errors.md)** — All LHP error codes (LHP-CFG/VAL/IO/ACT/DEP) with causes and fixes. Load when troubleshooting any LHP error.
- **[best-practices.md](references/best-practices.md)** — Enterprise best practices (BP-1 through BP-19, anti-patterns). Load when setting up a new project, reviewing/refactoring configs, designing templates/presets/substitutions, tiering data quality, choosing between streaming_table vs materialized_view, or answering "what's the right way to..." questions.

## Instructions

1. **Read project files first** — match existing patterns and conventions. Do not introduce a best-practice default that conflicts with an established project convention without flagging the trade-off.
2. **Reuse before authoring** — for any flowgroup/pipeline creation request, scan `templates/` and `blueprints/` first. If an existing template or blueprint fits the pattern, propose reusing it (`use_template:` / `use_blueprint:`) and confirm with the user before hand-writing new actions. Author fresh YAML only when nothing fits.
3. **Apply best-practice defaults** (see section above). When the project is new or silent on a choice, pick the BP default. Load [best-practices.md](references/best-practices.md) when designing new structures or refactoring.
4. **Validate substitution tokens** — ensure `${token}` has corresponding entry in substitution files
5. **Apply presets** — reference project presets where appropriate
6. **Generate valid YAML** — proper indentation, correct field nesting
7. **Explain behavior** — describe what the generated YAML will produce in Python/Spark Declarative Pipelines
8. **Suggest validation** — recommend `lhp validate --env dev` after changes
9. **For CDC/SCD2 patterns**:
   - Exclude CDC metadata columns (`__START_AT`, `__END_AT`) using `* except` in transforms
   - Use business timestamps (modified_at, created_at) for `sequence_by`
   - Add technical columns to `except_column_list` in cdc_config
   - Apply `operational_metadata` at action level, not write level
   - Use `* except` pattern for future-proof column selection and schema evolution
10. **For Kafka sources** — remind that key/value are binary, need deserialization transform
11. **For operational_metadata**:
    - Apply at action level (load, transform), not write level
    - For file sources: include file metadata (`_source_file_path`, `_source_file_name`, `_processing_timestamp`)
    - For non-file sources: For example `_processing_timestamp`
12. **For creating templates**:
    - Follow naming convention: `TMPL<number>_<source_type>_<function>.yaml`
    - Define clear, required parameters with descriptions
    - Use Jinja2 syntax for parameter substitution: `{{ param_name }}` — including inside inline `sql:` blocks
    - Quote array and string parameters in YAML: `keys: "{{ natural_keys }}"`
    - Provide defaults for optional parameters
    - Document the template purpose, parameters, and usage examples in templates/README.md
    - Test templates by creating example flowgroups before finalizing
13. **For using templates in flowgroups**:
    - Name flowgroup: `<domain>_<final_table>_TMPL<number>`
    - Reference template with `use_template: TMPL<number>_<source_type>_<function>`
    - Provide all required parameters under `template_parameters:`
    - Use natural YAML for objects and arrays (not JSON strings)
    - Keep optional parameters only if overriding defaults
    - If multiple flowgroups use the same template. use multi-flowgroup syntax.
14. **For blueprints** (same flowgroup repeated across sites/regions/tenants): define a blueprint under `blueprints/` and one instance file per variant (`use_blueprint:` + nested `parameters:`). Use `%{var}` for parameters; never `${...}` in `pipeline:`/`flowgroup:` fields. Load [blueprints.md](references/blueprints.md).
15. **For error troubleshooting**: Load errors.md, find the error code, follow resolution. Always suggest `lhp validate --env <env> --verbose`.
16. **For monitoring/event log setup**: Load monitoring.md. Require `event_log` before `monitoring`. Use `__eventlog_monitoring` alias in pipeline_config.yaml.
17. **Not all fields are required**: When showing YAML examples, annotate fields as required/optional. Only required fields must be present; optional fields have sensible defaults.
