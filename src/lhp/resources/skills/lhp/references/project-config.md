# Project Configuration Reference

## Table of Contents
- [Project Structure](#project-structure)
- [lhp.yaml](#lhpyaml)
- [Substitutions & Secrets](#substitutions--secrets)
- [Local Variables](#local-variables)
- [Operational Metadata](#operational-metadata)
- [CLI Commands](#cli-commands)
- [Multi-Flowgroup Files](#multi-flowgroup-files)

---

## Project Structure

```
my_project/
├── lhp.yaml                 # Project configuration
├── config/                  # Optional config files
│   ├── pipeline_config.yaml
│   └── job_config.yaml
├── pipelines/               # Pipeline flowgroup YAML files
├── templates/               # Reusable action templates
├── presets/                 # Configuration presets
├── substitutions/           # Environment configs (dev.yaml, prod.yaml)
├── schemas/                 # Table schema definitions
├── expectations/            # Data quality expectation JSON files
├── sql/                     # External SQL files
├── py_functions/            # Custom Python functions
├── generated/               # Generated Python code (auto-generated)
└── resources/               # Databricks Asset Bundle resources
    └── lhp/                 # LHP-managed (auto-generated)
```

## lhp.yaml

```yaml
name: my_project
version: "1.0"
description: "Project description"
required_lhp_version: ">=0.7.0,<1.0.0"  # Optional version pinning

operational_metadata:
  columns:
    _processing_timestamp:
      expression: "F.current_timestamp()"
      description: "When record was processed"
      applies_to: ["streaming_table", "materialized_view", "view"]
    _source_file_path:
      expression: "F.col('_metadata.file_path')"
      description: "Source file path"
      applies_to: ["view"]  # CloudFiles only!
    _record_hash:
      expression: "F.xxhash64(*[F.col(c) for c in df.columns])"
      description: "Hash for change detection"
      applies_to: ["streaming_table", "materialized_view", "view"]
```

## Substitutions & Secrets

### Processing Order

| Order | Syntax | Type | Scope |
|-------|--------|------|-------|
| 1st | `%{var}` | Local variable | Flowgroup-scoped |
| 2nd | `{{ param }}` | Template parameter | Template-scoped |
| 3rd | `${token}` or `{token}` | Environment substitution | From substitutions/env.yaml |
| 4th | `${secret:scope/key}` | Secret reference | Becomes dbutils.secrets.get() |

**Prefer `${token}` over `{token}`** for environment substitutions (avoids Python string format conflicts).

### Environment Substitution File

```yaml
# substitutions/dev.yaml
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
  gold_schema: gold
  landing_volume: /Volumes/dev/raw/landing
  landing_path: /mnt/dev/landing

secrets:
  default_scope: dev_secrets
  scopes:
    database_secrets: dev_db_secrets
    api_secrets: dev_api_secrets
```

### Secret References

```yaml
# In pipeline YAML
user: "${secret:database_secrets/username}"
password: "${secret:database_secrets/password}"
api_key: "${secret:key}"  # Uses default_scope
```

Generates: `dbutils.secrets.get(scope="dev_db_secrets", key="username")`

## Local Variables

Flowgroup-scoped variables using `%{var}` syntax. Resolved first (before templates).

```yaml
pipeline: acme_bronze
flowgroup: customer_pipeline

variables:
  entity: customer
  source_table: customer_raw

actions:
  - name: "load_%{entity}_raw"
    type: load
    source:
      type: delta
      database: "{catalog}.{raw_schema}"
      table: "%{source_table}"
    target: "v_%{entity}_raw"

  - name: "write_%{entity}_bronze"
    type: write
    source: "v_%{entity}_raw"
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: "%{entity}"
```

- Supports inline patterns: `prefix_%{var}_suffix`
- Recursive: variables can reference other variables
- Strict: undefined variables cause immediate error
- NOT shared across flowgroups

## Operational Metadata

**Usage in actions:**
- `operational_metadata: true` — all defined columns
- `operational_metadata: ["_col1", "_col2"]` — specific columns
- `operational_metadata: false` — disable all (overrides preset/flowgroup)

**Behavior:** Additive across preset + flowgroup + action levels (except `false` disables all).

**CloudFiles-only columns:** `_source_file_path`, `_source_file_size`, `_source_file_modification_time` — only available in views from CloudFiles loads, not downstream.

## CLI Commands

```bash
# Project setup
lhp init <project>              # Scaffold new project
lhp init <project> --bundle     # With Databricks Asset Bundle support

# Validation
lhp validate --env <env>        # Validate all configurations

# Code generation
lhp generate --env <env>                    # Generate Python DLT files
lhp generate --env <env> --include-tests    # Include test actions
lhp generate --env <env> --force            # Force regeneration
lhp generate --env <env> --dry-run --verbose  # Preview without writing
lhp generate --env <env> --force --pipeline-config config/pipeline_config.yaml  # Regen bundle resources

# Dependency analysis
lhp deps                                      # Full analysis (all formats)
lhp deps --format job --job-name <name>        # Generate orchestration job
lhp deps --format job --job-config config/job_config.yaml --bundle-output  # Job with config
lhp deps --format mermaid                      # Mermaid diagram
lhp deps --pipeline <name> --format json       # Analyze specific pipeline

# Inspection
lhp show <flowgroup> --env <env>              # Show specific flowgroup config
```

## Multi-Flowgroup Files

### Multi-Document Syntax (`---` separators)

```yaml
pipeline: raw_ingestions
flowgroup: customer_ingestion
use_template: csv_template
template_parameters:
  table_name: customer
---
pipeline: raw_ingestions
flowgroup: orders_ingestion
use_template: csv_template
template_parameters:
  table_name: orders
```

- No field inheritance between documents
- Best for different structures

### Array Syntax (with inheritance)

```yaml
pipeline: raw_ingestions
use_template: csv_template
presets:
  - bronze_layer

flowgroups:
  - flowgroup: customer_ingestion
    template_parameters:
      table_name: customer

  - flowgroup: orders_ingestion
    template_parameters:
      table_name: orders

  - flowgroup: special_ingestion
    presets:
      - silver_layer            # Override inherited presets
    template_parameters:
      table_name: special
```

**Inheritable fields:** `pipeline`, `use_template`, `presets`, `operational_metadata`, `job_name`

**Inheritance rules:**
- Key not present -> inherits document-level value
- Key present -> uses flowgroup-level value (overrides)
- Empty list `[]` -> explicit override (no inheritance)
- Type-agnostic override (bool can be overridden by list)

**Cannot mix** both syntaxes in the same file.
