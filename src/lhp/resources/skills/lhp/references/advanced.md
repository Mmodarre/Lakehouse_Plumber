# Advanced Features Reference

## Table of Contents
- [Databricks Asset Bundles](#databricks-asset-bundles)
- [Pipeline & Job Configuration](#pipeline--job-configuration)
- [Dependency Analysis](#dependency-analysis)
- [Multi-Job Orchestration](#multi-job-orchestration)
- [CI/CD Patterns](#cicd-patterns)
- [State Management](#state-management)

---

## Databricks Asset Bundles

LHP generates DAB pipeline resource YAML files; it does NOT replace DAB or deploy resources.

### Setup

```bash
lhp init --bundle my-data-platform
cd my-data-platform
# Edit databricks.yml with workspace details
lhp generate -e dev
databricks bundle deploy --target dev
```

### What LHP Does

- Generates `resources/lhp/*.pipeline.yml` using glob patterns
- Synchronizes resources with generated code
- Cleans up obsolete resource files
- Never modifies `databricks.yml` or files outside `resources/lhp/`

### Generated Resource Example

```yaml
# resources/lhp/bronze_load.pipeline.yml
resources:
  pipelines:
    bronze_load_pipeline:
      name: bronze_load_pipeline
      catalog: main
      schema: lhp_${bundle.target}
      libraries:
        - glob:
            include: ../../generated/bronze_load/**
      root_path: ${workspace.file_path}/generated/bronze_load/
```

---

## Pipeline & Job Configuration

### Pipeline Configuration

```yaml
# config/pipeline_config.yaml
project_defaults:
  serverless: true
  edition: ADVANCED
  channel: CURRENT

---
pipeline:
  - bronze_load
serverless: false
clusters:
  - label: default
    node_type_id: Standard_D16ds_v5
    autoscale:
      min_workers: 2
      max_workers: 10
notifications:
  - email_recipients: [team@company.com]
    alerts: [on-update-failure]
```

Usage: `lhp generate -e dev --pipeline-config config/pipeline_config.yaml`

### Job Configuration

```yaml
# config/job_config.yaml
project_defaults:
  max_concurrent_runs: 1
  performance_target: STANDARD

---
job_name:
  - bronze_ingestion_job
max_concurrent_runs: 2
timeout_seconds: 7200
email_notifications:
  on_failure: [data-engineering@company.com]
schedule:
  quartz_cron_expression: "0 0 2 * * ?"
  timezone_id: America/New_York
tags:
  environment: production
permissions:
  - level: CAN_MANAGE
    user_name: admin@company.com
```

Usage: `lhp deps --format job --job-config config/job_config.yaml --bundle-output`

---

## Dependency Analysis

Analyzes FlowGroup YAML to detect pipeline dependencies, execution stages, and external sources.

### Commands

```bash
lhp deps                                    # All formats
lhp deps --format job --job-name my_etl      # Orchestration job
lhp deps --format job --bundle-output        # Save to resources/
lhp deps --format mermaid                    # Mermaid diagram
lhp deps --pipeline bronze --format json     # Specific pipeline
```

### Output Formats

| Format | Description |
|--------|-------------|
| `text` | Human-readable report |
| `json` | Structured data |
| `dot` | GraphViz diagram |
| `mermaid` | Mermaid diagram |
| `job` | Databricks job YAML |
| `all` | All formats |

### Execution Stages

Pipelines in the same stage run in parallel:
```
Stage 1: raw_ingestion, api_ingestion (parallel, no dependencies)
Stage 2: bronze_layer (depends on Stage 1)
Stage 3: silver_layer (depends on Stage 2)
Stage 4: gold_layer (depends on Stage 3)
```

### Generated Job

```yaml
resources:
  jobs:
    data_warehouse_etl:
      name: data_warehouse_etl
      max_concurrent_runs: 1
      tasks:
        - task_key: raw_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.raw_pipeline.id}
        - task_key: bronze_pipeline
          depends_on:
            - task_key: raw_pipeline
          pipeline_task:
            pipeline_id: ${resources.pipelines.bronze_pipeline.id}
```

---

## Multi-Job Orchestration

Split flowgroups into separate Databricks jobs using `job_name` property.

```yaml
pipeline: bronze_ncr
flowgroup: pos_transaction_bronze
job_name:
  - NCR                        # Assigns to "NCR" job

actions:
  - name: load_pos_data
    type: load
    source:
      type: cloudfiles
      path: "/mnt/landing/ncr/*.parquet"
    target: v_pos_raw
```

**All-or-nothing rule:** If ANY flowgroup has `job_name`, ALL must have it.

Each unique `job_name` generates a separate job file + a master orchestration job.

---

## CI/CD Patterns

### Deployment Strategies

**Trunk-Based + Tag Promotion (Recommended):**
- Single `main` branch; short-lived feature branches
- Tags trigger deployments: `dev-*` → dev, `rc-*` → test, `v*` → prod
- Same commit SHA across all environments — only substitution files differ
- YAML is single source of truth; generated Python is ephemeral

**Branch-Per-Environment:**
- Separate branches (`dev`, `staging`, `prod`) with merge promotion
- Simpler for small teams but divergence risk increases over time
- Less recommended for LHP projects where substitution files handle env differences

**Key principles:**
1. YAML is single source of truth; generated Python is ephemeral
2. Same commit SHA deployed to all environments
3. Environment isolation via separate substitution files
4. `.lhp_state.json` not in git (regenerates in CI)

**Version pinning (lhp.yaml):**
```yaml
required_lhp_version: ">=0.7.0,<1.0.0"
```

### Approval Gates

- **Dev/Test:** Automatic deployment on tag push — fast iteration
- **Production:** Manual approval gate (GitHub Environment protection rules or similar)
- **Key:** Ensure the same commit SHA that passed test is promoted to prod — never rebuild from a different commit

### CI Pipeline Workflow

```bash
# In CI/CD
pip install lakehouse-plumber
lhp validate --env $ENV
lhp generate --env $ENV --force
lhp deps --format job --job-config config/job_config.yaml --bundle-output
databricks bundle deploy --target $ENV
```

---

## State Management

`.lhp_state.json` tracks generated files, checksums, and dependencies.

- Only changed files regenerated on subsequent runs
- Upstream changes trigger downstream regeneration
- `--force` flag skips state checking
- Not committed to git (regenerates in CI)
