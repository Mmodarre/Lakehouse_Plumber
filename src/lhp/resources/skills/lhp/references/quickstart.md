# Quickstart — First Project Setup

End-to-end path from nothing to generated pipeline code. Use when scaffolding a new project or wiring up `lhp.yaml` / `databricks.yml` / `substitutions/` / `config/pipeline_config.yaml` for the first time. For field-level detail see [project-config.md](project-config.md).

## 1. Scaffold

```bash
lhp init my_project        # creates the project IN THE CURRENT DIRECTORY; bundle ON by default
lhp init my_project --no-bundle   # skip the Declarative Automation Bundles setup
lhp init my_demo --sample  # ready-to-run TPC-H sample project (see below)
```

`init` refuses to run if `lhp.yaml` already exists (`LHP-IO-007`). It writes a live `lhp.yaml`, `.gitignore`, `README.md`, `.vscode/`, and a tree of **disabled example files** ending in `.tmpl` under `substitutions/`, `config/`, `presets/`, `templates/`, `blueprints/`, `schemas/`, `sql/`, `expectations/`, `pipelines/`. UC tag files (`tags_file`) live under `schemas/` alongside the schema files they can share. With bundle on (default) it also writes `databricks.yml` at the root and a `resources/` directory.

**Activate any example by renaming it to drop the `.tmpl` suffix** (e.g. `substitutions/dev.yaml.tmpl` → `substitutions/dev.yaml`), then edit the values.

### `--sample` — the TPC-H demo project

`lhp init <name> --sample` scaffolds a ready-to-run TPC-H medallion demo instead of the empty skeleton. It runs against the read-only `samples.tpch` catalog on any Unity Catalog workspace (zero data setup) and exercises the full feature surface: bronze ingest (delta load of nation/region + cloudfiles JSON orders + cloudfiles CSV lineitem via one reusable template), silver CDC both flavors (streaming `mode: cdc` with `apply_as_deletes`, and `mode: snapshot_cdc` + `source_function`), all four transform types, a gold materialized view from `sql/sales_by_nation.sql`, presets, every substitution syntax, operational metadata, a data-prep notebook (`notebooks/data_prep.py`), and a hand-authored job (`resources/sample_job.yml`).

- **Bundle required**: `--sample` + `--no-bundle` is a usage error (exit 2).
- Ships a ready `config/pipeline_config.yaml` (no `.tmpl` suffix). The project is bundle-enabled, so every run needs it: `lhp validate --env dev -pc config/pipeline_config.yaml`, then `lhp generate --env dev -pc config/pipeline_config.yaml`.
- The scaffolded `README.md` is the full walkthrough (edit catalog in `databricks.yml` **and** `substitutions/dev.yaml` → generate → `databricks bundle deploy` → `databricks bundle run sample_job`).

## 2. The four files you must get right

| File | Holds | Required minimum |
|------|-------|------------------|
| `lhp.yaml` | Project metadata, operational-metadata column defs | Only `name` is required. **`catalog`/`schema` do NOT live here.** |
| `substitutions/<env>.yaml` | Per-env tokens (`${...}`) and secret scopes | One file per environment (`dev.yaml`, `prod.yaml`). |
| `config/pipeline_config.yaml` | **`catalog` + `schema`** (required), pipeline runtime settings | Both catalog and schema, per-pipeline or in `project_defaults`. |
| `databricks.yml` | Declarative Automation Bundles: name, `include:`, targets | Target names **must match** your substitution env names. |

### `lhp.yaml` (minimal)

```yaml
name: my_project
version: "1.0"
```

### `substitutions/dev.yaml`

Top-level key is the environment name; it holds token→value pairs. A separate top-level `secrets:` block maps scopes.

```yaml
dev:
  catalog: dev_catalog
  bronze_schema: bronze
  silver_schema: silver
  landing_volume: /Volumes/dev/raw/landing

secrets:
  default_scope: dev_secrets
  scopes:
    database_secrets: dev_db_secrets
```

Reference tokens in flowgroups as `${catalog}`, `${bronze_schema}`, … and secrets as `${secret:database_secrets/username}`. **Never** use the bare-braces `{token}` form (deprecated, `LHP-DEPR-001`).

### `config/pipeline_config.yaml` (catalog + schema are mandatory)

```yaml
project_defaults:
  catalog: "${catalog}"
  schema: bronze
  serverless: true

---
pipeline: ingestion_dev
```

`catalog` and `schema` must **both** resolve to non-empty values, from this file only — there is no `lhp.yaml` setting and no fallback to `databricks.yml`. Missing/half-set/empty → `lhp generate` fails fast with `BundleResourceError`. Pass the file with `-pc` / `--pipeline-config` (**required whenever `databricks.yml` is present**).

## 3. Write the first flowgroup

```yaml
# pipelines/sales/bronze/orders.yaml
pipeline: ingestion_dev
flowgroup: orders_bronze

actions:
  - name: load_orders
    type: load
    readMode: stream
    source:
      type: cloudfiles
      path: "${landing_volume}/orders/*.csv"
      format: csv
    target: v_orders_raw

  - name: write_orders
    type: write
    source: v_orders_raw
    write_target:
      type: streaming_table
      catalog: "${catalog}"
      schema: "${bronze_schema}"
      table: orders_bronze
```

## 4. Validate, then generate

```bash
lhp validate --env dev        # structural + preflight checks (catalog/schema, tokens, references)
lhp generate --env dev -pc config/pipeline_config.yaml   # -pc required when databricks.yml present
```

- `validate`'s `--env` defaults to `dev`; `generate`'s `--env` is **required**.
- Output lands in `generated/<env>/`. `resources/lhp/` is wholly LHP-managed and rewritten every run.
- Add `--include-tests` to also process test actions; `--dry-run` to preview without writing.
- Every `generate` is a **full regenerate** — there is no incremental mode and no `--force` flag.

## 5. Deploy (bundle projects)

The generated `resources/lhp/<pipeline>.pipeline.yml` files are wired into `databricks.yml` via its `include:` block. Deploy with the standard Databricks CLI bundle commands (`databricks bundle deploy -t dev`). See [advanced.md](advanced.md) for orchestration jobs (`lhp dag --format job`) and CI/CD.
