# LHP Best Practices Reference

Authoritative guidance for LHP projects. Source: https://lakehouse-plumber.readthedocs.io/best_practices/index.html (subpages: project_structure, environments, performance, governance, testing).

Load this file when:
- Setting up a new LHP project structure
- Reviewing or refactoring existing LHP configurations
- The user asks "what's the right way to..." / "best practice for..."
- Writing bronze/silver/gold medallion pipelines
- Designing templates, presets, or substitutions
- Tiering data quality expectations
- Choosing between streaming_table vs materialized_view

## Table of Contents
- [Project Structure](#1-project-structure)
- [File & Subdirectory Organization](#2-file--subdirectory-organization)
- [Naming Conventions](#3-naming-conventions)
- [Template Design](#4-template-design)
- [Preset Strategy](#5-preset-strategy)
- [Substitutions & Environments](#6-substitutions--environments)
- [Local Variables](#7-local-variables)
- [FlowGroup Design](#8-flowgroup-design)
- [Load Actions](#9-load-actions)
- [Transform Actions](#10-transform-actions)
- [Write Actions](#11-write-actions)
- [Data Quality](#12-data-quality)
- [Operational Metadata](#13-operational-metadata)
- [Schema Management](#14-schema-management)
- [Validation & CI](#15-validation--ci)
- [Bundle Integration](#16-bundle-integration)
- [Medallion Pattern](#17-medallion-pattern)
- [Documentation](#18-documentation)
- [Anti-Patterns](#19-anti-patterns)

---

## 1. Project Structure

- **BP-1.1** Group pipeline YAMLs by business domain (`orders/`, `customers/`), not action type.
- **BP-1.2** Keep each YAML file **50–200 lines**. Avoid monolithic files with 15+ flowgroups.
- **BP-1.3** In mono-repos, use glob patterns in `lhp.yaml` to scope pipeline discovery per environment/team.
- **BP-1.4** Maintain separate directories for `presets/`, `templates/`, `substitutions/`, `pipelines/`.
- **BP-1.5** Use `CODEOWNERS` to require platform-team approval for changes to shared presets/substitutions/templates.

## 2. File & Subdirectory Organization

### Discovery Matrix

| File Type | Discovery | Subdirectories |
|-----------|-----------|----------------|
| Pipeline YAMLs | Full recursive | ✅ `pipelines/<system>/<layer>/` |
| SQL / Schema / Expectations | Referenced by path | ✅ allowed |
| Templates | Flat only (glob) | ❌ Use prefix naming |
| Presets | Flat only (glob) | ❌ Use prefix naming |
| Substitutions | Flat only | ❌ One file per environment |

- **BP-2.1** Layout pipelines as `pipelines/<system>/<layer>/` (e.g., `erp/bronze/`, `erp/silver/`).
- **BP-2.2** Mirror SQL structure to pipelines: `sql/<system>/<layer>/<description>.sql`.
- **BP-2.3** Store schemas as `schemas/<system>/<layer>/<description>.yaml`.
- **BP-2.4** Organize expectations as `expectations/<system>/<layer>/`.
- **BP-2.5** Group Python modules by function: `python_modules/transforms/`, `/datasources/`, `/sinks/`.
- **BP-2.6** Template prefix naming (no subdirs): `TMPLxxx_<layer>_<action>_<type>`.
- **BP-2.7** Preset prefix naming: `global_defaults`, `brz_standard`, `brz_cloudfiles_json`, `slv_cdc_scd2`.
- **BP-2.8** Use include patterns in `lhp.yaml` for team-scoped generation: `system_a/**/*.yaml`.

## 3. Naming Conventions

- **BP-3.1** `snake_case` everywhere (pipelines, flowgroups, actions, templates, presets, variables).
- **BP-3.2** Prefix pipelines with system + layer: `erp_bronze_pipeline`, not `pipeline_v2`.
- **BP-3.3** Flowgroups describe the flow: `erp_brz_raw_orders`, `erp_slv_orders_enriched`.
- **BP-3.4** Action names use verb-entity-modifier: `load_raw_orders`, `transform_validate_orders`, `write_orders_silver`.
- **BP-3.5** `${SCREAMING_SNAKE_CASE}` for environment tokens; `%{lower_snake_case}` for local variables.
- **BP-3.6** Do not abbreviate identifiers — clarity beats brevity in version-controlled configs.
- **BP-3.7** Template IDs: `TMPL001_brz_load_cloudfiles_standard`, `TMPL004_slv_transform_sql_enrichment`.
- **BP-3.8** Flowgroup files include template suffix: `erp_bronze_ingest_TMPL001.yaml`.
- **BP-3.9** Pipeline pattern: `<system>_<layer>_pipeline`.
- **BP-3.10** Preset pattern: `<scope>_<layer>_<purpose>` (e.g., `brz_standard`, `slv_cdc_scd2`, `gld_standard`).

## 4. Template Design

- **BP-4.1** Extract a template only after **3+ flowgroups** share the same pattern. Write concrete first, then abstract.
- **BP-4.2** Minimize parameters. Use `required`, `default`, `description` on each. Avoid 15+ parameters.
- **BP-4.3** Establish platform-owned **golden templates** for standard patterns (CloudFiles bronze, Delta snapshot, streaming table with DQE, SQL enrichment, MV).
- **BP-4.4** Prefix naming for templates (flat directory, no subdirs).
- **BP-4.5** Templates can declare presets. Merge order: template presets → flowgroup presets → explicit action config.
- **BP-4.6** Template parameters capture **unique** aspects; presets handle **standard** aspects (table properties, metadata, reader options).
- **BP-4.7** Reference external files with parameterized paths: `sql_path: "sql/{{ system }}/silver/enrich_{{ entity }}.sql"`.

## 5. Preset Strategy

- **BP-5.1** Design preset hierarchies with `extends`: `global_defaults` → `bronze_standard` → `orders_bronze`.
- **BP-5.2** Encode organizational standards in presets — group related properties (schema evolution, rescue columns, metadata).
- **BP-5.3** Cap total presets at **15–20 files** to avoid confusion and misuse.
- **BP-5.4** Verify effective config with `lhp validate --env <env> --verbose` (or `lhp diff --env <env>` to see the regenerated output) after preset merging/template expansion.
- **BP-5.5** Treat preset changes as **high blast radius** — run full project validation before merging.

## 6. Substitutions & Environments

- **BP-6.1** Directory-based env separation: `substitutions/dev.yaml`, `staging.yaml`, `prod.yaml`.
- **BP-6.2** Put all env-varying values in substitution tokens: catalog/schema names, storage paths, cluster policies, alert emails.
- **BP-6.3** Use the `global` section for values inherited by all environments.
- **BP-6.4** **Never hardcode secrets.** Always use `${secret:scope/key}`.
- **BP-6.5** Audit available tokens with `lhp substitutions --env <env>` before writing flowgroups.
- **BP-6.6** Standard medallion tokens: `bronze_catalog`, `silver_catalog`, `gold_catalog`, `landing_path_base`.

## 7. Local Variables

- **BP-7.1** Use `variables:` + `%{var}` for flowgroup-scoped repetition.
- **BP-7.2** Prefer locals over hardcoded repeats within one flowgroup.
- **BP-7.3** **Do not** use local variables for environment-specific values. `%{var}` = flowgroup-scoped; `${TOKEN}` = environment.

## 8. FlowGroup Design

- **BP-8.1** Use array syntax (`flowgroups:`) with field inheritance when multiple flowgroups share `pipeline`, `use_template`, `presets`, `operational_metadata`, or `job_name`.
- **BP-8.2** One pipeline per data domain. `orders_bronze` groups `raw_orders`, `raw_returns`, `raw_refunds`.
- **BP-8.3** Use `job_name` to aggregate flowgroups into Databricks Workflow jobs; generate via `lhp dag --format job`.
- **BP-8.4** Order actions **Load → Transform → Write → Test**.

## 9. Load Actions

- **BP-9.1** Always set on CloudFiles bronze: `cloudFiles.schemaEvolutionMode: rescue` + `cloudFiles.rescuedDataColumn: _rescued_data`.
- **BP-9.2** `readMode: stream` for bronze sources; `readMode: batch` for dimension/lookup tables.
- **BP-9.3** Three-part names with tokens for Delta loads: `${SILVER_CATALOG}.schema.table`.
- **BP-9.4** Rate-limit Auto Loader in prod via presets: `cloudFiles.maxFilesPerTrigger`, `cloudFiles.maxBytesPerTrigger`.
- **BP-9.5** Provide `cloudFiles.schemaHints` for critical columns where type inference risks downstream failures.

## 10. Transform Actions

- **BP-10.1** **Default to SQL** for silver/gold logic — readable, reviewable, broadly understood.
- **BP-10.2** Use external SQL files for transforms > ~5 lines: `sql_path: sql/<system>/<layer>/<name>.sql`.
- **BP-10.3** Reserve Python for UDFs, ML scoring, procedural logic SQL can't express. Signature depends on source count.
- **BP-10.4** Use `schema` transform for explicit column control (rename, cast). Apply `enforcement: strict` at silver.
- **BP-10.5** Use `data_quality` transform for DQE expectations (generates `@dp.expect_all*` decorators).
- **BP-10.6** Use `temp_table` transform for intermediate calcs (with `temporary: true`).

## 11. Write Actions

- **BP-11.1** **Default to materialized views** for silver/gold — always correct, reprocess on source changes.
- **BP-11.2** **Streaming tables** for bronze ingestion and CDC targets — optimal for append-only.
- **BP-11.3** On history tables: `table_properties: pipelines.reset.allowed: "false"` to prevent accidental full refresh.
- **BP-11.4** Prefer `cluster_columns` (liquid clustering) over `partition_columns`.
- **BP-11.4a** Use `cluster_by_auto: true` when clustering keys / cardinality are unknown — let Databricks pick and evolve them (mutually exclusive with `cluster_columns`).
- **BP-11.5** Always include `comment` on write targets (Unity Catalog descriptions).
- **BP-11.6** Use `spark_conf` on write targets for per-table tuning.
- **BP-11.7** CDC: use `mode: cdc` with explicit `cdc_config` (`keys`, `sequence_by`, `scd_type`, etc.).
- **BP-11.8** `once: true` on one-time backfill actions; doesn't affect ongoing streams.
- **BP-11.9** Multiple writes to the same streaming table auto-consolidate into multiple `append_flow` functions.
- **BP-11.10** `mode: snapshot_cdc` when source provides full snapshots (not CDF). Config key: `snapshot_cdc_config`; SCD key: `stored_as_scd_type`.
- **BP-11.11** Use `sink` write target for external destinations: delta, kafka, DataSink V2, ForEachBatch.

## 12. Data Quality

- **BP-12.1** **Tier expectations by layer:** Bronze = `warn` only; Silver = `drop` bad rows; Gold = `fail` on critical invariants.
- **BP-12.2** Centralize expectations in external DQE files: `expectations/<domain>/<layer>/<description>.yaml`.
- **BP-12.3** Name expectations descriptively: `valid_<column>_<constraint>` (e.g., `valid_order_id_not_null`, `valid_amount_positive`).
- **BP-12.4** Use the 9 test action types for cross-table validation: `row_count`, `uniqueness`, `referential_integrity`, `completeness`, `range`, `schema_match`, `all_lookups_found`, `custom_sql`, `custom_expectations`. Generate with `--include-tests`.
- **BP-12.5** To publish test results to an external system (via a `test_reporting` provider), set `test_id` on the test action — only `test_id`-tagged actions are published. The expectation must use `on_violation: warn`, **not** `fail`: a `fail` aborts the SDP flow before metrics are emitted, so the reporting hook never receives the result. Requires `--include-tests`.

## 13. Operational Metadata

- **BP-13.1** Define all operational metadata columns once in `lhp.yaml` with `expression`, `description`, `applies_to`, `enabled`, `additional_imports`.
- **BP-13.2** Package into layer presets: `bronze_standard`, `silver_standard`.
- **BP-13.3** Metadata is **additive** across preset → flowgroup → action levels (deep-merged, deduped).
- **BP-13.4** Use `applies_to` to control which targets receive each column (e.g., `input_file_name()` only on streaming tables from CloudFiles).

## 14. Schema Management

- **BP-14.1** Use `schema_file` on load actions for external DDL/YAML/JSON schemas.
- **BP-14.2** Use `schema` transform at the bronze→silver boundary (arrow-syntax renaming, type casting, strict enforcement).
- **BP-14.3** `enforcement: strict` at silver rejects unexpected columns — prevents schema drift.

## 15. Validation & CI

- **BP-15.1** Run `lhp validate` as a **blocking CI check** on every PR.
- **BP-15.2** `lhp generate --dry-run` to verify codegen without writing files.
- **BP-15.3** Maintain dry-run baselines in version control; diff against them to detect preset-change regressions.
- **BP-15.4** **Layered CI:** yamllint → JSON Schema → `lhp validate` → `lhp generate --dry-run` → baseline diff → pytest `--include-tests`.
- **BP-15.5** `lhp validate` runs the **same** structural and preflight checks as `lhp generate` (no-creator/duplicate `LHP-VAL-009`, blueprint/instance `LHP-VAL-041` family, test-reporting file existence `LHP-CFG-032`, bundle catalog/schema `LHP-CFG-026`). On a project containing `databricks.yml`, pass `--pipeline-config`/`-pc` to the CI `lhp validate` step (or `--no-bundle`) — without it validate fails fast with `LHP-CFG-023`, exactly like generate.
- **BP-15.6** `lhp validate` runs cross-flowgroup conflict detection on the **resolved** flowgroups (after presets, templates, and substitutions). A conflict introduced by resolution — e.g. a template that expands into two `create_table: true` actions targeting the same table — now fails `lhp validate`, not just `lhp generate`.
- **BP-15.7** `lhp generate`/`lhp validate` process flowgroups in a CPU-bound worker pool. Pool size precedence: `--max-workers N` flag → `LHP_MAX_WORKERS` env var → auto-default. Auto-default = `max(1, floor(detected_cpus * 0.8))` (20% headroom for the main thread + OS). Floored, so: 1→1, 2→1, 8→6, 16→12, 64→51.
- **BP-15.8** On a **dedicated batch host** for large projects, the 0.8 auto-default under-utilizes CPU. Set `LHP_MAX_WORKERS=<physical core count>` (or `--max-workers N` per run) to maximize generation throughput.
- **BP-15.9** Worker count clamps to ≥ 1: `LHP_MAX_WORKERS=0` runs sequentially (does not disable the pool); a non-integer value warns and falls back to auto-detect.

## 16. Bundle Integration

- **BP-16.1** `lhp dag --format job` generates DAB job resource definitions from dependency analysis.
- **BP-16.2** Bundle scaffolding is default in `lhp init`; use `--no-bundle` if managed separately.
- **BP-16.3** Store generated bundle resources in a dedicated directory (e.g., `bundle/generated/`) separate from hand-written configs.
- **BP-16.4** **`resources/lhp/` is exclusively LHP-managed.** Every `lhp generate` wipes it and regenerates one `<pipeline_name>.pipeline.yml` per pipeline. Never hand-edit anything under `resources/lhp/` — your changes will be overwritten on the next generate.
- **BP-16.5** **Place custom resource YAMLs (hand-written jobs, dashboards, secret scopes) under `resources/` at the top level or in any non-`lhp` subdirectory.** Files outside `resources/lhp/` are never touched by LHP. One exception: the monitoring job YAML at `resources/<name>.job.yml`, which LHP identifies by its sentinel header (`# Generated by LakehousePlumber - Monitoring Job`) and replaces on each run.
- **BP-16.6** **`catalog` and `schema` are required in `pipeline_config.yaml`** — set them per-pipeline or in a top-level `project_defaults` block. Do not rely on `databricks.yml` variables for catalog/schema; that pathway was removed in 0.8.7.

## 17. Medallion Pattern

| Layer | Target | Expectations | Metadata |
|-------|--------|--------------|----------|
| **Bronze** | `streaming_table` | `warn` only | `ingest_timestamp`, `source_file*` |
| **Silver** | `materialized_view` | `drop` bad rows | `updated_at` |
| **Gold** | `materialized_view` | `fail` on critical invariants | inherited |

- Environment promotion: identical YAML, per-env substitution files.
- Multi-pipeline orchestration: `job_name` + `lhp dag`.
- Multi-source ingestion: multiple load/write actions to the same table → auto-consolidated `append_flow`s.

## 18. Documentation

- **BP-18.1** Include `description` on every action and write target (becomes generated-code comments).
- **BP-18.2** Include `comment` on write targets (UC descriptions, queryable in Data Explorer).
- **BP-18.3** YAML comments explain **why**, not **what**.
- **BP-18.4** Use `lhp info` and `lhp stats` for project summaries.

## 19. Anti-Patterns (Never)

| Anti-Pattern | Harm | Fix |
|--------------|------|-----|
| Hardcoded catalog/schema | Breaks env promotion | Always use substitution tokens |
| `expect_or_fail` at bronze | One bad row stops pipeline | `warn` at bronze; `fail` only for critical invariants |
| Skipping `lhp validate` | Errors harder to diagnose post-gen | Always validate first |
| Streaming tables for joins/enrichment | Stale results when dimensions change | Use materialized views |
| Premature templating | Over-general, hard to use | Write 3+ concrete flowgroups first |
| Treating preset edits lightly | Silent effect on every pipeline | Full-project validate before merge |
| Skipping operational metadata | No audit trail in prod | Apply consistently via layer presets |
| Monolithic YAML files | Unreviewable | One pipeline per file, 50–200 lines |
| Secrets in substitution files | Leaks to version control | `${secret:scope/key}` only |
| Ignoring `_rescued_data` | Schema mismatches drop data silently | Always enable `cloudFiles.rescuedDataColumn` |
| Flat `sql/` with 100+ files | Unfindable | `sql/<system>/<layer>/` subdirectories |
| Subdirectories under `templates/` or `presets/` | Not discovered | Prefix-based flat naming |
| Generic names (`pipeline_v2`) | Meaningless at 500+ flowgroups | ID-based: `erp_brz_raw_orders` |
| Hand-editing files under `resources/lhp/` | Overwritten on next `lhp generate` | Put custom resources in `resources/` (top level) or a non-`lhp` subdirectory |
| Setting catalog/schema only in `databricks.yml` variables | Pathway removed in 0.8.7 — `lhp generate` fails fast with `BundleResourceError` | Move catalog/schema into `pipeline_config.yaml` (`project_defaults` or per-pipeline) |
