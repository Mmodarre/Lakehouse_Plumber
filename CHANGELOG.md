# Changelog

All notable changes to Lakehouse Plumber are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] — 2026-05-19

### Breaking changes

- **State file format**: replaced the monolithic `.lhp_state.json` with
  per-pipeline shards under a `.lhp_state/` directory
  (`<pipeline>.json` per pipeline plus a project-wide `_global.json`).
  Existing `.lhp_state.json` files are auto-removed by the orchestrator
  only after the first fully-successful run on the new format
  (`StatePersistence.maybe_remove_legacy_state`); partial-failure runs
  preserve the legacy file untouched so re-runs can recover.
- `StateManager` renamed to `ProjectStateManager`. A `StateManager`
  alias is retained via the module's `__getattr__` hook with a
  `DeprecationWarning` for one minor release; removed in 0.10.0.
  Update imports to `from lhp.core.state_manager import
  ProjectStateManager`.
- Direct access to `state.environments` via
  `ProjectStateManager.state` now raises `AttributeError`. Callers
  must use `ProjectStateManager.load_all_pipeline_shards(env)`, which
  returns the same `Dict[str, FileState]` shape — iteration logic is
  unchanged. Property-style lazy deprecation was not viable because
  the new aggregate read requires an `env` argument the attribute
  could not pass.
- `PythonFileCopier.apply_copy_record` no longer mutates state. It
  returns a list of `CopiedFileEntry` records and the caller invokes
  `track_generated_file` separately. In-tree callers route through
  `PipelineProcessor._apply_copy_records` automatically.

### Added

- `--no-state` flag for `lhp generate`: skip state-file generation
  entirely. Useful for CI runs that always use `--force` and don't
  rely on incremental regeneration.
- Per-pipeline state shards (`.lhp_state/<pipeline>.json`) — atomic
  per-pipeline commits via `os.replace`. Failed pipelines do not
  write a shard; sibling pipelines' shards are unaffected.
- `PipelineStateManager.untrack_generated_file` — removes a previously
  tracked file entry from this pipeline's shard. Called from the
  worker's empty-content cleanup path so state stays in sync when a
  flowgroup's actions collapse to no code (e.g. a test-only flowgroup
  regenerated without `--include-tests`).
- `LHPError.from_worker_exception` + `lhp_error_from_worker_failure`
  factory — reconstructs worker-side exceptions on the main thread
  while preserving the worker's original exception type via
  dual-inheritance subclasses (`LHPValidationError(LHPError,
  ValueError)`, `LHPFileError(LHPError, FileNotFoundError)`). Existing
  `except ValueError` / `except FileNotFoundError` handlers continue
  to catch worker failures.

### Changed

- Pipeline generation parallelises **per-pipeline** (one worker per
  pipeline) instead of per-flowgroup. Phase A (parse, codegen,
  format) and the intra-pipeline portion of Phase B (cross-flowgroup
  validation, `.py` writes, copied-module application, state
  tracking, test-reporting hook, atomic shard save) all run inside
  the worker — eliminating the main-thread GIL-starvation hot path.
- `_assemble_pipeline_outputs` and the associated main-thread Phase B
  machinery removed; the orchestrator's `generate_pipelines_by_fields`
  now dispatches pipelines, reconstructs aggregate failure errors,
  and finalises `_global.json` at end of batch.
- `ChecksumCache` no longer holds a `threading.Lock` (main-thread-only
  usage post-refactor).
- `cleanup_orphaned_files` now persists removals back to the affected
  per-pipeline shards via `StatePersistence.save_pipeline_shard`
  (previously a no-op TODO from Plan 1).
- `DependencyTracker.update_global_dependencies` is now skipped in
  pipeline-scope trackers (`for_pipeline`). The main thread's
  `ProjectStateManager.save_global` performs the per-env
  global-dependency refresh once at end of batch — `PipelineState`
  has no `global_dependencies` field.

### Removed

- `_PipelineProgress`, `_assemble_pipeline`, `_assemble_pipeline_outputs`
  machinery in the orchestrator (worker-internal now).
- `set_checksum_cache` method on `DependencyTracker` (cache is
  provided at factory-construction time via
  `DependencyTracker.for_project(..., checksum_cache=...)`).

### Internal

- Workers receive a `PipelineStateManager` scoped to **one** pipeline,
  never a `ProjectStateManager`. The worker entry
  `_process_pipeline_for_generate` accepts only `state_dir: Path` and
  `build_state: bool`; no cross-pipeline state object is reachable
  from the worker. The contract is statically enforceable via
  `__annotations__` introspection.
- Blueprint provenance map (`Dict[Tuple[str, str], BlueprintProvenance]`)
  now crosses the spawn boundary into workers; synthetic-flowgroup
  detection and blueprint-aware dependency resolution work the same
  in the worker as on the main thread.

## [0.8.7] — 2026-05-12

### Added

- **Bundle sync sub-phase instrumentation**: new `--perf` aggregate-stat
  categories expose the cost breakdown of `lhp generate`'s Bundle sync phase
  — `bundle_extract_keys`, `bundle_find_resource_files`,
  `bundle_existing_files`, `bundle_extract_catalog`,
  `bundle_update_databricks_yaml`, `bundle_create_resource`,
  `bundle_cleanup_orphans` — plus a restored `find_source_yaml` per-flowgroup
  timer in `core/orchestrator.py` (deleted between v0.8.0 and v0.8.6, causing
  measurement artifacts in `process_flowgroup` aggregates). The perf summary
  header also reports `pipelines_synced` and `resource_files_scanned` counts
  so timings can be normalised across runs of different project sizes. Zero
  overhead when `--perf` is not set.

### Fixed

- **Bundle sync O(P²) YAML re-parse**: `_find_all_resource_files_for_pipeline`
  previously re-parsed every `*.pipeline.yml` in `resources/lhp/` once per
  pipeline, so a project with 100 pipelines triggered ~10,000
  `yaml.safe_load` calls per sync. Added a per-sync memoisation cache
  (`BundleManager._pipeline_keys_cache`, initialised in
  `sync_resources_with_generated_files` and cleared in a `finally` block) so
  each unique resource file is parsed at most once per sync. On the
  `performance_testing` fixture (100 pipelines × ~101 files, 4,001 generated
  files), median Bundle sync drops from **13.46s** (v0.8.0) / **13.62s**
  (v0.8.6) to **0.25s** — a ~55× speedup. End-to-end `lhp generate --env dev
  --force` median wall time drops by ~10s. Behaviour is unchanged; the cache
  is opt-in (`None` outside of sync) so callers invoking
  `_extract_pipeline_keys_from_file` outside the sync path see the original
  uncached read.

## [0.8.6] — 2026-05-11

### Changed

- **`custom_datasource` and `custom_sink` generated output format** changed from
  *inline-embed* to *copy-and-import*. The user's PySpark `DataSource` /
  `DataSink` source file is now copied verbatim into a
  `custom_python_functions/` subdirectory beside the generated pipeline file,
  and the pipeline imports the class by name. Previously the user's class body
  (50–250 lines) was inlined into every generated file that registered a
  custom source/sink. **YAML user-facing surface is unchanged.** The new
  generated file is shorter, more diff-able, and consistent with the existing
  python-transform action pattern.

### Added

- **Cloudpickle registration for custom sources/sinks**: generated files now
  emit `_lhp_cloudpickle.register_pickle_by_value(custom_python_functions)`
  between the imports block and `PIPELINE_ID`. This is the one-line fix that
  makes the import-based pattern work across the local-Spark / executor
  boundary — PySpark's *vendored* cloudpickle is what serializes registered
  DataSource classes to executors, and only `register_pickle_by_value`
  against the vendored copy actually takes effect.

- **Import name-collision detection in `ImportManager.add_import`**: two
  `from … import …` lines that bind the same local name to *different*
  modules now raise `LHPValidationError` (LHP-VAL-021). This catches a class
  of silent shadowing bugs that affected `python` load/transform actions and
  (post-refactor) the new copy-and-import pattern. Existing projects with
  legitimately conflicting symbol names will see this as an error on the
  next regenerate; rename one of the conflicting symbols, or alias one of
  the imports, to resolve.

## [0.8.5] — 2026-04-24

### Added

- Job-config **pass-through**: any top-level key in `job_config.yaml` that is
  not one of LHP's explicitly handled keys (`max_concurrent_runs`, `queue`,
  `performance_target`, `timeout_seconds`, `tags`, `email_notifications`,
  `webhook_notifications`, `permissions`, `schedule`, `notebook_cluster`) is
  now rendered verbatim into the generated job YAML. Users can use
  newly-released Databricks Jobs API fields (trigger types, `continuous`,
  `run_as`, `git_source`, `health`, `parameters`, `environments`,
  `edit_mode`, `budget_policy_id`, …) without waiting for an LHP release
  that adds explicit support.

### Changed

- **Python dependency extraction tracks local-variable bindings**: patterns
  like `tbl = "cat.sch.t"; spark.read.table(tbl)` are now resolved.
  Reassignments and conditional branches emit the union of possible values.
  Variables whose value comes from function parameters, function return
  values, or string concatenation remain unresolvable — for those cases,
  declare an explicit `source:` on the action; for Python actions, parser
  output and explicit `source:` are unioned. Implemented as a new
  `_TableExtractor(ast.NodeVisitor)` with `_Scope`/`_Binding` classes in
  `utils/python_parser.py`, replacing the prior `ast.walk` over `ast.Call`
  that logged `ast.Name` arguments as unresolvable.
- **Jinja2 templates loaded via `PackageLoader`** instead of file-system
  loaders. Templates ship as package resources and are discovered through
  `importlib.resources`, removing the editable-install dependency on a
  source tree shadow. Applied in `utils/template_renderer.py`,
  `core/init_template_loader.py`, and `generators/base_generator.py`;
  `job_generator.py` retains `FileSystemLoader` for user-provided bundle
  template directories, which are not package resources.

### Fixed

- `lhp deps` now honors `trigger.file_arrival` (and any other Databricks
  Jobs API field) in `job_config.yaml`. Previously, keys the Jinja template
  didn't explicitly handle were silently dropped from the generated
  orchestration job YAML.
- **`lhp deps` extracts source tables from externalized write-target code**:
  materialized views and custom sinks using `write_target.sql_path`,
  `write_target.sql`, `write_target.module_path`, or
  `write_target.batch_handler` previously appeared to have no upstream
  dependencies, causing gold pipelines and silver-MV flowgroups to be
  reported as root nodes in the dependency graph. New
  `_iter_sql_bodies` / `_iter_python_bodies` generators in
  `core/services/dependency_analyzer.py` walk these externalized bodies
  the same way they walk inline SQL/Python.

## [0.8.4] — 2026-04-22

### Fixed

- **Missing `.j2` files in published wheels**: the `lhp.templates.monitoring`
  package was declared in `pyproject.toml` but its `*.j2` template files were
  not included by the wheel's `package-data` glob. As a result,
  `lhp generate` against a project that exercised monitoring would fail with
  `TemplateNotFound: monitoring/union_event_logs.py.j2` on installs from PyPI
  (editable installs from a checkout were unaffected because the source tree
  shadowed the missing package data). The package-data inclusion list now
  covers monitoring templates so wheel and editable installs render the same
  output.

## [0.8.3] — 2026-04-21

### Fixed

- **Cold-run race in the monitoring event-log union notebook**: the generated
  notebook ran *N* parallel streaming queries that each called
  `.toTable(TARGET_TABLE)`, so on a cold target table all *N* threads raced to
  `CREATE` the table — one thread won and the remaining *N−1* failed with
  `TABLE_OR_VIEW_ALREADY_EXISTS`. The notebook now pre-creates the target
  through an idempotent `_ensure_target_exists()` prologue that samples the
  schema from the first readable source event log before the
  `ThreadPoolExecutor` block starts. Existing projects must regenerate their
  monitoring notebook to pick up the prologue.

### Changed

- **Dedicated `monitoring.job_config_path` replaces the `__eventlog_monitoring`
  alias**: the monitoring workflow job is now configured from a separate,
  single-document `job_config.yaml` that describes only the monitoring
  workflow, rather than from a special-cased alias inside the shared
  `job_config.yaml`. `ProjectConfigLoader` validates that
  `monitoring.job_config_path` is set and that the file exists (tokenized
  paths are deferred to the orchestrator); `JobGenerator.generate_monitoring_job`
  now receives a resolved `job_config` dict from its caller. Users on 0.8.2
  with monitoring enabled should add `monitoring.job_config_path` to
  `lhp.yaml` and move the `__eventlog_monitoring` block from the shared
  `job_config.yaml` into the file it points at.

## [0.8.2] — 2026-04-17

### Added

- **Multi-CDC fan-in to one streaming table** (closes #113): multiple write
  actions in `mode: cdc` that share a `catalog.schema.table` target now
  combine into one `dp.create_streaming_table()` plus *N*
  `dp.create_auto_cdc_flow()` calls, each with its own `name=` and its own
  per-flow CDC parameters (`ignore_null_updates`, `apply_as_deletes`,
  `apply_as_truncates`, `column_list`, `except_column_list`). This is the
  CDC counterpart to the standard-mode append-flow fan-in LHP already
  supported. A new `CdcFanInCompatibilityValidator` raises `LHPConfigError`
  when shared fields (keys, `sequence_by`, SCD type, `track_history_*`,
  `partition_columns`, `table_properties`) disagree between fan-in
  participants, rejects mode-mixing on the same target, and rejects
  `source: [v1, v2] + mode: cdc` with guidance to split into one write
  action per source (this combination silently caused truncations on prior
  versions).
- **Per-pipeline event-log monitoring checkpoints** (fixes #96): replaces the
  single-query `UNION ALL` streaming flowgroup with a pair of artifacts whose
  checkpoints survive adding or removing monitored pipelines. A notebook
  (`monitoring/{env}/union_event_logs.py`) runs *N* independent streaming
  queries under a `ThreadPoolExecutor` with `trigger(availableNow=True)`, each
  with its own checkpoint at `{checkpoint_path}/{pipeline_name}`; a separate
  MV-only DLT flowgroup reads the populated Delta table; a Databricks
  workflow chains the two via `notebook_task` → `pipeline_task`. New required
  setting: `monitoring.checkpoint_path`. New tunables: `max_concurrent_streams`
  (default 10) and `enable_job_monitoring`.
- **`test` action type in the JSON schema**: `flowgroup.schema.json` now lists
  `"test"` in the action `type` enum (previously accepted only via runtime
  parsing).

### Changed

- **Env-scoped generation context replaces per-file composite checksums**: the
  per-file `file_composite_checksum` and `generation_context` on `FileState`
  are replaced by a single `last_generation_context` on `ProjectState`. The
  `--include-tests` flip detection is now a single env-wide comparison instead
  of *O(N)* per-file checksum recomputations. A new `StalenessCache` lets the
  display phase and per-pipeline filter share a single env-wide staleness
  scan per run, and `include_tests` is forwarded to orphan cleanup so
  `test_reporting_*` artifacts are reaped when the flag flips to `False`.
- **`generate` and `validate` now share one flowgroup discovery pass** rather
  than re-scanning at each phase; `FlowgroupDiscoverer` caches include
  patterns and the source-path index across calls within a run.
- **`lhp validate --include-tests` has parity with `lhp generate`**, filtering
  test actions out of validation when the flag is absent.
- **Hardened state persistence**: malformed or legacy state files now raise
  `LHPFileError` with actionable guidance instead of silently resetting.

### Deprecated

- **`{token}` substitution syntax**: docs, init templates, generator
  templates, and source comments are migrated to `${token}`. The deprecation
  is surfaced through `logger.warning` (rather than `DeprecationWarning`) so
  it reaches end users via normal CLI output. Migrate any `{token}` usages
  in YAML to `${token}`; the only legitimate non-`$` braces syntax remaining
  is `%{local_var}` for local variables.

## [0.8.1] — 2026-04-14

### Added

- **External test result reporting**: new `test_reporting` block in `lhp.yaml`
  generates a per-pipeline `_test_reporting_hook.py` event hook. The hook
  uses `@dp.on_event_hook` to accumulate DQ expectation results from
  `flow_progress` events and publishes them at pipeline terminal state via a
  user-supplied provider module declared by `module_path` and
  `function_name`. Actions gain an optional `test_id` field for linkage to
  external test management systems. Generated hooks, provider module copies,
  and `__init__.py` are tracked as pipeline artifacts and cleaned up when
  `test_reporting` is removed from `lhp.yaml`. `${token}` substitution is
  applied to provider module copies so secret/config tokens resolve at
  generate time.
- **Three built-in test-reporting providers**: `delta_test_reporter.py`
  (appends results to a pre-existing Delta table), `ado_test_reporter.py`
  (publishes to ADO Test Plans via a `test_case_mapping` config), and
  `ado_test_reporter_inline.py` (publishes to ADO where `test_id` is itself
  the ADO Test Case ID). Each implements the
  `publish_results(results, config, context, spark)` contract with
  `dry_run` support and structured logging.
- **`--include-tests` flag on `lhp validate`** for test-reporting validation
  parity with `lhp generate`.

## [0.8.0] — 2026-04-12

### Added

- **`source_function` parameters for snapshot CDC**: declare keyword arguments
  in `source_function` and have them bound via `functools.partial` at
  generation time. Makes snapshot functions reusable and testable outside LHP
  without baking substitution tokens into the function body. AST validation
  enforces keyword-only args (`*` separator) and rejects unknown parameter
  names.
- **`PerformanceTimer` utility** for structured timing instrumentation of
  generation phases.
- **Performance testing project** (`Example_Projects/performance_testing/`):
  synthetic 4000-flowgroup project across 100 per-domain pipelines (20
  domains × 5 layers) for realistic stress testing of discovery, staleness
  analysis, and generation.

### Changed

- **Large-project generation is ~8× faster**: the `find_source_yaml` *O(N×F)*
  bottleneck is replaced by a lazy source-path index on
  `FlowgroupDiscoverer`, reducing roughly 4M filesystem operations to a
  single-pass *O(1)* lookup — 500 s → 64 s for a 2000-flowgroup project. A
  per-run `ChecksumCache` with thread-safe locking ensures each file is read
  and hashed at most once during parallel generation; redundant discovery,
  hashing, and staleness analysis across phases of `generate` are
  eliminated; a single `CodeFormatter` instance is reused across worker
  threads (no more repeated `pyproject.toml` reads); and `--force` now wipes
  the output directory directly instead of running orphan detection over a
  pre-built `active_flowgroups` set.

### Fixed

- **CloudFiles `source.schema` now applies via `readStream.schema()` before
  `.load()`** instead of `df.schema()` after the load (#98). The previous
  ordering left Auto Loader unable to honor user-supplied schemas in some
  configurations.
- **CloudFiles path/file exclusion via `pathGlobFilter` and metadata-based
  filtering** (#87): Auto Loader pipelines can now exclude directories or
  individual files.

## [0.7.8] — 2026-04-08

### Added

- **`catalog`/`schema` namespace format** replaces the legacy
  `database: "catalog.schema"` field across load sources and write targets.
  YAML can now declare `catalog: my_cat` and `schema: my_schema` as separate
  keys. A new `namespace_normalizer` service transparently converts the old
  `database` shorthand and emits a deprecation warning, so existing projects
  keep generating identical output. Closes #100.
- **`source.schema` enforcement for CloudFiles loads**: the load action's
  `source.schema` is now applied on the `DataStreamReader` chain before
  `.load()` for Auto Loader sources, replacing the previously invalid
  post-load property access that silently dropped the user-supplied schema.
  New `source_schema_load` E2E fixture exercises the path end-to-end.
- **Mandatory pipeline configuration**: every pipeline must now have an
  explicit `pipeline_config.yaml`. The implicit fallback that derived
  catalog/schema from project-level defaults is deprecated and will be
  removed. Migration: add a `pipeline_config.yaml` (or per-environment
  override) for any pipeline that previously relied on the default lookup.
- **Supply chain security hardening**: all GitHub Actions are pinned to
  immutable SHA hashes; new workflows add `pip-audit` (SARIF), `bandit`
  (SARIF), `gitleaks`, `liccheck` license compliance, OpenSSF Scorecard,
  SLSA provenance generation, and reproducible builds via
  `SOURCE_DATE_EPOCH`. Adds `.pre-commit-config.yaml`, `CODEOWNERS`,
  `SECURITY.md`, Dependabot config, and pinned dev dependencies.

### Changed

- **Deterministic `depends_on` ordering** in generated orchestration job
  YAML. `job_generator` now sorts the `depends_on` list so generated job
  files are byte-identical across runs, eliminating spurious diffs when
  regenerating bundles.
- **Data quality templates use `withColumns`** (plural) in a single dict
  call instead of looped `withColumn` calls. Generated code is shorter
  and runs one projection per stage. Fixes #92.

### Fixed

- **CloudFiles description fallback** now formats the file format string
  correctly. Previously the fallback emitted the literal string
  `<built-in function format>` because the generator referenced Python's
  built-in `format()` instead of the `file_format` variable.
- **CloudFiles template dead batch branch** removed — the template had an
  unreachable batch read path that has been cleaned up alongside the
  schema-placement fix. Closes #98.

## [0.7.7] — 2026-03-17

### Added

- **Quarantine mode for data quality transforms**: new `mode: quarantine`
  on data-quality transform actions routes failed rows to a Dead Letter
  Queue table via `foreach_batch_sink` + `append_flow`, with automatic
  recycling of fixed records through Change Data Feed. Supports both
  CloudFiles and non-CloudFiles sources. Backed by a new `QuarantineConfig`
  model, schema and validator updates, a new `data_quality_quarantine.py.j2`
  template, and `get_all_expectations_as_drop()` in the DQE parser to
  coerce expectations into the quarantine pattern.
- **Delta load options validation**: the Delta load generator now
  validates the `options` block against the supported flag set and rejects
  incompatible combinations (for example, mixing `readChangeFeed` with
  options that don't apply to CDF reads). Documentation in
  `docs/actions/load_actions.rst` lists the supported options and their
  constraints.

### Fixed

- **Quote escaping in quarantine template**: applied the `tojson` Jinja2
  filter to the four remaining interpolation sites in
  `data_quality_quarantine.py.j2` (`inverse_filter`, `failed_rule_data`,
  and two related sites) to prevent `SyntaxError` when an expectation
  rule contains double quotes (e.g. `status = "active"`). The
  `_EXPECTATIONS` dict already used `tojson`; this brings the rest of the
  template in line.

## [0.7.6] — 2026-03-06

### Added

- **Declarative event log configuration**: new `event_log:` section in
  `lhp.yaml` with `catalog`, `schema`, and `name_suffix` (LHP token
  substitution supported). LHP injects an `event_log` block into every
  generated pipeline bundle resource automatically. Pipelines can override
  the project-wide setting (`event_log: {custom}`) or opt out
  (`event_log: false`) in `pipeline_config.yaml`. Closes #82.
- **Synthetic monitoring pipeline generation**: new `monitoring:` section
  in `lhp.yaml` generates a self-contained DLT pipeline that UNIONs every
  event log table in the project into a single streaming table and emits a
  default `pipeline_run_summary` materialized view (status, duration, and
  row metrics per update). Knobs: `pipeline_name`, `catalog`, `schema`,
  `streaming_table`, and `materialized_views` for custom MV definitions.
  The `__eventlog_monitoring` alias is recognised as a reserved pipeline
  target in `pipeline_config.yaml`.
- **`enable_job_monitoring: true`**: when set under `monitoring:`, LHP
  generates an additional `jobs_stats` materialized view via a Python load
  action that uses the Databricks SDK to correlate pipeline updates with
  the triggering job runs and enriches the output with both pipeline and
  job tags. The `jobs_stats_loader.py` is shipped as a package resource
  under `src/lhp/templates/monitoring/` and loaded via
  `importlib.resources`.
- **Instance pool support in pipeline clusters**: `instance_pool_id` and
  `driver_instance_pool_id` are now accepted in `pipeline_config.yaml`
  cluster blocks as alternatives to `node_type_id`. The Jinja2 template
  conditionally renders the pool fields and omits `node_type_id` when a
  pool is configured. Closes #83.
- **`sql_path` on materialized view write targets** and `batch_handler` /
  `foreachbatch` sink_type are now recognised by the Pydantic
  `WriteTarget` model, both JSON schemas, the MV field allowlist, and the
  MV validator. `custom_datasource` is also accepted as a load source
  type. Closes #85.
- **Self-contained materialized views** no longer require a load action.
  The validator and dependency resolver now exempt MV-only flowgroups
  that use `sql`, `sql_path`, or a CTE-only definition, so a gold MV can
  read directly from upstream tables without an explicit load.

### Changed

- **Documentation overhaul**: switched the Sphinx theme to Furo with dark
  mode and a custom OG image, added SEO `meta` descriptions to every
  page, reorganized the landing page (problem statement → value
  proposition → trimmed example → grouped features), and split the
  monolithic `actions_reference.rst` into per-type pages under
  `docs/actions/`. Extracted new standalone guides for substitutions,
  operational metadata, and dynamic templates. Toctrees are grouped into
  Getting Started, Configuration Guides, Deployment & Operations, and
  Reference. Added "Best Practices" sections. Closes #27.
- **Monitoring docs** now document `jobs_stats` as a materialized view
  (it was previously mis-typed as a streaming table) and include full
  schema tables for `events_summary` (16 columns) and `jobs_stats` (11
  columns).

### Fixed

- **Materialized view & write target validation stack (13 bugs)**: synced
  the validators, Pydantic models, and JSON schemas with what the
  generators and docs already supported. Removes the undocumented `name`
  alias for `table` across validator and generators, removes the invalid
  `path` field from the delta source allowlist, removes a dead
  `transform_fields` dict and unreachable dict-source handling from write
  generators, and fixes an orphaned transform to append (not raise) so
  accumulated errors like "must have at least one Load action" are
  preserved. Fixes the malformed
  `Reference_Templates/standard_ingestion.yaml`.

## [0.7.5] — 2026-02-17

### Added

- **Pipeline-level configuration entries** in `pipeline_config.yaml`:
  arbitrary Spark/DLT key-value pairs declared under a `configuration:`
  block are now rendered alongside the mandatory `bundle.sourcePath` in
  generated bundle resource YAML. Validation enforces a dict of
  string-only values, and `bundle.sourcePath` is filtered out to prevent
  duplication.
- **Pipeline environment dependencies propagation**: the `environment`
  section in `pipeline_config.yaml` is now rendered into generated bundle
  resource YAML, enabling pip package dependencies for DLT pipelines.
  Closes #74.
- **`lhp deps` refactor**: consolidated three duplicate source extraction
  implementations into a shared `source_extractor` module
  (`extract_action_sources`, `is_cdc_write_action`,
  `extract_cdc_sources`); replaced per-job re-analysis with NetworkX
  graph partitioning that filters the global graph by job membership;
  switched from `nx.find_cycle` (one cycle) to `nx.simple_cycles` (all
  cycles, capped at 20); and added a circular-dependency guard that
  skips job-format generation with a warning when cycles are detected
  while still emitting the other output formats.

### Changed

- **`lhp init` initializes in the current working directory** instead of
  creating a subdirectory; `project_name` is now used only for template
  rendering. The `--bundle` flag was flipped to `--no-bundle` so bundle
  (Databricks Asset Bundles) is the default. The generated bundle now
  includes a `bundle_uuid` field rendered into `databricks.yml`, and the
  `.gitignore` template adds `*.tmpl`, `.lhp/`, and `.bundle/` while
  dropping `.vscode/` (kept for IntelliSense schemas). Conflict detection
  switched from a directory-exists check to an `lhp.yaml` conflict
  check, with selective cleanup on failure instead of `shutil.rmtree`.
  Migration: existing users invoking `lhp init <name>` should now run it
  from inside the intended project directory and pass `--no-bundle` to
  opt out of bundle generation.
- **`DependenciesCommand` error propagation**: removed the
  error-swallowing `try/except` from `DependenciesCommand.execute()` and
  the `IOError` wrapping from `DependencyOutputManager.save_outputs()`.
  Errors now propagate to the CLI error boundary so failures are
  surfaced instead of silently downgraded.

### Fixed

- **SQL parser CTE name leak** that created false cross-CTE dependencies
  when one CTE referenced another; the parser now scopes CTE names per
  query and correctly handles subqueries and `UNION` / `INTERSECT` /
  `EXCEPT` set operations.

## [0.7.4] — 2026-01-19

### Added

- **Local variables in flowgroups**: new top-level `variables:` section in
  flowgroup YAML lets users define reusable values scoped to a single
  flowgroup, resolved before template parameters and environment
  substitution. Referenced via `%{var_name}` to reduce repetition and
  keep related values close to where they are used. Resolves #58.
- **Multi-target config for jobs and pipelines**: `job_name` and
  `pipeline` keys now accept a list of names in `job_config.yaml` and
  `pipeline_config.yaml`, applying the same configuration block to every
  entry. Duplicate names and empty lists are rejected with clear errors.
  Resolves #66.
- **Python 3.13 support**: CI now tests against Python 3.11, 3.12, and
  3.13. `pyproject.toml` dependencies updated to their latest compatible
  versions.

### Changed

- **Delta load action: unified `options` field.** YAML now uses a single
  `options:` map for Delta sources; the previous `reader_options`,
  `cdf_enabled`, and `cdc_options` fields are no longer supported and
  raise an error pointing users to the new structure. Migration: move
  keys from `reader_options:` and `cdc_options:` into `options:` (for
  example, `cdf_enabled: true` becomes `options: { readChangeFeed: "true" }`).
- **Minimum Python version raised to 3.11.** Python 3.8, 3.9, and 3.10
  are no longer supported. Resolves #72.

### Fixed

- Load generators (`CloudFiles`, `Delta`, `Kafka`) now validate that the
  `options` field is a dictionary and raise a user-friendly error rather
  than failing later during template rendering.
- `CodeFormatter` now logs the error type, traceback, and the first 500
  characters of the offending code when Black formatting fails, replacing
  silent or opaque failures.

## [0.7.3] — 2026-01-07

### Added

- **ForEachBatch sink**: new `foreachbatch` sink type lets users invoke a
  user-supplied Python `batch_handler` callable for each micro-batch
  produced by a streaming flow. Required keys (`module_path`,
  `batch_handler`) are validated up-front by `WriteActionValidator`, and
  the referenced module is tracked as a dependency so edits trigger
  regeneration. Documentation covers configuration, common use cases
  (REST APIs, external systems, custom merge logic), and best practices.
  Resolves #18.
- **Substitution in `python_transform` action fields**: `module_path`
  and `function_name` are now passed through the substitution engine,
  so values like `${python_modules_root}/cleanup.py` and `${env}_clean`
  resolve correctly per environment.

### Changed

- **Sink dependency tracking**: `StateDependencyResolver` now records
  `module_path` for both `foreachbatch` and `custom` sinks, so edits to
  those handler files participate in incremental regeneration alongside
  the YAML.
- Validators (`ConfigValidator`, `FlowgroupProcessor`,
  `PipelineValidator`) now re-raise `LHPError` as-is rather than wrapping
  it, producing consistent error formatting and preserving the original
  error context. Secret and flowgroup validation messages now include
  the detailed validation context.

## [0.7.2] — 2025-12-19

### Added

- **Per-source `readMode` for streaming tables**: each source in a
  streaming-table write action can now declare `readMode: stream` or
  `readMode: batch` independently. The generated code emits
  `spark.readStream` or `spark.read` per source, instead of forcing a
  single mode on the whole table. Resolves #22.
- **Parallel flowgroup processing**: `ActionOrchestrator` now processes
  flowgroups concurrently via a new `parallel_processor` module, with
  `PythonFileCopier` providing thread-safe file copy and conflict
  detection for the python-action copy step. Single-process behavior is
  preserved for diagnostics; speed-up is largest on projects with many
  flowgroups.
- **External-file dependency extraction from template parameters**:
  `StateDependencyResolver` heuristically detects file paths (schema
  files, SQL files, custom python modules) passed as template parameters
  and tracks them as dependencies, so edits trigger correct regeneration
  even when the path lives inside a template-expanded value.
- **Pipeline-generation summary**: generation now logs the number of
  files written vs. skipped by the smart writer, surfacing what the
  incremental path actually did.

### Changed

- **Cross-platform path normalization**: `StateDependencyResolver`,
  `DependencyTracker`, `StateCleanupService`, `PythonFileCopier`,
  `Action`, and `CodeGenerator` now normalize file paths to forward
  slashes via a new `utils/path_utils.py`. State files written on
  Windows and Linux are now interoperable, and generated python files
  use relative, environment-independent paths. Resolves #52.
- **Orchestrator refactor**: `Orchestrator` was restructured into helper
  functions for source extraction and batch processing. Action
  validators moved into a dedicated `validators/` package, and a new
  `OperationalMetadataService` centralizes operational-metadata handling
  (previously duplicated across generators). `generate_pipeline()` was
  removed in favor of the unified `generate_pipeline_by_field()` path.
- **Operational metadata import management**: `BaseActionGenerator` now
  consolidates metadata retrieval and import detection into a single
  `get_metadata_and_imports()` call, and registers expressions with
  `ImportManager` for semantic tracking. Imports declared by metadata
  expressions stay consistent with the file's actual imports.
- **Package description updated** in `pyproject.toml`, `README.md`, and
  `LLM.txt` from "Lakeflow Declarative Pipelines" to "Lakeflow Spark
  Declarative Pipelines", aligning with the upstream Databricks
  terminology adopted in 0.7.0.

### Fixed

- `create_table: false` is now correctly honored end-to-end (minor
  flow-only bug).
- `TableCreationValidator` now embeds a complete example configuration
  in its `LHPError`, so users can see the exact YAML shape needed to
  resolve a conflict rather than only the error class.

## [0.7.1] — 2025-11-26

### Added

- **Catalog and schema in `pipeline_config.yaml`**: new `catalog` and
  `schema` keys at the pipeline-config level let users set the Unity
  Catalog target per environment without repeating the value in every
  flowgroup. Values are validated and embedded in the generated
  `*.pipeline.yml` resource.
- **Multi-job orchestration via `job_name`**: flowgroups can now declare
  a `job_name`, grouping them into separate Databricks jobs. `lhp deps`
  generates per-job orchestration files plus a master orchestration job;
  an "all-or-nothing" validation rule prevents partially-tagged
  pipelines from producing an ambiguous job graph. Resolves #45.
- **Schema file support (YAML/JSON) in streaming-table and
  materialized-view writes**: `table_schema:` can now reference an
  external `.yaml`, `.yml`, or `.json` file in addition to inline
  DDL/SQL. A new `SchemaParser` converts the structured definition to
  DDL at generation time.
- **External-file schema for `cloudFiles.schemaHints`**: schema hints
  can now point at an external DDL or SQL file rather than being
  inlined. The referenced files are tracked by `StateDependencyResolver`
  so edits trigger regeneration.
- **`schema_transform` action: external files and strict/permissive
  modes.** The schema transform was reworked to support external schema
  files and fixed strict/permissive mode handling. Resolves #23.

### Changed

- **`schema` field renamed to `table_schema`** in write-action
  configuration. `schema` remains accepted for backward compatibility
  but is now documented as legacy; new code and examples should use
  `table_schema`. The rename disambiguates the field from PySpark's
  `schema` and from cloud-files schema hints.
- **Templates renamed** to drop characters that broke checkouts on
  Windows file systems.

### Fixed

- `SQLLoadGenerator` and `SQLTransformGenerator` now fall back to
  `Path.cwd()` when no `project_root` is available in the context,
  fixing failures in projects that invoke the API directly. Resolves
  #16.
- Error handling in `LakehousePlumberApplicationFacade` and
  `ActionOrchestrator` no longer duplicates error details across log
  lines and re-raises.

## [0.7.0] — 2025-11-10

### Changed

- **Generated code migrated from `dlt` to `pyspark.pipelines as dp`
  (Spark Declarative Pipelines API).** This is the headline change in
  0.7.0: Lakehouse Plumber now emits Lakeflow Spark Declarative
  Pipelines (SDP) code aligned with the current Databricks API,
  replacing the legacy DLT decorators across every code path.
  Specifically:
  - `import dlt` → `from pyspark import pipelines as dp`
  - `@dlt.table` → `@dp.materialized_view` (for materialized views)
  - `@dlt.view` → `@dp.temporary_view`
  - All `dlt.*` calls (e.g. `dlt.read_stream`,
    `dlt.create_streaming_table`) → `dp.*`
  - The deprecated `refresh_schedule` parameter on materialized views
    is no longer emitted.

  Import categorization also recognizes `pyspark.pipelines` as the
  DLT-equivalent module. Migration: YAML inputs are unchanged —
  regenerate the project (`lhp generate --env <env> --force`) to pick
  up the new decorators. Hand-edited `dlt.*` code in custom python
  actions must be updated to `dp.*` manually. Cluster/runtime must
  support the `pyspark.pipelines` API.

### Added

- **Sink framework for write actions.** New modular sink architecture
  (`BaseSink` + concrete implementations) lets write actions target
  destinations beyond Delta tables. Three sink types ship in 0.7.0:
  - **Delta sink**: writes to Delta tables with full streaming-table /
    materialized-view option support.
  - **Kafka sink**: streams output to Kafka topics with configurable
    serialization and partitioning. A dedicated `kafka_validator`
    checks broker URLs, topic names, and options up-front.
  - **Custom sink**: extensibility hook for user-supplied destinations.

  Sinks are configured under a `sink:` block in the write action.
  Existing Delta-only `write` actions continue to work unchanged.
  Resolves #17.
- **Unresolved-token validation** (`LHP-CFG-010`): a new
  `validate_no_unresolved_tokens()` step runs after substitution and
  recursively scans rendered config for stray `{token}` patterns
  (excluding `dbutils.secrets.get` references), with detailed context
  and fix suggestions. Circular substitution references are detected
  with a 10-iteration cap and surfaced as warnings.
- **`lhp show substitutions` command**: displays available substitution
  tokens for an environment, useful for diagnosing rendering issues
  caught by the new validator. Resolves #42.
- **Quarantine plumbing in expectations template**: the data-quality
  expectations Jinja template now uses named variables (fail / drop /
  warn) for expectation lists, in preparation for upcoming quarantine
  support. Resolves #39.

### Fixed

- Action-validator integration with the new write/sink schema; bundle
  manager updated to emit sink-aware pipeline resources.

## [0.6.5] — 2025-10-29

### Added

- **Kafka load source action**: new `type: kafka` load action generates
  code for `spark.readStream.format("kafka")` (or batch `spark.read`)
  with full option pass-through. Built on the same generator pattern as
  CloudFiles, with a dedicated `KafkaLoadGenerator`, Jinja2 template,
  action-registry entry, and config validator. Includes optional
  operational-metadata columns and works with both streaming and batch
  read modes. Closes #38.
- **Kafka auth reference templates**: shipped reference templates for
  the two most common managed-Kafka auth patterns — Azure Event Hubs
  with OAuth and AWS MSK with IAM authentication — plus documentation
  describing the required options and connection strings.

### Fixed

- **Quote/backslash escaping in load templates**: `cloudfiles`,
  `custom_datasource`, and `jdbc` load templates now correctly escape
  quotes and backslashes in option values. Previously, an option value
  containing a quote or a backslash (e.g., a regex pattern, a Windows
  path, or a JSON-encoded secret) could produce syntactically invalid
  generated Python or change the runtime value at generation time.

## [0.6.4] — 2025-10-29

### Added

- **Multi-flowgroup YAML files**: a single pipeline YAML file can now
  declare multiple flowgroups, in either multi-document syntax
  (multiple `---`-separated documents per file) or array syntax (a
  top-level list of flowgroup mappings). Previously, every flowgroup
  required its own file, which forced large pipelines into deep
  directory trees. Existing single-flowgroup files continue to parse
  unchanged. Closes #12, #28.
- **`lhp generate -pc <config-file>` regenerates DAB pipeline YAML on
  `--force`**: when the pipeline-config flag is supplied together with
  `--force`, the corresponding Databricks Asset Bundle pipeline YAML
  files under `resources/` are rewritten. Previously, `--force` only
  regenerated the Python pipeline code, leaving stale resource YAML on
  disk.
- **Anonymous usage telemetry with explicit opt-out**: `lhp generate`
  now emits aggregated, anonymous usage metrics (flowgroup and template
  counts, project/machine identifiers hashed) to help prioritize
  feature work. Two opt-out paths are honored: setting
  `LHP_DISABLE_ANALYTICS=1` in the environment, and running inside
  `pytest`. Documentation describes what is collected and how to turn
  it off.

### Fixed

- **Malformed YAML for cluster config in pipeline-config flow**: custom
  cluster blocks supplied via `lhp generate -pc` no longer produce
  syntactically invalid DAB pipeline YAML. Closes #37.

## [0.6.3] — 2025-10-28

### Fixed

- **Template-level presets are now applied** (#34): templates could
  declare a `presets:` list, but the field was missing from the
  `Template` model and the value was silently dropped — none of the
  listed presets were actually applied to generated actions. The
  `Template` model now carries `presets`, and `FlowgroupProcessor`
  applies them after template expansion with the documented precedence:
  flowgroup-level presets override template-level presets. Referencing
  a preset that does not exist now raises `ValueError` instead of
  failing silently. Projects that previously relied on the (broken)
  silent-skip behavior will see a hard error on regenerate — remove the
  bogus reference or create the missing preset file (e.g.,
  `bronze_layer.yaml`) to resolve.

## [0.6.2] — 2025-10-28

### Added

- **Customizable DLT pipeline configuration via YAML** (`-pc` /
  `--pipeline-config`): a new pipeline-config YAML file lets projects
  define DLT pipeline-level defaults (serverless, clusters,
  notifications, channel, edition, photon, configuration map, …) and
  per-pipeline overrides. The `BundleManager` loads this file and
  merges it into the generated DAB pipeline resource YAML, removing
  the need to hand-edit generated `resources/` files after every
  regenerate. Closes #13, #14, #31; resolves #29.
- **Customizable orchestration job configuration**: a complementary
  job-config file lets users set `max_concurrent_runs`, notifications,
  schedule/trigger, tags, and related Databricks Jobs API fields
  applied by `lhp deps` when generating the orchestration job YAML.
  Fixes #15, #21.
- **Bundle-mode job output to `resources/`**: when bundle output is
  enabled, generated job YAML files are written to the `resources/`
  directory so they are picked up by `databricks bundle deploy` without
  additional wiring. Fixes #26.

### Fixed

- **Python transform action: import resolution bug** affecting
  generated pipeline files has been corrected.

## [0.6.1] — 2025-10-01

### Fixed

- **Dependency detection bug**: a minor incorrect-edge issue in the
  `lhp deps` graph builder (introduced alongside the v0.6.0 dependency
  feature) has been corrected. Graphs now match the intended
  source-to-target relationships.

## [0.6.0] — 2025-10-01

### Added

- **Pipeline dependency analysis (`lhp deps`)**: new subcommand that
  walks every flowgroup, extracts source-table references from both
  SQL and Python action bodies, and produces a project-wide dependency
  graph. Output is available in multiple formats — `dot` (Graphviz),
  `json`, `text`, and `job` (a generated Databricks Jobs YAML that
  runs upstream pipelines before downstream ones), plus `all` which
  emits every format. Backed by a new `DependencyOutputManager`, a
  `PythonParser` that recognizes `spark.sql(...)`,
  `spark.read.table(...)`, and related patterns, and a `SQLParser`
  that handles joins, CTEs, and quoted / multi-part identifiers. This
  is the foundation `lhp deps` continues to build on in subsequent
  releases.

## [0.5.9] — 2025-09-15

### Changed

- **Architectural refactor: monolithic classes broken down into
  single-responsibility services.** `ActionOrchestrator` (~1300 lines),
  `StateManager` (~1300 lines), and the `lhp` CLI entrypoint (~1500
  lines) were decomposed into focused service modules. New modules
  include `core/services/` (`code_generator`, `flowgroup_discoverer`,
  `flowgroup_processor`, `pipeline_validator`,
  `generation_planning_service`), `core/state/` (`dependency_tracker`,
  `state_analyzer`, `state_persistence`, `state_cleanup_service`),
  `core/commands.py`, `core/factories.py`, `core/layers.py`,
  `core/strategies.py`, and `utils/template_renderer.py` /
  `utils/yaml_loader.py`. Public APIs were promoted from previously
  private methods so the orchestration pipeline is composable and
  testable. User-facing CLI behavior is unchanged.
- **CLI restructured into per-command modules under
  `src/lhp/cli/commands/`.** Each command (`generate`, `validate`,
  `init`, `show`, `state`, `stats`, plus `list_*`) now lives in its
  own file behind a shared `base_command.py`. The single 1500-line
  `main.py` is now a thin dispatcher.

### Removed

- **`src/lhp/bundle/yaml_processor.py` removed.** The legacy YAML
  processor module and its tests were deleted; bundle YAML
  modifications now go through `bundle/databricks_yaml_manager.py`
  (ruamel.yaml-backed) and the new template-renderer service. No
  user-facing YAML syntax change.
- **`LegacyGenerateCommand` and its 1300-line test suite removed.**
  Generation now flows exclusively through the new `GenerateCommand`
  implementation introduced earlier in the 0.5.x series.

### Fixed

- **Deterministic ordering in `BundleManager`.** Iterating over
  pipelines for bundle resource sync is now sorted, eliminating
  spurious diffs between consecutive `lhp generate` runs on different
  platforms.

### Added

- **End-to-end integration test fixture project under `tests/e2e/`**
  (`testing_project/`) covering bronze/silver/gold pipelines, test
  actions, custom Python functions, and a multi-environment
  substitution setup. Used by the new E2E suite to catch generation
  regressions across the whole orchestrator-to-bundle path.
- **CI: JUnit XML reports and Codecov integration.** Test runs in
  GitHub Actions now publish JUnit XML and upload coverage to Codecov.

## [0.5.2] — 2025-09-02

### Added

- **Automatic `databricks.yml` variable management.** `lhp generate`
  now scans generated Python files for the first `catalog.schema`
  pattern, locates the matching variable names in the current
  environment's `substitutions/<env>.yaml`, and writes the resolved
  per-environment values into the `variables:` block of every matching
  target in `databricks.yml`. Two variables are populated:
  `default_pipeline_catalog` and `default_pipeline_schema`. This
  removes the need to hand-edit `databricks.yml` after changing
  substitution values.
- **ruamel.yaml-backed `DatabricksYAMLManager`.** The new
  `src/lhp/bundle/databricks_yaml_manager.py` is the only place in LHP
  that uses ruamel.yaml; it preserves comments, quoting, and key order
  when updating `databricks.yml` variables. Other YAML operations
  continue to use PyYAML.
- New runtime dependency: `ruamel.yaml>=0.17.0`.

### Changed

- **Bundle pipeline-resource template now emits static variable
  references** (`${var.default_pipeline_catalog}` /
  `${var.default_pipeline_schema}`) instead of inlining catalog/schema
  values extracted from generated Python. Catalog/schema selection is
  now driven entirely by `databricks.yml` variables, which LHP
  populates from substitutions.
- **`resources/lhp/` is now flat** — per-environment subdirectories
  (`resources/lhp/dev/`, `resources/lhp/prod/`, …) were removed;
  resource files live directly under `resources/lhp/`. The bundle
  template references
  `${workspace.file_path}/generated/${bundle.target}/<pipeline>/**` so
  each environment still gets its own deployed artifact set. Existing
  projects regenerating with `lhp generate` will see resource YAML
  files relocate; commit the move.
- **`lhp init --bundle` scaffolding refreshed.** Generated
  `databricks.yml` now defines top-level `variables:` for
  `default_pipeline_catalog` / `default_pipeline_schema`, adds a `tst`
  target between `dev` and `prod`, and switches `prod`/`tst` to
  service-principal `run_as` and permission stubs.
- **Pre-flight validation of `databricks.yml` targets.** Generation
  now fails fast with `MissingDatabricksTargetError` if any
  substitution environment lacks a matching `targets.<env>` block in
  `databricks.yml`.

## [0.5.1] — 2025-09-01

### Added

- README link to the ReadTheDocs documentation site.
- `black>=23.0.0` is now a runtime dependency (previously only a
  dev-time requirement); generated code formatting works in
  environments that install LHP without dev extras.

## [0.5.0] — 2025-08-29

### Added

- **Test actions.** A new top-level `ActionType.TEST` plus nine
  `test_type` values — `row_count`, `uniqueness`,
  `referential_integrity`, `completeness`, `range`, `schema_match`,
  `all_lookups_found`, `custom_sql`, `custom_expectations`. Each test
  is emitted as a DLT expectation with a configurable `on_violation`
  of `fail`, `warn`, or `drop`. Tests are validated by a new
  `TestActionValidator` and generated by `TestActionGenerator`
  (`src/lhp/generators/test/`). See `docs/test_actions.rst`.
- **`lhp generate --include-tests` flag.** Test actions are skipped
  by default for faster CI builds; pass `--include-tests` (or run a
  test-only environment) to emit them. Flowgroups that contain only
  tests produce no Python file when `--include-tests` is not set.
- **Per-environment generated output.** `lhp generate --env <env>`
  now writes to `generated/<env>/<pipeline>/` by default instead of a
  single `generated/` tree, so dev/tst/prod artifacts coexist without
  overwriting each other. Override with `--output`.
- **`required_lhp_version` in `lhp.yaml`.** Projects can pin the
  framework to a PEP 440 specifier (`==`, `~=`, `>=,<`). `lhp generate`
  and `lhp validate` fail with a clear `LHP-CFG-007/008` error when
  the installed version is out of range. Bypass with
  `LHP_IGNORE_VERSION=1` (intended for emergencies, not production).
- **Per-environment bundle resources.** Bundle resource files are
  written under `resources/lhp/<env>/` so each target deploys its own
  resource set; the pipeline template emits `libraries.glob` and
  `root_path` paths scoped to the environment.
- **CI/CD reference documentation** (`docs/cicd_reference.rst`, ~2000
  lines) covering GitHub Actions, Azure DevOps, and Bitbucket
  workflows for Asset-Bundle deployments. A sample
  `lakehouse-cicd.yml` workflow ships in the ACME example project.
- New runtime dependency: `packaging>=23.2` (used for version-specifier
  checking).

### Changed

- **`lhp generate --format` flag removed.** Black formatting is now
  always applied to generated code; the redundant opt-in flag was
  dropped. Behavior is equivalent to the previous `--format` on, so
  no migration is needed beyond removing the flag from scripts.
- **`lhp generate` warns when the requested `--env` has no matching
  target in `databricks.yml`.** Generation continues, but the warning
  surfaces the missing target before deploy time.
- **Empty flowgroups are now skipped silently.** A flowgroup whose
  actions all evaluate to no-ops (typically a tests-only flowgroup
  without `--include-tests`) no longer produces an empty `.py` file
  or a state entry.

## Earlier Releases (v0.2.6-alpha – v0.4.1) — 2025-07-10 through 2025-08-18

Fourteen tagged releases (`V0.2.6-alpha` through `v0.4.1`) spanning
roughly five weeks of early development. These predate the v0.5.0
test-actions / per-environment-output milestone and were primarily
rapid internal iteration on the core surface area. Pull requests were
not yet routine; nearly all changes landed as direct pushes. What
follows is a condensation, not a per-version diary.

### Added

- **Initial PyPI release.** `V0.2.6-alpha` (2025-07-10) was the first
  tagged release on PyPI; the CI publish workflow and PyPI
  version-check guard landed shortly after, followed by ReadTheDocs
  and GitHub Pages publishing.
- **Databricks Asset Bundles integration** (`v0.3.1`, PR #6,
  2025-07-21). Introduced `src/lhp/bundle/` (manager, template
  fetcher, YAML processor, exceptions, bundle-detection), the
  `lhp init --bundle` flag, automatic resource-YAML synchronization
  under `resources/lhp/`, and the `databricks.yml.tmpl` scaffold. The
  flag became the default in v0.7.5.
- **State tracking and staleness detection** (`v0.2.12`–`v0.2.13`).
  Introduced `.lhp_state.json`, the `lhp state` command, the
  state-display service, composite-checksum staleness logic, and
  dependency discovery — the basis of incremental regeneration still
  used today.
- **`create_table` field and append-flow API** for streaming-table
  writes, with orchestrator-level cross-flowgroup validation
  (`V0.2.6-alpha`).
- **VS Code IntelliSense** for LHP YAML via `lhp setup-intellisense`,
  with JSON schemas and editor configuration (`v0.2.7`+).
- **Include-pattern filtering** in `lhp.yaml` to scope which YAML
  files are processed (`v0.2.7`).
- **Pipeline-field-based flowgroup discovery** — the orchestrator now
  groups flowgroups by their `pipeline:` field rather than directory
  layout, allowing one directory tree to contribute to many pipelines
  (`v0.2.7`).
- **Table tags** on streaming tables and materialized views
  (`v0.3.3`).
- **Temp-table transform** action (`v0.2.15`).
- **Operational metadata** integration across load and transform
  generators, with template-level column-sort logic.
- **Custom PySpark DataSource as a load action** (`v0.4.0`,
  2025-08-04). New `custom_datasource` generator and `import_manager`
  utility for detecting and rewriting user imports — the foundation
  of the `custom_datasource` / `custom_sink` surface area today.
- **Multi-platform, multi-version CI** — Linux, macOS, Windows across
  Python 3.8–3.12, with forward-compatible type-annotation refactors
  and Windows-specific logging fixes (`v0.3.6`).
- **`lhp init` Jinja2 template system** (`v0.3.4`). The scaffolded
  project layout — `pipelines/`, `presets/`,
  `substitutions/dev|tst|prod.yaml`, `expectations/`, `schemas/`,
  `templates/`, `bundle/` — was introduced here as `*.j2`/`*.tmpl`
  assets in `src/lhp/templates/init/`.

### Removed

- The legacy `src/lhp/notebook/` module (`deployment.py`,
  `interface.py`, `widgets.py`, ~1,700 LOC) was deleted in `v0.3.4`
  and replaced with the Jinja2 init-template loader. Projects
  scaffolded prior to v0.3.4 used a notebook-based deployment flow
  that no longer exists.

For per-commit detail across this era, see
`git log V0.2.6-alpha..v0.4.1`.

[Unreleased]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.6...HEAD
[0.8.6]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.5...v0.8.6
[0.8.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.4...v0.8.5
[0.8.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.3...v0.8.4
[0.8.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.2...v0.8.3
[0.8.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.1...v0.8.2
[0.8.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.8.0...v0.8.1
[0.8.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.8...v0.8.0
[0.7.8]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.7...v0.7.8
[0.7.7]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.6...v0.7.7
[0.7.6]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.5...v0.7.6
[0.7.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.4...v0.7.5
[0.7.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.3...v0.7.4
[0.7.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.2...v0.7.3
[0.7.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.1...v0.7.2
[0.7.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.7.0...v0.7.1
[0.7.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.5...v0.7.0
[0.6.5]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.4...v0.6.5
[0.6.4]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.3...v0.6.4
[0.6.3]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.2...v0.6.3
[0.6.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.1...v0.6.2
[0.6.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.6.0...v0.6.1
[0.6.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.9...v0.6.0
[0.5.9]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.2...v0.5.9
[0.5.2]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.1...v0.5.2
[0.5.1]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.5.0...v0.5.1
[0.5.0]: https://github.com/Mmodarre/Lakehouse_Plumber/compare/v0.4.1...v0.5.0
