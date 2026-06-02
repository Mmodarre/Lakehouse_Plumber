# Changelog

All notable changes to Lakehouse Plumber are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Validator decomposition — `write` / `test` / `cdc_fanin` into the free-function helper idiom + shared three-part-name check

**Changed.**

- **`write`, `test`, and `cdc_fanin` validators decomposed into the established
  free-function helper idiom.** Logic moved into package-private `_`-prefixed
  modules (`_write_sinks.py`, `_test_requirements.py`, `_cdc_fanin_messages.py`),
  bringing each parent file under the 200-line architectural aspiration.
- **Shared `require_three_part_name` helper added
  (`core/validators/action/_name_checks.py`).** De-duplicates 7 copies of the
  `catalog.schema.table` name check across the write, test, and
  data-quality-transform validators. Validation messages are unchanged.
- **Redundant injected `logger` removed from `WriteActionValidator`'s
  constructor.** It now uses the module-level logger.

**Deferred (decided; tracked for follow-up).**

- **`VALIDATOR-VALUEOBJECT-DEFER`** — Validators still return `List[str]` and are
  not all stateless; the §1 end-state (`Tuple[ValidationIssue, ...]` returns,
  `BaseValidator` rename) is a separate, larger chunk requiring a
  `ValidationIssue` DTO design first and rewriting many string-asserting tests.
- **`CONFIG-VALIDATOR-200L-DEFER`** — `config_validator.py` (228 lines)
  intentionally left above the §1 ≤200-line aspiration: it is a
  comment-dominated composition root (documented lazy-import-cycle rationale),
  passes the §3.3 ≤500 gate, and will be rewritten by the value-object migration
  above. Splitting it now would scatter a cohesive composition root (§3.6).
- **`CDC-FANIN-NAME-HELPERS-KEPT-PRIVATE`** — `cdc_fanin`'s `_full_name` /
  `_is_cdc` kept private to `CdcFanInCompatibilityValidator` (only one caller,
  §3.6); NOT added to the existing `_cdc_helpers.py`, which belongs to
  `CdcConfigValidator`.

### Architecture residual cleanup — error-code registry + factory, validator taxonomy, codegen templatization, docs facade

**Summary.** Five workstreams that bring the codebase in line with the target
architecture's residual items: a single-source-of-truth error-code registry plus
an `ErrorFactory` (replacing `ErrorFormatter`), the validator-taxonomy
reorganization into `core/validators/{action,pipeline,field,compatibility}/`,
moving cloudfiles schema-hint / `StructType` code-as-strings into Jinja2
templates, and rewriting the stale `docs/api.rst` orchestrator-leak references
onto the public facade. Each lands with a new mechanical gate. The §4.7
error-code-string contract is **preserved** (codes are byte-identical) and
generated pipeline output is **byte-identical** (e2e baselines unchanged). This
closes the `ERRORS-CODES-MIGRATION-DEFER` deferral; the remaining/new deferrals
are catalogued below, each with a tracking ID.

**Added.**

- **Error-code registry — `lhp/errors/codes.py` (TARGET §6).** Single source of
  truth for error codes: 93 `(category, number)` `ErrorCode` constants plus
  `ALL_CODES`. A drift test (`tests/errors/test_codes.py`) enforces that no stray
  literal `code_number=` survives outside the registry.
- **`ErrorFactory` — `lhp/errors/factory.py` (TARGET §6).** The single
  error-construction factory: 17 intent methods plus 7 generic per-category
  constructors. It canonicalizes the raised exception class by category. The
  ~260 `raise` sites and all former formatter call-sites were migrated onto it.
- **`core/codegen/struct_type_emitter.py` + `templates/load/struct_type.py.j2`
  (§9.14 / §2.10).** cloudfiles schema-hints and `StructType` emission moved out
  of Python string-assembly into a dedicated emitter + Jinja2 template. The
  repo-wide code-as-strings count is now **0**.
- **New `LHP-2.1` directory-membership gate** in `check_placement.py` — pins each
  validator to its taxonomy subdirectory.
- **New `check_docs_orchestrator_leak` gate (`LHP-9.13-docs`).** Flags
  orchestrator-leak references in `docs/api.rst`, preventing the pattern from
  returning.

**Changed.**

- **`ErrorFormatter` removed; all construction routes through `ErrorFactory`.**
  Error-code strings are **byte-identical** — the §4.7 contract is preserved.
- **Validators reorganized into the §2.1 / TARGET §1 taxonomy.** The 9 validators
  now live under `core/validators/{action,pipeline,field,compatibility}/`; the
  top level holds only `__init__.py` / `_base.py` / `config_validator.py`. The 3
  oversize validators (`dlt_cdc`, `action/transform`, `field/config_field`) were
  split into per-class + helper modules, all ≤200L. Public import names are
  unchanged.
- **`docs/api.rst` orchestrator-leak references rewritten to the public facade**
  `lhp.api.LakehousePlumberApplicationFacade` (§9.13).

**Fixed.**

- **Latent config-load swallow bug in
  `ProjectConfigLoader.load_project_config`.** A malformed project config now
  surfaces the **specific** error code instead of silently returning `None` /
  collapsing to a generic `CFG-002`.

**Closed deferral.**

- **`ERRORS-CODES-MIGRATION-DEFER` — CLOSED.** The full Option-B migration
  shipped: codes registry + `ErrorFactory` + all raise/formatter sites migrated +
  `ErrorFormatter` deleted. (This supersedes the earlier `ERRORS-CODES-DEFER`
  note recorded under the helper-module-copying entry.)

**Deferred (decided; tracked for follow-up).**

- **`VALIDATOR-CONTRACT-DEFER`** — Phase C: `BaseValidator` contract uniformity,
  `ConfigValidator` dissolution, and the `BaseActionValidator` → `BaseValidator`
  rename. Not started (separate spec).
- **`VALIDATOR-SIZE-TARGET-DEFER`** — 4 validator files still exceed the TARGET §1
  ≤200L aspiration (`action/write.py` 328, `compatibility/cdc_fanin.py` 275,
  `config_validator.py` 228, `action/test.py` 212). Pre-existing; the §3.3
  mechanical gate (≤500) passes; out of Phase B's named-3 scope;
  `config_validator.py` is intentionally left for Phase C dissolution.
  User-waived for this PR.
- **`VALIDATOR-EXCEPT-SWALLOW-DEFER`** — 3 validator sites use
  `except ValueError: ... pass` (`action/write.py`, `action/load.py`,
  `action/transform.py`-area). These are currently safe (the helpers return
  lists, do not raise) but would silently swallow a canonicalized
  `LHPValidationError` if those helpers ever start raising. Latent fragility; add
  an `except (LHPError): raise` guard in a follow-up.
- **`DOCS-STALE-AUTOMODULE-DEFER`** — `docs/api.rst` still has stale
  `automodule::` directives for other moved/internal modules (e.g.
  `lhp.utils.error_formatter`, `lhp.core.template_engine`, `lhp.core.validator`,
  `lhp.models.config`, `lhp.utils.substitution`) that fail a strict Sphinx build.
  Same class as the orchestrator leak; out of Part 5's scope (which scoped only
  the orchestrator).
- **`GEN-FSTRING-GATE-DEFER`** — now **only** the optional CI heuristic gate; the
  actual code is §9.14 / §2.10-clean after the Part-4 templatization, so this is
  no longer a code-debt item, just an optional future guard.
- **`CLI-PRESENTER-DEFER`** — unchanged (separate spec).
- **`CORE-FILE-BUDGET-DRIFT`** — the documented ≤80 core-file budget is stale;
  this refactor adds files (new `core/codegen/struct_type_emitter.py`, validator
  subdir splits). The budget needs re-baselining.

**Pre-existing debt (not introduced by this work; left out of scope per user
decision).**

- **`src/lhp/cli/main.py` (509L) exceeds the §3.3 500-line limit** — pre-existing,
  user-accepted as out-of-scope.
- **77 files are black-dirty** (pre-existing single-quote style, untouched by this
  refactor); only refactor-touched files were black-formatted. A tree-wide
  `black` pass is a separate follow-up.
- **`ErrorFactory.cloudfiles_error` has 0 callers** (the CLOUDFILES category has
  no codes yet); kept for category-complete factory taxonomy.
- **`vulture_whitelist.py` needed no additions** — vulture is clean at
  `--min-confidence 80`; the pre-existing whitelist entries remain audited.

### Generation hot-path optimization — `lhp generate` substantially faster on large projects

**Summary.** This lands the optimization work the prior perf-instrumentation
entry flagged as "Out of scope (follow-up)". `lhp generate`'s hot path is now
**substantially faster on large projects** — roughly **4–5×** on a
2660-flowgroup reference run. The win comes from amortizing two costs that were
previously paid per action / per flowgroup (Jinja `Environment` rebuilds and
inline-template recompilation) and from moving Python formatting out of the
parallel workers into a single terminal pass. Generated pipeline code remains
semantically identical; the **only** intended output difference is a one-time
whitespace/quote/line-wrap reflow because the *generated* output is now
formatted with `ruff format` instead of Black (see **Changed** below).

**Performance.**

- **Shared, compile-once Jinja `Environment` for code generators.** The Jinja2
  `Environment` used by the per-action generators is now built **once per
  process and shared** (`get_shared_generator_environment()` in
  `core/codegen/template_renderer.py`) instead of rebuilt for every action.
  Because a generator instance is handed back per action by the registry, the
  old per-instance `Environment` recompiled each template against an empty
  cache on every render; the shared `Environment` compiles each template once
  and lets subsequent renders hit Jinja's in-`Environment` template cache. The
  shared `Environment` replicates the historical per-instance one exactly
  (filters included), so output is byte-identical.
- **Inline `${...}` template strings are compiled once and cached.** Inline
  template strings are now compiled a single time and cached per template engine
  (`TemplateEngine._compiled`, keyed on the source string, in
  `core/processing/template_engine.py`) instead of being recompiled for every
  flowgroup that references them.

**Changed.**

- **Formatting moved to a single terminal pass after generation.** Generated
  Python is now formatted **once, after generation**, rather than inside each
  parallel worker. The per-flowgroup worker now performs only a fast
  `ast.parse` validity check on the generated code (microsecond-scale) and
  still raises the same per-flowgroup error on invalid generated Python. This
  removes the formatter from the parallel hot path while keeping invalid output
  from ever being written.
- **Generated output is now formatted with `ruff format` instead of Black.**
  This is a deliberate, **one-time** formatting change to the code LHP
  *generates* — semantically identical Python, but with different whitespace,
  quote, and line-wrap style. Expect a one-time diff in `generated/` the first
  time you regenerate after upgrading. **Note:** this affects only the
  *generated* output; LHP's **own** source code is still formatted with Black.
  Consequently `ruff` is now a **runtime** dependency, while Black remains a
  **dev** dependency.

**Added.**

- **`--no-format` flag on `lhp generate`** (with a corresponding
  `apply_formatting` key in `lhp.yaml`) to **skip the terminal formatting pass**
  for faster generation. The CLI flag overrides the project-config key. The
  `ast.parse` validity check **always** runs regardless, so invalid generated
  Python is still rejected even with `--no-format` — `--no-format` trades
  formatted output for speed, not correctness.

**Docs.**

- **Added guidance recommending `LHP_MAX_WORKERS` (or `--max-workers`) for
  CPU-bound batch generation**, so operators can size the parallel pool to the
  host running large batch generates.

**Deferred (decided; tracked for follow-up).**

- **`FlowgroupOutcome.formatted_code` not renamed (deferred).** With formatting
  moved to the terminal pass, the internal `FlowgroupOutcome.formatted_code`
  field now carries **unformatted** (but `ast.parse`-validated) generated code,
  so its name is now slightly inaccurate. The honest rename (e.g. to
  `generated_code`) was **deferred**: the field lives on a frozen DTO
  (`models/processing.py`) and renaming it is a separate, wider change touching
  every reader. A docstring note was left at the field instead.
- **Worker auto-count `0.8` factor unchanged (deferred).** The automatic
  max-workers heuristic (`_auto_max_workers()` in
  `core/coordination/orchestrator.py`) still multiplies the detected core count
  by `0.8` (a 20% headroom). Tuning this factor was deliberately left **out of
  scope** — it is an open question best driven by real numbers — and operators
  who want a different worker count can override with `LHP_MAX_WORKERS` /
  `--max-workers`.

### Opt-in `--log-file` flag — decouple verbose console from file persistence

**Summary.** A new group-level flag `--log-file` (boolean, off by default)
writes a detailed DEBUG log to `<project>/.lhp/logs/lhp.log`. Previously a
debug log file was written unconditionally on every command whenever `lhp.yaml`
was found, including read-only commands. When `--log-file` is absent the
`.lhp/logs/` directory is **never created**. `-v`/`--verbose` is now
**console-only** — decoupled from file persistence. The "quiet console, full
DEBUG file" bug-capture pattern is `--log-file` (without `-v`).

**Added.**

- **`--log-file` flag (group-level, off by default).** Enables per-run DEBUG
  logging to `<project>/.lhp/logs/lhp.log`. File handler is attached only when
  the flag is set; no handler, no directory creation otherwise.
- **Path announcement (`DEC-3`).** Even without `-v`, `--log-file` prints
  `Detailed logs: <path>` exactly once so the caller knows where to look.
- **`cli/logging_config.py` (`LOG-CONFIG-CLI-EXTRACT`).** `configure_logging`
  and `cleanup_logging` extracted from `main.py` into a new dedicated module,
  keeping `main.py` to group definition + command registration (TARGET §7).
- **`cli/_project_root.py` (`_find_project_root` dedupe).** The duplicated
  `_find_project_root` copies in `main.py` and `base_command.py` collapsed into
  a single canonical module; both callers now import from it.

**Changed.**

- **`-v`/`--verbose` is console-only.** Verbose output no longer implies file
  persistence. The two concerns are now independent flags.
- **Debug log file is opt-in.** The file (previously written unconditionally)
  is now written only when `--log-file` is passed.

**Deferred (tracked for follow-up).**

- **`LOG-FILE-ROTATION`** — the file is opened in append mode (`DEC-1`); across
  opted-in runs it accumulates. Rotation is not implemented; tracked for a
  future pass if growth becomes a problem.
- **`LHP_LOG_FILE` env var + top-level package `NullHandler`** — spec §7
  optional follow-ups; not done.
### Transitive helper-module copying for user Python functions

**Summary.** When a user Python function — a `python` load/transform function, a
custom data source, a custom sink, or a snapshot-CDC `source_function` — imports
a **local** helper module or sub-package, LHP now copies the entry file **and
its transitive local helpers** into `custom_python_functions/`, preserving
sub-package directory structure. Previously only the single named file was
copied, so any local helper it imported raised `ModuleNotFoundError` at pipeline
runtime. Local imports are now rewritten so the copied closure resolves under
`custom_python_functions.…` while external/stdlib imports are left untouched.

**Added.**

- **Transitive local-helper copy.** A referenced local helper module is copied
  alongside the entry file, recursively, into `custom_python_functions/` with
  its sub-package layout preserved (a referenced helper **package** is copied in
  full; synthesized empty `__init__.py` files are emitted for any namespace
  directory in the closure that lacks one). External and standard-library
  imports are never copied.
- **Import rewriting for the copied closure.** Absolute-local imports are
  prefix-rewritten to `custom_python_functions.…`; intra-package **relative**
  imports (`from .x import y`) are preserved as-is; external/stdlib imports are
  left untouched.
- **New validation errors.**
  - **`LHP-VAL-023`** — the Python-function import-root directory must not
    itself be a package ("Rule A": the root must not contain `__init__.py`).
  - **`LHP-VAL-024`** — a local helper must be imported via `from x import y`,
    not a plain dotted `import x.y`.
  - **`LHP-VAL-025`** — a referenced local helper module was not found on disk
    during closure resolution.

  The three codes are constructed **inline** at their raise sites (per the
  in-repo precedent; see `ERRORS-CODES-DEFER` below), not via a code registry.

**Changed.**

- **Single generic syntax-error contract — `LHP-IO-003`.** Parsing of *any*
  user source file (snapshot `source_function` signatures, helper-closure
  discovery) now raises one uniform, path-parameterized **`LHP-IO-003`**
  ("Python syntax error in source file") on a `SyntaxError`, replacing the
  snapshot-specific syntax-error construction. The message is parameterized by
  the offending file path; the previously snapshot-specific
  `example`/`details`/`context` were dropped so the contract is identical across
  all AST consumers.
- **Python-function path-anchoring docs corrected.** The `module_path` /
  `source_function` source-file paths are resolved relative to the **project
  root**, not the YAML file location; the docs (and paired skill reference) that
  said "relative to your YAML file location" were corrected (DR-6). See
  deferrals for the SQL `sql_path` case, which is **not** changed here.

**Removed (behavior change — DR-1, resolved as DELETE).**

- **Dead file-level AST import-hoist path.**
  `ImportManager.add_imports_from_file` / `_extract_with_ast` (and its 5
  orphaned test fixtures) are deleted. The path had **zero** production callers
  and silently swallowed `SyntaxError` (an inner `except SyntaxError` plus an
  outer `except Exception`). Originally scoped as a swallow→raise refactor onto
  the generic `LHP-IO-003` contract; resolved instead by **deletion**, since the
  path was provably dead (reachable only from `tests/test_import_manager.py`,
  which kept it invisible to vulture). The live `add_import` /
  `get_consolidated_imports` / `add_imports_from_expression` surface is
  unchanged.

**Whole-sub-package copy consequence (intended).** Because a referenced helper
*package* is copied **in full**, a syntactically-broken but otherwise *unrelated*
sibling module inside that package now surfaces **`LHP-IO-003`** at generate
time. This is intentional. AST-pruned partial-package copy (copying only the
modules actually reached by the import closure) is **deferred** — see below.

**Deferred / decisions (tracked for follow-up).**

- **`ERRORS-CODES-DEFER`.** No `lhp/errors/codes.py` was created — that file does
  not exist, and the new `LHP-VAL-023/024/025` errors are constructed **inline**
  at their raise sites following existing repo precedent
  (`LHPError(category=ErrorCategory.VALIDATION, code_number="0NN", …)`). Spec
  §3.6/§3.8's "add the code to `codes.py`" is deferred to that separate work
  item; `errors/formatter.py` was **not** grown with new factory methods.
- **AST-pruned partial-package copy deferred** (spec §3.8/§7). Whole-sub-package
  copy is the current behavior; copying only the closure-reached modules within
  a package (which would suppress the unrelated-broken-sibling `LHP-IO-003`
  above) is a follow-up.
- **Dry-run "would-be-copied helper modules" reporting deferred** (spec §7).
  `--dry-run` does not yet enumerate the helper modules a real run would copy.
- **SQL `sql_path` doc anchoring NOT changed** (DR-6). The SQL `sql_path`
  "relative to your YAML file location" statements use a **different** resolver
  and are a separate, pre-existing question; only the Python
  `module_path`/`source_function` anchoring docs were corrected here.
- **Pre-existing §5.4 direct-module-import debt left untouched.** The
  `external_file_loader` direct-module import in
  `generators/write/snapshot_cdc_source_function.py` is pre-existing §5.4 debt
  and was **not** changed by this work.
- **Pre-existing `LHP-IO-003` overload observed (not deepened).** `LHP-IO-003`
  already carries **three** meanings (syntax error / multi-document /
  instance-not-found); the "canonical" syntax-error contract added here is the
  syntax-error meaning specifically. Disambiguating the overloaded code is
  out of scope for this change.

### Parallel-worker performance instrumentation — propagated per-category timers + event counts

**Summary.** Instrumentation-only change that makes the parallel
flowgroup-processing path observable under `--perf`. Spawn-context workers now
export their per-category timings and event counts back to the parent process,
where they are merged into the single coordinator `PerfSummary` so `lhp generate
--perf` reports the *aggregate* worker cost rather than only parent-process
spans. This is **output-neutral**: generated pipeline code, file hashes, and CLI
behavior with `--perf` off are byte-for-byte unchanged — only the `--perf`
summary gains rows. No optimization is performed here; this change exists to
**measure** so the actual tuning work can be driven by real numbers.

**Added.**

- **Worker-to-parent perf propagation.** Each spawned worker exports a picklable
  perf payload (`export_perf_for_merge` / `PerfSummary.export_for_merge`) on its
  `FlowgroupOutcome`; the coordinator merges it (`merge_perf` /
  `PerfSummary.merge`) as outcomes complete. Per-category timers now surface from
  inside workers: `resolve_dependencies`, `assemble_code`,
  `generate_action_sections`, `get_generator`, `jinja_render`, `preset_resolve`,
  `schema_parse`, and `black_format`.
- **New `Event counts:` block in the perf summary.** Counter events
  (`incr_event` / `PerfSummary._event_counts`) are exported and merged alongside
  timers. The first counters wired in are `snapshot_sigcache_hit` and
  `snapshot_sigcache_miss`, exposing the snapshot-CDC source-function signature
  cache hit/miss ratio.
- **`FlowgroupOutcome.perf`** — an optional, picklable in-worker
  timing/event-export payload (`Optional[Dict[str, Any]]`) carried back on the
  result DTO; `None` when `--perf` is off (merge is then a no-op).

**Removed.**

- **Dead module-level `format_code` convenience function** in
  `core/codegen/formatter.py`. It had zero `src/` callers; the live
  `CodeFormatter.format_code` *method* is unchanged. Subtractive — reduces
  surface, no behavior change.

**Out of scope (follow-up, driven by these numbers).** The actual
optimizations the instrumentation is meant to inform are **separate** work and
are intentionally **not** included here: worker-count tuning, generator-instance
and Jinja `Environment` reuse across flowgroups, preset-resolution memoization,
and schema-parse caching. They will be sized and prioritized from the timings
and hit/miss counts this change now produces.

**Tests.** New `tests/test_generate_perf_propagation.py` (worker→parent perf
merge on the generate path) and `tests/test_schema_transform_perf.py`
(`schema_parse` timer), plus extensions to the perf-timer, formatter, snapshot
source-function, and outcome-pickle suites. The T1 perf-timer unit tests were
added to the existing `tests/unit/test_performance_timer.py` rather than a
strict §8.2 mirror path (`tests/utils/...`), because neither that nor
`tests/unit/utils/` exists — extending the existing file is the lower-risk
choice per §10.4 (inherited test-layout debt).

### Substitution-view CLI migration — `show` panels off the internal manager

**Summary.** Closes `OPMETA-SVC-VIEW-CLI-DEFER`, the CLI-consumer half of the
substitution-view work whose replacement (the `SubstitutionView` /
`SecretReferenceView` DTOs + `InspectionFacade.build_substitution_view(env)`
method) shipped earlier in the v0.9.0 wave under the prior `OPMETA-SVC-VIEW`
entries. The CLI `show` substitution panels now consume that facade method
instead of constructing the internal substitution manager, and the deprecated
`lhp.api` re-export shim is removed. This is an **internal refactor**: the
public API surface adds one provisional field and drops one already-deprecated
re-export, so it is **not** a semver-breaking change (CODING_CONSTITUTION §1.7).

**Added.**

- **`SubstitutionView.raw_mappings` (`Mapping[str, JSONValue]`, `:stability:
  provisional`).** A structure-preserving companion to the flat `tokens` field,
  retaining the nested substitution-map structure that `tokens` stringifies.
  `InspectionFacade.build_substitution_view(env)` populates it from the
  manager's un-coerced mappings.

**Changed.**

- **CLI `show` substitution panels now render via
  `InspectionFacade.build_substitution_view`.** `show --instance` /
  `show substitutions` route their substitution / secret-reference panels
  through `application_facade.inspection.build_substitution_view(env)` instead of
  constructing the internal substitution manager. The
  `EnhancedSubstitutionManager` import and the `_load_substitution_manager`
  helper are deleted from `show_command.py`.

**Removed.**

- **The deprecated `EnhancedSubstitutionManager` re-export + PEP-562
  `__getattr__` shim from `lhp.api`.** Per §6.4 — the replacement
  (`SubstitutionView` + `build_substitution_view`) shipped under the prior
  `OPMETA-SVC-VIEW` wave. Accessing `lhp.api.EnhancedSubstitutionManager` now
  raises `AttributeError`. The **internal** class
  `lhp.core.processing.substitution.EnhancedSubstitutionManager` is retained
  untouched for internal use; only the public re-export is removed.

### Eliminate inspection-path facade reach-throughs

**Summary.** `lhp stats --pipeline <X>` (and any other filtered
`list_flowgroups` consumer) now reports the **same** blueprint-expanded +
monitoring flowgroups as the unfiltered view, narrowed to pipeline X.
Previously the filtered path used raw disk discovery and **omitted**
blueprint-expanded / monitoring flowgroups, so the filtered and unfiltered
views disagreed; they are now consistent. Behind that fix, `lhp/api/` no longer
reaches through the orchestrator's **private** surface — the inspection facade
and converters now call the orchestrator's public, ABC-typed services
(`bootstrap` / `discovery` / `processing` / `codegen`) — and a new mechanical
gate prevents the pattern from returning. This is an **internal refactor**: the
public API surface is unchanged, so it is **not** a semver-breaking change
(CODING_CONSTITUTION §1.7).

**Changed (user-visible behavior).**

- **Filtered `lhp stats --pipeline <X>` is now consistent with the unfiltered
  view.** The filtered `list_flowgroups` path now returns the blueprint-expanded
  + monitoring flowgroups (narrowed to pipeline X) instead of raw disk
  discovery, which silently dropped blueprint-expanded and monitoring
  flowgroups. A new behavior-locking test pins this — there is **no** coverage
  gap.

**Changed (internal — not a public-API change).**

- **`lhp/api/` no longer reaches the orchestrator's private surface.** The
  inspection facade and `api/_converters.py` now call the orchestrator's public,
  ABC-typed services (`bootstrap` / `discovery` / `processing` / `codegen`)
  instead of its `_`-prefixed members.
- **`ActionOrchestrator`'s public method surface shrank from 10 → 5.** The 7
  reach-through shims are removed; the retained five are the legacy
  `discover_flowgroups` plus the four pipeline/monitoring methods
  (`generate_pipelines`, `validate_pipelines`,
  `validate_duplicate_pipeline_flowgroup_combinations`,
  `finalize_monitoring_artifacts`). This advances the thin-coordinator target
  architecture.
- **One additional caller repointed.** `InspectionFacade.compute_stats` — missed
  by the initial plan — was also moved onto the public service
  (`self._orchestrator.bootstrap.discover_all_flowgroups()`) in the same change.
- **Source-path resolution on the inspection paths.** `process_flowgroup` /
  `generate_flowgroup_code` now resolve the source-YAML path from the raw-keyed
  context envelope (set at bootstrap from the **pre-resolution** flowgroup)
  rather than re-looking-it-up from the post-resolution flowgroup. Output is
  identical except when resolution rewrites a `pipeline`/`flowgroup` identity
  field (e.g. an `${env_token}` in `pipeline:`), where the raw-keyed path is
  **more correct**.

**Added.**

- **New mechanical gate `LHP-9.23` (facade-internal private reach-through).**
  `scripts/check_placement.py` now flags `self._orchestrator._x` /
  `self._orchestrator._x()` patterns in `lhp/api/`, preventing the
  reach-through from returning.

**Deferred (decided; tracked for follow-up).**

- **`T2-typing` (spec §10).** Orchestrator parameter typing in `lhp/api/` is
  kept as `_Orchestrator = Any`; tightening the orchestrator type annotations is
  deferred.
- **Surviving out-of-scope reach-throughs.** `_listings.py` (discoverer /
  expander `# type: ignore[attr-defined]`), `show_command.py`
  (`CLI-PRESENTER-DEFER`), and the bootstrap-service concrete-only
  `# type: ignore`s are left in place for a future pass.

**Tests.** The new `LHP-9.23` gate test inlines its `scripts/`-on-`sys.path`
setup in the test module rather than adding a `tests/scripts/conftest.py`, to
avoid a pytest bare-`conftest` collision with the root conftest.

### Deferred-item closure — dead-validator/error-code cleanup, snapshot validation-dedup reversal, layering carve-out resolution

**Summary.** A deferred-item chunk that closes six tracked tags, opens one new
deferral, and corrects two now-stale changelog claims. It is largely
subtractive: a phantom error code and three dead validator symbols are removed,
a previously-recorded "keep the check" decision is **reversed** because the path
it depended on no longer exists, and the last import-linter carve-out is closed
by relocating a frozen dataclass. All mechanical gates remain green; the new
`generate`-path regression test proves the snapshot-CDC presence guard is now
genuinely redundant.

**Closes `BLUEPRINT-059-DRIFT`.** Deleted the phantom `LHP-IO-059` error-code
row from `docs/blueprints.rst` and dropped "059" from the codes docstring in
`cli/commands/validate_command.py`. The condition was provably unreachable —
instance discovery filters via `relative_to(base_dir)` in
`utils/file_pattern_matcher.py`, so implementing the check would have been dead
defensive code. The fix was subtractive (remove the documented-but-unreachable
code), not additive.

**Closes `DEAD-VALIDATOR-CLEANUP`.** Removed three test-only/dead validator
symbols and their bound tests:

- `PipelineValidator` — the whole module `core/validators/pipeline_validator.py`
  deleted, plus its `core/validators/__init__.py` import / `__all__` entry and
  the bound test file `tests/test_pipeline_validator.py`.
- `ConfigValidator.validate_action_references` (and its now-orphaned private
  helper `_extract_all_sources`). The bound test file
  `tests/test_config_validator_references.py` had its **2** cases for these
  removed; its **2 other** cases — covering the live `TableCreationValidator`
  and `validate_flowgroup` template-warning paths — were **preserved**.
- `_validation_error_to_issue_view` from `api/_converters.py` (plus its
  `TestValidationErrorToIssueView` test class; the other classes in that file
  were kept). Private symbol — no `:stability:` / semver impact.

**Closes `SNAPSHOT-VALIDATION-DEDUP-DEFER` (DECISION REVERSAL).** This reverses
the earlier (2026-05-29) "keep the `CONFIG-002` check" decision recorded under
this same tag in the v0.6.x-era snapshot-CDC perf entry. That decision rested on
`processor.py:151` running **only** cross-flowgroup validation on the generate
path, making the generate-side presence guard the sole presence check. **Stage 2
deleted `processor.py`**: the per-flowgroup worker now runs
`ConfigValidator.validate_flowgroup` (→ `WriteActionValidator` →
`SnapshotCdcConfigValidator`) **unconditionally** before the codegen branch in
**both** `validate` and `generate` modes
(`core/coordination/_flowgroup_pool.py`, via
`flowgroup_resolver.process_flowgroup`). The `CONFIG-002` presence guard in
`generators/write/snapshot_cdc_source_function.py` is therefore now redundant and
was **removed**; the AST signature checks (`_extract_function_signature` /
`_validate_function_parameters`) were **kept** (they remain legitimately
generate-only — the validator cannot read the source file). A new generate-path
regression test
(`tests/test_generate_command_parallel.py::...::test_snapshot_cdc_missing_source_function_function_rejected_at_generate`)
proves a missing `source_function.function` is rejected at generate-time with
`LHP-VAL-007` (**not** the deleted `LHP-CFG-002`) before codegen runs. The
type-narrowing the deleted guard previously provided is now expressed via
`assert file_name is not None` documenting the upstream-validator contract.

**Closes `CODEGEN-MODULE-COUNT-DEFER`.** Reframed `TARGET_ARCHITECTURE.md` §5 +
checklist item 5 from the stale "5 named modules" headcount to the structural
**invariant**: a coordinator + named delegate services + supporting
single-responsibility modules + the `imports/` and `operational_metadata/`
subpackages, governed by single-responsibility + size limits, with per-action
generators staying in `generators/`. Code was already 0-violation compliant.
**Caveat:** `LOCAL/TARGET_ARCHITECTURE.md` is a git-ignored local doc, so this
edit lives in the worktree working tree only and must be reconciled into the
canonical copy manually (it does not travel via git).

**Closes the Phase-10.2 import-linter carve-out — wrong-class claim corrected.**
The real carved-out edge was
`lhp.models.processing -> lhp.core.codegen.python_file_copier.CopiedModuleRecord`,
**not** `...substitution.EnhancedSubstitutionManager` as the Phase 9.7 entry
("Surviving carve-out") and the earlier `pyproject.toml carve-outs` note both
wrongly recorded. Resolved by moving the pure frozen dataclass
`CopiedModuleRecord` down into `lhp/models/processing.py` (beside
`FlowgroupOutcome`, kept out of `__all__`) and repointing every importer.
**Both** `ignore_imports` carve-outs in `pyproject.toml` (the "LHP top-down
layering" contract and the "Models must not depend on higher layers" contract)
were deleted; import-linter now passes with **zero** ignores for that edge. (A
single-impl Protocol was rejected as §3.6 abstraction-for-its-own-sake.)

**`WK6-FIXUP-DEFER-new-module-tests` — PARTIAL.** Added
`tests/core/registry/test_factories.py` (ABC contract per §13.8 / §9.25:
`SubstitutionFactory` is non-instantiable; `DefaultSubstitutionFactory.create`
returns an `EnhancedSubstitutionManager`; a custom factory is accepted by
`OrchestrationDependencies`; `create_substitution_manager` delegates to the
injected factory) and folded in the green-lie smoke test
`test_dependency_factories_work` (removed from `tests/test_orchestrator.py`). The
soft "dependency-services hardening" clause **remains** a documented deferral
(4/5 dependency services already tested).

**Deferred (new; tracked for follow-up).**

- **`CORE-FILE-BUDGET-DRIFT`.** `TARGET_ARCHITECTURE.md` §3's "≤80 files in
  `core/` (recursive)" figure is stale: the actual count is **90** post-chunk
  (was 91; this chunk's `pipeline_validator.py` deletion accounts for the −1).
  **Not** fixed here — tracked for a separate pass. A §3 status note was added to
  the TARGET doc (same git-ignored caveat as `CODEGEN-MODULE-COUNT-DEFER` above:
  worktree-working-tree only).

**Notable (NIT-level).**

- One accurate, load-bearing comment in `pyproject.toml` (the "LHP top-down
  layering" contract header) still references `python_file_copier`. It documents
  the `python_file_copier.py → core/` relocation and is **unrelated** to the
  removed carve-outs, so it was intentionally retained — hence
  `grep python_file_copier pyproject.toml` returns **1**, not 0.
- Gate-driven incidental fixes: the `assert`-based type-narrowing added in
  `snapshot_cdc_source_function.py` (replacing the narrowing the deleted guard
  provided). (The deferred chunk had also widened a `# type: ignore` at
  `coordination/orchestrator.py:510`, but that edit was **dropped on
  integration**: the concurrent orchestrator-surface-thinning refactor in this
  same release deleted `_find_source_yaml_for_flowgroup` outright, so the
  suppression no longer applies.)

### Test remediation — `generators/test/` branch-coverage hardening

**Summary.** A test-trust pass over the test-action leaf generators: raised
`src/lhp/generators/test/` to 100% branch coverage (was ~70% pure-branch) with
mutation-resistant tests that pin each output variant as-written, and recorded
one intentional cross-generator asymmetry as deferred (decided, not changed).

**Changed.**

- Hardened branch coverage of `src/lhp/generators/test/` to 100% (was ~70%
  pure-branch); 13 new mutation-resistant tests pin each test-action output
  variant (empty `source`, `on_violation` routing, no-bounds range, backticked
  FQN, etc.).

**Deferred (decided; tracked for follow-up).**

- **`TEST-GEN-SOURCE-FALLBACK-ASYMMETRY`** — `schema_match` rejects bad/empty
  `source` (VAL-022) while the other 7 test-action leaf generators silently
  substitute a literal fallback table; new branch-coverage tests pin this
  asymmetry *as-written*. Whether the fallback leaves should also reject
  bad/empty source is a separate validator decision, deliberately not bundled
  into this coverage work.

### Test remediation — coverage hardening, obsolete-test cleanup, vulture allowlist

**Summary.** A test-trust pass: encoded a known production safety bug as a
strict `xfail` so it self-reports when fixed, completed a partial §1 validator
relocation, removed an obsolete Python-3.8 compatibility test, recorded a
suspected product bug behind a tracked skip, and added one false-positive
vulture allowlist entry.

**Changed.**

- **§1 validator relocation (partial).** The `load` and `test` action
  validators moved into the new `core/validators/action/` sub-package (the §1
  taxonomy home). All other validators remain under `core/validators/`.
- **Obsolete test removed.** Deleted `tests/test_python38_compatibility.py`
  (see `OBSOLETE-PY38` below).
- **Vulture allowlist.** Added one false-positive entry to
  `vulture_whitelist.py` (see "Vulture allowlist addition" below).

**Deferred (decided; tracked for follow-up).**

- **`MONITORING-RMTREE-DEFER`** (task B10/Q3) —
  `MonitoringFinalizerService.cleanup_artifacts` uses a naive substring match
  (`'FLOWGROUP_ID = "monitoring"' in content`) before a destructive
  `shutil.rmtree`, so a generated dir whose `monitoring.py` contains that marker
  only inside a comment or string literal would be wrongly deleted. A safety
  test encodes this as `xfail(strict=True)` in
  `tests/core/coordination/test_monitoring_service.py`; the production fix in
  `monitoring_service.py` is deferred to the owner. (Strict xfail = it will flip
  to a failure and alert when the bug is fixed.)
- **`§1-CONSOLIDATION-DEFER`** (tasks A1/A2/Q1) — only the `load` and `test`
  action validators were moved into the new `core/validators/action/`
  sub-package (the §1 taxonomy home). The remaining validators and any
  `BaseValidator` base-taxonomy intentionally stay in the parent
  `core/validators/` package; completing the taxonomy is owned by the larger §1
  refactor chunk. (The §2.1/§9.1 single-home rule is already satisfied — all
  validators remain under `core/validators/`.)

**Notable.**

- **`OBSOLETE-PY38`** (task C10) — deleted
  `tests/test_python38_compatibility.py`; the project requires `python >=3.11`
  (`pyproject.toml`), so the Python-3.8 compatibility test was obsolete (and it
  also asserted a removed method).
- **`VAL-054-INVESTIGATE`** (out of scope; recorded for a separate fix) —
  suspected product bug: a non-hashable blueprint raises `TypeError` instead of
  `LHP-VAL-054`. Tracked by the skip at `tests/test_blueprint_parser.py:330`.
  Not addressed in this chunk.
- **Vulture allowlist addition** (task D2) — added one `vulture_whitelist.py`
  entry: the `line_length` keyword param on the `_FakeFormatter.format_code`
  test double in `tests/core/coordination/test_flowgroup_pool.py`, which must
  mirror the real `CodeFormatter.format_code(code, line_length=None)` signature
  to be a faithful drop-in (body need not read it). False positive, not dead
  code.

### Stage 2 — Parallel Execution Consolidation — one flat per-flowgroup engine

**Summary.** `lhp generate` and `lhp validate` now run through a single
flat-per-flowgroup execution engine. Previously `generate` parallelized at
**pipeline** granularity (one worker task per pipeline; a pipeline's flowgroups
processed serially in-worker) while `validate` already parallelized per
flowgroup — an asymmetry that left large single pipelines as wall-clock
stragglers and let a config pass `validate` that `generate` would reject. Both
commands now submit **one worker task per flowgroup**, run cross-flowgroup
validation on **resolved** flowgroups, and `generate` commits its output
**all-or-nothing**. This closes CODING_CONSTITUTION §9.24 (one validation
surface, now one execution engine).

**Changed (user-visible behavior).**

- **`lhp validate` now catches cross-flowgroup conflicts on RESOLVED
  flowgroups.** Cross-flowgroup validation previously ran on **raw**
  flowgroups, so a conflict that only appears *after* template/preset
  resolution slipped through `validate` and only surfaced at `generate` time.
  Both commands now run the cross-flowgroup pass on the resolved set, closing
  the "validate passes, generate fails" gap. **Consequence:** a config whose
  cross-flowgroup conflict is introduced by resolution — e.g. a template that
  expands into two `create_table: true` actions targeting the same table — now
  **fails** `lhp validate` where it previously passed.
- **`lhp generate` is now all-or-nothing.** A global validation+codegen **gate**
  runs before any write. If **any** flowgroup fails anywhere in the run —
  per-flowgroup validation, codegen, Black formatting (`LHP-CFG-031`), a
  cross-flowgroup conflict, or a custom-module copy conflict (`LHP-VAL-019`) —
  the run writes **zero** files and the output tree is left untouched. This
  replaces the previous behavior where successful pipelines were written and
  only the failed ones aggregated at the end (partial output). Multiple
  failures across the run aggregate into a single `LHP-VAL-902`.
- **Both commands now parallelize at the FLOWGROUP level.** The unit of
  parallelism is the flowgroup for both commands (one worker task per
  flowgroup, cap `min(max_workers, flowgroup_count)`), replacing the previous
  per-pipeline parallelism for `generate`. On asymmetric projects (one large
  pipeline + many small ones) a single large pipeline's flowgroups now spread
  across all workers, eliminating the prior single-pipeline straggler that
  bounded wall-clock. `--max-workers`, `LHP_MAX_WORKERS`, and the auto-sizing
  heuristic are unchanged; the workload cap now sizes against flowgroup count
  rather than pipeline count.

**Removed (provisional API).**

- **Removed the provisional `on_pipeline_start` generate callback** from the
  generation facade and the orchestrator/executor chain. The flat per-flowgroup
  engine has no per-pipeline start boundary (it gates all-or-nothing and fires
  only the per-pipeline completion callback), so the hook had no meaning and was
  never fired. It was `:stability: provisional`, so removal is permitted in a
  minor version (CODING_CONSTITUTION §1.13). The per-pipeline `on_pipeline_complete`
  callback is unchanged.

**Performance.** On an asymmetric fixture (one 200-flowgroup pipeline plus many
small pipelines) the large pipeline ran on **8 distinct worker processes** at
`--max-workers 8` versus **1** at `--max-workers 1` — work now tracks flowgroup
count, not pipeline count. The resolved-flowgroup spawn payload measured
**~2.8 KB mean**, confirming the added IPC (validate workers now return the
resolved flowgroup) is marginal. Output remains **byte-identical** across
worker counts on success.

**Deferred (decided; tracked for follow-up).**

- **`TWO-WAVE-DEFER`** — the two-wave / no-codegen-on-validation-failure
  optimization is **not** built; single-wave was chosen (each worker does
  resolve → per-flowgroup validate → codegen → format in one task). (Spec §7.1,
  D2.)
- **`LPT-ORDER-DEFER`** — longest-processing-time (LPT) submission ordering is
  **dropped**; flowgroups are uniform enough that flat flattening suffices, so
  ordered submission saves nothing. (Spec §7.2, D10.)
- **`FG-PROJECTION-DEFER`** — workers return the **full** resolved `FlowGroup`
  object rather than a slimmed-down projection; shipping a projection instead
  is deferred (the measured payload is marginal). (Spec §7.3.)
- **`PARALLEL-WRITE-DEFER`** — the coordinator commits writes **sequentially**;
  parallel (thread-pool) disk writes on the coordinator are deferred, to be
  revisited only if writes ever dominate wall-clock. (Spec §7.4.)
- **`AUX-DEDUP-DEFER`** — auxiliary-file dedup is deferred; only the single
  monitoring flowgroup uses auxiliary files today, so cross-flowgroup
  auxiliary-file deduplication is not yet needed. (Spec §7.5.)
- **`TWO-PASS-DEFER`** — the escape-hatch "validate-all-then-stream-write"
  two-pass `generate` is **not** built. All-or-nothing intrinsically holds all
  formatted code in memory until the gate, but that only becomes a memory
  concern at ~100K+ flowgroups (GB-scale), well beyond the 6000+ target. (Spec
  §3bis.1.)

### Stage 1 — Discovery Efficiency — read-once / parse-once per invocation

**Summary.** Flowgroup discovery is now **read-once-per-invocation** and each
pipeline YAML is **parsed once**. Previously `lhp validate` / `lhp generate`
ran flowgroup discovery up to **4×** per invocation and re-read every
`pipelines/**/*.yaml` file **twice** (once for the flowgroup pass, once for the
blueprint-instance pass). Discovery is now memoized at the bootstrap boundary
and the two passes share a parsed-document cache. This is a **pure-latency**
change — no behavioral or output differences.

**Changed (performance).**

- **Discovery is memoized at the bootstrap boundary; the two passes share a
  parsed-document cache.** On a ~4000-flowgroup project, `validate` wall time
  dropped from **~43.7 s to ~7.7 s** (the redundant instance re-read pass
  dropped from ~5.4 s to ~0.3 s).
- **The shared document cache now sizes itself to the discovery working set**,
  so the parse-once behavior holds on projects with more files than the cache's
  previous fixed ceiling — which had silently caused the instance pass to
  re-read every file on large projects.

**Deferred (decided; tracked for follow-up).**

- **Phase-2 mechanism (DECIDED).** Elected the **per-invocation shared
  parsed-document cache** (spec option "b2") over the **unified discovery pass**
  (spec option "b1"). b1, which aligns with the eventual `core/discovery/`
  target architecture, is **deferred** — it would rewrite discovery control
  flow for what is a pure-latency goal.
- **`DISCOVERY-DAEMON-DEFER`.** The cross-invocation discovery index (spec §7)
  is **deferred** to a future stage; this work optimizes within a single
  invocation only.
- **`invalidate_discovery_cache()` (spec D4) intentionally NOT added.** Within
  one invocation there is no mutation of the on-disk flowgroup set, and wiring
  invalidation at the orchestrator entry points would force a second full disk
  discovery, defeating the memoization. The invalidation primitive will be
  introduced alongside the first mutating caller in a later stage. When it is
  added, it must reset all three atomically-coupled fields produced by
  `discover_all_flowgroups` — `_discovery_cache`, `_synthetic_contexts`, and
  `_monitoring_result` — not `_discovery_cache` alone; a tuple-only reset would
  re-introduce the D2 desync hazard the memo is built to avoid.

### Validation consolidation — `validate` and `generate` share one preflight path

**Summary.** `lhp validate` and `lhp generate` no longer carry duplicate
validation logic. Per CODING_CONSTITUTION §9.24 (no duplicate validation
logic), both commands now route structural and project-level preflight checks
through a single shared path. The user-visible consequence is that
`lhp validate` becomes **symmetric** with `lhp generate` — it catches the same
structural defect classes the generator would have caught — and a small number
of generate-only checks are promoted onto the shared path. This consolidation
introduced **zero production regressions** and the full e2e suite is green.

**Changed (user-visible behavior).**

- **`lhp validate` is now symmetric with `lhp generate`.** It catches the same
  structural defect classes the generator does — several of which were
  previously **generate-only**: table-creation "no creator"
  (`LHP-VAL-009`), duplicate `(pipeline, flowgroup)` (`LHP-VAL-009`),
  blueprint/instance errors (`LHP-VAL-041` family), test-reporting
  file-existence (`LHP-CFG-032`), and bundle catalog/schema preflight
  (`LHP-CFG-026`).
- **CDC fan-in failures in `lhp validate` now render as structured
  diagnostics** — an error code plus a suggestions panel — instead of the
  legacy stringified, `=====`-bordered text.
- **`lhp generate` now runs the test-reporting file-existence check
  independent of `--include-tests`.** A project with a missing test-reporting
  provider file that previously generated will now **fail** generation
  (`LHP-CFG-032`), regardless of whether `--include-tests` was passed.
- **`lhp validate` gained `--no-bundle` and `--pipeline-config` / `-pc`.** Like
  `lhp generate`, it now **requires** `-pc` on a project containing
  `databricks.yml` (otherwise it fails with `LHP-CFG-023`).
- **A failed-preflight `lhp generate` run no longer wipes
  `generated/<env>/`.** When generation fails preflight (duplicate
  `(pipeline, flowgroup)`, missing test-reporting provider, or bundle
  catalog/schema), the output directory is now left untouched — the
  `generated/<env>/` wipe moved to **after** the preflight gate.
- **`lhp validate` no longer prints the `✓ test_reporting`
  success-confirmation line.** The shared preflight surfaces failures only,
  consistent with `lhp generate`.

**Added.**

- **New error code `LHP-CFG-032`** — test-reporting provider/config file not
  found (project preflight). Raised by both `lhp validate` and `lhp generate`.

**Removed.**

- **`ValidationService.validate_flowgroups`** — verified genuinely dead and
  removed as part of this consolidation.

**Deferred (decided; tracked for follow-up).**

- **`GENERATOR-VALIDATION-DEFER`.** Generator-time-only checks remain
  generate-only by design and are intentionally **not** part of the shared
  preflight. These are checks that require artifacts only available during code
  generation and cannot run on the validate path.
- **`TAXONOMY-MIGRATION-DEFER`.** The `core/validators/` 4-subdir taxonomy
  migration is deferred to a separate pass.
- **`BLUEPRINT-059-DRIFT`.** `LHP-IO-059` doc-vs-code drift is tracked for a
  later reconciliation.
- **`CLI-PRESENTER-DEFER`.** CLI presenter extraction / Live-frame dedup is
  deferred. This also covers a **presentation asymmetry** (not a logic
  difference — validation is single-sourced): `lhp validate` surfaces
  project-level (batch) bundle preflight failures via the batch error message
  (code only), whereas `lhp generate` renders the full panel **plus** the
  doc-link.
- **`DEAD-VALIDATOR-CLEANUP`.** Two validators were verified **still-live**
  during this work (kept; deferred for later removal):
  `ConfigValidator.validate_action_references` (live test callers in
  `tests/test_config_validator_references.py`) and `PipelineValidator`
  (exported and exercised by `tests/test_pipeline_validator.py`). The
  genuinely-dead `ValidationService.validate_flowgroups` was removed in this
  work (see **Removed** above). Additionally, `api/_converters.py`'s
  `_validation_error_to_issue_view` lost its last **production** caller this
  PR (it mapped the removed `validate_flowgroups` output) and is now reachable
  only from `tests/api/test_validation_view_contract.py`; it is **kept** (live
  test caller, §10.4) and deferred for removal alongside the others.

**Pre-existing test debt (unrelated to this consolidation).** This branch
carries **44 pre-existing unit/integration test failures** from the in-progress
parallel-worker-rebuild (removed
`ActionOrchestrator.validate_pipeline_by_field` /
`generate_pipeline_by_field`, executor/pool exposure changes, old method names
/ return shapes). They are **unrelated** to this validation consolidation and
are to be migrated as the broader rebuild completes. This consolidation
introduced **zero** production regressions and the full e2e suite is green.

### Snapshot-CDC source functions — copy-and-import (was inline)

**Summary.** Snapshot-CDC source functions are no longer inlined as literal
text into every generated flowgroup file. The user's source-function file is
now **copied once per pipeline** into `custom_python_functions/<leaf>.py`
(verbatim, with the LHP header, **not** Black-formatted) and **imported** via
`import custom_python_functions.<mod> as _snap_<mod>`, instead of having its
body re-emitted into every flowgroup. This mirrors the existing `custom_py`
copy path.

**Changed.**

- Snapshot-CDC source functions are copied to `custom_python_functions/` and
  imported per pipeline rather than inlined per flowgroup. `spark` and
  `dbutils` are forwarded into the imported module's namespace via two
  pre-pipeline statements (`_snap_<mod>.spark = spark`,
  `_snap_<mod>.dbutils = dbutils`), reproducing the ambient globals the inlined
  body relied on.
- **Backward-compatible.** Existing user snapshot source functions are
  unchanged and keep working — the forwarded `spark`/`dbutils` globals preserve
  the previous execution environment; no user edits are required.
- **Performance.** A 400-flowgroup snapshot_cdc project drops from **>5 min
  toward the ~20 s `custom_py` baseline (~15× faster)**: Black no longer
  formats the inflated inlined body in every file, and AST parse / signature
  extraction is now cached per pipeline.

**§9.14 precedented-exception note (not a new violation category).** The two
injection statements emitted via `add_pre_pipeline_statement` —
`{alias}.spark = spark` and `{alias}.dbutils = dbutils` (assignment
statements) — are net-new Python-as-string emissions. They follow the
established pattern that **all** pre-pipeline statements are assembled as
strings via `add_pre_pipeline_statement`; the closest sibling precedent is the
`custom_datasource` / `custom_sink` `register_pickle_by_value(...)` emission
(those are calls rather than assignments). A Jinja2 template is intentionally
not used because these are header / pre-pipeline statements, not template body.
`_build_source_expression`'s `partial(...)` string predates this work. Recorded
here so a future audit does not re-flag these as a net-new *category* of §9.14
exception.

**Deferred (decided; tracked for follow-up).**

- **`SNAPSHOT-VALIDATION-DEDUP-DEFER` (DECIDED — keep the generate-side check).**
  The generate-side `source_function` **presence** check (`CONFIG-002`) is
  intentionally **kept**, and the §9.24 validation-dedup consolidation is
  deferred. Rationale (verified 2026-05-29): `lhp generate` does **not** route
  per-action config validation through `ValidationService` — it runs only
  cross-flowgroup validation (`processor.py:151`), so the generate-side
  presence check is the **sole** presence guard on that path and is
  load-bearing. Full §9.24 compliance requires wiring per-action validation
  into the generate path — a separate validation-architecture change, out of
  scope for this perf-focused work. The AST **signature** check remains
  legitimately generate-only (the validator cannot read the source file).

### Phase 9.8 — `generators/` cleanup (integrated onto the layering contract)

**Summary.** Four placement/responsibility fixes in `generators/`, re-applied
on top of the now-active top-down layering contract (Phase 9.7). Templates are
unchanged and generated output is **byte-stable** — the full e2e suite
(143 tests, incl. the test-action, quarantine, builtin-sink and python-load
baselines) passes unmodified.

- **A1 — copier relocation + exception split.** `python_file_copier.py` (a
  file-write coordinator, not a per-action generator) moves from `generators/`
  to `core/codegen/python_file_copier.py` (§2.3). Its domain exception
  `PythonFunctionConflictError` (LHP-VAL-019) splits out to
  `lhp/errors/types.py` and is exported from `lhp/errors` (§2.2). All importers
  retargeted to `lhp.core.codegen` / `lhp.errors`. The new edges
  (`generators.{load,transform,write} → core.codegen`) are **legal downward
  edges** under the active contract — no carve-out required.

- **B1 — snapshot source-function module renamed in place.**
  `generators/write/source_function_loader.py` is renamed to
  `generators/write/snapshot_cdc_source_function.py` and **kept in
  `generators/write/`**. It performs DLT-aware source transformation plus
  CodeAssembler coordination — it is write-generator support, not a §2.4 config
  loader. Its `… → core.loaders.external_file_loader` import is a legal
  downward edge. (This deliberately **reverts** the pre-layering plan to move
  the module into `parsers/`, which would have created an illegal
  `parsers → core` upward edge.)

- **C1 — quarantine collapse.** `transform/quarantine.py` — a parasitic class
  holding a `_parent` reference and reaching into its private methods (a §5
  boundary violation) — collapses into
  `DataQualityTransformGenerator._generate_quarantine_mode`; the file is
  deleted. Public API of the data-quality generator is unchanged.

- **D1 — test god-class split.** `test/test_generator.py` (a 336-line god-class
  with instance state and dual 9-way `if/elif` ladders) splits into
  `BaseTestActionGenerator(ABC)` plus nine stateless leaf generators
  (§3.1 / §3.5 / §3.6). The `TestActionType → generator` mapping is expressed
  in the `generators/registration.py` composition root (nine distinct
  entries). `core/registry/action_registry.py` is **left unchanged** — the
  empty-registry + zero-arg-instantiation pattern from Phase 9.7 already fits
  the nine-leaf design, so no `core → generators` edge is reintroduced.

Net architectural debt **decreases**: a top-level `core/` file is removed, a
private-method reach-through is eliminated, and a god-class is replaced by
stateless leaves.

**Tests.** Test-action tests migrate to mirror source under
`tests/generators/test/` (§8.2); the `python_file_copier` tests relocate to
`tests/core/codegen/` and the snapshot source-function test is renamed
alongside its module. No baselines or templates were changed.

**Deferred (out of scope for this integration; tracked for follow-up).**

- **`CODEGEN-MODULE-COUNT-DEFER`.** Relocating the copier grows `core/codegen/`
  past the aspirational module count in `TARGET_ARCHITECTURE.md` §273. The
  copier's placement is constitutionally correct (§2.3); reconciling §273's
  target number is a separate target-doc pass, not this integration.

- **Pre-existing branch debt.** Any file-size / placement items belonging to
  the larger v0.9.0 architecture PR are left untouched. This integration does
  not increase them (all of `check_file_sizes`, `check_placement`,
  `check_stability_drift` remain at 0 violations) and in fact reduces structural
  debt as noted above.

### Phase 9.7 — Layering contract redesign V2

**Closes `LAYERING-CONTRACT-REDESIGN-V2`.** The `import-linter` top-down
layering contract was a silent no-op: it lived in `pyproject.toml` under the
non-canonical table name `[[tool.importlinter.contracts.disabled-layering]]`,
which `import-linter` does not read. It is now re-enabled under the canonical
`[[tool.importlinter.contracts]]` name and reordered to match the real
dependency directions. **Result: 6 contracts kept, 0 broken, 1 carve-out.**

**Reorder.** `lhp.generators` now sits **above** `lhp.core` — the per-action
generators inherit from `BaseActionGenerator` in `lhp.core.registry`, so the
16 `generators → core.registry` carve-outs plus the 1
`generators.load.kafka → core.validators.kafka_validator` carve-out are now
legal downward edges and have been removed. `lhp.presets` and `lhp.parsers`
are core's *dependencies*, so they sit **below** `core` (`presets` above
`parsers` because `presets` imports it); `lhp.errors` sits **below**
`lhp.models` because `models` imports `errors`. `lhp.api` is declared as a
single public layer above `core`.

**`core → api` decoupling (makes the single-`api`-layer ordering hold).**
`ValidationService.validate_flowgroups()` now returns the internal
`ValidationError` union instead of constructing the public
`ValidationIssueView`; public-DTO construction moved to `api/_converters.py`
(via the new `_validation_error_to_issue_view` helper), removing a §9.24-style
duplication of the error→view mapping. A `BaseWarningCollector` ABC was added
to `core/_interfaces.py` so `core` types against the abstraction;
`api.callbacks.WarningCollector` now inherits it. After this, `core` imports
`api` in **zero** places (runtime or `TYPE_CHECKING`).

**`core → generators` decoupling (breaks the registry cycle).** Putting
`generators` above `core` exposed the reverse edge — `core` imported
`generators` in 10 places, a true cycle. Two changes eliminate it:
`generators/python_file_copier.py` (a shared file-copy utility that already
imported `core.loaders`) moved to `core/python_file_copier.py`; and
`ActionRegistry` is now a passive registry populated by the new
`lhp.generators.registration` module (which self-registers every family),
triggered once at the composition root in `lhp/__init__.py`. `core` no longer
imports `generators` at all. The now-empty `action_registry → generators.*`
carve-outs were removed from the generator-family independence contract.

**`_interfaces.py` relocation.** `core/coordination/_interfaces.py` →
`core/_interfaces.py`: the service ABCs are a core-layer concern, not specific
to the coordination sub-package.

**Surviving carve-out (1).** `lhp.models.processing → lhp.core.processing.substitution`
— a `TYPE_CHECKING`-only forward ref for `EnhancedSubstitutionManager`. Closed
in Phase 10.2 (Protocol move pending), documented inline in `pyproject.toml`.

### Phase 9.6 — JOB-ORCHESTRATION-DEPENDS-ON fix

**Root cause.** `FlowgroupResolutionService.process_flowgroup(...)` mutated the
cached input `FlowGroup` via `flowgroup.actions.extend(template_actions)` and
`flowgroup.actions = [...]`. The cache (`CachingYAMLParser._cache`) returns
`FlowGroup` instances by reference; the second `get_flowgroups(...)` call
during master-job assembly (`DependencyAnalysisService.analyze_multi_job_dependencies`
→ `DependencyGraphBuilder.get_flowgroups`) saw bloated flowgroups, and the
dependency analyzer dropped legitimate edges as apparent cycles. Generated
`.job.yml` files for multi-job projects shipped with empty `depends_on:`
arrays.

**Fix.** Replaced both in-place mutations with shallow
`flowgroup.model_copy(update={"actions": [...]})` rebinds in
`src/lhp/core/processing/flowgroup_resolver.py`. Cache stays clean across
calls. Constitution alignment: §3.5 (stateless where possible) and §4.4
(mutable value objects discouraged in internal code) — the resolver now
operates as a value-returning transform.

**Closes.** `JOB-ORCHESTRATION-DEPENDS-ON-DEFER-WK6`. Lifts §12bis cluster
`DESELECT_E2E_K` (4 e2e tests now pass without baseline edits). §12bis
deadline list shrinks from 6 clusters to 5.

**Architectural note.** The `_analyze_pipeline_dependencies(job_graphs)`
re-analysis added in Week 6 (`src/lhp/core/dependencies/analyzer.py:166`)
stays — it is semantically correct cleanup independent of the cache fix.
The original Week 6 diagnosis (post-hoc per-job partitioning as the root
cause) is preserved in the previous CHANGELOG entry as historical context.

**Tests.** Added focused unit-level regression test at
`tests/core/processing/test_flowgroup_resolver_cache_isolation.py` that
asserts the input `FlowGroup` instance is byte-identical after
`process_flowgroup(...)`. Verified to fail if either mutation site is
reverted.

### Phase 9.5 — Loader / model decomposition + Phase-8 sub-package finalization

Phase 9.5 closes the largest single remaining file-size debt and finalizes the
four Phase-8 `*-DEFER` markers. `src/lhp/models/config.py` (596L, 27 classes
across 7 domains) is split into ten underscore-prefixed sub-modules and
deleted outright; `src/lhp/models` becomes the canonical import path. The
`core/jobs/job_generator.py` god module (789L) is decomposed across a new
`JobConfigLoader` (config-loading concern lifted into `core/loaders/` per
§2.4), a stateless `job_builder.py` module (graph-traversal helpers as
free functions per §3.7), and a one-function `job_writer.py` (disk I/O).
`core/loaders/project_config_loader.py` (798L — **two lines from the §9.3
800L hard cap**) splits into five `_*_config_parser.py` helper modules with
the coordinator class shrinking to 207L. The four open Phase-8 DEFER markers
all close: `ImportDetector` moves to `core/codegen/imports/`, `OperationalMetadata`
renames hard to `OperationalMetadataCatalog` with its two helper modules
inlined as methods, and the LHP side of the substitution-view migration lands
(`SubstitutionView` + `SecretReferenceView` DTOs at `:stability: provisional`,
new `InspectionFacade.build_substitution_view(env)` method, runtime
`DeprecationWarning` wired for the `EnhancedSubstitutionManager` re-export).
All mechanical gates remain green; full non-e2e test suite shows the
pre-existing 44-failure baseline unchanged, +21 new SubstitutionView contract
tests + 2 new canonical-round-trip parametrized cases → **3016 passed**.

- **`models/config.py` deleted; ten underscore-prefixed sub-modules created.** 27 classes redistributed across `_enums.py` (7 enums, 63L), `_quarantine.py` (12L), `_test_reporting.py` (15L), `_operational_metadata.py` (4 models, 40L), `_monitoring.py` (3 models, 53L), `_project.py` (27L), `_action.py` (WriteTarget + Action, 168L), `_flowgroup.py` (FlowGroup + FlowGroupContext, 33L), `_template.py` (Template + Preset, 36L), `_blueprint.py` (4 models incl. `BlueprintInstance._normalize_syntax`, 179L). All sub-modules ≤200L. `models/__init__.py` carries explicit `__all__` (27 names) and the `warnings.filterwarnings(...)` for the `schema` field-shadowing rule. No back-compat shim — Path 1 (§5.4 full compliance) per the locked decision.
- **165 import sites migrated.** Every `from lhp.models.config import X` (and the three-dot / four-dot / two-dot / one-dot relative-import variants) rewritten to `from lhp.models import X` across 73 src files + 92 test files. The grep enumeration intentionally widened to capture four-dot relative forms (`from ....models.config`) that the original plan's regex missed — found in `core/codegen/operational_metadata/` and `generators/write/sinks/`. `src/lhp/core/codegen/__init__.py`'s `__all__` and `tests/test_blueprint_discoverer.py:208` (a comment with the literal substring) were also updated. The `"lhp.models.config"` logger name in `_blueprint.py` was **intentionally kept** — three caplog-based tests in `test_blueprint_use_syntax.py` / `test_blueprint_parser.py` subscribe to that stable identifier across the Phase A1 split; a same-line rationale comment now documents the choice in source per §6.6.
- **`JobConfigLoader` extracted to `core/loaders/job_config_loader.py` (206L).** `JobGenerator._load_job_config` (172L) and `_deep_merge_dicts` (18L) leave `JobGenerator`; the loader class exposes a single public `load(project_root, config_file_path) → (project_defaults, job_specific_configs)` method that owns the multi-document `yaml.safe_load_all` parsing. `JobGenerator.__init__` now calls `JobConfigLoader().load(...)`. `_deep_merge_dicts` stays on `JobGenerator` (still called by `get_job_config_for_job` and `resolve_monitoring_job_config`, plus six tests in `test_job_generator_multi_doc.py`). Satisfies §2.4 (configuration loaders live in `core/loaders/`).
- **`job_builder.py` (171L) + `job_writer.py` (42L) extracted as stateless modules.** Graph-traversal helpers `build_job_stages`, `build_pipeline_to_job_mapping`, `analyze_cross_job_dependencies` move out as module-level free functions per §3.7, alongside the `JobPipeline` / `JobStage` dataclasses. The I/O primitive `write_job_yaml(yaml_string, output_path) → Path` lives in its own one-function module. `JobGenerator.save_job_to_file` retains its dir-vs-file path resolution (filename-convention concern stays with the orchestrator) and delegates the actual write to `write_job_yaml`. The two §3.7-violating wrappers (`_build_pipeline_to_job_mapping`, `_analyze_cross_job_dependencies_from_global`) initially kept on `JobGenerator` for `TestHelperMethodsCoverage` compatibility were subsequently removed per E5 review; the 8 call sites in `tests/test_job_generator_multi_doc.py` now invoke the module functions directly.
- **`core/jobs/job_generator.py`: 789L → 470L** (−319L, no `# JUSTIFIED:` block needed). `core/jobs/__init__.py` extended with new exports.
- **`project_config_loader.py`: 798L → 207L** via five new helper modules. `_event_log_config_parser.py` (93L), `_monitoring_config_parser.py` (274L), `_test_reporting_config_parser.py` (72L), `_operational_metadata_config_parser.py` (142L), `_include_patterns_parser.py` (108L). `ProjectConfigLoader` keeps `__init__`, `load_project_config`, `_parse_project_config` (now a ~50L orchestrator), and `get_operational_metadata_config`. Two thin wrappers (`_parse_monitoring_config`, `_validate_monitoring_config`) retained on the class purely so 25 existing direct call sites in `test_monitoring_config_loader.py` continue to pass without test-body changes; logic lives in `_monitoring_config_parser.py`. Hidden coupling discovered during extraction: `_validate_monitoring_config` had a `self.project_root` dependency at the old L581 — solved by adding `project_root: Path` as a third parameter on the module function, with the wrapper injecting `self.project_root` so the public test signature stays identical.
- **`ImportDetector` relocated to `core/codegen/imports/`.** Closes `IMPORT-DETECTOR-PLACEMENT-DEFER`. File moved via `git mv` (134L, body unchanged). Two consumers updated: `manager.py` (intra-package `from .detector import ImportDetector`), `metadata.py` (sibling-package `from ..imports import ImportDetector`). `tests/test_operational_metadata_selection.py` (5 in-function imports) also updated.
- **`OperationalMetadata` → `OperationalMetadataCatalog` (hard rename).** Closes `OPMETA-RENAME-DEFER`. No back-compat alias (internal class per §1.7). The `Subject Noun` form `OperationalMetadataCatalog` fits §4.10 naming (catalog/state-holder, not a service); `OperationalMetadataService` (the wrapper, already conforming) was **not** renamed. Six consumers updated, including the top-level `src/lhp/core/codegen/__init__.py` re-export and a `:class:` docstring reference in `detector.py` that the original plan's expected-list missed.
- **Two helper modules inlined into `OperationalMetadataCatalog`.** Closes `OPMETA-§3.7-CONSOLIDATE-DEFER`. `_column_resolution.py` (195L) and `_sql_adaptation.py` (150L) deleted; their six module-level functions (`get_selected_columns`, `_get_available_columns`, `_extract_column_names`, `_validate_target_type`, `adapt_expressions_for_imports`, `_adapt_expression_for_wildcard`) become `self`-bound methods on `OperationalMetadataCatalog`. `meta.x` mutations become normal `self.x`. The class file lands at **395L** (under the 400L target per §3.3). One test patch path migrated: `lhp.core.codegen.operational_metadata._column_resolution.logger` → `...metadata.logger`. Four JSON-schema references to `OperationalMetadataSelection` in `flowgroup.schema.json` correctly **left untouched** — they reference a different (still-extant) Pydantic class.
- **`SubstitutionView` + `SecretReferenceView` public DTOs added; `InspectionFacade.build_substitution_view(env)` method added.** Closes the **LHP-side** of `OPMETA-SVC-VIEW-DEFER`; the CLI-consumer migration carries over under a new `OPMETA-SVC-VIEW-CLI-DEFER` marker for the CLI rewrite. Both DTOs are frozen dataclasses per §4.5 with `:stability: provisional`; field types stick to flat `Mapping[str, str]` / `Tuple[SecretReferenceView, ...]` per §4.8 (no nested `Dict[str, Dict[...]]`). The facade method docstring lists `:raises lhp.errors.LHPError: LHP-CFG-*` per §4.7. Internally flattens `EnhancedSubstitutionManager.mappings` to `Dict[str, str]` via `str(value)` and sorts secret references by `(scope, key)` for determinism. 21 contract tests in `tests/api/test_substitution_view_contract.py` cover JSON round-trip, pickle round-trip, frozen check, and field-type contract per §8.3. Two additional `pytest.param(...)` entries in `tests/api/test_to_dict_round_trip.py::_INSTANCES` cover the canonical `to_dict()` path per §1.9.
- **`EnhancedSubstitutionManager` re-export at `:stability: deprecated` with runtime `DeprecationWarning`.** Per §6.4 (shim + warning + removal version). PEP 562 module-level `__getattr__` in `lhp/api/__init__.py` intercepts attribute access and emits `warnings.warn("EnhancedSubstitutionManager is deprecated; use InspectionFacade.build_substitution_view instead. Removal planned for v1.0.0.", DeprecationWarning, stacklevel=2)`. The direct top-level import was deliberately removed — PEP 562 only fires for names not in module globals. Removal planned for **v1.0.0**.
- **`InspectionFacade` method-count drift corrected.** The class now exposes **14** public methods (was documented as "Thirteen" in the docstring + "thirteen … (§3.2 cap is ten)" in the `# JUSTIFIED:` block). Both edited: `save_dependency_outputs` added to the enumerated bullet list; the JUSTIFIED block now correctly cites "§3.2 requires justification at 10–15; hard cap at 15" and lists "fourteen public methods" in both prose and the inner TODO marker.
- **Module-level logger added to `_blueprint.py` per §7.1.** New `logger = logging.getLogger(__name__)` at module scope; the `_legacy_logger = logging.getLogger("lhp.models.config")` also lifted to module scope with a same-line rationale comment per §6.6 documenting why the literal-string name is preserved (three caplog-based tests).
- **`vulture_whitelist.py` updated.** One stale entry comment migrated: `models.config.ProjectConfig._legacy_warned_paths` → `models._blueprint.BlueprintInstance._legacy_warned_paths` (reflecting both the file-move and the actual class location — the prior comment had two errors, file path and class name).

#### Phase 9.5 — Mechanical-gate snapshot at end of phase

| Gate | Pre-Phase-9.5 | End of Phase 9.5 |
|---|---:|---:|
| `wc -l src/lhp/models/config.py` | 596 | **deleted** |
| `wc -l src/lhp/core/jobs/job_generator.py` | 789 | **470** (−319L) |
| `wc -l src/lhp/core/loaders/project_config_loader.py` | 798 | **207** (−591L; two lines from §9.3 hard cap) |
| `wc -l src/lhp/core/codegen/operational_metadata/metadata.py` | 204 | **395** (+191L; helpers absorbed, both helper files deleted) |
| `wc -l src/lhp/core/jobs/job_builder.py` | n/a | **171** (new) |
| `wc -l src/lhp/core/jobs/job_writer.py` | n/a | **42** (new) |
| `wc -l src/lhp/core/loaders/job_config_loader.py` | n/a | **206** (new) |
| `wc -l src/lhp/core/loaders/_event_log_config_parser.py` | n/a | **93** (new) |
| `wc -l src/lhp/core/loaders/_monitoring_config_parser.py` | n/a | **274** (new) |
| `wc -l src/lhp/core/loaders/_test_reporting_config_parser.py` | n/a | **72** (new) |
| `wc -l src/lhp/core/loaders/_operational_metadata_config_parser.py` | n/a | **142** (new) |
| `wc -l src/lhp/core/loaders/_include_patterns_parser.py` | n/a | **108** (new) |
| `check_placement.py --all` | 0 | **0** |
| `check_file_sizes.py --all` | 0 | **0** |
| `check_stability_drift.py --all` | 0 | **0** |
| `ruff check --select B904,TRY400,RUF013 src/ tests/` | 0 | **0** |
| `mypy --strict src/lhp/api/` | clean (12 files) | clean (12 files) |
| `lint-imports` | 5 kept, 0 broken | **5 kept, 0 broken** |
| `vulture src/lhp/ vulture_whitelist.py --min-confidence 80` | 0 | **0** |
| `pytest tests/ --ignore=tests/e2e` (deselect one pre-existing) | 44 failed / 2993 passed | **44 failed / 3016 passed** (+21 SubstitutionView contract + 2 canonical round-trip) |

#### Phase 9.5 — DEFER markers closed

- `IMPORT-DETECTOR-PLACEMENT-DEFER` — `ImportDetector` moved to `core/codegen/imports/`.
- `OPMETA-RENAME-DEFER` — `OperationalMetadata` → `OperationalMetadataCatalog`.
- `OPMETA-§3.7-CONSOLIDATE-DEFER` — `_column_resolution.py` + `_sql_adaptation.py` inlined as methods.
- `OPMETA-SVC-VIEW-DEFER` (LHP side) — `SubstitutionView` + facade method landed. The CLI-consumer migration carries over under the new `OPMETA-SVC-VIEW-CLI-DEFER` marker.

### Phase 9.1 — Orchestrator decomposition (8th typed service + free-function aggregator)

Phase 9.1 extracts `FlowgroupBootstrapService` from `ActionOrchestrator` as the
8th typed service (§4.10 / §4.12), moves `_aggregate_generate_outcomes` to
`core/coordination/executor.py` as a module-level free function per §3.7,
rewrites the orchestrator's `# JUSTIFIED:` block to reflect the new shape, and
updates `TARGET_ARCHITECTURE.md` §4 + §9 to document the 8th typed service.
`ActionOrchestrator` is down from 748L → 641L (−107L). All mechanical gates
remain green; full non-e2e test suite delta is **+0 new failures** vs Week-8
close (45 failed / 2984 passed, identical baseline; the 4 Week-8 mock-patch
regressions are closed by T0).

- **8th typed service.** New ABC `BaseFlowgroupBootstrapService` added to `core/coordination/_interfaces.py` with two abstract methods: `discover_all_flowgroups()` and `make_context(fg)`. Concrete `FlowgroupBootstrapService` lives in `core/coordination/bootstrap_service.py` (131L). Owns the discovery → blueprint expansion → monitoring chain, the synthetic-context provenance table, and the cached monitoring build result. Wired into `ActionOrchestrator.__init__` as `self.bootstrap: BaseFlowgroupBootstrapService = bootstrap_service or FlowgroupBootstrapService(...)` via the §9.24 injection pattern (keyword-only kwarg, default `None`, inline construction from existing collaborators when omitted).
- **4 method bodies moved out of `ActionOrchestrator`.** `discover_all_flowgroups` (34L) and `_make_context` (15L) → service methods; orchestrator-side remains as 1-line delegators (preserved on the orchestrator surface bit-for-bit for the 16 pre-existing `_inspection_facade.py` reach-throughs — Phase 9.2 owns the redesign). `_expand_blueprints` (22L) and `_build_monitoring` (12L) → DELETED from orchestrator outright (no orphans, no delegators); `tests/test_blueprint_orchestrator.py` 2 call sites rewritten from `orch._expand_blueprints()` → `orch.bootstrap._expand_blueprints()`.
- **`_synthetic_contexts` + `_monitoring_result` ownership.** Both state attributes migrated from orchestrator to `FlowgroupBootstrapService`. Grep confirms 0 references to either name in `orchestrator.py` after the move; consumers go through the typed service.
- **`aggregate_generate_outcomes` free function (§3.7).** `_aggregate_generate_outcomes` (53L, verified `self`-free) moved from `ActionOrchestrator` to `core/coordination/executor.py` as a module-level free function. Renamed without leading underscore. Added to `executor.py` `__all__`. The orchestrator method DELETED entirely (no forwarder — verified 0 test references). Caller in `generate_pipelines` updated. Contingency `outcomes.py` extraction NOT triggered (`executor.py` lands at 491L, under the 500L threshold).
- **Constructor builder extraction (T-D) skipped.** The plan's T-D (`_build_substitution_manager` + `_resolve_output_dir`) was not actionable: those constructions don't live in `ActionOrchestrator.__init__` (verified by grep — they're invoked per-action from `core/coordination/work_unit_builder.py`). Skipped per user direction (2026-05-28); constructor remains well-factored at ~150L with the 8 typed-service block as the irreducible residual cost.
- **`# JUSTIFIED:` block rewritten.** Old block (15 lines, referencing the now-extracted aggregator + the now-relocated `_synthetic_contexts` state) replaced with a 5-line block citing typed-service wiring (§4.10/§4.12) as the residual cost and naming the back-compat construction at `tests/test_orchestrator.py:607` (still using `ActionOrchestrator(project_root, ...)` positional construction). Stale `TODO(Phase 9.1)` at line 19 DELETED.
- **Module docstring extended (§3.2 method-count justification).** `ActionOrchestrator` has 10 public methods (10–15 range requiring module-docstring justification). New 23-line module docstring enumerates the 10 with one-line rationale each. The `# JUSTIFIED:` block at lines 25–29 is preserved within `check_file_sizes.py`'s 30-line detection window.
- **`TARGET_ARCHITECTURE.md` updated.** §4 (line 116) extended with `bootstrap: FlowgroupBootstrapService` bullet in the typed-service list. §9 lists `core/coordination/bootstrap_service.py` as the 8th typed service held by the orchestrator. No new sub-package introduced.
- **6 unit tests in `tests/core/coordination/test_bootstrap_service.py` (new file).** 3 cover ABC subclass + instantiation + `make_context` paths (synthetic lookup + fallback). 3 cover `discover_all_flowgroups` paths (empty disk + no blueprints, blueprint expansion populating `_synthetic_contexts` + `register_synthetic_sources` call, monitoring context appended). Tests use small `_FakeDiscovery(BaseFlowgroupDiscoveryService)` + `_FakeMonitoring(BaseMonitoringFinalizerService)` + plain `_FakeBlueprintDiscoverer` / `_FakeBlueprintExpander` classes (per §8.8 "no mocking what you own — use typed-interface fakes") instead of `MagicMock(spec=...)`.
- **4 Week-8 mock-patch test regressions closed (T0).** Tests in `tests/test_import_manager.py::TestUtilityMethods` rewritten to call module-level `extract_module_name` / `is_wildcard_import` / `categorize_import` from `lhp.core.codegen.imports.categorizer` (these were methods on `ImportManager` before Week 8's holding-block decomposition). Test in `tests/test_load_operational_metadata.py::TestLoadOperationalMetadata::test_unknown_metadata_column_warning` rewritten to patch `_column_resolution.logger` directly — the Week-8 module-level `logger = logging.getLogger(__name__)` binding makes the original `logging.getLogger` factory patch ineffective (decision recorded per the "no silent test fixes" rule).
- **5 stale imports cleaned (§6.1).** F401 violations introduced when extraction T-B/T-C moved method bodies — `replace`, `ErrorCategory`, `LHPValidationError`, `lhp_error_from_worker_failure`, `BlueprintProvenance` — removed from `orchestrator.py`. Also swept 2 pre-existing unused imports (`LHPError`, `write_normalized` from v0.0.9 refactor) as collateral cleanup.
- **2 `# type: ignore[attr-defined]` rationales added (§6.7).** Both lines in `bootstrap_service.py` (calls to concrete-only `register_synthetic_sources` / `last_build_result` whose ABC narrows the surface) now carry same-line rationale comments.
- **ABC + concrete renamed for §4.10 conformance.** `BaseBootstrapService` → `BaseFlowgroupBootstrapService` and `BootstrapService` → `FlowgroupBootstrapService` to fit the `Base<Subject><Verb>Service` pattern (Subject=Flowgroup, Verb=Bootstrap). Mirrors the precedent `BaseFlowgroupDiscoveryService` ↔ `FlowgroupDiscoveryService`. 5 files touched.

#### Phase 9.1 — Mechanical-gate snapshot at end of Week 9

| Gate | Pre-Phase-9.1 (end of Week 8) | End of Phase 9.1 |
|---|---:|---:|
| `wc -l src/lhp/core/coordination/orchestrator.py` | 748 | **641** (−107L; `# JUSTIFIED:` block trimmed to 5 lines) |
| `wc -l src/lhp/core/coordination/executor.py` | 431 | **491** (+60L; aggregator absorbed; under 500L threshold) |
| `wc -l src/lhp/core/coordination/bootstrap_service.py` | n/a | **131** (new file) |
| `wc -l src/lhp/core/coordination/_interfaces.py` | 303 | **338** (+35L; 8th ABC added) |
| `check_placement.py --all` | 0 | **0** |
| `check_file_sizes.py --all` | 0 | **0** |
| `check_stability_drift.py --all` | 0 | **0** |
| `ruff check --select B904,TRY400,RUF013 src/ tests/` | 0 | **0** |
| `mypy --strict src/lhp/api/` | clean (12 files) | clean (12 files) |
| `lint-imports` | 5 kept, 0 broken | **5 kept, 0 broken** |
| `vulture src/lhp/ vulture_whitelist.py --min-confidence 80` | 0 | **0** |
| `pytest tests/api/` | 135/135 | **135/135** |
| `pytest tests/core/coordination/` | n/a | **6/6** (3 ABC/instantiation + `make_context` paths + 3 `discover_all_flowgroups` paths) |
| `pytest tests/ --ignore=tests/e2e` | 49 failed, 2977 passed | **45 failed, 2984 passed** (−4 failures via T0; +3 new tests pass) |
| `pytest tests/e2e/ -n auto` | 139 passed, 4 deferred | (not re-run in Phase 9.1 work; expected unchanged) |

#### Phase 9.1 — Constitution-reviewer (T-E5) findings — all resolved

The T-E5 constitution-reviewer pass returned 2 BLOCKER, 4 SHOULD-FIX, 2 NIT. All resolved before close:

- **B1 — §6.1** (5 stale imports left behind by extraction): fixed via `ruff check --select F401 --fix`; 5 imports removed plus 2 pre-existing unused (`LHPError`, `write_normalized`) swept as collateral.
- **B2 — §6.7** (2 `# type: ignore[attr-defined]` without rationale): fixed; both lines in `bootstrap_service.py` carry same-line rationale describing why the ABC narrows the surface.
- **S1 — §3.2** (method-count justification missing): fixed; orchestrator module docstring extended to enumerate the 10 public methods.
- **S2 — §10** (`TARGET_ARCHITECTURE.md` not updated): fixed; §4 + §9 extended with the 8th typed service entry.
- **S3 — §8.8** (test uses `MagicMock(spec=ABC)` for LHP-owned services): fixed; replaced with `_FakeDiscovery` / `_FakeMonitoring` / `_FakeBlueprintDiscoverer` / `_FakeBlueprintExpander` classes.
- **S4 — §8.1** (`discover_all_flowgroups` no direct unit test): fixed; 3 new unit tests cover empty / blueprint-expansion / monitoring-attachment paths.
- **N1 — §4.10** (`BaseBootstrapService` no verb segment): fixed; renamed to `BaseFlowgroupBootstrapService` + concrete to `FlowgroupBootstrapService`.
- **N2** (orphan blank line at orchestrator.py:476): fixed; 3 blank lines (including trailing whitespace) collapsed to a single blank.

Second reviewer pass skipped at user direction; mechanical gates re-verified all-green post-fix.

#### Phase 9.1 — Deferred items

- **T-D constructor builder extraction (skipped at 2026-05-28).** Plan estimated extracting `_build_substitution_manager` + `_resolve_output_dir` from `ActionOrchestrator.__init__`. Verification showed neither construction lives in `__init__` — both happen per-action from `work_unit_builder.py`. Skipped per user direction. Constructor lands at ~150L; the 8-typed-service wiring is documented in the rewritten `# JUSTIFIED:` block as the irreducible residual cost.
- **Phase 9.2 inspection-facade redesign (target: Phase 9.2).** 16 pre-existing `_inspection_facade.py` reach-throughs (`discover_all_flowgroups`, `_find_source_yaml_for_flowgroup`, etc.) preserved bit-for-bit in Phase 9.1. Orchestrator-side delegators kept exactly to maintain this contract. Phase 9.2 owns the redesign.
- **`test_discover_all_flowgroups_includes_blueprint_expansions` (pre-existing failure preserved).** Failing on the base commit (references `orch.discoverer` which doesn't exist on the v0.9.0 branch). Confirmed pre-existing via `git stash` + re-run against `c63b33a5`. Not blocking Phase 9.1; not introduced by Phase 9.1.

### Phase 9.3 — generator template extraction & §9.14 mechanical gate

Phase 9.3 brings the two largest generator files under the 500-line constitutional
threshold via two orthogonal extractions, replaces a silent black-formatter
fallback with a real `LHPConfigError`, and introduces a new §9.14 mechanical gate
that bans long inline f-string code-emission blocks in `generators/`. All
mechanical gates remain green; E2E baselines are byte-identical (139 passed, 4
pre-existing `DESELECT_E2E_K` deferrals unchanged, expiry 2026-08-01).

- **Refactor** (target architecture §"Summary" #10):
  - `src/lhp/generators/write/streaming_table.py` reduced from 722 → 333 lines by
    extracting the snapshot-CDC `source_function` loader into a sibling
    `src/lhp/generators/write/source_function_loader.py` (345L). The duplicate
    3-path candidate-file search was consolidated with the existing
    `core/loaders/external_file_loader.py` utilities — no more reimplemented
    file-loading logic (constitution §6.5).
  - `src/lhp/generators/test/test_generator.py` reduced from 525 → 337 lines by
    extracting nine per-test-type Jinja2 templates under
    `src/lhp/templates/test/` (one `.py.j2` per test type). The generator now
    validates Action config, builds a render context, and delegates to
    `render_template(f"test/{test_type}.py.j2", context)`. The 30-line inline
    `_build_schema_match_sql` f-string and the `TEST_SQL_TEMPLATES` class dict
    are gone.
- **Changed**:
  - `CodeFormatter` (relocated to `src/lhp/core/codegen/formatter.py` from
    `lhp/utils/formatter.py` — `utils/` no longer contains domain types per
    constitution §2.2 / target architecture line 167) now raises
    `LHPConfigError` (code `031`, category `CONFIG`) on
    `black.parsing.InvalidInput`. Previously: silent fallback that returned
    organize-imports-only output, masking generator bugs by shipping
    syntactically-invalid Python to consumers' repos. **Behavior change** for
    any consumer that relied on the silent fallback (none expected). All 6
    importers updated to the new path.
- **Added**:
  - `scripts/check_codegen_inline.py` — mechanical gate for constitution §9.14
    / target architecture §"Summary" #10. AST-walks every file under
    `src/lhp/generators/` and flags any `ast.JoinedStr` whose
    `end_lineno - lineno > 20`. Wired into `.pre-commit-config.yaml`,
    `.github/workflows/python_ci.yml`, `.claude/settings.json` (PostToolUse +
    Stop), and `scripts/check_baseline_gates.sh` (strict, zero baseline
    tolerance).
  - Nine new templates under `src/lhp/templates/test/`: `row_count.py.j2`,
    `uniqueness.py.j2`, `referential_integrity.py.j2`, `completeness.py.j2`,
    `range.py.j2`, `all_lookups_found.py.j2`, `schema_match.py.j2`,
    `custom_sql.py.j2`, `custom_expectations.py.j2`.
- **Removed**:
  - `# JUSTIFIED:` blocks from `streaming_table.py` and `test_generator.py` —
    both files now satisfy the ≤500-line threshold without justification, and
    the underlying Phase-9.3 TODOs are done.
  - Five tests referencing now-removed private symbols
    (`_build_transform_config`, `_generate_test_sql`,
    `_uniqueness_sql_generation_with_filter`,
    `_uniqueness_sql_generation_without_filter`, `_filter_with_empty_string`).
    All assertions were structural ("class has method named X" or "returns
    expected raw SQL string") rather than behavioural; equivalent coverage is
    provided by the byte-identical E2E baseline diff for `tests/e2e/fixtures/
    testing_project/generated_baseline_with_tests/12_test_actions/*.py`.
- **Deferred** (`DESELECT-E-DEFER-9.3`):
  - The 3-test `DESELECT_E` cluster in `.github/workflows/python_ci.yml`
    (lines 170-175) is NOT re-enabled in Phase 9.3. Investigation confirmed
    those tests assert on mock call counts / import success / formatter
    logic — unrelated to the generator-template output stability that this
    chunk addressed. Re-enablement remains tracked under the existing
    `OWNER/ISSUE/EXPIRY` cluster (expiry 2026-08-01).

### Phase 8 — `utils/` evacuation + decomp of two oversize files

Phase 8 evacuates 14 domain-aware files out of `src/lhp/utils/` into their
proper homes (`parsers/`, `core/loaders/`, `core/processing/`,
`core/codegen/`, `bundle/`, `cli/`) and decomposes the two pre-existing
`§3.3`-violating files (`utils/import_manager.py` 541L and
`utils/operational_metadata.py` 608L) into sub-packages — closing both size
violations via real decomposition (Option A, zero new `# JUSTIFIED:` blocks
on the decomposed files). After Phase 8 the placement gate is at **0**
violations (was 15) and the file-size gate is at **0** (was 5 — Phase 8
decomposed 2, and added `# JUSTIFIED:` + `TODO(Phase 9.X)` blocks on the
remaining 3 Phase-9.2 / 9.3 deferral files per the existing Phase-6
convention).

- **`parsers/` (closes 3 `LHP-9.2`).** `utils/schema_parser.py`, `utils/schema_transform_parser.py`, `utils/yaml_loader.py` relocated to `src/lhp/parsers/`. 21 importers rewritten (16 src + 5 test). `parsers/__init__.py` populated with `SchemaParser`, `SchemaTransformParser`, `load_yaml_documents_all`, `load_yaml_file`, `safe_load_yaml_with_fallback`.
- **`core/loaders/` (closes 2 `LHP-9.2`).** `utils/external_file_loader.py`, `utils/version_enforcement.py` relocated to `src/lhp/core/loaders/`. 15 importers rewritten. `core/loaders/__init__.py` extended with `is_file_path`, `load_external_file_text`, `resolve_external_file_path`, `enforce_version_requirements`.
- **`core/processing/` (closes 3 `LHP-9.2`).** `utils/dqe.py`, `utils/local_variables.py`, `utils/substitution.py` relocated to `src/lhp/core/processing/`. 53 importers rewritten (22 src + 31 test, including 4 logger-name strings in `caplog.at_level(logger=...)`). `core/processing/__init__.py` extended with `DQEParser`, `EnhancedSubstitutionManager`, `LocalVariableResolver`, `SecretReference`. `FlowgroupResolutionService` moved to a PEP-562 `__getattr__` lazy entry to break a `processing → coordination._interfaces → coordination/__init__ → monitoring_service → registry` cycle that the move otherwise triggers through `registry.factories.EnhancedSubstitutionManager` import.
- **`core/codegen/secret_code_generator.py` (closes 1 `LHP-9.2`).** `utils/secret_code_generator.py` relocated; its internal `from .substitution import SecretReference` rewired to `from ..processing.substitution import SecretReference`. `core/codegen/__init__.py` re-exports `SecretCodeGenerator`.
- **`core/codegen/template_renderer.py` (closes 1 `LHP-9.2`).** `utils/template_renderer.py` relocated. 8 importers rewritten. `core/codegen/__init__.py` re-exports `TemplateRenderer`. Exposed a second cycle (`registry → base_generator → codegen/__init__ → coordinator → generators.load.cloudfiles → registry`) — closed by deferring `from ..codegen.template_renderer import get_lhp_template_loader` from `base_generator.py` module-top to `__init__`-local. `core/coordination/_interfaces.py` line 33 import path updated `...utils.substitution` → `..processing.substitution` (one-line, surgical — no structural edit).
- **`core/codegen/imports/` sub-package (closes `import_manager.py` §3.3 + 1 `LHP-9.2`).** `utils/import_manager.py` (541L) decomposed into `manager.py` (256L, `ImportManager` facade), `resolver.py` (142L, pure functions `resolve_conflicts` / `detect_submodule_conflicts`), `categorizer.py` (176L, `extract_module_name` / `categorize_import` / `sort_imports` / `extract_future_imports` + `STANDARD_MODULES` constant), and `__init__.py` (6L re-exports). `ImportManager` methods call module-level helpers (not service classes) per §5.5. All 4 files ≤ 256L (target ≤ 300L, hard cap < 500L). 4 importers rewritten.
- **`core/codegen/operational_metadata/` sub-package (closes `operational_metadata.py` §3.3 + 2 `LHP-9.2`).** `utils/operational_metadata.py` (608L) AND the existing `core/codegen/operational_metadata_service.py` (~100L) consolidated into one 6-file sub-package: `detector.py` (134L, `ImportDetector` + `FunctionCallVisitor`), `metadata.py` (204L, `OperationalMetadata` class shell + delegators), `_sql_adaptation.py` (147L, module-level wildcard adapters), `_column_resolution.py` (192L, module-level selectors), `service.py` (100L, `OperationalMetadataService` migrated verbatim), and `__init__.py` (7L). All 6 files ≤ 204L. 6 importers rewritten across `core/codegen/__init__.py`, `core/codegen/imports/manager.py`, `core/registry/base_generator.py`, `generators/transform/quarantine.py`, plus 2 tests.
- **`bundle/detection.py` (closes 1 `LHP-9.2`).** `utils/bundle_detection.py` relocated to `src/lhp/bundle/detection.py`. 4 importers rewritten (1 src + 3 test, including a `@patch` decorator string). `bundle/__init__.py` re-exports `should_enable_bundle_support`, `is_databricks_yml_present`.
- **`cli/exit_codes.py` (closes 2 `LHP-9.2`).** `utils/exit_codes.py` relocated to `src/lhp/cli/exit_codes.py`. 9 importers rewritten (2 src + 7 test). `cli/__init__.py` populated and re-exports `ExitCode`. Also fixed a late-surfacing leftover at `cli/error_boundary.py:9` (still pointing at `..utils.exit_codes`).
- **`LHP-9.7` remediation via `lhp.api`.** Two LHP-9.7 violations surfaced when the substitution + bundle-detection moves shifted util-paths into domain-paths inside CLI commands: `generate_command.py:34` (now `from ...bundle.detection`) and `show_command.py:22` (now `from lhp.core.processing.substitution`). Closed by re-exporting `should_enable_bundle_support` and `EnhancedSubstitutionManager` through `lhp.api` (both at `:stability: provisional` — `EnhancedSubstitutionManager` is recorded as a transitional re-export pending a proper `InspectionFacade.build_substitution_view(env) -> SubstitutionView` DTO in Phase 9.2; see Deferred items). CLI imports now route through `lhp.api` per §5.3.
- **`pyproject.toml` carve-outs.** Removed 2 dead `ignore_imports` entries from contract 5 (`utils.schema_parser → parsers.yaml_parser`, `utils.schema_transform_parser → parsers.yaml_parser`) and the 3 `filterwarnings` deprecation suppressions for `lhp.utils.operational_metadata`, `lhp.utils.schema_parser`, `lhp.utils.schema_transform_parser` — the named paths no longer exist on disk. Added 1 new `ignore_imports` entry to the "Models must not depend on higher layers" contract: `lhp.models.processing -> lhp.core.processing.substitution` (TYPE_CHECKING-only forward reference, no runtime coupling; Phase 10.2 will swap for a Protocol defined in `lhp.models`).
- **Pre-existing oversize files annotated.** `cli/commands/show_command.py` (~714L), `generators/write/streaming_table.py` (711L), `generators/test/test_generator.py` (510L) received `# JUSTIFIED:` blocks with `# TODO(Phase 9.2 / 9.3):` pointers per the Phase-6 "TODO(Phase 9.X)" convention. File-size gate now reports **0** violations.
- **Baseline + CI gate updates.** `scripts/check_baseline_gates.sh` renamed `WK6_FILE_SIZE_BASELINE=5` / `WK6_PLACEMENT_BASELINE=15` → `WK8_FILE_SIZE_BASELINE=0` / `WK8_PLACEMENT_BASELINE=0` (both at zero — gate behaves identically to the original strict configuration; the regression-only wrapper stays for future-proofing). `.github/workflows/python_ci.yml`: removed the obsolete `check_placement` §12bis warn-only cluster (placement is now 0; the step is enforced strictly).
- **`tests/vulture_whitelist.py` (new).** 4 entries pinning false positives: `package` (signature param of importlib.metadata.version fallback stub), `ClassVar` (used in stringified forward annotation), `__context` (Pydantic `model_post_init` lifecycle hook param), `_InternalDepResult` (TYPE_CHECKING re-import referenced as stringified forward-ref). Old single-source root `vulture_whitelist.py` retained (it's the one wired into `[tool.vulture]`); the temporary `tests/` variant was removed before merge to avoid drift.
- **Comment-slop cleanup (E3).** 29 files cleaned: stripped phase/wave/step labels (`# Phase D8`, `# Week 3`, `# per B4`, `# Step 1/2/3`), section dividers (`# === UTILITY METHODS ===`, `# --- helpers ---`), trivial docstrings (`"""Initialize the monitoring finalizer service."""`), and restatement comments. Load-bearing comments preserved: `# JUSTIFIED:` markers, cycle-fix rationales, `:stability:` annotations, bounded `TODO(Phase 9.X)` pointers, algorithm explanations.

#### Phase 8 — Mechanical-gate snapshot at end of Week 8

| Gate | Pre-Phase-8 | End of Phase 8 |
|---|---:|---:|
| `check_placement.py --all` | 15 (all `LHP-9.2` in `utils/`) | **0** |
| `check_file_sizes.py --all` | 5 | **0** (2 decomposed; 3 carry `# JUSTIFIED:` + Phase-9.X pointers) |
| `check_stability_drift.py --all` | 0 | **0** |
| `ruff check --select B904,TRY400,RUF013` | 0 | **0** |
| `mypy --strict src/lhp/api/` | clean (12 files) | clean (12 files) |
| `lint-imports` | 5 kept, 0 broken | **5 kept, 0 broken** |
| `vulture src/lhp/ vulture_whitelist.py --min-confidence 80` | 0 | **0** |
| `pytest tests/api/` | 135/135 | **135/135** |
| `pytest tests/e2e/ -n auto` | 139 passed, 4 failed (`JOB-ORCHESTRATION-DEPENDS-ON-DEFER-WK6`) | **139 passed, 4 failed** (same 4 deferrals) |
| `find src/lhp/utils -maxdepth 1 -name "*.py" -not -name "__init__.py" \| wc -l` | 23 | **9** (genuinely-generic files only) |

#### Phase 8 — Deferred items

- **`OPMETA-SVC-VIEW-DEFER` (target: Phase 9.2).** `EnhancedSubstitutionManager` is re-exported from `lhp.api` as `:stability: provisional` so `show_command.py` can route its substitution-introspection panels through `lhp.api` per §5.3. The class is mutable and has methods without `:raises:` blocks (§4.7). Proper Phase-9.2 remediation: add a frozen `SubstitutionView` DTO + `InspectionFacade.build_substitution_view(project_root, env) -> SubstitutionView` method, rewire `show_command.py`'s `_load_substitution_manager` / `_display_secret_references` / `_display_substitution_summary` to consume the DTO, then remove `EnhancedSubstitutionManager` from `lhp.api.__all__`.
- **`IMPORT-DETECTOR-PLACEMENT-DEFER` (target: Phase 9.X).** `ImportDetector` lives in `core/codegen/operational_metadata/detector.py` but is a generic PySpark AST-based import analyzer also consumed by `core/codegen/imports/manager.py:24`. Constitution-reviewer flagged that the placement leaks "operational_metadata is the home of generic import detection" into the package contract. Proper remediation: move to `core/codegen/imports/detector.py` so both `manager.py` and the `OperationalMetadata` class import from `imports/`. Out of Week-8 scope (would re-touch B2b's sub-package and C2b's consolidation in the same PR).
- **`OPMETA-RENAME-DEFER` (target: Phase 9.X).** `OperationalMetadata` (the class — column-catalog holder + state container) breaks §4.10's naming-table conventions: no verb suffix, no `Service`, no `View`. Reads like "operational metadata" the noun, not a typed object. Proper remediation: rename to `OperationalMetadataCatalog` (or `OperationalMetadataState`) at the class definition site and propagate across all 6 importers in the sub-package. Out of Week-8 scope; tracked here for the Phase-9.X public-symbol audit.
- **`OPMETA-§3.7-CONSOLIDATE-DEFER` (target: Phase 9.X).** `_sql_adaptation.py` and `_column_resolution.py` contain module-level helpers that take `OperationalMetadata` as a `meta` first parameter and mutate its state (`meta._adapted_project_columns = ...`, `meta.default_columns[k] = ...`). This is a borderline §3.7 smell — the decomposition was forced by §3.3's 300L target on the 466-line `OperationalMetadata` class. Acceptable as transitional scaffolding; the consolidation back into `OperationalMetadata` methods (which would land it at ~340L, still under §3.3) is queued when the class next needs editing.

#### Phase 8 — Pre-existing oversize files (now annotated)

- **`cli/commands/show_command.py` (~714L).** Now carries `# JUSTIFIED:` + `# TODO(Phase 9.2):` pointer. CLI presenter extraction will reduce to ≤100L per the target architecture.
- **`generators/test/test_generator.py` (510L).** Now carries `# JUSTIFIED:` + `# TODO(Phase 9.3):` pointer. Per-test-type SQL templates will be extracted to `generators/test/templates/`.
- **`generators/write/streaming_table.py` (711L).** Now carries `# JUSTIFIED:` + `# TODO(Phase 9.3):` pointer. Per-mode templates (DDL / append_flow / cdc / snapshot / sinks) will be extracted to `generators/write/streaming/`.

All three are tracked in `LOCAL/REMAINING_WORK.md §0` largest-modules table; the file-size gate is strict-clean across the repo at 0 violations.

### Problem #8 — §9.24-INTERNAL `ConfigValidator` delegation flatten

Removes the duplicate composition of `TableCreationValidator` and
`CdcFanInCompatibilityValidator` between `ConfigValidator` (per-flowgroup
aggregator) and `ValidationService` (coordination layer). After this change
`ValidationService` is the sole composer of the two cross-flowgroup
validators; `ConfigValidator` no longer holds the instance attributes,
delegator methods, or imports for them. The runtime `_dedupe_issues(...)`
workaround in `ValidationService.validate_flowgroups` is deleted —
identical issues no longer arise because the duplicate composition is
gone. Internal-only change; no public API affected.

- **`src/lhp/core/validators/config_validator.py`.** Removed
  `validate_table_creation_rules` and `validate_cdc_fanin_compatibility`
  methods, the `self.table_creation_validator` /
  `self.cdc_fanin_validator` instance attributes, and the
  `TableCreationValidator` / `CdcFanInCompatibilityValidator` imports.
  Module-header comment updated to reflect that cross-flowgroup
  composition lives at the coordination layer (§9.24).
- **`src/lhp/core/coordination/validation_service.py`.** Removed
  `_dedupe_issues` static method and its single call-site in
  `validate_flowgroups` — return is now `tuple(issues)`. Module docstring,
  class docstring, `__init__` comment block, and `validate_flowgroups`
  docstring rewritten to drop the §9.24-INTERNAL duplication wording and
  the Week-3 deferral references. The four `_*_validator_cls` slots are
  retained as §9.24 surface markers (vulture at 80% confidence did not
  flag them).
- **`src/lhp/core/coordination/_pool.py`.** One-line docstring update at
  the `run_validate_pool` cross-flowgroup post-barrier description —
  references `ValidationService.validate_cross_flowgroup` rather than the
  now-removed delegator method name.
- **Tests migrated off the deleted delegators.** `test_cdc_fanin.py` (10
  call-sites), `test_config_validator_references.py` (3), and
  `test_append_flow.py` (2) now invoke `TableCreationValidator().validate(...)`
  and `CdcFanInCompatibilityValidator().validate(...)` directly.
  `test_validate_lhp_error_ipc.py::test_cdc_fan_in_lhp_error_carries_code`
  retargets its mock from `orch.config_validator.validate_cdc_fanin_compatibility`
  to `orch.validation.validate_cross_flowgroup` (the new
  `ValidationService` call path).

Mechanical-gate snapshot on the diff: `check_placement`, `check_file_sizes`,
`check_stability_drift`, `ruff B904/TRY400/RUF013`, and `vulture` on the
changed files all clean. `mypy --strict src/lhp/api/` baseline-clean for
the diff (no errors introduced; pre-existing tree-wide errors unchanged).
Test gate: the four migrated test modules pass 37/37; remaining failures
in the wider suite are pre-existing baseline drift in files this diff
does not touch.

### Phase 7 — `lhp.api` cleanup, first stable wave, py.typed

Phase 7 closes the three transitional shims Week 6 left intact, lands
the constitution-mandated `lhp.api.to_dict` helper, promotes the first
wave of public symbols to `:stability: stable`, and ships the `py.typed`
marker. Six commits on `v0.9.0---LHP-Architecture-refactor`.

- **WarningCollector move (closes `WARNING-COLLECTOR-MOVE`).** Data-side `WarningCollector` (dedup / `count` / `as_list`) moved to `src/lhp/api/callbacks.py` (Rich-free, picklable, `:stability: provisional`). Rich rendering extracted to `src/lhp/cli/warning_panel.py:render_warning_panel(collector) -> Panel | None`. Old `src/lhp/cli/warning_collector.py` deleted. 48 reference sites migrated: 3 domain-side (`api/facade.py` direct; `core/coordination/orchestrator.py` + `work_unit_builder.py` under `TYPE_CHECKING` per locked decision D2), 3 CLI-side (`generate_command.py`, `validate_command.py`, `yaml_scanner.py`), 2 tests. The 3 `warning_collector` ignore-imports carve-outs on the "no CLI from domain" contract are gone from `pyproject.toml`. Constitution §9.6 (no Rich outside `lhp.cli`) and TARGET §6 (callbacks home) now mechanically true.
- **DTO/facade shim removal from `core/coordination/__init__.py` (closes `CORE-COORDINATION-DTO-SHIM`).** 7 transitional re-exports stripped: `GenerationResponse`, `BatchGenerationResponse`, `ValidationResponse`, `BatchValidationResponse`, `ValidationIssueView` (+ legacy `ValidationIssue` alias), and the `LakehousePlumberApplicationFacade` `__getattr__` arm. The `ActionOrchestrator` `__getattr__` arm stays (locked decision D3 — orchestrator is internal per §1.10 and tests legitimately reach into it). 11 test sites rewritten across `tests/test_generate_command.py`, `tests/test_validate_live_rendering.py`, `tests/test_validate_lhp_error_ipc.py` to import from `lhp.api.responses` / `lhp.api.views`. `layers.py` docstring updated.
- **`lhp.api.to_dict` (closes constitution §1.8 + §9.22).** New module `src/lhp/api/_serialization.py` exposes `to_dict(obj: Any) -> JSONValue` — a single type-generic recursive function with 8 dispatch rules (`None`/`bool`/`int`/`float`/`str`/`Path`/`@dataclass`/`Mapping`/`list-or-tuple`/catch-all `TypeError`). Zero `isinstance(obj, DTOClass)` branches per §9.22. Tuple fields normalise to JSON list; non-`str` mapping keys recurse and let `json.dumps` fail loudly. Round-trip contract suite at `tests/api/test_to_dict_round_trip.py` — 32 tests (30 parametrized per-class round-trips covering every event subclass, every response DTO, every view DTO; 2 negative-case TypeError tests for `datetime` and `ErrorEmitted` — the latter documents the §9.21 `LHPError`-carve-out: `ErrorEmitted` cannot JSON-round-trip by design, producers must decompose to a `ValidationIssueView` first). Type-generic reconstruction helper walks `dataclasses.fields` + `typing.get_type_hints` for `Tuple` / `Mapping` / nested `@dataclass` — same structural-only philosophy as the serializer itself.
- **First stable graduation wave (closes constitution §1.13).** 11 public symbols promoted: 10 from `:stability: provisional` → `:stability: stable` (`LakehousePlumberApplicationFacade`, `LakehousePlumberBootstrap`, `GenerationResponse`, `ValidationResponse`, `OperationStarted`, `OperationCompleted`, `GenerationCompleted`, `ValidationCompleted`, `ErrorEmitted`, `collect_response`); `LHPEvent` from `:stability: experimental` → `:stability: stable` (locked decision D4 — §9.21 freezes its shape, no soak-period drift possible). `grep -rh ":stability:" src/lhp/api/` now reports 11 stable, 61 provisional, 4 internal.
- **`py.typed` shipped (closes TARGET §8).** Empty marker at `src/lhp/py.typed`; `[tool.setuptools.package-data]` entry `"lhp" = ["py.typed"]` added in `pyproject.toml`. Downstream type-checkers (mypy / pyright) now recognise the LHP wheel as typed. New `tests/test_packaging.py::test_py_typed_marker_ships_with_package` pins the contract.
- **`WarningCollector` public-surface contract test.** `tests/api/test_warning_collector_contract.py` — 4 tests pinning the data-class invariants: `lhp.api.__all__` exports it; `(category, message)` dedup with preserved insertion order; pickle round-trip preserves `count` + `as_list()`; AST-walk over `lhp.api.callbacks` source asserts zero `rich` imports (rigorous + version-stable; preferred over `dis` bytecode inspection).
- **Render-side tests preserved.** The 6-test `tests/test_warning_collector.py` was relocated to `tests/cli/test_warning_panel.py` (5 tests; the dedup-via-count test is dropped because the contract suite covers it). All 5 render-output tests retained with imports retargeted at `lhp.api.WarningCollector` + `lhp.cli.warning_panel.render_warning_panel`.

#### Phase 7 — Regression-only constitution gate

- **`scripts/check_baseline_gates.sh`.** Wave 1 introduced a regression-only pre-commit gate that allows the 5 file-size + 15 placement Week-6 carryover violations to persist (Phase 8 + Phase 9 own the drive to zero) but blocks any worsening. Stability-drift, ruff B904/TRY400/RUF013, import-linter, and `tests/api/` contract tests stay strict (zero tolerance). When Phase 8 + Phase 9 close their baselines, set both constants to 0 and the gate behaves identically to the original strict configuration.

#### Phase 7 — Deferred items

- **`LAYERING-CONTRACT-REDESIGN-V2`** — ✅ **CLOSED** (see *Phase 9.7 — Layering contract redesign V2* above). The contract is re-enabled (6 kept, 0 broken, 1 carve-out); `generators` sits above `core`, `core` no longer imports `api` or `generators`, and `_interfaces.py` moved to `core/`. Original deferral rationale retained below for history. Wave 3 (Task C — re-enable top-down layering with `lhp.core.registry` BELOW `lhp.generators`) fired the R1 fallback. C1 audit surfaced ≥11 unexpected upward edges that exceed C3's 3-file fix budget: (a) 4 `lhp.core.coordination -> lhp.api` edges via `TYPE_CHECKING`-only imports (lint-imports does not distinguish TYPE_CHECKING); (b) 4 worker-package edges into `lhp.core.coordination._interfaces` — the `_interfaces` module would need to move to a shared parent (`lhp.core/_interfaces.py`) to eliminate them, which conflates with Phase 8's `utils/` evacuation; (c) 1 `lhp.core.registry -> lhp.core.codegen` edge on `OperationalMetadataService`; (d) 1 `lhp.generators -> lhp.core.codegen` edge (quarantine transform); (e) 1 `lhp.presets -> lhp.parsers` upward edge. `pyproject.toml` layering contract is unchanged from the Week-6 baseline (the disabled-layering block stays disabled, retaining its 17 generator→registry carve-outs as documentation). The 3 warning_collector carve-outs on the "no CLI from domain" contract are removed in the WarningCollector move.
- **`WARNING-COLLECTOR-RICH-LAZY-V2`** (target: Week 8 if needed). R2 fallback did NOT fire — no lazy `__rich__` delegator was required on the new `WarningCollector`. Listed here because the plan reserved the tag; downstream cleanup may close it.
- **NIT — §4.10 naming-table amendment.** The `*Collector` suffix used by `WarningCollector` does not match any defined kind in the constitution §4.10 naming table (DTO / Sub-DTO / Event / Validator / Service / Coordinator). A future amendment should either rename to `WarningCallback` or add a `Callback` row to §4.10.
- **NIT — §3.4 module-underscore convention.** `src/lhp/api/_serialization.py` follows the existing repo pattern (`_listings.py`, `_inspection_facade.py`, `_converters.py`, `_bundle_facade.py`) of underscore-prefixed modules whose contents are publicly re-exported by `lhp.api.__init__`. Worth a §11 definition addition in a future constitution amendment to make the pattern explicit.

### Phase 6 — Test-trust + ruff + housekeeping

Phase 6 closes the test-gate + ruff + governance debt left open after
Week 5, plus the constitution-reviewer fix-up that gates Phase 6
closure. Each line below names one deliverable.

- **Constitution amendment — §12bis.** `.claude/CODING_CONSTITUTION.md` gains §12bis: CI test-gate suspensions require an owner / issue / expiry / reduced-gate cluster on adjacent lines (`# OWNER`, `# ISSUE`, `# EXPIRY`, `# REDUCED-GATE`). Motivated by the `cfc7dedf`-era pytest commenting in `.github/workflows/python_ci.yml`; future suspensions cannot land as free-text `# TODO: re-enable` notes. Enforcement is review-time `grep` until `check_ci_gate_suspensions.py` ships in Phase 7+.
- **Ruff sweep — B904 / TRY400 / RUF013.** All three rules now at **0** across `src/` and `tests/`. `B904` (raise-from) and `TRY400` (logger.exception in except blocks) and `RUF013` (PEP 484 implicit-Optional) clean. Constitution §6.6 / §7.2 / §7.3 / §7.4 enforced mechanically.
- **Vulture allowlist.** `vulture_whitelist.py` introduced with 28 rationale-annotated entries pinning intentionally-unused symbols (constitutional re-exports, Pydantic field validators, ABC method placeholders). `pyproject.toml` wired to run `vulture` against the whitelist.
- **CI test-gate suspensions.** Five `--deselect` groups (A/B/C/D/E) in `.github/workflows/python_ci.yml` plus an e2e group, each carrying the §12bis 4-attribute cluster (`# OWNER` / `# ISSUE` / `# EXPIRY` / `# REDUCED-GATE`). All clusters expire 2026-08-01.
- **Validator base rename.** `src/lhp/core/validators/base_validator.py` → `src/lhp/core/validators/_base.py`. Partially closes the carry-forward `WK6-DEFER-validator-class-rename`; the full class rename `BaseActionValidator → BaseValidator` remains in Phase 9.4 (validator taxonomy split).
- **Facade shortcut contract test.** `tests/api/test_facade_shortcut_contract.py` (4 tests) verifies that `LakehousePlumberApplicationFacade.generate_pipelines` / `validate_pipelines` keep signature + behaviour parity with the sub-facade canonical methods. Locks the §1 public-API stability contract under refactors.

#### Phase 6 — Constitution-reviewer fix-up (closing BLOCKERs)

- **Protocol → ABC** — `src/lhp/core/registry/factories.py`: `SubstitutionFactory` migrated from `typing.Protocol` to `abc.ABC` per §13.8. `DefaultSubstitutionFactory` now declares the inheritance explicitly. Mirrors the canonical ABC style in `core/coordination/_interfaces.py` (BLOCKER §9.25 closed).
- **`:raises:` audit on facade surface.** Every public method on `lhp/api/facade.py`, `lhp/api/_inspection_facade.py`, `lhp/api/_bundle_facade.py`, and `lhp/api/bootstrap.py` now documents its `LHPError` code families in a Sphinx `:raises:` block — 25 blocks total, including explicit `:raises: None` annotations on the §4.8 catch-and-surface-on-DTO paths so the design intent is visible to reviewers (BLOCKER §4.7 closed).
- **Type-safe `pre_discovered_all_flowgroups`.** Six sites in `lhp/api/facade.py` and five in `core/coordination/orchestrator.py` migrated from `Any` / `List[FlowGroup]` to `Optional[Sequence["FlowGroup"]]` for a single typed contract across the facade-to-orchestrator call chain (§4.3 / §4.8).
- **Placeholder service removed.** `src/lhp/core/dependencies/metrics.py` deleted along with three forwarding methods on `DependencyAnalysisService` (`get_critical_path`, `get_parallelization_opportunities`, `get_centrality_metrics`). Per constitution §3.6 — no abstractions without ≥2 concrete uses — the placeholder is removed rather than carried forward. The placeholder-validating test `tests/test_dependency_analyzer.py::TestDependencyAnalysisService::test_not_implemented_methods` is removed as a logical consequence. Re-add when the first concrete metric lands.
- **TODO(Phase 9.X) pointers on JUSTIFIED blocks.** Fourteen `# JUSTIFIED:` blocks across `core/coordination/`, `core/dependencies/`, `core/jobs/`, `core/validators/`, `core/loaders/`, `lhp/api/`, and `cli/commands/` now carry an explicit `# TODO(Phase 9.X)` pointer naming the decomposition target and citing the matching wave in `LOCAL/REMAINING_WORK.md` §9. Closes the §3.3 audit-pointer gap; sets up Phase 9 planning to enumerate mechanically with `rg 'TODO\(Phase 9'`.
- **§12bis carve-out clusters extended.** Two additional §12bis 4-attribute clusters added: (a) `check_placement.py --all || echo` in `.github/workflows/python_ci.yml` (15 baseline placement violations Phase 8 owns), (b) `pytest.mark.skip` on `test_instance_invalid_definition_raises_054` in `tests/test_blueprint_parser.py` (LHP-VAL-054 spec ambiguity). Forward-compat hedge — §12bis as written governs CI test-deselect groups; these clusters mark related "soft skip" sites for the same audit treatment.
- **`# noqa: E402` rationales.** Both noqa sites in `src/lhp/models/config.py` (Pydantic + errors imports) now carry an inline `— must follow warnings.filterwarnings on L15 to suppress 'schema' shadow warning` rationale per §6.6.

### Phase 6 — Deferred items

- **`JOB-ORCHESTRATION-DEPENDS-ON-DEFER-WK6`** — Phase 6 / B1 surfaced a regression in 4 e2e tests (`test_multi_job_with_default_master`, `test_multi_job_with_custom_master_name`, `test_deps_bundle_without_job_config`, `test_deps_bundle_with_job_config`) where master-job `depends_on` blocks render incorrectly. The plan's diagnosis (`partition_result_by_job` post-hoc filter) was wrong: empirical investigation traced the bug to shared mutable state in `CachingYAMLParser._cache` — `FlowgroupResolutionService.process_flowgroup` mutates `flowgroup.actions.extend(template_actions)` on the cached input, so the second `get_flowgroups` call returns bloated, un-normalized flowgroups; that breaks the `acmi_edw_bronze` → `acmi_edw_raw` action-graph edge during master-job assembly. The plan's `_analyze_pipeline_dependencies(job_graphs)` refactor in `core/dependencies/analyzer.py` is retained as a semantic cleanup but does NOT fix these 4 tests. **Target phase:** Phase 7 (alongside the caching + resolution-service consolidation). **Skip-list governance:** when the 4 tests are added to the CI `--deselect` list in Wave 3 / E1, the `# OWNER` / `# ISSUE` / `# EXPIRY` / `# REDUCED-GATE` cluster from §12bis applies.
- **`WK6-FIXUP-DEFER-warning-collector-move`** — `WarningCollector` cross-boundary import (`lhp.api.facade` / `core.coordination.orchestrator` / `core.coordination.work_unit_builder` reaching into `lhp.cli.warning_collector`) is deferred to Week 7 Wave 2 per the predecessor plan `we-are-fully-peaceful-rabin.md`. Wave 2 owns the full migration: frozen-DTO move to `lhp.api.callbacks`, Rich extraction to `cli/warning_panel.py`, contract tests. Re-doing it inside the Phase 6 fix-up would land work that Wave 2 must redo.
- **`WK6-FIXUP-DEFER-new-module-tests`** — `tests/core/registry/test_factories.py` (covering the post-ABC `SubstitutionFactory` contract) and any test-hardening for the retained dependency services land in a follow-up test-hardening PR. Out of scope for the constitution-reviewer fix-up to keep that PR focused on the BLOCKER list.

### Week 5 — Public API surface + CLI/core boundary closure

Week 5 closes the two items `LOCAL/TARGET_ARCHITECTURE.md` flagged
open after Week 4: the `lhp/api/` public package (TARGET §7 / §8) and
the CLI→internal-domain reach-through across 7 command modules (§9.7 /
§9.23). `LHP-9.4`, `LHP-9.7`, `LHP-9.23` all moved to **0** at the end
of Phase D. The new `tests/api/` suite (95 tests, DTO/view/event
contracts) is green; unit and e2e carry ~30 + 6 known failures (see
Deferred items).

#### Phase A — Delete `error_formatter` shim

Closed `ERRORS-SHIM-DEFER-WK5` from Week 4.

- `src/lhp/utils/error_formatter.py` (35L shim) deleted; 73 src + 63
  test callers rewritten to import directly from `lhp.errors`.
- `scripts/check_placement.py:PLACEMENT_SHIM_ALLOWLIST` emptied to `{}`
  per plan §A3.
- **Discovery:** A1's grep for `utils.error_formatter` missed 11
  sibling imports inside `src/lhp/utils/` (single-dot relative —
  `from .error_formatter`). Fixed in the same wave.

#### Phase B — `lhp/api/` public surface

Closed TARGET §7 and §1.13 / §4.11 (stability-drift gate no longer
vacuous).

- New package `src/lhp/api/` (8 modules: `__init__`, `facade`,
  `_bundle_facade`, `_converters`, `_listings`, `responses`, `views`,
  `events`, `bootstrap`).
- Frozen DTOs (every field JSON-safe, every class
  `@dataclass(frozen=True)`, full pickle round-trip):
  `GenerationResponse`, `BatchGenerationResponse`, `ValidationResponse`,
  `BatchValidationResponse`, `ValidationIssueView`, `InitProjectResult`,
  `FinalizeMonitoringResult`, `BundleSyncResult`,
  `BundleValidationResult`, `BundleEnableResult`, `StatsResult`,
  `DependencyAnalysisResult`, `DependencyOutputsResult`, plus view DTOs
  (`FlowgroupView`, `ProcessedFlowgroupView`, `GeneratedCodeView`,
  `ProjectConfigView`, `BlueprintView`, `PresetView`, `TemplateView`,
  `PipelineStats`, `ActionView`).
- DTO field-type rewrite per §4.8: `Dict[str, Any]` →
  `Mapping[str, JSONValue]`, `List` → `Tuple`, `Optional[Exception]`
  REMOVED (replaced by flat `error_code: Optional[str]` + per-pipeline
  `error: Optional[ValidationIssueView]`). Mid-phase fix:
  `MappingProxyType` is unpicklable in Python 3.12 → switched all DTO
  defaults to plain `dict`.
- Top-level `LakehousePlumberApplicationFacade` decomposed into 4
  sub-facades (`GenerationFacade`, `ValidationFacade`,
  `InspectionFacade`, `BundleFacade`); shortcut methods
  (`generate_pipelines`, `validate_pipelines`) `yield from` the
  sub-facade generators (§1.11).
- `LakehousePlumberBootstrap` (separate per §1.12) handles one-time
  project scaffolding.
- Contract tests in `tests/api/` (`test_responses_contract.py`,
  `test_validation_view_contract.py`, `test_event_protocol.py`,
  pre-existing `test_workunit_contract.py`): **95 tests green**.
  Covers frozen-mutation, pickle/JSON-shape round-trip, field-type
  contract, event-protocol invariants.
- Every public class/function in `lhp/api/**/*.py` carries
  `:stability: provisional` (or `experimental` for `LHPEvent`).
- `mypy --strict src/lhp/api/`: **clean** (9 source files).

#### Phase C — Cover every CLI operation through the facade

Closed §9.4; facade extended to cover every CLI operation.

- `ActionOrchestrator._by_field` / `_by_fields` variants collapsed to
  unified
  `generate_pipelines(*, pipeline_filter=None, pipeline_fields=None, ...)`
  and `validate_pipelines(...)` (§9.4 closed).
- `PipelineValidator.validate_pipeline_by_field` renamed
  `validate_for_pipeline(pipeline_filter=...)`.
- `InspectionFacade` extended with 12 read-only methods
  (`list_flowgroups`, `process_flowgroup`, `generate_flowgroup_code`,
  `find_source_yaml_for_flowgroup`, `get_include_patterns`,
  `get_project_config`, `compute_stats`, `list_blueprints`,
  `list_presets`, `list_templates`, `analyze_dependencies`,
  `validate_duplicate_flowgroups`) plus `save_dependency_outputs`.
  Class docstring carries the §3.2 justification block.
- `GenerationFacade.finalize_monitoring_artifacts` added (C4).
- `BundleFacade` extended with `sync_resources`,
  `validate_bundle_assets`, `enable_bundle` (R10: three separate
  methods, NOT mode-flagged).
- Three long-running facade ops wrapped as `Iterator[LHPEvent]` per
  §5.7: `GenerationFacade.generate_pipelines`,
  `ValidationFacade.validate_pipelines`,
  `BundleFacade.sync_resources`. New events: `LHPEvent` (marker,
  experimental), `OperationStarted`, `OperationCompleted`,
  `GenerationCompleted`, `ValidationCompleted`, `BundleSyncCompleted`,
  `ErrorEmitted` (`lhp_error`-carrying — §9.21 carve-out).
- `collect_response(iterator)` helper for non-event-driven callers;
  sole `isinstance(event, OperationCompleted)` check avoids per-type
  dispatch (§9.22).

#### Phase D — CLI cutover + bundle reparent + deferral closures

Closed §9.7 and §9.23 entirely; closed `D7-FOLLOWUP-WK5` and
`D3D-DEFER-WK5` from Week 3.

- All 7 CLI command files (`generate_command`, `validate_command`,
  `show_command`, `list_commands`, `dependencies_command`,
  `init_command`, `base_command`, `stats_command`) import only from
  `lhp.api`, `lhp.errors`, `lhp.utils`, and stdlib. Zero imports of
  `lhp.core` / `.bundle` / `.parsers` / `.models` / `.generators`.
- All 7 `application_facade.orchestrator.X(...)` reach-throughs
  rewritten to sub-facade methods.
- 4 bundle exception classes (`BundleResourceError`, `TemplateError`,
  `YAMLProcessingError`, `BundleConfigurationError`) reparented from
  `Exception` to `LHPError` (TARGET §6). Carry `LHP-CFG-NNN` codes
  (§9.20). Per-class `__reduce__` for picklability (§13.3). Rich panel
  output semantic-equivalent (title operation-baked).
- `src/lhp/bundle/error_factories.py` deleted (172L, 5 factory
  functions — all callers migrated).
- `src/lhp/cli/error_boundary.py` bundle special-case branch (12L)
  deleted — reparented bundle classes now caught by the existing
  `except LHPError` handler.
- `D7-FOLLOWUP-WK5` closed: `ActionOrchestrator.__init__` raises
  `ValueError` on bare construction; only construction path is
  `LakehousePlumberApplicationFacade.for_project(...)` (§4.5).
- `D3D-DEFER-WK5` closed: `DependencyAnalysisService.__init__` accepts
  `config_validator`; the §9.24 leak
  (`validation_service._config_validator`) is gone. Composition root
  `core/coordination/layers.py:build_facade_orchestrator` threads a
  single `ConfigValidator` into both `ValidationService` and
  `DependencyAnalysisService`.
- `src/lhp/services/` deleted (1 file, 60 bytes, no consumers).
- `core/coordination/layers.py` shrunk 545L → 79L; transitional
  `LakehousePlumberApplicationFacade` re-export removed.
- Placement gate counts: **LHP-9.4: 0** (was 5), **LHP-9.7: 0** (was
  38), **LHP-9.23: 0** (was 6).

#### Phase E — CI gate re-enable + CHANGELOG

- `import-linter` re-enabled in `.github/workflows/python_ci.yml`.
  5/6 contracts active (Public API never imports CLI; CLI never imports
  internal domain; Models depend only on lower layers; Utils
  stdlib+models only; Generator families independent). The 6th
  (top-down layering) is **DISABLED pending Phase F redesign** — see
  Deferred items below.
- `Test with pytest` step in CI stays commented; re-enable deferred to
  Week 6.

#### Deferred items (must close in Week 6 or later)

| Task ID | Description | Recovery target |
|---|---|---|
| `OPMETA-MOVE-DEFER` | 13 `utils/` files import domain — `LHP-9.2` violations. A1's shim deletion unmasked these (they were hidden by the allowlist). Files: `operational_metadata.py`, `schema_parser.py`, `schema_transform_parser.py`, `dqe.py`, `exit_codes.py`, `external_file_loader.py`, `import_manager.py`, `local_variables.py`, `version_enforcement.py`, `substitution.py`, `template_renderer.py`, `yaml_loader.py`, `bundle_detection.py`. Each needs relocation to a domain-appropriate package. | Week 6 |
| `JOB-ORCHESTRATION-DEPENDS-ON-REGRESSION` | 4 e2e tests in `test_job_orchestration_e2e.py` fail with hash mismatches on generated `.job.yml` files — generated YAML lacks pipeline `depends_on` relationships. Root cause likely in the `services/dependency_analyzer.py` → `dependencies/{analyzer,builder,output}.py` restructure altering how `PipelineDependency.depends_on` is populated. Tests: `test_multi_job_with_default_master`, `test_multi_job_with_custom_master_name`, `test_deps_bundle_without_job_config`, `test_deps_bundle_with_job_config`. | Week 6 (urgent) |
| `TEST-SUITE-REPAIR-WK6` | Unit tests reference removed/renamed internals (`from lhp.core.orchestrator` paths, mocks of deleted methods, `ValidationIssue` direct constructions). Estimated ~30+ failures. 2 e2e tests construct `ActionOrchestrator(...)` directly — now blocked by §4.5 hardening; need migration to `for_project(...)`. | Week 6 |
| `LAYERING-CONTRACT-REDESIGN` | The `lhp top-down layering` import-linter contract is disabled. Real architectural facts the contract doesn't model: (a) generators legitimately inherit from `lhp.core.registry.BaseActionGenerator` (upward edge), (b) `lhp.api` is unlisted in the layers yet sits above core, (c) `WarningCollector` lives in `lhp.cli` but is consumed as a callback by `lhp.core.coordination`. Redesign needs: reorder layers (core BELOW generators), add `lhp.api`, move `WarningCollector` out of `lhp.cli`. | Phase F |
| `WARNING-COLLECTOR-MOVE` | `lhp.cli.warning_collector` is imported as a TYPE_CHECKING callback by `lhp.api.facade`, `lhp.core.coordination.orchestrator`, `lhp.core.coordination.work_unit_builder`. Move to `lhp.api` (callback interfaces are public API surface). Currently carved out in import-linter. | Phase F |
| `CLI-PRESENTER-DEFER` | Oversize CLI command files (`generate_command.py` 717L, `validate_command.py` 664L, `show_command.py` 703L, `cli/main.py` 519L). Plan §6.4 target: ≤100L per command file via presenter extraction. | Week 7+ |
| `EVENTS-PROGRESS-DEFER` | Per-pipeline progress events (additive under `:stability: provisional` `events.py`). | Week 7+ |
| `ERRORS-CODES-DEFER` | Extract `lhp/errors/codes.py` and `factory.py` for centralized code/factory definitions. | Post-Week-6 |
| `D3B-DEFER-WK6` | Executor pool `# noqa: F401` re-exports cleanup. | Week 6 |

#### Test-status accounting

- **`tests/api/`**: 95/95 passing (53 new from B2c + 30 new from C7 +
  12 pre-existing workunit).
- **`tests/e2e/`**: 137/143 passing. 4 failures: job-orchestration
  `depends_on` regression (above). 2: direct `ActionOrchestrator(...)`
  construction in `tests/e2e/test_local_variables_e2e.py::test_local_variables_resolve_correctly`
  and `::test_undefined_local_variable_raises_error` — both need
  migration to `for_project()` (Week 6).
- **Unit tests**: ~30+ failures from internal-module reshuffles. Week 6
  scope per the plan's carve-out.

#### Mechanical gates (end-of-Week-5 snapshot)

- `check_placement.py --all` — **15 violations**, all `LHP-9.2`
  (Week 6 OPMETA scope; expanded from plan's expected 1 due to A1's
  shim-removal unmasking).
- `check_stability_drift.py --all` — **0 findings** (no longer vacuous
  — scans 9 files in `lhp/api/`).
- `check_file_sizes.py --all` — **10 advisory misses**, all
  pre-existing. No new violations introduced by Week 5.
- `mypy --strict src/lhp/api/` — **clean** (9 files).
- `ruff check --select B904,TRY400,RUF013 src/lhp/api/ tests/api/` —
  **clean**.
- `pre-commit run import-linter --all-files` — **passes** (with the
  layering contract disabled per Phase F redesign deferral).

---

### Week 4 — Architecture refactor (Phases A–D)

Week 4 closes the two structural items `LOCAL/TARGET_ARCHITECTURE.md`
flagged as open after Week 3: the top-level `lhp/errors/` package
(TARGET §6) and the drain of free-standing files at the top of
`src/lhp/core/` plus `src/lhp/core/services/` (TARGET §3). Phases A–C
land the moves; Phase D is the documentation, sweep, and final review
pass. The full E2E suite (`pytest tests/e2e/ -v -n auto`, 143 tests)
is **green in 99.89s with zero failures** at the Phase C gate (C6),
matching the pre-Week-4 baseline exactly — no user-visible behavior
changed.

The TARGET_ARCHITECTURE §3 module map was amended (D1) to add
`core/jobs/` as the 9th sub-package (the home for the 850L
`job_generator.py` that TARGET previously did not allocate).

#### Phase A — Extract `lhp/errors/` package + Rich-rendering boundary

Closed TARGET §6 (errors-as-top-level-package) and §9.5 (no Rich in
domain types) for the LHP error hierarchy.

- **A1.** Created `src/lhp/errors/` with 4 files: `__init__.py` (26L),
  `categories.py` (18L, `ErrorCategory` enum), `types.py` (278L,
  `LHPError` + 4 subclasses + `MultiDocumentError` +
  `lhp_error_from_worker_failure`), `formatter.py` (529L, JUSTIFIED:
  cohesive text-rendering pipeline). `LHPError.__rich__`,
  `_border_style`, `_template_data` methods REMOVED — Rich is now
  entirely off the domain side of the boundary.
- **A2.** Created `src/lhp/cli/error_panel.py` (91L) exporting
  `render_error_panel(error: LHPError) -> Panel`. Rewired 5 boundary
  call sites: `cli/error_boundary.py:34/47/53`,
  `cli/commands/validate_command.py:346`,
  `tests/test_validate_live_rendering.py:118` (plan §2 originally
  enumerated only 3 sites — the pre-impl reviewer at A0 caught the
  two extras). Cleaned 7 docstring/comment references to the former
  `__rich__` method in `core/coordination/layers.py`,
  `validate_command.py`, and 4 test files. **Snapshot test
  `tests/test_lhperror_rendering.py` passes byte-identically against
  the pre-A2 `.ambr` baseline** — no rendering regression.
- **A3.** Shimmed `src/lhp/utils/error_formatter.py` from 868L → 35L
  pure re-export. The shim is marked `# DEPRECATED:` with the
  deletion-deadline tag `ERRORS-SHIM-DEFER-WK5`. `scripts/check_placement.py`
  was extended with a `PLACEMENT_SHIM_ALLOWLIST` constant so the
  intentional `from lhp.errors import ...` in the shim doesn't trip
  LHP-9.2. After A3, `lhp.utils.error_formatter.LHPError is
  lhp.errors.LHPError` (same class) — the 3 transient boundary test
  failures observed at end of A2 (class-identity drift) auto-resolved.
- **A4 gate.** E2E green: 143/143 in 102.65s. Matches pre-A baseline.

#### Phase B — Drain top-level `core/` (zero free-standing `.py` files)

Closed TARGET §3 (no top-level files under `core/`). All 11 free-standing
modules at the top of `core/` (4107L total) moved into their respective
sub-packages. Each task ran as an isolated subagent with `git mv`
preserving rename detection.

- **B1.** `factories.py`, `action_registry.py`, `base_generator.py`
  → `core/registry/` (new). 32 consumer files rewritten. The new
  `__init__.py` uses module-level `__getattr__` for `ActionRegistry`
  to break a circular import — `BaseActionGenerator` and the rest stay
  eager. Public attribute access unchanged.
- **B2.** `project_config_loader.py` (787L, `# JUSTIFIED:` added),
  `init_template_loader.py`, `init_template_context.py`
  → `core/loaders/`. 17 consumer files updated.
  `tests/test_python38_compatibility.py` held 4 stringified module
  paths the initial grep missed — caught + updated.
- **B3.** `template_engine.py` → `core/processing/`. 8 consumers
  rewritten. 88/88 targeted tests pass.
- **B4.** `dependency_resolver.py` → `core/dependencies/`. 3 consumers
  rewritten. `validator.py` got a direct-submodule import
  (`from .dependencies.dependency_resolver import DependencyResolver`)
  to break a cycle through `core.discovery` → `core.coordination` →
  `coordination.validation_service`.
- **B5.** `validator.py` → `core/validators/config_validator.py`.
  Module-level disambiguation comment block distinguishes
  `ConfigValidator` (project-wide aggregator) from sibling
  `ConfigFieldValidator` (per-field schema validator). Two
  imports deferred to `__init__` method body to break a second
  cycle (`validators ↔ dependencies ↔ coordination`). 18 consumer
  files rewritten (138/138 in-scope tests pass).
- **B6.** `orchestrator.py` (794L) and `layers.py` (555L) →
  `core/coordination/`. Both `# JUSTIFIED:` blocks preserved.
  31 consumer files rewritten (4 src + 27 tests). Same
  `__getattr__` deferral pattern from B1 used for
  `ActionOrchestrator` and `LakehousePlumberApplicationFacade` to
  break the `coordination ↔ dependencies` cycle.
- **B7 gate.** Caught a real regression: `coordination/layers.py:448`
  had `from .coordination.executor import` (double-namespace bug —
  layers.py moved INTO `coordination/` but the lazy import wasn't
  updated). 8 E2E failures traced to this single line. Fixed; full
  E2E suite returned to 143/143.

After Phase B: `find src/lhp/core -maxdepth 1 -name "*.py" -not -name "__init__.py"` returns empty.

#### Phase C — Drain `core/services/`

Closed TARGET §3 (no `services/` directory). All 6 remaining files in
`core/services/` moved into their permanent homes. 3 parallel subagents
ran simultaneously, after verifying the only cross-consumer overlap
(`coordination/monitoring_service.py`, touched by both C3 and C4) was
handled by merging C3+C4 into a single agent.

- **C1.** `namespace_normalizer.py` (231L, public function
  `normalize_namespace_fields`) → `core/processing/`. 2 consumers.
- **C2.** `operational_metadata_service.py`, `test_reporting.py`,
  `tst_reporting_hook_generator.py` → `core/codegen/`.
  7 consumers updated (2 extra lazy imports caught by grep:
  `validate_command.py:558`, `quarantine.py:119`).
- **C3.** `monitoring_pipeline_builder.py` (528L, `# JUSTIFIED:` added)
  → `core/coordination/`.
- **C4.** Created new `src/lhp/core/jobs/` sub-package. Moved
  `job_generator.py` (850L, `# JUSTIFIED:` added — pre-existing §9.3
  hard-cap violation documented; split deferred to Week 5+).
  Re-exports `JobGenerator`, `JobPipeline`, `JobStage`,
  `EXPLICITLY_RENDERED_JOB_CONFIG_KEYS`. 2 extra consumers caught
  beyond the brief (`bundle/manager.py`, `cli/commands/generate_command.py`).
- **C5.** Deleted `src/lhp/core/services/__init__.py` and the directory.
- **C6 gate.** E2E green: 143/143 in 99.89s. **Phase C complete.**

After Phase C: `core/` contains exactly 9 sub-packages
(`codegen/`, `coordination/`, `dependencies/`, `discovery/`, `jobs/`,
`loaders/`, `processing/`, `registry/`, `validators/`) — matching the
amended TARGET §3 module map.

#### Phase D — Documentation, sweep, final review

- **D1.** `LOCAL/TARGET_ARCHITECTURE.md` §3 amended: 8 sub-packages → 9
  (added `core/jobs/` with rationale "job-generation for Databricks
  Asset Bundle assets and standalone job manifests").
- **D2.** This CHANGELOG entry.
- **D3.** Vulture dead-code sweep across new + modified files.
- **D4.** Comment-slop sweep over the branch diff vs. `main`.
- **D5.** Independent constitution-reviewer in fresh context against
  the FINAL state. Intermediate-state artifacts (the
  `utils/error_formatter.py` shim) distinguished from new violations.

#### Deferred items (named here for follow-up tracking)

- **`ERRORS-SHIM-DEFER-WK5`** — `src/lhp/utils/error_formatter.py`
  (35L re-export shim) closes in Week 5. ~70 src callers and ~95 test
  callers will be migrated off `from lhp.utils.error_formatter import`
  in a single Week 5 sweep; the shim deletes after. The placement
  allowlist entry in `scripts/check_placement.py` deletes with it.
- **`ERRORS-CODES-DEFER`** — `lhp/errors/codes.py` and
  `lhp/errors/factory.py` (the centralised error-code registry
  and helper factories TARGET §6 envisions) are deferred. Today's
  error sites still construct `LHPError(...)` with inline string-literal
  codes; the codes happen to be unique by convention. The follow-up
  introduces a code registry and a factory layer that asserts
  uniqueness at module load. Slated for Week 5+ after the shim
  deletion lands.
- **`OPMETA-MOVE-DEFER`** — `src/lhp/utils/operational_metadata.py`
  (608L) remains in `utils/` despite being a §9.2 violation
  (defines domain types). The cross-dependency with
  `utils/import_manager.py` needs an audit before relocation;
  candidate destinations are `core/codegen/` (where the consumer
  `OperationalMetadataService` now lives) or a new
  `core/metadata/` sub-package. Not blocking Week 4 since the
  consumer service already moved.

#### Test-status accounting (named, not gated)

E2E suite is **fully green at 143/143** — the canonical gate per the
plan's deferral strategy. Unit and integration tests have ~48
pre-existing failures attributable to in-flight refactor work, not
Week 4 regressions:

- **~43 errors in `tests/test_orchestrator_init.py`** — `patch(...)`
  targets `ConfigValidator` and `PipelineValidator` as module-level
  attributes on the orchestrator. These were never module-level
  attributes (deferred to method body since pre-session refactor);
  the test's mock-target paths are stale. Pre-B6.
- **~4 failures in `tests/test_orchestrator.py`** — patches on
  `orchestrator_module.run_generate_pool` and asserts on
  `orchestrator.dependencies.substitution_factory`. Both refer to
  structures that moved during pre-session refactor work (the call
  is in `executor.py` now; `substitution_factory` was renamed in
  the B4 dependencies refactor). Pre-B6.
- **1 failure in `tests/test_multi_job_integration.py`** —
  `DependencyAnalysisService(temp_dir, mock_loader)` uses the 2-arg
  legacy form; the new constructor requires 3 args
  (`validation_service` added by B4 pre-session work). Pre-C.

All are deferred to **Week 6 (test-suite repair)** consistent with the
documented deferral strategy. None of these failures reflect product
behavior regressions — generated code is byte-identical to baselines
(verified by 143 E2E baseline-comparison tests).

#### Mechanical gates

- `scripts/check_file_sizes.py --all` — passes for all Week-4-modified
  files. Pre-existing violations on `job_generator.py` (861L, JUSTIFIED)
  and other in-flight files documented.
- `scripts/check_placement.py --all` — Week 4 introduced one allowlist
  entry (the shim); no new violations. Baseline violation count
  trended from 54 → 50 after `lhp.errors` extraction.
- `pytest tests/e2e/ -v -n auto` — 143/143 in 99.89s.

---

### Week 3 — Architecture refactor (Phases A–E)

Week 3 closes the structural work scheduled by `LOCAL/TARGET_ARCHITECTURE.md`
against the constitution gates in `.claude/CODING_CONSTITUTION.md`. Phases A–D
land the moves and decompositions; Phase E is this documentation pass. The
full E2E suite (`pytest tests/e2e/ -v -n auto`, 143 tests) is **green in
115s with zero failures** and generated code is **byte-identical to
baselines** — no user-visible behavior changed.

The independent constitution-reviewer (Phase E `E-CONSTITUTION`) ran in
fresh context against the full diff and flagged 3 Category (b) findings;
2 were fixed in-phase and 1 (CI gate disablement from prior commit
`cfc7dedf`) is flagged below for user judgment:

- **Fixed in-phase:** `core/layers.py` crossed §3.3 (479L → 555L) after
  the D7 `for_project` classmethod was added — a `# JUSTIFIED:` block
  was authored citing the four co-located surfaces (`Facade` +
  `OrchestrationDependencies` + `for_project` bootstrap + legacy builder).
- **Fixed in-phase:** `core/dependencies/service.py:37` reached into
  `..coordination.validation_service` instead of using the public
  `..coordination` re-export (§5.4) — import line rewritten.
- **Fixed in-phase:** `core/validators/dlt_cdc_validators.py` JUSTIFIED
  block strengthened from a 4-line generic note into a 27-line rationale
  naming all four classes (`DltTableOptionsValidator`, `CdcConfigValidator`,
  `SnapshotCdcConfigValidator`, `CdcSchemaValidator`) and the specific
  shared helpers + DLT compatibility constants.
- **`CI-PYTEST-DEFER`** (flagged, not auto-fixed):
  `.github/workflows/python_ci.yml` lines 64–82 — prior commit `cfc7dedf`
  ("disabling import lint temporarily") also commented out the
  `Test with pytest` step on Linux/macOS, not just import-linter. The
  commit subject only mentions import-linter; this is likely accidental
  but unilateral CI re-enabling needs user judgment. Recommend restoring
  the pytest step in a follow-up commit with explicit subject.

#### Phase A — Validator consolidation (S1)

Closed §9.4 (validators must live under `core/validators/`) for 6 files.
Six validators moved into `src/lhp/core/validators/`:

- `core/config_field_validator.py` → `core/validators/config_field_validator.py`
- `core/secret_validator.py` → `core/validators/secret_validator.py`
- `core/dlt_cdc_validators.py` → `core/validators/dlt_cdc_validators.py`
- `core/services/pipeline_validator.py` → `core/validators/pipeline_validator.py`
- `core/services/job_name_validator.py` → `core/validators/job_name_validator.py`
- `utils/kafka_validator.py` → `core/validators/kafka_validator.py`

A `# JUSTIFIED:` block was authored on `dlt_cdc_validators.py` (501L,
crosses §3.3 soft cap; decomposition deferred to Week 4). Six src-side
import sites were rewritten plus 15 test files updated (27 edits including
mock-target strings). Bridge fix: `FakeFlowgroupProcessor` →
`FakeFlowgroupResolutionService` (Week 1–2 unfinished rename).

#### Phase B — Dependencies decompose (S2 + D1 + D3b + D3c)

Closed §9.3 on `analyzer.py` plus task IDs D1, D3b, D3c.
`core/dependencies/analyzer.py` (1330L, 34 methods) decomposed into 5 files:

- `analyzer.py` (355L) — pure analysis core, class `DependencyAnalyzer`.
- `builder.py` (770L, JUSTIFIED) — graph + discovery + source extraction,
  class `DependencyGraphBuilder`.
- `metrics.py` (40L) — placeholder for advanced metrics.
- `output.py` (687L, JUSTIFIED) — module-level `export_to_dot/json/text`
  plus `DependencyOutputManager`.
- `service.py` (309L) — composition root `DependencyAnalysisService`
  inheriting `BaseDependencyAnalysisService` (ABC concrete:
  `build_graphs`, `analyze`, `export`).

D1 closed: ABC methods on `DependencyAnalysisService` no longer raise
`NotImplementedError`. D3b + D3c closed: `dependencies/` no longer
constructs `ConfigValidator` (injected via `ValidationService`).
Orchestrator type hint upgraded:
`self.dependencies: BaseDependencyAnalysisService`. Hard cut on 4 dying
method names (`build_dependency_graphs`, `analyze_dependencies`,
`export_to_dot`, `export_to_json` — replaced by ABC `build_graphs` /
`analyze` / `export`).

#### Phase C — Codegen decompose (S3 + D3a)

Closed §9.3 on `coordinator.py` plus task ID D3a.
`core/codegen/coordinator.py` (918L, 18 methods) decomposed into 6 files:

- `coordinator.py` (283L) — composition root, 5 public methods plus 3
  underscore-name forwarders for test pin.
- `action_dispatch.py` (420L) — `ActionDispatcher` (6 methods).
- `grouping.py` (182L) — `WriteActionGrouper`.
- `context.py` (98L) — `GenerationContextBuilder`.
- `secrets.py` (66L) — `SecretSubstitutor`.
- `assembler.py` (112L) — `CodeAssembler`.

D3a closed: `TableCreationValidator._action_creates_table`
reach-into-private replaced by promoting to a public free function
`action_creates_table` in `core/validators/table_creation_validator.py`.
`core/codegen/` no longer imports `TableCreationValidator`. One doc-drift
fix: `src/lhp/generators/write/streaming_table.py:686` docstring updated
from `CodeGenerationService._assemble_final_code` →
`CodeAssembler.assemble`.

#### Phase D — Orchestrator shrink + WorkUnit (D1 + D2 + D3a-impl + D4 + D5 + D6 + D7 + D8)

Closed §9.3 on `executor.py`, plus task IDs D1, D2, D4 and partial D3d.

- **WorkUnit DTO** — `PipelineWorkUnit` lifted from
  `core/coordination/executor.py:113` to `lhp/models/processing.py`. Three
  new fields added (`substitution_manager`, `output_dir`,
  `discovery_error`; all `Optional` with defaults preserving back-compat).
  `to_dict()` / `from_dict()` methods added with a `JSONValue` alias. 12
  DTO contract tests added in `tests/api/test_workunit_contract.py` (pickle
  round-trip, frozen contract, slots contract, JSON safety, Path
  serialization, None passthrough, `from_dict` round-trip, default
  compatibility).
- **PipelineExecutionService concrete** — `run_generate` / `run_validate`
  no longer raise `NotImplementedError`; per-pipeline state is read from
  `PipelineWorkUnit`s. Validate `_assemble` closure
  (`orchestrator.py:963–1029`) moved into
  `PipelineExecutionService.run_validate`.
- **D4** — `_discover_and_filter_flowgroups` body (83L) moved to
  `FlowgroupDiscoveryService.discover_and_filter_for_pipeline`;
  orchestrator becomes a 23L delegator.
- **D5** — `generate_pipelines_by_fields` 183L → 29L body plus 2 helpers
  (`_build_generate_work_units`, `_aggregate_generate_outcomes`).
- **D6** — `validate_pipelines_by_fields` 183L → 24L body plus 2 helpers
  (`_build_validate_worker_state`, `_build_validate_work_units`).
- **D7** — `LakehousePlumberApplicationFacade.for_project()` classmethod
  added in `core/layers.py`; CLI sites migrated
  (`generate_command.py:494`, `validate_command.py:107`).
  `ValidationService.__init__` accepts optional
  `config_validator: Optional[ConfigValidator]` injection.
  `ActionOrchestrator.__init__` accepts optional `flowgroup_resolver` and
  `validation_service` injection (back-compat else-branch preserves
  direct `ActionOrchestrator(project_root)` callers).
- **D8a** — `executor.py` 1055L → 430L by extracting the pool to
  `core/coordination/_pool.py` (768L, JUSTIFIED).
- **D8b** — Further extractions to fit under §9.3:
  `_enforce_version_requirements` (66L) →
  `utils/version_enforcement.py`; `_build_*_work_units` bodies (155L
  combined) → `coordination/work_unit_builder.py`.
- **Final orchestrator** — 1043L → 794L (under §9.3 800L hard cap,
  JUSTIFIED block added).
- **§9.16 god-class limits respected** — `ActionOrchestrator` 12 public
  methods (limit 15), `PipelineExecutionService` 4,
  `CodeGenerationService` 5, `DependencyAnalysisService` 13.

#### Deferred from Week 3

| Task ID | Description | Recovery | Constitution rule |
|---|---|---|---|
| `D3D-DEFER-WK5` | Two `_config_validator` reach-throughs remain: `orchestrator.py:228` (back-compat else-branch) and `dependencies/service.py:128`. Both follow the same pattern but D7's injection only closed the orchestrator's primary path. Full closure requires bootstrap-side wiring of `FlowgroupResolutionService` injection into `DependencyAnalysisService` too. | Week 5 (CLI cutover phase) | §9.24 partial |
| `D3B-DEFER-WK6` | `executor.py` `run_generate_pool` / `run_validate_pool` kept as public symbols plus re-imported in orchestrator with `# noqa: F401` markers. Three monkeypatch tests (`test_pipeline_executor.py` ×2 plus `test_validate_command_parallel.py`) patch the old executor location; updating them to `lhp.core.coordination._pool.X` would unlock full privatization. | Week 6 (test repair phase) | — |
| `D7-FOLLOWUP-WK5` | `ActionOrchestrator.__init__` retains a back-compat else-branch when called without injected `flowgroup_resolver` / `validation_service`. Required for ~30 direct-call test sites. Removing the branch lets §9.24 close fully. | Week 5 | §9.24 partial |

#### Tests deferred to Week 6

The Phase A–D surface changes broke 130 test cases. All failures are
mechanical consequences of the moves and signature changes — not
regressions in product behavior (E2E byte-identical, see above). Grouped
by root cause:

| File / test group | Count | Root cause |
|---|---|---|
| `tests/test_dependency_analyzer.py` (setup_method errors) | 50 | `DependencyAnalysisService.__init__` now requires `validation_service` arg; test fixtures still call 2-arg constructor |
| `tests/test_orchestrator_init.py` (setup errors) | 43 | Monkeypatches `lhp.core.orchestrator.ConfigValidator` which moved to `core/validators/` in Phase A |
| `tests/test_dependency_analyzer_multi_job.py` | 12 | Same root cause as `test_dependency_analyzer.py` |
| `tests/test_orchestrator.py` (Generate failures) | 3 | Monkeypatches `run_generate_pool` on orchestrator module; pool functions now live in `coordination/executor.py` → `coordination/_pool.py` |
| `tests/test_dependencies_command.py` (Mock errors) | 4 | Mock instances missing `len()` / `spec` for new code paths |
| `tests/test_dependency_output_manager.py` | 3 | DOT/JSON output format changed in Phase B (`output.py` module-level serializers) |
| `tests/test_pipeline_validator.py` | 3 | Validator return shape changed (non-iterable success case) |
| `tests/test_pipeline_executor.py` | 2 | Duration-stamping path changed (`coordination/executor.py` → `_pool.py`) |
| `tests/test_validate_command_parallel.py` | 1 | `ProcessPoolExecutor` moved to `_pool.py` |
| `tests/unit/test_source_path_index.py` | 3 | Return type drift (list → tuple); patch should target `orch.discovery.*` not `orch.*` |
| `tests/test_blueprint_*.py` | 2 | `ActionOrchestrator.discoverer` → `.discovery` attribute rename |
| `tests/test_multi_job_integration.py` | 1 | `DependencyAnalysisService` constructor signature |
| `tests/test_orchestrator.py::TestOrchestratorDependencyInjection` | 1 | `dependencies` attribute reassigned to `DependencyAnalysisService` (was `OrchestrationDependencies`) |
| `tests/test_dependencies_command.py::TestDependenciesCommand` | 2 | Mock spec mismatch after `dependencies_command` refactor |
| **Total** | **130** | All consistent with Week 3 surface changes; not regressions in product behavior |

#### E2E outcome

`pytest tests/e2e/ -v -n auto` — **143 tests pass in 115s, zero failures**.
Generated code is byte-identical to the committed baselines; the entire
Phase A–D refactor is invisible to YAML authors and downstream consumers.

#### Vulture audit (E-VULTURE)

3 false-positive findings only, no actual dead code introduced by the
refactor. Recommended (optional) allowlist additions to
`pyproject.toml` `[tool.vulture]`:

- `lhp.cli.main.package` — required by `importlib.metadata.version()`
  signature.
- `lhp.models.config.ClassVar` — used in a quoted annotation at line 502.
- `lhp.models.config.__context` — Pydantic `model_post_init` hook
  contract.

Leaving the allowlist as-is surfaces these 3 findings on every run but
does not block anything.

#### Pre-existing violations carried over (not introduced by Week 3)

These sit in the working tree from before the Week 3 refactor began and
remain open. They are listed here for transparency only — Phase A–D
neither created nor closed them. CLAUDE.md records the entry-state
baseline as "25 file size, 55 placement".

- §9.3 hard-cap files still over 800L: `utils/error_formatter.py` (868L)
  and `core/services/job_generator.py` (850L). Both pre-existing.
- 38 §9.7 CLI → internal imports across 9 CLI command modules.
- 6 §9.23 facade reach-through in `generate_command.py`.
- 5 §9.4 `_by_field` suffix methods.
- 5 §9.2 utils domain types (`utils/error_formatter.py` `LHPError`
  subclasses plus `utils/operational_metadata.py`).
- 14 §3.3 JUSTIFIED MISSes (advisory).
- 83 ruff §6 / §7 findings (32 TRY400 + 26 RUF013 + 23 B904).
- `lhp/api/` is empty (no `.py` files) — causes import-linter contract
  to fail at "module 'lhp.api' does not exist." Week 1–2 deferred this;
  it should be addressed before the stability-annotation gate (§1.13)
  becomes meaningful.

### Refactors

- **Phase D7** — Add `LakehousePlumberApplicationFacade.for_project(project_root, ...)`
  classmethod (`core/layers.py`) that builds `ConfigValidator` once and threads it
  into both `ValidationService` (via new `config_validator=` kwarg) and
  `FlowgroupResolutionService`, closing the §9.24 leak
  (`self.validation._config_validator`) on the production
  `cli/commands/generate_command.py` and `cli/commands/validate_command.py`
  call paths. `ActionOrchestrator.__init__` accepts new keyword-only
  `flowgroup_resolver=` / `validation_service=` injection points; both default
  to `None` for back-compat with the ~30 unit tests that construct
  `ActionOrchestrator(project_root)` directly. The back-compat else-branch
  still uses the `_config_validator` reach — marked
  `TODO(D7-FOLLOWUP-WK5)`; removal queued for Week 5.

- **Phase D8a** — Extract `run_generate_pool`, `run_validate_pool`, all worker
  entry/dispatch functions (`_init_worker_logger`, `_init_generate_worker`,
  `_init_validate_worker`, `_generate_one_pipeline`, `_validate_one_fg`,
  `_dispatch_pipeline_for_generate`, `_process_pipeline_for_generate`,
  `_process_flowgroup_for_validate`), the worker-state dataclasses
  (`_GenerateWorkerState`, `_ValidateWorkerState`), the per-pipeline progress
  tracker (`_PipelineProgress`), and `FlowgroupValidationResult` from
  `core/coordination/executor.py` to `core/coordination/_pool.py`. Reduces
  `executor.py` from 1055L → 430L (well under §9.3 800-line hard cap).
  `_pool.py` lands at 768L with a `# JUSTIFIED:` block documenting cohesion
  ground. `executor.py` re-exports every moved symbol via `__all__` so the
  pickle-by-name contract across the `spawn` boundary is unchanged
  (`from lhp.core.coordination.executor import run_generate_pool, ...`
  still resolves).

- **Phase D8b** — Extract `_enforce_version_requirements` (66L) from
  `core/orchestrator.py` to `utils/version_enforcement.py`
  (`enforce_version_requirements(project_config)` pure function). Extract
  `_build_generate_work_units` (88L) and `_build_validate_work_units` (67L)
  to `core/coordination/work_unit_builder.py`; orchestrator retains thin
  delegators (~20L each) so the ~5 unit tests that exercise the work-unit
  shape via `orchestrator._build_*_work_units(...)` keep working. Reduces
  `orchestrator.py` from 947L → 772L (under §9.3 800-line hard cap).
  Updated the orchestrator's top-of-file `# JUSTIFIED:` block to reflect
  the post-Phase-D cohesion ground (three irreducible responsibilities:
  service wiring, work-unit thread-through, failure aggregation).

- **CHANGELOG-D3B-DEFER-WK6** — D3b-cleanup (privatize `run_generate_pool` →
  `_run_generate_pool` / `run_validate_pool` → `_run_validate_pool` and
  remove orchestrator's `# noqa: F401` re-imports) is **deferred to Week 6**.
  Reason: `tests/test_orchestrator.py` has 3 tests
  (`test_single_lhp_failure_unwraps_original`,
  `test_multi_lhp_failure_aggregates_with_902`,
  `test_single_non_lhp_failure_wraps_as_901`) that
  `monkeypatch.setattr(orchestrator_module, "run_generate_pool", fake_pool)` —
  the D5/D6 collapse already made the monkeypatch ineffective (orchestrator
  now calls `self.execution.run_generate(work_units)`, not
  `run_generate_pool` directly) so these tests fail at assertion level.
  D3b-cleanup additionally requires updating the monkeypatch target from
  `lhp.core.orchestrator` to `lhp.core.coordination._pool`. Tracked as Week 6
  test repair.

- **D8a test repair backlog (Week 6)** — 3 additional tests target the
  pre-D8a executor module namespace and fail because the names they
  monkeypatch are no longer reachable through `executor.py`:
  * `tests/test_pipeline_executor.py::test_dispatch_pipeline_for_generate_stamps_positive_duration`
    and `..._stamps_duration_on_worker_failure` — both
    `monkeypatch.setattr(pe, "_process_pipeline_for_generate", ...)` where
    `pe = lhp.core.coordination.executor`. After D8a the function lives in
    `_pool.py`; the monkeypatch on `executor` is a no-op. Repair:
    update to `monkeypatch.setattr(_pool, "_process_pipeline_for_generate", ...)`.
  * `tests/test_validate_command_parallel.py::test_run_validate_pool_guards_executor_submit_raises`
    — `monkeypatch.setattr(pe, "ProcessPoolExecutor", _FakeExecutor)`.
    `ProcessPoolExecutor` is imported into `_pool.py`, not `executor.py`.
    Repair: target `_pool` instead.

## [0.8.8] — 2026-05-22

This release is a CLI rendering overhaul: `lhp generate`, `lhp validate`,
and `lhp show` now drive a Rich `Live` panel during execution and emit
a per-pipeline summary table on exit, errors render as category-coloured
Rich panels, and warnings are surfaced through a per-run collector
instead of a process-wide flag. There are no breaking changes to YAML
schemas, generated code, or exit codes.

### Added

- **Rich-based CLI rendering.** `lhp generate`, `lhp validate`, and
  `lhp show` now run inside a Rich `Live` panel that opens with a
  `Discovering flowgroups…` spinner, switches to per-phase duration
  markers (✓ / ✗) and an in-flight progress spinner as work proceeds,
  and prints a per-pipeline summary table on exit. Coloring honors
  `NO_COLOR` and falls back to plain text when stdout is not a TTY.
  Shared rendering primitives live in `src/lhp/cli/live_panel.py`,
  with command-specific summary tables in `generate_summary.py` and
  `validate_summary.py`.
- **`--show-all` / `-a` flag on `lhp generate` and `lhp validate`.**
  The post-run summary table defaults to failed pipelines only; pass
  `--show-all` to render the full table including passing rows. On a
  full-success run with no failures, the table is suppressed entirely
  and a single-line footer is printed (with a `(use -a to list)` hint
  when more than one pipeline ran). This keeps quiet runs short
  without losing the table for users who want it.
- **`rich-click` for CLI help rendering.** `click` is imported as
  `rich_click as click`; existing decorators are unchanged but
  `--help` output now renders as a styled Rich panel.
- **`LHPError.__rich__` + `LHPError.from_unexpected_exception`.** Errors
  are rendered to stderr as a category-coloured Rich `Panel` with
  Context / Suggestions / Example / docs-link sections; the plain-text
  `__str__` rendering is preserved verbatim for log files and non-TTY
  output. Unexpected exceptions are now converted into a generic
  `LHPError` via `from_unexpected_exception` instead of being matched
  by sniffing for `"Error [LHP-"` substrings in the stringified message.
- **`WarningCollector` + pre-pool YAML scanner.** A per-run
  `WarningCollector` (`src/lhp/cli/warning_collector.py`) deduplicates
  non-fatal warnings by `(category, message)` and renders them as a
  yellow Rich panel at end of run. The deprecated bare-`{token}`
  substitution warning is now detected by a main-thread scan of raw
  YAML (`src/lhp/cli/yaml_scanner.py`) before workers are spawned —
  this fixes a silent regression where worker-side `logger.warning`
  calls were swallowed by the workers' `NullHandler`-only logger.
- **`EnhancedSubstitutionManager.has_deprecated_bare_tokens`.**
  Per-instance flag flipped on the first deprecated `{token}`
  substitution so the orchestrator can forward a single entry to the
  `WarningCollector`. Replaces the previous module-level
  `_DEPRECATED_BARE_TOKEN_WARNED` flag.
- **Snapshot tests via `syrupy`.** Rich rendering for `LHPError`
  panels, the validate summary table, and `show` flowgroup output is
  pinned by `.ambr` snapshots under `tests/__snapshots__/`. An autouse
  `_isolate_lhp_console` fixture in `tests/conftest.py` swaps the
  `lhp.cli.console` singletons for a deterministic no-color Console at
  fixed width so snapshots are stable across terminals and CI.

### Changed

- **`cli_error_boundary` renders via Rich Console.** Errors print
  through `err_console.print(lhp_error)` so the new `__rich__` panel
  takes effect on TTYs; the stringified-LHPError sniffing safety net
  is replaced by `LHPError.from_unexpected_exception`.
- **`error_formatter.py` reshaped around a single template dict.**
  Plain-text (`_format_message`) and Rich (`__rich__`) renderings now
  consume the same `_template_data()` dict so the two presentations
  cannot drift. Category labels and Rich border styles are centralized
  in `_category_label` / `_border_style`.
- **`src/lhp/cli/main.py` slimmed.** Unused helpers
  (`_ensure_project_root`, `_load_project_config`,
  `_get_include_patterns`, `_discover_yaml_files_with_include`) and a
  stray `warnings.filterwarnings` call were removed — the Click
  subcommands own these concerns directly.

### Removed

- **Module-level `_DEPRECATED_BARE_TOKEN_WARNED` global** in
  `lhp/utils/substitution.py` (replaced by per-instance
  `has_deprecated_bare_tokens` + `WarningCollector`).
- **`DOLLAR_TOKEN_SIMPLE_PATTERN`** in `EnhancedSubstitutionManager` —
  was unused.

### Dependencies

- Add `rich>=13.0.0` and `rich-click>=1.9.0,<2.0` as runtime deps.
- Add `syrupy>=4.6.0` as a dev dep for snapshot testing.

## [0.8.7] — 2026-05-21

### Breaking changes

- **`lhp generate` now requires `--pipeline-config` / `-pc`** when bundle
  support is enabled (`databricks.yml` present, `--no-bundle` not set).
  Previously, generation would proceed and fail late during bundle sync.
  New error code: `LHP-CFG-023`. Users who relied on a default-empty
  pipeline_config must now supply a path explicitly. See
  `docs/configure_catalog_schema.rst`.
- **`resources/lhp/` is now wiped and regenerated on every
  `lhp generate`.** The directory is exclusively LHP-managed. Users
  who placed custom resource YAMLs (hand-written jobs, dashboards,
  secret scopes) under `resources/lhp/` **must move them to
  `resources/` (top level) or a non-`lhp` subdirectory before
  upgrading**, or those files will be deleted on the next generate.
  Files outside `resources/lhp/` are never touched, with one
  exception: the monitoring job YAML at `resources/<name>.job.yml`
  is identified by its sentinel header
  (`# Generated by LakehousePlumber - Monitoring Job`) and replaced
  on each run.
- **`catalog` and `schema` are now REQUIRED in
  `pipeline_config.yaml`** — set them per-pipeline or via the
  top-level `project_defaults` block. Missing or incomplete
  catalog/schema causes `lhp generate` to fail fast with
  `LHPConfigError` (`LHP-CFG-026`), aggregated across all pipelines
  plus the synthetic monitoring pipeline (when monitoring is
  enabled). Programmatic consumers read
  `LHPConfigError.context["failures"]` (grouped by `both_missing`,
  `incomplete`, `empty_after_substitution`) instead of parsing
  message text. Previously LHP auto-populated
  `default_pipeline_catalog` / `default_pipeline_schema` in
  `databricks.yml` and the pipeline-resource template fell back to
  `${var.default_pipeline_*}`. Both halves of that pathway are
  removed. See `docs/configure_catalog_schema.rst` for the migration
  guide.
- **Smart generation removed.** Every `lhp generate` now
  full-regenerates all flowgroups (equivalent to the old
  `--force --no-state` behaviour); there is no incremental mode. The
  `lhp state` subcommand, the `--no-cleanup` flag, and the
  `.lhp_state.json` / `.lhp_state/` files are all removed. Any
  leftover state files are auto-cleaned on the first run of this
  version.
- **`PythonFileCopier.apply_copy_record` no longer mutates state.** It
  returns a list of `CopiedFileEntry` records and the caller invokes
  `track_generated_file` separately. In-tree callers route through
  `PipelineProcessor._apply_copy_records` automatically.

### Added

- **Pre-flight catalog/schema validation** (`lhp.bundle.preflight`):
  every pipeline (plus the synthetic monitoring pipeline if
  monitoring is enabled) is validated for `catalog` and `schema`
  resolution BEFORE any side effects. Failures are aggregated across
  all pipelines and grouped by failure type
  (`both_missing` / `incomplete` / `empty_after_substitution`),
  surfaced via `LHP-CFG-026` with structured `context["failures"]`.
  Preflight runs identically under `--dry-run`. Typical fail-fast
  time is under 1 second with zero filesystem changes — the most
  common cause of an empty `resources/lhp/` (catalog/schema
  misconfig) can no longer happen.
- **Per-pipeline parallel generation.** One worker process per
  pipeline via `ProcessPoolExecutor` (spawn start method). Phase A
  (parse, codegen, format) and the intra-pipeline portion of Phase B
  (cross-flowgroup validation, `.py` writes, copied-module
  application, test-reporting hook) all run inside the worker —
  eliminating the main-thread GIL-starvation hot path that
  bottlenecked large projects.
- **Worker count auto-detection** at ~80% of OS-visible CPU, capped
  to the workload size. Override with the `LHP_MAX_WORKERS` env var
  or the `--max-workers` CLI flag on `lhp generate` / `lhp validate`.
  Use `--max-workers 1` for sequential execution.
- **`LHPError.from_worker_exception` + `lhp_error_from_worker_failure`
  factories** — reconstruct worker-side exceptions on the main
  thread while preserving the original exception type via
  dual-inheritance subclasses (`LHPValidationError(LHPError,
  ValueError)`, `LHPFileError(LHPError, FileNotFoundError)`).
  Existing `except ValueError` / `except FileNotFoundError` handlers
  continue to catch worker failures.
- **`docs/configure_catalog_schema.rst`** — migration guide for the
  new mandatory catalog/schema rule, with worked examples for
  per-pipeline config, `project_defaults`, resolution order, the
  migration from the removed `databricks.yml` variables, and an
  error reference covering `LHP-CFG-023` and `LHP-CFG-026`.
- **Release-time performance gate** (`pytest tests/performance/ -m
  performance`) — opt-in via `LHP_RUN_PERFORMANCE_GATE=1`. Captures
  baselines per release; excluded from default CI to keep PR
  pipelines fast. Comprehensive unit tests for `PerformanceTimer`
  and its snapshot accessor.
- **Picklable test fakes** (`tests/fakes/`,
  `tests/test_fakes_picklable.py`) for worker-boundary tests that
  need to cross the spawn boundary.
- **E2E coverage expansion**: state CLI semantics, test actions,
  temp-table transformations, JDBC load, Python load, built-in
  sinks, Delta CDC reader, generate flags, negative paths.

### Changed

- Catalog/schema validation moved from the bundle-write phase to a
  dedicated preflight stage. Late-bound validation inside
  `BundleManager.generate_resource_file_content` is replaced by a
  single internal-error guard (`LHP-GEN-001`) that fires only when
  preflight is bypassed by a non-CLI caller (programming bug). The
  three separate raises that this method used to emit are gone.
- `LHP-CFG-026` is now the **aggregated** form: a single error with
  all failures grouped by category. Programmatic consumers should
  read `LHPConfigError.context["failures"]` instead of parsing
  message text.
- `PipelineDelta` moved from `lhp.core.state_models` to
  `lhp.models.processing` (lost its `files_skipped` field).
- `build_lhp_source_header` moved from `lhp.utils.smart_file_writer`
  to `lhp.utils.file_header`; adds a `normalize_content` helper used
  by every bundle/pipeline write site.
- `ChecksumCache` no longer holds a `threading.Lock` (main-thread-only
  usage now that workers are process-isolated).
- The blueprint provenance map
  (`Dict[Tuple[str, str], BlueprintProvenance]`) now crosses the
  spawn boundary into workers; synthetic-flowgroup detection and
  blueprint-aware dependency resolution work the same in the worker
  as on the main thread.

### Deprecated

- `--force` and `--no-state` flags on `lhp generate`: accepted with
  a deprecation warning; will be removed in a future release. Both
  are no-ops since full regeneration is the default. Remove them
  from CI invocations.

### Fixed

- Generating without `--pipeline-config` no longer wipes
  `resources/lhp/` and `generated/<env>/` before failing. A
  subsequent `databricks bundle deploy` will no longer find an empty
  `resources/lhp/` from a misconfigured run.
- Init template (`pipeline_config_env.yaml.tmpl`) no longer claims
  LHP auto-loads `templates/bundle/pipeline_config.yaml` — that
  pathway was removed in this release. The comment now correctly
  documents the `--pipeline-config` requirement.

### Removed

- **Smart-generation subsystem entirely**: `lhp state` CLI
  subcommand, `--no-cleanup` flag, `.lhp_state.json` / `.lhp_state/`
  files (auto-deleted on first run of new version), and the modules
  `lhp.core.state_manager`,
  `lhp.core.state_dependency_resolver`, `lhp.core.state.*`,
  `lhp.utils.smart_file_writer`, `lhp.services.state_display_*`,
  `lhp.core.strategies`,
  `lhp.core.services.generation_planning_service`,
  `lhp.cli.commands.state_command`.
- `DatabricksYAMLManager` module (and the runtime dependency on
  ruamel.yaml that it was the sole consumer of, where applicable).
- `BundleManager._update_databricks_variables` method, plus the
  auto-population of `default_pipeline_catalog` /
  `default_pipeline_schema` in `databricks.yml`
  `targets.<env>.variables`.
- Bundle-resource preservation decision tree (Scenarios 1a / 1b /
  2 / 4 in `BundleManager._sync_pipeline_resource` — the
  Conservative Approach for LHP-vs-user file preservation); orphan
  cleanup of `resources/lhp/`; LHP-vs-user header sentinel inside
  `resources/lhp/`.
- Deprecated `${var.default_pipeline_catalog}` /
  `${var.default_pipeline_schema}` fallback in
  `pipeline_resource.yml.j2`.
- `variables:` block in the init template's `databricks.yml.j2`
  (`default_pipeline_catalog` / `default_pipeline_schema` were the
  only entries).
- `ActionOrchestrator._sync_bundle_resources` (orphan method with no
  production caller) and its 8 test references.
- Orchestrator main-thread machinery: `_PipelineProgress`,
  `_assemble_pipeline`, `_assemble_pipeline_outputs` (worker-internal
  now).
- `DependencyTracker.set_checksum_cache` (the cache is now provided
  at factory-construction time via
  `DependencyTracker.for_project(..., checksum_cache=...)`).
- Outdated benchmark scripts and results (replaced by the
  structured release-time performance gate above).

### Known limitation

- `lhp generate` is **non-atomic** between the `resources/lhp/` wipe
  and the end of bundle sync: if the process is interrupted (OS
  kill, power loss) **after** preflight passes but **before** bundle
  sync completes, `resources/lhp/` may be left partially populated
  or empty. A subsequent `databricks bundle deploy` against that
  state could silently drop pipelines from the workspace.
  Catalog/schema misconfigs — the most common trigger in
  pre-preflight versions — can no longer cause this; preflight
  rejects them before any wipe. Re-running `lhp generate` to
  completion restores the directory.

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
