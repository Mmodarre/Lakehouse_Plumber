# Error Reference

When LHP encounters a problem, it displays a structured error with code `LHP-{CATEGORY}-{NUMBER}`.

Terminal output includes: error code, description, context, fix suggestions, and example.

**Tip:** Use `--verbose` with any LHP command for additional debug information.

---

## Error Categories

| Category | Prefix | Description |
|----------|--------|-------------|
| Configuration | `CFG` | Invalid/conflicting settings in YAML, presets, templates, or bundle config |
| Validation | `VAL` | Missing required fields, invalid values, structural problems in actions |
| I/O | `IO` | Files not found, read/write failures, format issues |
| Action | `ACT` | Unknown action types, subtypes, or preset names |
| Dependency | `DEP` | Circular dependencies (error) plus advisory extraction warnings from `lhp dag` |
| Deprecation | `DEPR` | Soft-deprecation warnings for fields/syntax slated for removal; surfaced as warnings, not failures |
| General | `GEN` | Worker exceptions, unexpected errors, internal-error guards (mostly post-0.8.7 parallel-generation failures) |

---

## Configuration Errors (LHP-CFG)

| Code | Trigger | Fix |
|------|---------|-----|
| **CFG-001** | Same option in multiple places (e.g., `format` + `cloudFiles.format`) | Remove the legacy field, keep only `options:` format |
| **CFG-006** | `event_log` is not a YAML mapping | Define as mapping: `event_log: {catalog: ..., schema: ...}` |
| **CFG-007** | `event_log` missing `catalog` or `schema` | Add both required fields, or `enabled: false` |
| **CFG-008** | Invalid `monitoring` config (not a mapping, missing event_log, bad MVs) | See [monitoring.md](monitoring.md) troubleshooting table |
| **CFG-009** | YAML parsing error (bad indent, unquoted special chars) | Quote strings with `:` `{` `}` `[` `]`; use YAML linter |
| **CFG-010** | Deprecated field name | Replace with the field name shown in error message |
| **CFG-012** | Missing required template parameters | Add `template_parameters:` with all required params |
| **CFG-021** | Module / skill / bundle YAML processing error (covers import-time errors, skill command failures, and bundle-resource parsing) | Check the error context for the specific cause; for bundle sites validate `databricks.yml` syntax and UTF-8 encoding |
| **CFG-023** | `--pipeline-config` not passed and bundle support enabled (preflight) | Pass `--pipeline-config config/pipeline_config.yaml`, or `--no-bundle` to skip bundle resource generation |
| **CFG-024** | Bundle template fetch error | Check network; verify template path/URL |
| **CFG-025** | Bundle configuration structural error | Review `databricks.yml` against DAB docs |
| **CFG-026** | Aggregated catalog/schema preflight failure (see below) | Add `catalog`/`schema` to `project_defaults` or per-pipeline; for empty-after-substitution failures, check `substitutions/<env>.yaml` |
| **CFG-027** | Template not found | Check spelling; run `lhp list templates` |
| **CFG-031** | Generated Python source failed to parse (`ast.parse` SyntaxError) — in-worker syntax guard; names the offending flowgroup | Almost always an LHP generator/template bug — file a bug report with the failing flowgroup YAML; turn on DEBUG logging to inspect the generated source; if authoring a custom template or snapshot-CDC `source_function`, verify embedded Python with `python -m py_compile` |
| **CFG-032** | Test-reporting provider/config file not found (preflight) | Create the file at `test_reporting.module_path` (and `config_file` if set) in `lhp.yaml`, or fix the path. Runs on both `lhp validate` and `lhp generate`, independent of `--include-tests` |
| **CFG-033** | `ruff format` terminal pass exited non-zero — generated code written but not formatted; error carries ruff's exit code + stderr/stdout | Inspect ruff's output for the offending file; confirm ruff is installed and the generated tree is valid Python; re-run with `--no-format` to skip formatting and inspect the raw code |
| **CFG-034** | `ruff` executable not found for the generated-code formatting pass (not in the active env's scripts dir or on `PATH`) | ruff ships as an LHP runtime dependency — `pip install ruff`, or reinstall LHP (`pip install lakehouse-plumber`); in isolated/custom envs ensure ruff is on `PATH` |
| **CFG-054** | Invalid/malformed blueprint instance definition — `use_blueprint:`/`blueprint:` reference is not a single non-empty string (list, mapping, empty, or null), or the instance doc otherwise fails to parse | Set `use_blueprint: <blueprint_name>` to one non-empty string naming an existing blueprint; do not use a list/mapping/empty value (one of the instance-shape errors `CFG-047`–`058`) |
| **CFG-060** | Malformed top-level `wheel` block in `lhp.yaml` — not a mapping, or `artifact_volume` is not a string (the `/Volumes/...` shape is checked later, see `CFG-061`) | Define `wheel:` as a mapping with an optional `artifact_volume:` set to a single string path |
| **CFG-061** | A pipeline uses `packaging: wheel` but `wheel.artifact_volume` is missing/empty or resolves (post-substitution) to a non-`/Volumes/` path — serverless installs custom wheels only from a UC volume | Set `wheel.artifact_volume` to a `/Volumes/...` path for the env; verify `${tokens}` resolve; or set the pipeline back to `packaging: source` |
| **CFG-062** | Invalid `sandbox:` block in `lhp.yaml` — not a mapping, unknown `strategy` (v1 supports `table` only), or `allowed_envs: []` (empty list would forbid sandbox in every environment) | Define `sandbox:` as a mapping; use `strategy: table`; omit `allowed_envs` for unrestricted, or list at least one env. See [sandbox.md](sandbox.md) |
| **CFG-063** | Invalid sandbox `table_pattern` — missing `{namespace}` or `{table}` placeholder (both required), unrecognized placeholder, conversion (`!r`) / format spec (`:>10`), or literal text outside `[A-Za-z0-9_]` | Use only the `{namespace}` and `{table}` placeholders with letters/digits/underscores between them, e.g. `{namespace}__{table}` |
| **CFG-064** | Invalid sandbox profile `.lhp/profile.yaml` — unreadable/bad YAML, non-mapping root, missing top-level `sandbox:` key, or failed validation (`namespace` regex, empty `pipelines`) | `namespace` must match `^[a-z][a-z0-9_]{0,63}$`; `pipelines` must be a non-empty list of names or globs; nest both under a top-level `sandbox:` key |
| **CFG-065** | `--sandbox` run against an environment not listed in `lhp.yaml` `sandbox.allowed_envs` | Run against an allowed env, or have the team add the env to `allowed_envs` (absent `allowed_envs` = unrestricted) |

## Validation Errors (LHP-VAL)

| Code | Trigger | Fix |
|------|---------|-----|
| **VAL-001** | Missing required field (source, target, type) | Add the field shown in error; check action type requirements |
| **VAL-002** | Multiple validation issues in one action | Fix each `✗` item in the error list |
| **VAL-006** | Invalid field value (typo, wrong context) | Check spelling; use valid values for the field |
| **VAL-007** | Invalid `readMode` | Use `stream` or `batch` only |
| **VAL-008** | Wrong data type (string where dict expected) | Check YAML structure matches expected format |
| **VAL-010** | Both `__eventlog_monitoring` alias and real pipeline name in config | Use only one — alias or real name |
| **VAL-011** | Multiple validation causes; commonly: eventlog alias misuse OR schema column-type syntax. See source for the specific site. | Check the error context for the specific cause |
| **VAL-012** | Invalid source format (string where dict needed) | Provide full source config with `type`, `path`, etc. |
| **VAL-062** | A pipeline's `packaging` value is not `source` or `wheel` (case-sensitive; e.g. `wheels`, `whl`, `Wheel`) | Use exactly `source` or `wheel` |
| **VAL-063** | Malformed `depends_on` entry on an action — not a non-empty string, more than three dot-separated parts, or a blank dotted part | Each entry must be a well-formed table ref: `catalog.schema.table`, `schema.table`, or `table` (no blank parts) |
| **VAL-064** | A sandbox profile `pipelines` entry matched zero pipelines (all offending entries aggregated into one error), or an exact entry names the monitoring pipeline (cannot be sandboxed) | Fix the entry against the available pipeline names listed in the error; remove the monitoring pipeline entry (globs silently skip it) |
| **VAL-065** | *Warning, category `sandbox`* — a sandbox-renamed sink table is also produced by an out-of-scope pipeline (mixed producer); the rename still proceeds | Bring the other producing pipeline into the profile scope, or accept the split; `--strict` promotes this warning to a failure |
| **VAL-066** | *Warning, category `sandbox`* — an in-scope Python table read whose argument statically resolves to an in-scope table but is not a plain string literal (bound variable, f-string, concatenation, `.format`, or constant-key container subscript) could not be rewritten; the source is left untouched. Generate-only in v1 — `lhp validate --sandbox` does not emit it | Make the table argument a plain string literal in the copied module, or accept that it reads the shared table; `--strict` promotes this warning to a failure |
| **VAL-067** | *Warning, category `sandbox`* — opaque dynamic SQL: a `spark.sql(...)` body whose table refs are runtime-determined (a variable/call result, or an f-string with ≥2 interpolated name parts) can be neither verified nor rewritten. Generate-only in v1. NOTE: an opaque *table read* (`spark.read.table(runtime_name)`) is instead wrapped in a generated `__lhp_sandbox_table(...)` shim — no warning | Reference in-scope tables with a literal/statically-resolvable name in the SQL body, or accept that the statement reads the shared tables; `--strict` promotes this warning to a failure |
| **VAL-902** | All-or-nothing aggregator — one or more flowgroups failed anywhere in a parallel `lhp generate` run (per-flowgroup validation, codegen, generated-source parse failure `LHP-CFG-031`, cross-flowgroup conflict, or copy conflict `LHP-VAL-019`); raised by the coordinator gate after all worker results are joined, before any files are written | Re-run with `--verbose` for full stack; re-run with `--log-file` to capture a debug log at `<project>/.lhp/logs/lhp.log` and attach it to the bug report; every listed failure must be fixed — the run wrote zero files |

## I/O Errors (LHP-IO)

| Code | Trigger | Fix |
|------|---------|-----|
| **IO-001** | Referenced file not found | Check path spelling; paths are relative to YAML file location |
| **IO-003** | Wrong document count (empty file or unexpected `---`) | Schema/expectations files must have exactly 1 document; `---` separators only for flowgroup files |
| **IO-022** | `lhp inspect-wheel` given a wheel *path* that does not exist (path-mode only; a pipeline-name selector reports a missing build as `GEN-001`) | Check the `.whl` path; run `lhp generate` to build it first; or inspect by pipeline name with `-e <env>` |
| **IO-023** | `lhp inspect-wheel` path is not a usable `.whl` file — a directory, or a file without the `.whl` suffix | Name the built `.whl` under `generated/<env>/_wheels/<pipeline>/dist/`, not a directory or other file; or inspect by pipeline name |
| **IO-024** | `lhp inspect-wheel` target ends in `.whl` but is not a valid zip archive (truncated/corrupt) | Rebuild with `lhp generate`; LHP wheels are deterministic, so a clean rebuild reproduces it |
| **IO-025** | `--sandbox` run but the personal profile `.lhp/profile.yaml` does not exist at the project root | Create gitignored `.lhp/profile.yaml` with a top-level `sandbox:` key declaring `namespace` and `pipelines` (globs allowed); see [sandbox.md](sandbox.md) |

## Action Errors (LHP-ACT)

| Code | Trigger | Fix |
|------|---------|-----|
| **ACT-001** | Unknown type, subtype, sink type, or preset name | Check spelling; error includes "Did you mean?" and valid values |

## Dependency Errors (LHP-DEP)

| Code | Trigger | Fix |
|------|---------|-----|
| **DEP-001** | Circular dependency (A → B → C → A) | Break the cycle; error shows full path. Use `lhp dag --format dot` to visualize |
| **DEP-002** | *Advisory warning, never fails a run* — recognized Python table-read (e.g. `spark.read.table(...)`, `spark.sql(...)`) whose table argument is not statically resolvable (helper-call result, unbound function arg, runtime-only value) | Declare the upstream with `depends_on` on the action (additive; entries validated by `VAL-063`) |
| **DEP-003** | *Advisory warning, never fails a run* — a SQL body could not be parsed for table extraction (one warning per unparseable body; it contributes zero edges) | Fix the SQL (Databricks dialect), or declare upstreams with `depends_on` |

DEP-002/003 surface on `lhp dag` (default-on): stderr summary (count header, up to 10 lines `LHP-DEP-00x fg.action (file:line): message`, overflow `... and N more (see JSON output)`, one `depends_on` hint), JSON output (top-level `warnings` array always present + `metadata.total_warnings`), and the text report — NOT in DOT output or job YAML. Public API: `lhp.api.DependencyWarningView` (provisional) on `DependencyAnalysisResult.warnings`.

## Deprecation Warnings (LHP-DEPR)

Warnings, not failures — generation/validation still succeed. They flag usage slated for removal.

| Code | Deprecated usage | Replacement |
|------|------------------|-------------|
| **DEPR-001** | Bare-braces `{token}` substitution syntax | Use `${token}` (only non-`$` braces form is `%{local_var}` for local variables) |
| **DEPR-002** | `database` field | Use `catalog` + `schema` |
| **DEPR-003** | Schema-transform `enforcement` key | Remove the key |
| **DEPR-004** | `database_suffix` field | Use `catalog` + `schema` |

> **Note:** Legacy `blueprint:` / `use_blueprint:` mixed syntax is **not** a soft deprecation — it is a hard `LHP-VAL-061` error.

## General Errors (LHP-GEN)

| Code | Trigger | Fix |
|------|---------|-----|
| **GEN-001** | Internal-error guard: preflight bypassed for bundle resource generation (see below) | Programming bug — invoke `bundle.preflight.validate_catalog_schema` first |
| **GEN-901** | Worker exception during a parallel `lhp generate` run — a child process (one flowgroup task) raised an exception and the orchestrator reconstructed it via `lhp_error_from_worker_failure` | Re-run with `--verbose` for full stack; re-run with `--log-file` to capture a debug log at `<project>/.lhp/logs/lhp.log` and attach it to the bug report |
| **GEN-902** | Unexpected non-LHP, non-Bundle exception wrapped by `from_unexpected_exception` (CLI fallback path) | Re-run with `--verbose` for full stack; re-run with `--log-file` to capture a debug log at `<project>/.lhp/logs/lhp.log` and attach it to the bug report |

> **Note (0.8.7+):** `GEN-901`, `GEN-902`, and `VAL-902` are the codes users will most often encounter after a parallel-generation failure — they wrap worker-side exceptions and aggregate failures from the coordinator. `lhp generate` parallelizes at the **flowgroup** level (one worker task per flowgroup) and is **all-or-nothing**: `VAL-902` aggregates every flowgroup that failed anywhere in the run, and when it fires **no files are written**. Re-run with `--log-file` to capture a debug log at `<project>/.lhp/logs/lhp.log` (opt-in; not written by default).

## Pre-flight validation: LHP-CFG-023, LHP-CFG-026, LHP-CFG-032

`lhp generate` runs these preflight checks **before** any side effects
(directory wipes, code generation, bundle YAML writes). When preflight
fails, `generate` aborts before touching the filesystem — `generated/<env>/`
is left intact, not wiped. This untouched-output guarantee is not limited to
preflight: `lhp generate` is **all-or-nothing** — if *any* flowgroup fails
anywhere in the run (per-flowgroup validation, codegen, generated-source parse
failure `LHP-CFG-031`, a cross-flowgroup conflict, or a custom-module copy
conflict `LHP-VAL-019`), the run writes **zero** files and leaves the output tree
untouched, aggregating multiple failures into a single `LHP-VAL-902`. `lhp
validate` runs the **same** preflight
checks (it gained `--no-bundle` and `--pipeline-config` / `-pc` to match;
on a project containing `databricks.yml` it likewise requires `-pc` or
fails with `LHP-CFG-023`). `LHP-CFG-023` and `LHP-CFG-026` surface as
`LHPConfigError` with `doc_link` pointing to the Configure catalog/schema
docs page.

### LHP-CFG-023 — `--pipeline-config` is required

**Message:** `--pipeline-config is required when bundle support is enabled`

**Cause:** `databricks.yml` exists in the project root (bundle support is
enabled), `--no-bundle` was not passed, and `--pipeline-config` / `-pc` was
omitted. Without that flag, `PipelineConfigLoader` loads empty defaults and
every pipeline would fail catalog/schema validation later — so preflight
raises this up-front, before any work.

**Fix:** Pass `--pipeline-config config/pipeline_config.yaml` (or your
project's equivalent path). To skip bundle resource generation entirely
without supplying the flag, pass `--no-bundle`.

### LHP-CFG-026 — aggregated catalog/schema validation

**Message:** `Catalog/schema validation failed for N pipeline(s)`

**Cause:** One or more pipelines (including the synthetic monitoring
pipeline when monitoring is enabled in `lhp.yaml`) had missing or empty
`catalog` / `schema` after deep-merging `DEFAULT_PIPELINE_CONFIG` →
`project_defaults` → per-pipeline, then applying substitutions from
`substitutions/<env>.yaml`.

Failures are aggregated across **all** pipelines and grouped into three
categories. The structured payload lives on `LHPConfigError.context["failures"]`:

```python
{
    "both_missing": ["pipeline_a", "pipeline_b"],
    "incomplete": [{"pipeline_name": "...", "catalog": "...", "schema": None}],
    "empty_after_substitution": [{"pipeline_name": "...", ...}],
}
```

#### Both `catalog` and `schema` missing

Pipelines listed under `both_missing` have neither key set after deep merge.
Most common cause: `pipeline_config.yaml` has no `project_defaults` block
and no per-pipeline overrides.

**Fix:** Add `catalog` and `schema` to `project_defaults` (project-wide) or
to each pipeline's own entry (per-pipeline).

#### Incomplete pairing (one of catalog/schema set, the other missing)

Pipelines listed under `incomplete` have exactly one of `catalog` or
`schema` defined. LHP requires both or neither — partial configuration is
treated as a typo, not as a fallback request.

**Fix:** Add the missing key. If the intent was to inherit one half from
`project_defaults`, remove the other half from the per-pipeline entry so
both values resolve from the same layer.

#### Empty after substitution

Pipelines listed under `empty_after_substitution` have a `catalog` or
`schema` that resolved to whitespace-only strings after
`substitutions/<env>.yaml` was applied. The keys exist, but evaluate to
nothing meaningful.

**Fix:** Inspect the substitution file for the failing environment. Run
`lhp substitutions --env <env>` to print resolved values. Add or correct
entries that resolve to empty strings.

> **Note:** Pre-0.8.7 versions raised three separate `LHP-CFG-026` errors —
> one per category, on the first failing pipeline. Current preflight walks
> every pipeline once and aggregates into a single error. Parse
> `LHPConfigError.context["failures"]` for the stable contract.

### LHP-CFG-032 — test-reporting provider/config file not found

**Message:** `Test reporting <module_path|config_file> not found: <path>`

**Cause:** `lhp.yaml` has a `test_reporting` section, but the
`module_path` provider file (or the optional `config_file`) does not exist
at the resolved path. This is a project preflight check: it runs on both
`lhp validate` and `lhp generate`, **independent of `--include-tests`** — a
project with a missing provider file fails `generate` even without the
flag.

**Fix:** Create the provider module at `test_reporting.module_path` (and
the YAML at `config_file` if you set one), or correct the path. Paths are
relative to the project root. See the Test Result Reporting docs for the
provider contract.

## LHP-GEN-001 — internal-error guard (preflight bypassed)

**Message:** `Internal error: preflight bypassed for bundle resource generation`

**Cause:** A non-CLI caller invoked `BundleManager.generate_resource_file_content`
directly without running preflight first. Users should never see this — if
they do, it indicates a programming bug.

**Fix:** Verify the CLI invocation path runs through
`generate_command.execute()` which calls `validate_catalog_schema` before
`BundleManager`. For programmatic callers, invoke
`bundle.preflight.validate_catalog_schema` first.

---

## Common Before/After Fixes

### CFG-001: Configuration Conflict

```yaml
# BEFORE (error)
source:
  type: cloudfiles
  path: /data/events/
  format: json              # Legacy field
  options:
    cloudFiles.format: json  # Same setting — conflict!

# AFTER (fixed)
source:
  type: cloudfiles
  path: /data/events/
  options:
    cloudFiles.format: json  # Use only options format
```

### VAL-001: Missing Source

```yaml
# BEFORE (error)
actions:
  - name: load_customers
    type: load
    target: v_raw_customers
    # Missing: source!

# AFTER (fixed)
actions:
  - name: load_customers
    type: load
    source:
      type: cloudfiles
      path: /data/customers/
      options:
        cloudFiles.format: csv
    target: v_raw_customers
```

### CFG-009: YAML Parsing / Quoting Braces

```yaml
# BEFORE (error)
source:
  path: /data/${date}/events   # Value with ${...} / braces needs quoting
  comment: Load events: raw    # Colon needs quoting

# AFTER (fixed)
source:
  path: "/data/${date}/events"
  comment: "Load events: raw"
```

### IO-001: File Not Found

```yaml
# BEFORE (error)
sql_file: sqls/transform.sql   # Wrong directory name

# AFTER (fixed)
sql_file: sql/transform.sql    # Correct path
```

---

## Diagnostic Commands

```bash
lhp validate --env <env>                  # Validate all configurations
lhp validate --env <env> --verbose        # Verbose — extra debug info
lhp list templates                        # List available templates
lhp list presets                          # List available presets
lhp dag --format dot                      # Visualize dependencies (spot cycles)
lhp diff --env <env>                      # Show what `lhp generate` would change on disk
lhp substitutions --env <env>             # List substitution tokens for env
```

---

## Symptom-Based Troubleshooting

Task-shaped entries — start from what the user sees, find the fix. Use these
before grepping the catalog above.

### `lhp generate` aborts with an error banner

Read the error code prefix:

- `LHP-CFG-*` — fix YAML/preset/template/bundle config
- `LHP-VAL-*` — fix missing/invalid fields in actions
- `LHP-IO-*` — fix file path (paths are relative to FlowGroup YAML)
- `LHP-ACT-*` — fix typo in action type/sub_type/preset name
- `LHP-DEP-*` — break the dependency cycle shown in the message (`DEP-002`/`DEP-003` are `lhp dag` advisories, never generate failures)
- `LHP-GEN-*` — worker / unexpected exception (re-run with `--verbose`; re-run with `--log-file` to capture `<project>/.lhp/logs/lhp.log`)

Apply the numbered fix suggestions in the terminal output, then re-run.

### `lhp validate` lists multiple errors for one action

Fix the **first** error first — later errors often cascade. For `LHP-VAL-002`,
each `✗` marker is a separate issue. Run `lhp validate --env <env> --verbose`
to surface the merged/expanded view (after preset merge + template expansion).

### Pipeline deploys but does not run / runs stale code

1. Run `lhp generate --env <env>` before `databricks bundle deploy --target <env>`.
2. `--env` and `--target` must match.
3. Check `resources/lhp/` contains the pipeline resource file.

### YAML completion / IntelliSense not working

JSON Schemas ship as package data under `lhp/schemas/` (source checkout:
`src/lhp/schemas/`). The editor must map them to `pipelines/*.yaml`,
`presets/*.yaml`, `templates/*.yaml`, `substitutions/*.yaml`. Reload editor
window after LHP install/upgrade.

### Substitutions not resolved (literal `${token}` in output)

Substitution order: `%{local_var}` → `{{ template_param }}` → `${env_token}` →
`${secret:scope/key}`. Bare-braces `{token}` is **deprecated**.

1. `lhp substitutions --env <env>` — list known tokens.
2. Add missing token to `substitutions/<env>.yaml`.
3. Token must appear inside a string value, not as a YAML key.
4. `lhp diff --env <env>` — inspect the regenerated output; unresolved
   tokens appear unchanged there.

### Preset/template/blueprint edits not picked up

Every `lhp generate` regenerates all FlowGroups from current YAML. Confirm the
preset/template is actually referenced in the flowgroup YAML (`lhp diff --env <env>`
shows the regenerated output).

### CLI flags reference

For the `lhp generate` flag reference, see `project-config.md` (CLI Commands section) or run `lhp generate --help`.

### POSIX exit codes (from `src/lhp/utils/exit_codes.py`)

| Code | Meaning | LHP categories |
|------|---------|----------------|
| 0 | Success | — |
| 1 | General error | unknown |
| 64 | Usage error | bad CLI args |
| 65 | Data error | VAL, DEP, ACT |
| 66 | No input | IO |
| 70 | Software error | internal bug |
| 74 | I/O error | permissions, disk |
| 78 | Config error | CFG, CloudFiles |

Script integrations should branch on exit code, not on stderr text.
