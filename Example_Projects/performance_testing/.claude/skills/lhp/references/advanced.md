# Advanced Features Reference

## Table of Contents
- [Databricks Asset Bundles](#databricks-asset-bundles)
- [Pipeline & Job Configuration](#pipeline--job-configuration)
- [Dependency Analysis](#dependency-analysis)
- [Multi-Job Orchestration](#multi-job-orchestration)
- [CI/CD Patterns](#cicd-patterns)
- [Custom Python Functions & Local Helpers](#custom-python-functions--local-helpers)

---

## Databricks Asset Bundles

LHP generates DAB pipeline resource YAML files; it does NOT replace DAB or deploy resources.

### Setup

Bundle is enabled by default. Use `--no-bundle` to opt out.

```bash
lhp init my-data-platform
cd my-data-platform
# Edit databricks.yml with workspace details (host, run_as)
lhp generate -e dev
databricks bundle deploy --target dev
```

### What LHP Does

- `lhp init` (default) scaffolds `databricks.yml` (targets: dev/tst/prod) and `resources/lhp/`
- Generates `resources/lhp/*.pipeline.yml` using glob libraries
- Conservative sync: creates missing files, leaves LHP-owned files untouched, backs up user-edited files to `.bkup`, deletes orphans when a pipeline directory is removed
- Never modifies `databricks.yml` (except DEPRECATED auto-detect variable update, removed in v1.0.0) or files outside `resources/lhp/`
- Target names in `databricks.yml` must match substitution filenames under `substitutions/`

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

Usage: `lhp dag --format job --job-config config/job_config.yaml --bundle-output`

---

## Dependency Analysis

Analyzes FlowGroup YAML to detect pipeline dependencies, execution stages, and external sources.

### Commands

```bash
lhp dag                                    # All formats
lhp dag --format job --job-name my_etl      # Orchestration job
lhp dag --format job --bundle-output        # Save to resources/
lhp dag --format dot                        # GraphViz diagram
lhp dag --format json                       # Structured dependency graph
lhp dag --blueprint <name>                  # restrict analysis to one blueprint
lhp dag --pipeline <name>                   # restrict analysis to one pipeline (job format skipped under filters)
lhp dag --trust-depends-on                  # actions with depends_on skip body extraction (declared deps authoritative)
```

Blueprint synthetic flowgroups are ALWAYS fully expanded (one graph node per instance) — required so per-instance `pipeline:` values aren't dropped from the graph/JSON/job YAML. `--expand-blueprints` is a deprecated, ignored no-op (prints a deprecation notice); `--blueprint <name>` restricts analysis to one blueprint (unchanged).

### Output Formats

`--format` accepts a comma-separated list; `all` (the default) emits every format.

| Format | Description |
|--------|-------------|
| `text` | Human-readable report |
| `json` | Structured data |
| `dot` | GraphViz diagram |
| `job` | Databricks job YAML |
| `all` | All formats |

### What Extraction Resolves

- **SQL** (sqlglot, Databricks dialect; multi-statement): `FROM`/`JOIN` reads incl. inside `MERGE`/`INSERT`/CTAS (write targets excluded — reads only), `stream()`/`live()`/`snapshot()` wrappers unwrapped, CTE names excluded, quoted identifiers output unquoted, string literals containing `FROM` never mis-extracted. The SQL-transform `$source` placeholder is excluded (the action's declared `source:` view carries that edge). Substitution tokens (`${token}`, `${secret:scope/key}`, legacy `{token}`) survive byte-for-byte, even mid-segment: `FROM cat.sch.tbl${suffix}` extracts `cat.sch.tbl${suffix}`. Unparseable body → zero edges + ONE `LHP-DEP-003` advisory (never an error).
- **Python**: recognized Spark reads (`spark.table`, `spark.read`/`readStream.table`, `spark.read.format("delta"|"iceberg"|"hive"|"unity_catalog").table`/`.load`, `spark.catalog.tableExists`/`.dropTempView`, `spark.sql`) resolve through literals, module constants, conditional reassignment (union), `+`/`"{}.{}".format(...)`, string methods (`.replace`/`.upper`/`.lower`/`.strip`/`.lstrip`/`.rstrip`/`sep.join`), f-strings over bound values, and `for`-loops over statically-foldable iterables (unrolled — one read per element).
- **Inter-procedural (within a file):** function params (union of arg values across the file's call sites + YAML-bound + signature defaults), user-function return values (so `spark.read.table(helper(x))` resolves), `a or b`/`a and b` folds to operand-set union, collection builtins `list`/`tuple`/`sorted`/`set`/`dict.fromkeys` fold as identity, `for`-loops over foldable iterables incl. dict-key iteration. Memoized, cycle-guarded (recursion→unknown), depth-cap 20, value-set-cap 256; never speculative (`os.environ` etc. stay unresolved).
- **YAML parameters resolve like codegen applies them** (entry function looked up at module top level by name; signature mismatch binds nothing): python transform `parameters:` dict passed positionally (3rd arg with ≥1 source view, 2nd with none); python load `source.parameters` dict as 2nd arg (`function_name` default `get_df`); snapshot_cdc `source_function.parameters` bound as keyword-only kwargs via `functools.partial`. `parameters["k"]` and `parameters.get("k", default)` resolve.
- **Not resolved → `LHP-DEP-002`:** genuinely dynamic values (`os.environ`/runtime I/O), a param fed a dynamic value at a call site, class attributes, or anything past the depth/value-set caps → declare the edge with `depends_on`.
- **Token byte-fidelity:** `${...}` tokens are never resolved at dag time; matching canonicalizes ONLY lowercase + backtick/whitespace stripping. Producer and consumer must use identical token bytes — `${catalog}.x` does not match `{catalog}.x` (`${token}` recommended; `{token}` deprecated).

### Extraction Warnings (advisory — never errors, never fail a run)

| Code | Meaning | Fix |
|------|---------|-----|
| `LHP-DEP-002` | Recognized Python table-read whose argument is not statically resolvable. Message names the unresolved expr: ``Cannot statically resolve the table argument of `spark.read.table(...)` — the value of `os.environ['TBL']` is only known at runtime.`` | `depends_on` on the action (entries validated by `LHP-VAL-063`) |
| `LHP-DEP-003` | SQL body could not be parsed (one per body; zero edges from it) | Fix the SQL, or `depends_on` |

A **non-empty `depends_on` suppresses that action's DEP-002/003 advisories** (per action, not per read — an action with 2 opaque reads + 1 declared upstream stops warning about the 2nd too), while still unioning its edges additively.

Surfaces (default-on): warnings are **aggregated per read site** (grouped by `(code, file, line, message)` — one record per unresolved read, listing every affected action). `lhp dag` stderr shows a grouped `Extraction warnings` table (`Code` | `Unresolved read` file:line | `Reason` | `Affected` [count + first 3 `fg.action`, `+N more`] | `Add depends_on in` [distinct YAML paths]), capped at 50 sites, under header `N unresolved read site(s) affecting M action(s):`, plus one `depends_on` hint. JSON output: top-level `warnings` array always present; entries carry `code`/`message`/`flowgroup`/`action`/`suggestion`/`file_path`/`line` + `edit_yaml_path`/`affected_actions` (array of `{flowgroup,action,edit_yaml_path}`)/`affected_count`; `metadata.total_warnings` counts distinct sites, `metadata.total_warning_occurrences` counts site×action pairs. Text report (`DEPENDENCY EXTRACTION WARNINGS`): one block per site with `Affected (N): ...` (cap 5) + `Add depends_on in: ...`. NOT in DOT output or job YAML. Public API: `lhp.api.DependencyWarningView` (+ `edit_yaml_path`/`affected_actions`/`affected_count`) and new `AffectedActionView` (both provisional) on `DependencyAnalysisResult.warnings`.

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
lhp generate --env $ENV
lhp dag --format job --job-config config/job_config.yaml --bundle-output
databricks bundle deploy --target $ENV
```

---

## Custom Python Functions & Local Helpers

User Python entry modules — python load/transform (`module_path`), custom data source
(`module_path`), custom sink (`module_path`), and snapshot CDC `source_function` — are
copied into `generated/<pipeline>/custom_python_functions/` at generate time. All of these
paths are resolved relative to the **project root** (not the YAML file's location).

**Transitive local-helper copying.** When an entry module imports a local helper
module/sub-package, LHP copies the entry **and** its transitive local helpers into
`custom_python_functions/`, **mirroring sub-package structure**:

```
custom_python_functions/
├── __init__.py                 # LHP package marker
├── entry_module.py             # entry, flat; local imports rewritten
└── helpers/                    # referenced helper sub-package, copied in full
    ├── __init__.py             # copied from your project
    └── date_change.py          # copied; relative imports preserved
```

The directory holding the entry file is the **import root**. An import is *local* iff its
top dotted segment resolves to a `.py` file or package under that root.

- **Rule A:** the import root must NOT itself be a package (no `__init__.py` at its top) →
  `LHP-VAL-023`. Keep the entry flat; put helpers in a sub-directory.
- **Rule B (whole-sub-package copy):** a referenced helper **package is copied in full**
  (every `.py` under it), structure preserved — so `__init__.py` side effects and
  intra-package imports keep working.
- **Import rewriting:** absolute-local imports are prefix-rewritten
  (`from helpers.x import y` → `from custom_python_functions.helpers.x import y`, aliases
  kept); **intra-package relative imports (`from .x import y`) are preserved unchanged**
  (first-class inside helper packages); external/stdlib imports are untouched.
- **Plain-dotted-local** (`import helpers.x`) is rejected with `LHP-VAL-024` — use
  `from helpers.x import ...` instead.
- **Missing local helper** → `LHP-VAL-025`.
- A syntactically broken sibling inside a copied package surfaces `LHP-IO-003` at generate
  time (a consequence of whole-package copy).

**Pickle interaction (custom data source / sink):** `register_pickle_by_value` registers the
top-level `custom_python_functions` package, so every copied descendant
(`custom_python_functions.helpers.*`) is pickled by value with no change to the registration
line — which is why helpers are mirrored under `custom_python_functions/` rather than placed
elsewhere on `sys.path`.
