# E2E Test Architecture

## How End-to-End Tests Work in Lakehouse Plumber

This document explains the architecture, design decisions, and execution flow of the E2E test suite. For a quick reference on running tests and using helper methods, see [E2E_TEST_QUICK_REFERENCE.md](E2E_TEST_QUICK_REFERENCE.md).

---

## Table of Contents

- [Overview](#overview)
- [The Fixture Project](#the-fixture-project)
- [Test Isolation Model](#test-isolation-model)
- [Baseline Comparison System](#baseline-comparison-system)
- [CLI Invocation Pattern](#cli-invocation-pattern)
- [Test Lifecycle](#test-lifecycle)
- [Test Suites](#test-suites)
  - [Bundle Manager (BM) Scenarios](#bundle-manager-bm-scenarios)
  - [Job Orchestration](#job-orchestration)
  - [Pipeline Configuration](#pipeline-configuration)
  - [Local Variables](#local-variables)
- [State File Tracking](#state-file-tracking)
- [Shared Fixtures (conftest.py)](#shared-fixtures-conftestpy)
- [Adding New E2E Tests](#adding-new-e2e-tests)
- [Updating Baselines](#updating-baselines)

---

## Overview

The E2E tests validate the complete LHP workflow end-to-end: from reading YAML pipeline definitions, through code generation and template rendering, to producing Databricks Asset Bundle resource files. Unlike unit tests that mock dependencies, E2E tests exercise the real CLI, real file I/O, and real template engine against a self-contained fixture project.

**Test files:**

| File | Purpose |
|------|---------|
| `test_bundle_manager_e2e.py` | Bundle resource lifecycle (BM-1 through BM-15) |
| `test_job_orchestration_e2e.py` | Job generation, multi-job mode, master jobs |
| `test_pipeline_config_e2e.py` | Pipeline config with clusters, notifications, tags |
| `test_local_variables_e2e.py` | `%{local_var}` resolution in flowgroups |
| `test_multi_job_generation_e2e.py` | Multi-job CLI integration |

---

## The Fixture Project

All E2E tests share a single, self-contained project at:

```
tests/e2e/fixtures/testing_project/
```

This is a complete LHP project modelled on a TPC-H data warehouse. It contains everything LHP needs to run `generate` and `deps` commands:

```
testing_project/
├── lhp.yaml                        # Project config (name: acme_edw)
├── databricks.yml                   # Bundle config (dev/tst/prod targets)
├── databricks_baseline.yml          # Expected databricks.yml after generation
│
├── config/
│   ├── pipeline_config.yaml         # Per-pipeline settings (clusters, edition, etc.)
│   └── job_config.yaml              # Job orchestration settings
│
├── pipelines/                       # Flowgroup YAML definitions
│   ├── 01_raw_ingestion/            # CloudFiles CSV/JSON/Parquet loads
│   ├── 02_bronze/                   # Bronze transforms (SQL + Python)
│   ├── 03_silver/                   # Silver dimension tables
│   ├── 04_gold/                     # Gold analytics views
│   ├── 04_Modelled/                 # Fact tables
│   ├── 05_kafka_ingestion/          # Kafka sources
│   ├── 06_custom_datasource/        # Custom PySpark data sources
│   └── 09_test_python/              # Python function integration
│
├── substitutions/                   # Environment token files
│   ├── dev.yaml                     # {catalog} → acme_edw_dev, etc.
│   ├── tst.yaml
│   └── prod.yaml
│
├── presets/                         # Shared action defaults
│   ├── bronze_layer.yaml
│   ├── cloudfiles_defaults.yaml
│   ├── default_delta_properties.yaml
│   └── write_defaults.yaml
│
├── templates/                       # Jinja2 flowgroup templates
│   └── ingestion/
│       ├── csv_ingestion_template.yaml
│       └── parquet_ingestion_template.yaml
│
├── py_functions/                    # Python functions (embedded/copied patterns)
│   ├── sample_func.py              # Copied pattern (import-based)
│   └── partsupp_snapshot_func.py   # Embedded pattern (inline)
│
├── schemas/                         # Table schema definitions
├── expectations/                    # Data quality rules (JSON)
├── sql/                             # SQL transform files
│
├── generated_baseline/              # Expected generated Python output
│   └── dev/
│       ├── acmi_edw_raw/            # 8 flowgroup .py files
│       ├── acmi_edw_bronze/         # 5 flowgroup .py files
│       ├── acmi_edw_silver/         # 4 flowgroup .py files
│       └── ...
│
└── resources_baseline/              # Expected bundle resource output
    ├── lhp/
    │   ├── acmi_edw_raw.pipeline.yml
    │   ├── acmi_edw_bronze.pipeline.yml
    │   └── ...                      # 8 pipeline resource files
    └── indvidual_jobs/
        ├── j_one.job.yml            # Individual job baselines
        └── acme_edw_master.job.yml  # Master job baseline
```

**Key design decision:** The fixture project is **read-only** during tests. Each test gets an isolated copy (see next section), so the fixture itself is never modified.

---

## Test Isolation Model

Every test method gets a completely fresh, isolated copy of the fixture project. This prevents cross-test contamination and ensures tests can run in any order.

```
┌──────────────────────────────────────────────────┐
│  For each test method:                           │
│                                                  │
│  1. Create temp directory (/tmp/xxx/)            │
│  2. Deep-copy fixture → /tmp/xxx/test_project/   │
│  3. cd into the copy                             │
│  4. Wipe generated/ and resources/lhp/           │
│  5. Run the test                                 │
│  6. cd back to original directory                │
│  7. Delete temp directory                        │
└──────────────────────────────────────────────────┘
```

This is implemented via two layers:

### Layer 1: `conftest.py` — `isolated_project` fixture

Creates a fresh temp directory and handles cleanup (including Windows file-locking workarounds):

```python
@pytest.fixture
def isolated_project():
    temp_dir = Path(tempfile.mkdtemp())
    yield temp_dir
    shutil.rmtree(temp_dir)  # with Windows retry logic
```

### Layer 2: `setup_test_project` — autouse fixture on the test class

Copies the fixture, sets up paths, and initialises empty working directories:

```python
@pytest.fixture(autouse=True)
def setup_test_project(self, isolated_project):
    fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
    self.project_root = isolated_project / "test_project"
    shutil.copytree(fixture_path, self.project_root)

    os.chdir(self.project_root)

    self.generated_dir = self.project_root / "generated" / "dev"
    self.resources_dir = self.project_root / "resources" / "lhp"

    # Start fresh: wipe and recreate working dirs
    self._init_bundle_project()

    yield
    os.chdir(self.original_cwd)
```

The `_init_bundle_project()` method deletes `generated/dev/` and `resources/lhp/` then recreates them empty. Baseline directories (`generated_baseline/`, `resources_baseline/`) are left intact for comparison.

---

## Baseline Comparison System

The core verification mechanism is **SHA-256 hash comparison** against pre-generated baseline files.

### Two types of baselines

| Directory | Contains | Compared by |
|-----------|----------|-------------|
| `generated_baseline/dev/` | Expected Python output files | `_compare_directory_hashes()` |
| `resources_baseline/lhp/` | Expected bundle resource YAML | `_compare_file_hashes()` |

### How hash comparison works

**Single file** (`_compare_file_hashes`):

```python
def _compare_file_hashes(self, file1, file2) -> str:
    hash1 = hashlib.sha256(file1.read_bytes()).hexdigest()
    hash2 = hashlib.sha256(file2.read_bytes()).hexdigest()
    if hash1 != hash2:
        return "Hash mismatch: ..."
    return ""  # identical
```

**Directory** (`_compare_directory_hashes`):

```python
def _compare_directory_hashes(self, generated_dir, baseline_dir) -> list:
    # 1. Collect all files from both directories (recursive)
    # 2. Report files only in generated (extras)
    # 3. Report files only in baseline (missing)
    # 4. For files in both: compare SHA-256 hashes
    # 5. On mismatch: include unified diff (first 50 lines) for debugging
    return differences  # empty list = all match
```

### Typical usage in a test

```python
def test_baseline_hash_comparison_dev_environment(self):
    exit_code, output = self.run_bundle_sync()
    assert exit_code == 0

    hash_differences = self._compare_directory_hashes(
        self.project_root / "generated" / "dev",
        self.project_root / "generated_baseline" / "dev"
    )
    assert not hash_differences, f"{len(hash_differences)} differences found"
```

### Why hashes, not string comparison?

- Binary-safe: catches encoding and line-ending differences
- Fast: no need to parse or normalise output
- Deterministic: if the hash matches, the content is byte-identical
- Debugging-friendly: on failure, the diff is printed for the first mismatched files

---

## CLI Invocation Pattern

E2E tests invoke the real LHP CLI using Click's `CliRunner`, which runs the CLI in-process without spawning a subprocess:

```python
from click.testing import CliRunner
from lhp.cli.main import cli

runner = CliRunner()
result = runner.invoke(cli, [
    '--verbose', 'generate', '--env', 'dev', '--force'
])

exit_code = result.exit_code
output = result.output
```

Two helper methods wrap this:

| Method | CLI command | Notes |
|--------|------------|-------|
| `run_bundle_sync()` | `lhp generate --env dev --force` | Default generation, no pipeline config flag |
| `run_bundle_sync_with_pipeline_config()` | `lhp generate --env dev --pipeline-config config/pipeline_config.yaml --force` | Explicitly passes pipeline config |

Both return `(exit_code, output)` tuples.

---

## Test Lifecycle

Here is the complete flow of a single test:

```
Session start
  └─ conftest: configure_test_logging (session scope)
  └─ conftest: clean_logging (autouse, per-test)

Per test method:
  ┌─ SETUP ──────────────────────────────────────┐
  │ 1. clean_logging: close all log handlers     │
  │ 2. isolated_project: create /tmp/xxx/        │
  │ 3. setup_test_project:                       │
  │    a. Copy fixture → /tmp/xxx/test_project/  │
  │    b. cd into project                        │
  │    c. Wipe generated/ and resources/lhp/     │
  └──────────────────────────────────────────────┘
  ┌─ TEST ───────────────────────────────────────┐
  │ 1. (Optional) Modify fixture files           │
  │ 2. Run CLI: lhp generate / lhp deps         │
  │ 3. Assert exit code                          │
  │ 4. Compare output with baselines             │
  │ 5. Validate file existence, content, state   │
  └──────────────────────────────────────────────┘
  ┌─ TEARDOWN ───────────────────────────────────┐
  │ 1. cd back to original directory             │
  │ 2. clean_logging: close all log handlers     │
  │ 3. isolated_project: delete /tmp/xxx/        │
  └──────────────────────────────────────────────┘
```

---

## Test Suites

### Bundle Manager (BM) Scenarios

The largest test suite validates the bundle resource file lifecycle — the decision logic for when to create, update, preserve, back up, or delete resource files.

#### Core lifecycle scenarios (BM-1 through BM-7)

| ID | Precondition | Action | Expected |
|----|-------------|--------|----------|
| BM-1 | LHP file exists, unchanged | Generate | File preserved (mtime unchanged) |
| BM-1.1 | LHP file exists | Generate with `--pipeline-config` + `--force` | File regenerated with config |
| BM-2 | User-managed file (no LHP header) | Generate | Backup created (`.bkup`), replaced with LHP |
| BM-3 | No resource file | Generate | New LHP resource file created |
| BM-4 | Resource file exists, no Python | Generate | Orphan resource deleted |
| BM-5 | Multiple resource files | Generate | Warning/error issued |
| BM-6 | Mixed headers | Generate | Correct LHP vs user detection |
| BM-7 | Missing generated/ directory | Generate | Directory auto-created |

#### Python function dependency scenarios (BM-8 through BM-13)

These test the **incremental regeneration** system — when a Python function changes, only dependent generated files are rebuilt.

**Two function patterns exist:**

- **Embedded** (`partsupp_snapshot_func.py`): Function code is inlined into the generated file. Change triggers regeneration of 1 file.
- **Copied** (`sample_func.py`): Function is copied to `custom_python_functions/` and imported. Change triggers regeneration of 2 files (the copy + the importer).

The dependency tracking relies on `.lhp_state.json` (see [State File Tracking](#state-file-tracking)).

#### Conflict & overwrite scenarios (BM-14, BM-15)

- **BM-14**: Same function modified — file updated in place, no duplication
- **BM-15**: Two different functions targeting the same destination — raises `PythonFunctionConflictError`

### Job Orchestration

Tests the `lhp deps` command for generating Databricks orchestration jobs.

**Scenarios tested:**

1. **Single job** (default): `lhp deps -b` → single orchestration `.job.yml`
2. **With job config**: `lhp deps -b -jc config/job_config.yaml` → configured job
3. **Multi-job with master**: Uncomment `job_name` in flowgroups → individual jobs + master job
4. **Multi-job custom master name**: Custom `master_job_name` in job config
5. **Multi-job without master**: `disable_master_job: true` → individual jobs only

Multi-job tests use a helper `uncomment_job_names()` that toggles `# job_name:` lines in flowgroup YAML files to enable multi-job mode.

### Pipeline Configuration

Tests that `pipeline_config.yaml` values (clusters, notifications, tags, event_log, etc.) are correctly rendered into bundle resource YAML. Validates:

- YAML structural correctness (no concatenated lines)
- Cluster configuration (node types, autoscale, policy)
- Substitution token resolution in all config fields
- Baseline hash match for the comprehensive cluster config

### Local Variables

Tests the `%{local_var}` substitution feature. Validates:

- Variables resolve correctly throughout the flowgroup
- Undefined variables raise `LHPError`
- Generated Python output contains resolved values, no unresolved `%{...}` patterns

---

## State File Tracking

LHP writes `.lhp_state.json` after each generation run. E2E tests use this to verify incremental regeneration behaviour.

```json
{
  "version": "1.0",
  "last_updated": "2025-02-16T...",
  "environments": {
    "dev": {
      "generated/dev/acmi_edw_bronze/customer_bronze.py": {
        "checksum": "abc123...",
        "file_composite_checksum": "def456...",
        "pipeline": "acmi_edw_bronze",
        "flowgroup": "customer_bronze",
        "source_yaml": "pipelines/02_bronze/customer_bronze.yaml",
        "file_dependencies": {
          "py_functions/sample_func.py": {
            "checksum": "ghi789...",
            "type": "python_function"
          }
        }
      }
    }
  }
}
```

**Test pattern for dependency tracking:**

```python
# Phase 1: Generate and capture state
self.run_bundle_sync()
baseline_state = self._load_state_file()

# Phase 2: Modify a Python function
self._modify_python_function("py_functions/sample_func.py", "test change")

# Phase 3: Regenerate
self.run_bundle_sync()
updated_state = self._load_state_file()

# Phase 4: Assert only dependent files were regenerated
regen_count = self._validate_python_dependency_regeneration(
    baseline_state, updated_state,
    "py_functions/sample_func.py",
    expected_dependent_files
)
assert regen_count == expected_count
```

---

## Shared Fixtures (conftest.py)

Located at `tests/conftest.py`, these fixtures are available to all test files:

| Fixture | Scope | Purpose |
|---------|-------|---------|
| `configure_test_logging` | session | Suppress noisy loggers (`lhp`, `urllib3`) |
| `clean_logging` | per-test, autouse | Close all log handlers before/after each test |
| `isolated_project` | per-test | Temp directory with platform-safe cleanup |
| `mock_logging_config` | per-test | Mock `configure_logging` to prevent file creation |
| `temp_project_with_logging_cleanup` | per-test | Temp dir + minimal logging config |
| `windows_safe_tempdir` | per-test | Platform-specific cleanup with retry logic |
| `create_flowgroup` | per-test | Factory for creating `FlowGroup` model objects |
| `sample_flowgroups_with_job_name` | per-test | 3 flowgroups all with `job_name` |
| `sample_flowgroups_mixed_job_name` | per-test | 4 flowgroups, 2 with/2 without `job_name` |
| `sample_multi_doc_job_config` | per-test | Multi-document `job_config.yaml` in temp dir |
| `mock_dependency_result` | per-test | Mock `DependencyAnalysisResult` |

---

## Adding New E2E Tests

### Step 1: Decide what to test

E2E tests are for **cross-cutting workflows** that involve the CLI, file system, templates, and configuration together. If you're testing a single function or class in isolation, write a unit test instead.

### Step 2: Add fixture data (if needed)

If your test needs new pipeline YAML, schemas, or substitution tokens, add them to `tests/e2e/fixtures/testing_project/`.

### Step 3: Add baselines (if needed)

If your test compares output against expected results:

1. Run the generation manually to produce the expected output
2. Copy the output into `generated_baseline/` or `resources_baseline/`
3. Commit the baselines — they become the source of truth

### Step 4: Write the test

Follow the existing pattern:

```python
def test_my_new_feature(self):
    """Description of what this tests."""
    # Arrange: modify fixture if needed
    # ...

    # Act: run CLI
    exit_code, output = self.run_bundle_sync()
    assert exit_code == 0, f"Generation failed: {output}"

    # Assert: check output
    resource_file = self.resources_dir / "my_pipeline.pipeline.yml"
    assert resource_file.exists()

    # Assert: compare with baseline (if applicable)
    baseline = self.project_root / "resources_baseline" / "lhp" / "my_pipeline.pipeline.yml"
    hash_diff = self._compare_file_hashes(resource_file, baseline)
    assert hash_diff == "", f"Baseline mismatch: {hash_diff}"
```

### Step 5: Run and verify

```bash
# Run just your test
pytest tests/e2e/test_bundle_manager_e2e.py::TestBundleManagerE2E::test_my_new_feature -v

# Run all E2E tests to check for regressions
pytest tests/e2e/ -v
```

---

## Updating Baselines

When a template, generator, or config loader change produces **intentionally different output**, the baselines must be updated. This is expected when:

- Adding a new section to the bundle resource template
- Changing code generation output format
- Adding new default pipeline config values

### Process

1. **Verify the change is correct** — manually inspect the generated output
2. **Regenerate baselines:**
   ```bash
   cd tests/e2e/fixtures/testing_project
   lhp generate --env dev --force --pipeline-config config/pipeline_config.yaml
   ```
3. **Copy generated output to baselines:**
   ```bash
   # For Python files
   cp -r generated/dev/* generated_baseline/dev/

   # For resource files
   cp resources/lhp/*.pipeline.yml resources_baseline/lhp/
   ```
4. **Run E2E tests to confirm all hashes match:**
   ```bash
   pytest tests/e2e/ -v
   ```
5. **Commit the updated baselines** alongside the code change

> **Important:** Never update baselines to make a failing test pass without understanding *why* the output changed. A hash mismatch is the test doing its job — investigate first.

---

*Last updated: February 2026*
