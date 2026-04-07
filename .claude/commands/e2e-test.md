# E2E Test Assistant — Lakehouse Plumber

You are an expert assistant for creating, maintaining, and running E2E tests in the Lakehouse Plumber (LHP) project.

## User Request
$ARGUMENTS

---

## E2E Test Architecture

### Fixture Project
All E2E tests share a single fixture project at `tests/e2e/fixtures/testing_project/`. This is a complete LHP project (TPC-H data warehouse) that is **read-only** — each test gets an isolated deep copy in a temp directory.

### Test Files

| File | Purpose |
|------|---------|
| `test_bundle_manager_e2e.py` | Bundle resource lifecycle (BM-1 through BM-15) |
| `test_job_orchestration_e2e.py` | Job generation, multi-job mode, master jobs |
| `test_pipeline_config_e2e.py` | Pipeline config with clusters, notifications, tags |
| `test_local_variables_e2e.py` | `%{local_var}` resolution in flowgroups |
| `test_multi_job_generation_e2e.py` | Multi-job CLI integration |
| `conftest.py` | Shared fixtures (session and per-test scope) |

### Test Isolation Model
Every test method gets a completely fresh copy:
1. Create temp directory (`/tmp/xxx/`)
2. Deep-copy fixture → `/tmp/xxx/test_project/`
3. `cd` into the copy
4. Wipe `generated/` and `resources/lhp/` (baselines stay intact)
5. Run the test
6. `cd` back to original directory
7. Delete temp directory

---

## Required Class Structure

Every E2E test class MUST follow this pattern:

```python
import hashlib
import json
import os
import shutil
from pathlib import Path

import pytest
import yaml
from click.testing import CliRunner

from lhp.cli.main import cli


@pytest.mark.e2e
class TestMyFeatureE2E:
    """E2E tests for [feature description]."""

    @pytest.fixture(autouse=True)
    def setup_test_project(self, isolated_project):
        """Create isolated copy of fixture project for each test."""
        fixture_path = Path(__file__).parent / "fixtures" / "testing_project"
        self.project_root = isolated_project / "test_project"
        shutil.copytree(fixture_path, self.project_root)

        self.original_cwd = os.getcwd()
        os.chdir(self.project_root)

        self.generated_dir = self.project_root / "generated" / "dev"
        self.resources_dir = self.project_root / "resources" / "lhp"

        self._init_bundle_project()

        yield
        os.chdir(self.original_cwd)

    def _init_bundle_project(self):
        """Wipe and recreate working directories."""
        if self.generated_dir.exists():
            shutil.rmtree(self.generated_dir)
        self.generated_dir.mkdir(parents=True, exist_ok=True)

        if self.resources_dir.exists():
            shutil.rmtree(self.resources_dir)
        self.resources_dir.mkdir(parents=True, exist_ok=True)
```

---

## CLI Invocation Helpers

Use Click's `CliRunner` (in-process, no subprocess):

```python
def run_bundle_sync(self) -> tuple:
    """Run 'lhp generate --env dev --force'. Returns (exit_code, output)."""
    runner = CliRunner()
    result = runner.invoke(cli, ['--verbose', 'generate', '--env', 'dev', '--force'])
    return result.exit_code, result.output

def run_bundle_sync_with_pipeline_config(self) -> tuple:
    """Run generation with explicit pipeline config. Returns (exit_code, output)."""
    runner = CliRunner()
    result = runner.invoke(cli, [
        '--verbose', 'generate', '--env', 'dev',
        '--pipeline-config', 'config/pipeline_config.yaml', '--force'
    ])
    return result.exit_code, result.output

def run_deps_command(self, *args) -> tuple:
    """Run 'lhp deps [args]'. Returns (exit_code, output)."""
    runner = CliRunner()
    result = runner.invoke(cli, ['deps', *args])
    return result.exit_code, result.output
```

---

## Baseline Comparison Helpers

The core verification mechanism is SHA-256 hash comparison:

```python
def _compare_file_hashes(self, file1: Path, file2: Path) -> str:
    """Compare two files by SHA-256. Returns '' if identical, error string if different."""
    def get_hash(f):
        return hashlib.sha256(f.read_bytes()).hexdigest()
    h1, h2 = get_hash(file1), get_hash(file2)
    if h1 != h2:
        return f"Hash mismatch: {file1.name} ({h1[:12]}) != {file2.name} ({h2[:12]})"
    return ""

def _compare_directory_hashes(self, generated_dir: Path, baseline_dir: Path) -> list:
    """Compare directories recursively. Returns list of differences (empty = match)."""
    differences = []
    gen_files = {f.relative_to(generated_dir): f for f in generated_dir.rglob("*") if f.is_file()}
    base_files = {f.relative_to(baseline_dir): f for f in baseline_dir.rglob("*") if f.is_file()}

    for rel in gen_files.keys() - base_files.keys():
        differences.append(f"EXTRA: {rel}")
    for rel in base_files.keys() - gen_files.keys():
        differences.append(f"MISSING: {rel}")
    for rel in gen_files.keys() & base_files.keys():
        h1 = hashlib.sha256(gen_files[rel].read_bytes()).hexdigest()
        h2 = hashlib.sha256(base_files[rel].read_bytes()).hexdigest()
        if h1 != h2:
            differences.append(f"CHANGED: {rel}")
    return differences
```

---

## State File Helpers

For testing incremental regeneration and dependency tracking:

```python
def _load_state_file(self) -> dict:
    """Load .lhp_state.json from project root."""
    state_file = self.project_root / ".lhp_state.json"
    if state_file.exists():
        return json.loads(state_file.read_text())
    return {}

def _extract_py_function_checksum(self, state: dict, py_function_path: str) -> str:
    """Extract checksum for a Python function from state file."""
    for file_info in state.get("environments", {}).get("dev", {}).values():
        deps = file_info.get("file_dependencies", {})
        if py_function_path in deps:
            return deps[py_function_path].get("checksum", "")
    return ""

def _get_dependent_generated_files(self, state: dict, py_function_path: str) -> list:
    """Get generated files that depend on a Python function."""
    dependents = []
    for file_path, file_info in state.get("environments", {}).get("dev", {}).items():
        if py_function_path in file_info.get("file_dependencies", {}):
            dependents.append(file_path)
    return dependents
```

---

## Test Patterns

### Pattern 1: Basic Generation + Validation
```python
def test_my_feature_generates_correctly(self):
    """Verify [feature] generates expected output."""
    exit_code, output = self.run_bundle_sync()
    assert exit_code == 0, f"Generation failed: {output}"

    resource_file = self.resources_dir / "pipeline_name.pipeline.yml"
    assert resource_file.exists(), "Resource file should be created"
```

### Pattern 2: Baseline Comparison
```python
def test_my_feature_matches_baseline(self):
    """Verify generated output matches baseline."""
    exit_code, output = self.run_bundle_sync()
    assert exit_code == 0

    differences = self._compare_directory_hashes(
        self.project_root / "generated" / "dev",
        self.project_root / "generated_baseline" / "dev"
    )
    assert not differences, f"{len(differences)} differences found:\n" + "\n".join(differences)
```

### Pattern 3: State-Based Dependency Testing
```python
def test_dependency_triggers_regeneration(self):
    """Verify modifying [source] regenerates dependent files."""
    # Phase 1: Baseline
    self.run_bundle_sync()
    baseline_state = self._load_state_file()

    # Phase 2: Modify source
    self._modify_python_function("py_functions/sample_func.py", "test change")

    # Phase 3: Regenerate and compare
    self.run_bundle_sync()
    updated_state = self._load_state_file()

    # Phase 4: Assert selective regeneration
    regen_count = self._validate_python_dependency_regeneration(
        baseline_state, updated_state,
        "py_functions/sample_func.py",
        expected_dependent_files
    )
    assert regen_count == expected_count
```

### Pattern 4: File Modification Before Generation
```python
def test_modified_config_changes_output(self):
    """Verify [config change] produces different output."""
    # Modify fixture file in the isolated copy
    config_file = self.project_root / "config" / "pipeline_config.yaml"
    content = config_file.read_text()
    content = content.replace("old_value", "new_value")
    config_file.write_text(content)

    # Generate with modified config
    exit_code, output = self.run_bundle_sync_with_pipeline_config()
    assert exit_code == 0

    # Validate the change is reflected
    resource = self.resources_dir / "pipeline.pipeline.yml"
    assert "new_value" in resource.read_text()
```

### Pattern 5: Error/Failure Validation
```python
def test_invalid_config_raises_error(self):
    """Verify [invalid input] causes expected failure."""
    # Introduce invalid configuration
    config = self.project_root / "lhp.yaml"
    config.write_text("invalid: yaml: content: [")

    exit_code, output = self.run_bundle_sync()
    assert exit_code != 0, "Should fail with invalid config"
    assert "error" in output.lower() or "Error" in output
```

---

## Common Assertions

```python
# Exit codes
assert exit_code == 0, f"Command should succeed: {output}"
assert exit_code != 0, "Command should fail"

# File existence
assert file_path.exists(), f"Expected {file_path.name} to exist"
assert not file_path.exists(), f"Expected {file_path.name} to NOT exist"

# LHP header detection
assert self.has_lhp_header(file), "Should have LHP header"

# Content checks
assert "expected_string" in content
content = yaml.safe_load(resource_file.read_text())
assert content is not None

# Hash comparison
hash_diff = self._compare_file_hashes(file1, file2)
assert hash_diff == "", f"Baseline mismatch: {hash_diff}"

# Directory comparison
differences = self._compare_directory_hashes(gen_dir, baseline_dir)
assert not differences, f"{len(differences)} differences"
```

---

## Test Naming Conventions

- **Bundle manager scenarios:** `test_BM{ID}_{snake_case_description}` (e.g., `test_BM8a_embedded_python_function_regeneration`)
- **Feature tests:** `test_{feature}_{scenario}` (e.g., `test_baseline_hash_comparison_dev_environment`)
- **Error tests:** `test_{feature}_raises_error` or `test_{feature}_failure`
- All test classes: `@pytest.mark.e2e` decorator
- Class names: `Test{Feature}E2E`

---

## Fixture Project Key Paths

```python
self.project_root                                           # Root of isolated test project
self.generated_dir                                          # generated/dev/
self.resources_dir                                          # resources/lhp/
self.project_root / "generated_baseline" / "dev"            # Known-good Python output
self.project_root / "resources_baseline" / "lhp"            # Known-good resource YAML
self.project_root / "config" / "pipeline_config.yaml"       # Pipeline cluster config
self.project_root / "config" / "job_config.yaml"            # Job orchestration config
self.project_root / "lhp.yaml"                              # Project config
self.project_root / ".lhp_state.json"                       # State file (after generation)
self.project_root / "pipelines"                             # Flowgroup YAML definitions
self.project_root / "py_functions"                          # Python function modules
self.project_root / "substitutions"                         # Environment token files
```

---

## Running Tests

```bash
# All E2E tests (ALWAYS run full suite after any change)
pytest tests/e2e/ -v -n auto

# Specific test file
pytest tests/e2e/test_bundle_manager_e2e.py -v

# Specific test method
pytest tests/e2e/test_bundle_manager_e2e.py::TestBundleManagerE2E::test_BM1_preserve_existing_lhp_managed_file -v

# With print output visible
pytest tests/e2e/ -v -s

# Filter by keyword
pytest tests/e2e/ -k "BM8"
pytest tests/e2e/ -k "baseline"
```

---

## Updating Baselines

### Principle
The agent writes baseline files directly — never rely on generation + script to produce baselines. The agent must understand what the expected output should be and write it.

### Diff verification (MANDATORY)
Before finalizing any baseline update, verify the diff between old and new baseline content. Confirm that:
- ONLY changes related to the current development work are present
- No unintended side effects or unrelated changes leaked in
- If unexpected differences appear, STOP and investigate before proceeding

### For a new test (targeted change)
1. Understand what the test expects as output
2. Write ONLY the baseline file(s) needed for that specific test into the appropriate baseline directory (`generated_baseline/dev/` or `resources_baseline/lhp/`)
3. Ask the user which CLI flags the generation will use
4. Run the **full** E2E test suite to confirm no regressions: `pytest tests/e2e/ -v`

### For an app-wide behavior change
When a change affects how the app generates output (e.g., template change, generator logic change), ALL affected baseline files must be carefully updated:
1. Identify every baseline file affected by the change
2. Update each one manually with the correct expected content
3. Verify the diff — only expected changes should be present, nothing else
4. Ask the user which CLI flags the generation will use
5. Run the **full** E2E test suite to confirm no regressions: `pytest tests/e2e/ -v`

### Regression check (MANDATORY)
Every E2E change — new test, baseline update, fixture modification — MUST be followed by running the full test suite:
```bash
pytest tests/e2e/ -v
```
Never skip this step. A passing individual test is not sufficient; the full suite must pass.

### Reset script (reference)
A reset script exists at `tests/e2e/scripts/reset_testing_project.sh` for cleaning up the fixture project's working directories. It handles:
- **Always:** Delete `.lhp/`, `.lhp_state.json`, git restore `databricks.yml`
- `--empty-generated` / `--empty-resources` — Wipe working directories
- `--move-generated` / `--move-resources` — Move working output to baseline dirs
- Default (no args) = dry-run mode; use `--execute` to apply

**Never update baselines without understanding WHY the output changed.**

---

## Adding New Fixture Data

If your test needs new pipeline YAML, schemas, or config:
1. Add files to `tests/e2e/fixtures/testing_project/`
2. Write the expected baseline output manually
3. Verify the diff — only your new additions should appear
4. Run the **full** E2E test suite: `pytest tests/e2e/ -v`
5. Commit fixture data + baselines together

---

## Instructions

When the user asks you to work with E2E tests:

1. **Creating a new test:** Read the target test file first. Follow the exact class structure, naming conventions, and helper patterns shown above. Place the test in the most appropriate existing test file, or create a new file only if the feature doesn't fit any existing suite.

2. **Maintaining tests:** When baselines need updating, write them directly. Verify the diff to ensure only intended changes are present. When fixing a failing test, investigate the root cause — a hash mismatch means the test is working correctly and the output changed.

3. **Running tests:** Always run the **full** E2E test suite (`pytest tests/e2e/ -v`) after any change. A passing individual test is not sufficient.

4. **Adding fixture data:** Add to the shared fixture project, write baselines manually, verify the diff, and commit together.

5. **Always read existing test files** before adding new tests to maintain consistency with established patterns.

6. **Always ask the user** which CLI flags to use for generation when working with baselines.

---

## Gotchas

Known Claude failure points — check these proactively:

- **Regenerating baselines instead of writing them manually.** Baselines must be hand-written by understanding the expected output, NOT by running generation and copying the result.
- **Forgetting to run the full E2E suite.** A single passing test is never sufficient. Always run `pytest tests/e2e/ -v -n auto` after any change.
- **Diffing against generated output instead of baselines.** The comparison is always generated vs. baseline directory, not generated vs. generated.
- **Modifying the shared fixture project without considering other tests.** Changes to `tests/e2e/fixtures/testing_project/` affect ALL tests. Always verify the full suite.
- **Using `--include-tests` without asking.** Test action generation requires explicit `--include-tests` — never assume it. Ask the user which CLI flags to use.
- **Hash mismatches are not bugs in the test.** A hash mismatch means the output changed — investigate WHY the output changed before touching baselines.
- **Creating new test files unnecessarily.** Place tests in the most appropriate existing file first. Only create a new file if the feature truly doesn't fit.
