# E2E Testing Quick Reference Guide
## Lakehouse Plumber

### Running Tests

```bash
# Run all e2e tests
pytest tests/e2e/

# Run specific test file
pytest tests/e2e/test_bundle_manager_e2e.py

# Run specific test method
pytest tests/e2e/test_bundle_manager_e2e.py::TestBundleManagerE2E::test_BM1_preserve_existing_lhp_managed_file

# Run with verbose output
pytest tests/e2e/ -v

# Run with print statements visible
pytest tests/e2e/ -s

# Run specific category
pytest tests/e2e/ -k "baseline"
pytest tests/e2e/ -k "BM1"
```

---

## Test File Overview

| File | Size | Purpose | Status |
|------|------|---------|--------|
| `test_bundle_manager_e2e.py` | 83KB | Bundle Manager integration | ✅ Complete |
| `test_pipeline_config_e2e.py` | 11KB | Pipeline configuration | ✅ Complete |
| `test_multi_job_generation_e2e.py` | 6.5KB | Multi-job orchestration | ⏳ TODOs |

---

## Bundle Manager Test Scenarios (BM-X)

| ID | Test Name | What It Tests | Expected Outcome |
|----|-----------|---------------|------------------|
| BM-1 | `test_BM1_preserve_existing_lhp_managed_file` | Preserve unchanged LHP file | No modifications |
| BM-1.1 | `test_BM1_1_regenerate_existing_lhp_managed_file_with_pipeline_config` | Force regen with config | File regenerated |
| BM-2 | `test_BM2_backup_and_replace_user_managed_file` | Replace user file | Backup created |
| BM-3 | `test_BM3_create_new_resource_file_when_missing` | Create missing file | New file created |
| BM-4 | `test_BM4_delete_orphaned_resource_file` | Delete orphans | Orphan removed |
| BM-5 | `test_BM5_error_on_multiple_resource_files` | Multiple files warning | Warning issued |
| BM-6 | `test_BM6_header_based_lhp_detection` | Header detection | LHP vs user files |
| BM-7 | `test_BM7_output_directory_missing` | Missing directory | Auto-created |
| BM-8a | `test_BM8a_embedded_python_function_regeneration` | Embedded Python regen | 1 file regenerated |
| BM-8b | `test_BM8b_copied_python_function_regeneration` | Copied Python regen | 2 files regenerated |
| BM-9 | `test_BM9_diagnostic_dependency_resolution` | Dependency tracking | Analysis output |
| BM-10 | `test_BM10_staleness_analysis_diagnostic` | Staleness detection | Diagnostic info |
| BM-11 | `test_BM11_embedded_pattern_staleness_diagnostic` | Embedded vs copied | Pattern comparison |
| BM-12 | `test_BM12_pipeline_decision_logic_comparison` | Decision logic | Consistency check |
| BM-13 | `test_BM13_cli_vs_orchestrator_analysis_comparison` | CLI vs API | Behavior match |
| BM-14 | `test_BM14_python_function_overwrite_behavior` | File overwrite | Single file updated |
| BM-15 | `test_BM15_python_function_conflict_failure` | Conflict detection | Error raised |

---

## Common Test Patterns

### 1. Basic Generation Test

```python
def test_basic_generation(self):
    # Run generation
    exit_code, output = self.run_bundle_sync()
    
    # Validate success
    assert exit_code == 0, f"Generation should succeed: {output}"
    
    # Validate file created
    resource_file = self.resources_dir / "pipeline_name.pipeline.yml"
    assert resource_file.exists()
```

### 2. Baseline Comparison Test

```python
def test_baseline_comparison(self):
    # Generate files
    exit_code, output = self.run_bundle_sync()
    assert exit_code == 0
    
    # Compare with baseline
    baseline_dir = self.project_root / "generated_baseline" / "dev"
    generated_dir = self.project_root / "generated" / "dev"
    
    hash_differences = self._compare_directory_hashes(generated_dir, baseline_dir)
    assert not hash_differences, f"Found {len(hash_differences)} differences"
```

### 3. State Management Test

```python
def test_state_tracking(self):
    # Initial generation
    exit_code, output = self.run_bundle_sync()
    baseline_state = self._load_state_file()
    
    # Modify source
    self._modify_python_function("py_functions/sample_func.py", "test change")
    
    # Regenerate
    exit_code, output = self.run_bundle_sync()
    updated_state = self._load_state_file()
    
    # Validate checksums changed
    assert baseline_state != updated_state
```

### 4. Multi-Environment Test

```python
def test_multi_environment(self):
    environments = ["dev", "tst", "prod"]
    
    for env in environments:
        env_dir = self._clean_and_generate(env)
        assert env_dir.exists()
        
        # Validate files generated
        files = list(env_dir.rglob("*.py"))
        assert len(files) > 0
```

---

## Helper Methods Quick Reference

### File Management

```python
# Create user file (no LHP header)
resource_file = self.create_user_resource_file("pipeline_name")

# Check for LHP header
has_header = self.has_lhp_header(resource_file)

# Count resource files
count = self.count_resource_files("pipeline_name")

# Get backup files
backups = self.get_backup_files("pipeline_name")
```

### Generation Commands

```python
# Run generation
exit_code, output = self.run_bundle_sync()

# Run with pipeline config
exit_code, output = self.run_bundle_sync_with_pipeline_config()
```

### State Management

```python
# Load state file
state = self._load_state_file()

# Extract Python function checksum
checksum = self._extract_py_function_checksum(state, "py_functions/func.py")

# Get dependent files
dependent = self._get_dependent_generated_files(state, "py_functions/func.py")
```

### Comparison

```python
# Compare directory hashes
differences = self._compare_directory_hashes(generated_dir, baseline_dir)

# Compare file hashes
diff = self._compare_file_hashes(file1, file2)

# Get file structures with sizes
structure = self._get_file_structure_with_sizes(directory)
```

### Modification

```python
# Modify Python function
self._modify_python_function("py_functions/sample_func.py", "test comment")

# Enable include filtering
original_content = self._enable_include_filtering(lhp_config_file)

# Restore config
self._restore_lhp_config(lhp_config_file, original_content)
```

---

## Test Fixture Structure

```
testing_project/
├── config/
│   ├── job_config.yaml          # Job orchestration config
│   └── pipeline_config.yaml     # Pipeline cluster config
├── pipelines/                   # Flowgroup YAML files
│   ├── 01_raw_ingestion/        # Raw layer
│   ├── 02_bronze/               # Bronze layer
│   ├── 03_silver/               # Silver layer
│   ├── 04_gold/                 # Gold layer
│   ├── 04_Modelled/             # Modelled layer
│   └── 09_test_python/          # Python functions test
├── py_functions/                # Python function modules
│   ├── partsupp_snapshot_func.py  # Embedded pattern
│   └── sample_func.py             # Copied pattern
├── schemas/                     # Data schemas
├── substitutions/               # Environment variables
│   ├── dev.yaml
│   ├── tst.yaml
│   └── prod.yaml
├── generated/                   # Working directory (cleaned)
│   └── dev/
├── generated_baseline/          # Known-good baseline
│   ├── dev/
│   ├── tst/
│   └── prod/
├── resources/                   # Working resources (cleaned)
│   └── lhp/
├── resources_baseline/          # Known-good baselines
│   └── lhp/
│       ├── acmi_edw_raw.pipeline.yml
│       ├── acmi_edw_bronze.pipeline.yml
│       └── ...
├── lhp.yaml                     # Project config
└── databricks.yml               # Bundle config
```

---

## Common Assertions

```python
# File existence
assert file_path.exists(), "File should exist"
assert not file_path.exists(), "File should not exist"

# Exit codes
assert exit_code == 0, f"Command should succeed: {output}"

# Header detection
assert self.has_lhp_header(file), "Should have LHP header"
assert not self.has_lhp_header(file), "Should not have LHP header"

# Content validation
assert "expected_string" in content
assert re.search(r'pattern', content)

# YAML validity
parsed = yaml.safe_load(content)
assert parsed is not None

# Hash comparison
assert baseline_hash != updated_hash, "Checksum should change"
assert hash_differences == [], "Files should match baseline"

# Count validation
assert len(files) == expected_count
assert regenerated_count == 1, f"Expected 1 file, got {regenerated_count}"
```

---

## Environment Variables & Paths

```python
# Common paths in tests
self.project_root           # Root of test project
self.generated_dir          # generated/dev/
self.resources_dir          # resources/lhp/

# Baseline paths
baseline_generated = self.project_root / "generated_baseline" / "dev"
baseline_resources = self.project_root / "resources_baseline" / "lhp"

# State file
state_file = self.project_root / ".lhp_state.json"
```

---

## Debugging Tips

### 1. View Test Output

```bash
# See print statements
pytest tests/e2e/test_bundle_manager_e2e.py::TestBundleManagerE2E::test_BM1 -s

# Verbose output
pytest tests/e2e/ -v
```

### 2. Inspect Generated Files

```python
# Add to test for debugging
print(f"Generated files: {list(self.generated_dir.rglob('*.py'))}")
print(f"Resource content:\n{resource_file.read_text()}")
```

### 3. Check State File

```python
# Add to test for debugging
state = self._load_state_file()
import json
print(json.dumps(state, indent=2))
```

### 4. Compare Hash Differences

```python
# Detailed diff output
hash_differences = self._compare_directory_hashes(gen_dir, baseline_dir)
for i, diff in enumerate(hash_differences, 1):
    print(f"{i}. {diff}")
```

### 5. Check CLI Output

```python
# Print full CLI output
exit_code, output = self.run_bundle_sync()
print("=" * 80)
print("CLI Output:")
print(output)
print("=" * 80)
```

---

## Test Execution Flow

```
1. Setup (autouse fixture)
   ├── Copy fixture to isolated temp dir
   ├── Change to project directory
   ├── Initialize working directories
   └── Set up common paths

2. Test Method Execution
   ├── Perform test operations
   ├── Run CLI commands
   ├── Modify files
   └── Collect results

3. Assertions
   ├── Validate exit codes
   ├── Check file existence
   ├── Compare hashes
   └── Verify content

4. Teardown (autouse fixture)
   ├── Restore original directory
   ├── Clean up logging
   └── Remove temp directory
```

---

## Key Test Concepts

### LHP Header Detection

```python
# LHP-managed file (has header)
# Generated by LakehousePlumber - Bundle Resource

resources:
  pipelines:
    pipeline_name:
      ...

# User-managed file (no header)
resources:
  pipelines:
    pipeline_name:
      ...
```

### Python Function Patterns

**Embedded Pattern:**
- Function embedded in generated file
- Source: `py_functions/func.py` → `generated/pipeline/flowgroup.py`
- Change triggers: 1 file regeneration

**Copied Pattern:**
- Function copied to directory
- Source: `py_functions/func.py` → `generated/pipeline/custom_python_functions/func.py`
- Imported by: `generated/pipeline/flowgroup.py`
- Change triggers: 2 file regenerations

### State File Structure

```json
{
  "environments": {
    "dev": {
      "generated/dev/pipeline/file.py": {
        "checksum": "abc123...",
        "file_composite_checksum": "def456...",
        "file_dependencies": {
          "py_functions/func.py": {
            "checksum": "ghi789..."
          }
        },
        "source_yaml": "pipelines/bronze/flowgroup.yaml",
        "pipeline": "pipeline_name",
        "flowgroup": "flowgroup_name"
      }
    }
  }
}
```

---

## Troubleshooting

### Issue: Tests fail on Windows with file locking

**Solution:** Tests include Windows file-locking handling via `clean_logging` fixture

### Issue: Hash comparison fails but files look the same

**Solution:** Check for:
- Line ending differences (CRLF vs LF)
- Trailing whitespace
- Generated timestamps
- Non-deterministic ordering

### Issue: Test leaves behind temp directories

**Solution:** Check that:
- `isolated_project` fixture is used
- `original_cwd` is restored
- No exceptions during cleanup

### Issue: Baseline files don't match generated files

**Solution:**
1. Regenerate baselines: `lhp generate --env dev`
2. Copy to baseline directory
3. Commit new baselines if legitimate change

---

## Best Practices

✅ **DO:**
- Use `isolated_project` fixture for test isolation
- Compare files using hashes, not content strings
- Include descriptive assertion messages
- Clean up after tests
- Use helper methods for common operations

❌ **DON'T:**
- Modify fixture files directly
- Rely on test execution order
- Use hardcoded paths
- Skip cleanup in finally blocks
- Create tests that depend on external state

---

**Last Updated:** November 24, 2025  
**Version:** 1.0

