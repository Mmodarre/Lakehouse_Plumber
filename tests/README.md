# LakehousePlumber Test Suite

This comprehensive test suite follows the patterns established by BurrowBuilder while testing all requirements from the LakehousePlumber Requirements Document.

## Test Structure

### 1. Integration Tests (`test_integration.py`)
Core end-to-end tests based on requirements examples:
- **Bronze Ingestion Pattern**: CloudFiles → Operational Metadata → Streaming Table
- **JDBC with Secrets**: Database connections with secret management
- **CDC Silver Layer**: Delta CDC → Transformations → SCD Type 2
- **Template Usage**: Template expansion and parameter substitution
- **Data Quality Expectations**: DLT expectations integration

### 2. Advanced Features Tests (`test_advanced_features.py`)
Tests for complex scenarios and edge cases:
- **Python Sources & Transforms**: Python module integration
- **Temp Tables**: Temporary streaming table creation
- **Schema Transforms**: Type casting and column mapping
- **Many-to-Many Relationships**: Complex action dependencies
- **Operational Metadata**: Different configuration methods
- **Error Handling**: Invalid configurations, circular dependencies
- **Preset Inheritance**: Multi-level preset chains

### 3. CLI Tests (`test_cli_comprehensive.py`)
Comprehensive command-line interface testing:
- **Project Management**: init, validate, generate
- **Listing Commands**: list-presets, list-templates
- **Information Commands**: info, stats, show
- **Environment Support**: dev/staging/prod configurations
- **Secret Validation**: Automatic secret reference checking
- **Dry Run & Formatting**: Preview and code formatting options

### 4. Performance Tests (`test_performance.py`)
Stress and performance testing:
- **100+ FlowGroups**: Generate in < 10 seconds
- **Memory Usage**: < 500MB for large projects
- **Complex Dependencies**: Fast resolution algorithms
- **Large SQL**: Handle complex business logic queries
- **1000 Actions**: Extreme stress test

### 5. Existing Tests
The project already includes specific tests for:
- Substitution management
- Template engine
- Action registry
- Operational metadata
- DQE parsing
- Individual generators

## Running Tests

### Run All Tests
```bash
pytest tests/ -v
```

### Run Specific Test Categories
```bash
# Integration tests only
pytest tests/test_integration.py -v

# CLI tests only
pytest tests/test_cli_comprehensive.py -v

# Performance tests (excluding slow tests)
pytest tests/test_performance.py -v -m "not slow"

# All tests including slow ones
pytest tests/test_performance.py -v
```

### Run with Coverage
```bash
pytest tests/ --cov=lhp --cov-report=html --cov-report=term
```

### Run Parallel Tests
```bash
pytest tests/ -n auto
```

## Test Requirements Coverage

### Core Concepts ✅
- [x] Pipeline → DLT pipeline mapping
- [x] FlowGroup collections
- [x] Load/Transform/Write actions
- [x] Action relationships and dependencies

### Architecture ✅
- [x] Preset system with inheritance
- [x] Template system with parameters
- [x] Environment management
- [x] Secret substitution

### Configuration Examples ✅
All examples from requirements are tested:
- [x] Bronze layer preset
- [x] Bronze ingestion template
- [x] Customer ingestion flowgroup
- [x] JDBC with secrets
- [x] CDC silver layer

### CLI Interface ✅
All commands tested:
- [x] `lhp init`
- [x] `lhp validate`
- [x] `lhp generate`
- [x] `lhp list-presets`
- [x] `lhp list-templates`
- [x] `lhp show`
- [x] `lhp info`
- [x] `lhp stats`

### Success Criteria ✅
- [x] Generate valid DLT Python code
- [x] Support all major DLT features
- [x] Secure secret integration
- [x] Clear error messages
- [x] Template system
- [x] Performance requirements

## Test Data

Test fixtures create realistic project structures with:
- Multiple pipelines and flowgroups
- Complex action dependencies
- Real-world SQL transformations
- Secret references
- Preset inheritance chains
- Template usage

## Continuous Integration

Recommended CI configuration:

```yaml
# .github/workflows/tests.yml
name: Tests

on: [push, pull_request]

jobs:
  test:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        python-version: [3.8, 3.9, "3.10", "3.11"]
    
    steps:
    - uses: actions/checkout@v3
    - name: Set up Python
      uses: actions/setup-python@v4
      with:
        python-version: ${{ matrix.python-version }}
    - name: Install dependencies
      run: |
        pip install -e .
        pip install pytest pytest-cov pytest-xdist
    - name: Run tests
      run: |
        pytest tests/ -v --cov=lhp --cov-report=xml
    - name: Upload coverage
      uses: codecov/codecov-action@v3
```

## Adding New Tests

When adding new features:

1. **Unit Test**: Test the component in isolation
2. **Integration Test**: Test with other components
3. **CLI Test**: Add command support if applicable
4. **Performance Test**: Consider scalability impact
5. **Documentation**: Update this README

## Test Utilities

Common test patterns:

```python
# Create temporary project
with tempfile.TemporaryDirectory() as tmpdir:
    project_root = Path(tmpdir)
    # ... setup project structure

# Create CLI runner
runner = CliRunner()
result = runner.invoke(cli, ['command', '--flag'])

# Assert code generation
assert "@dlt.view()" in generated_code
assert "expected_content" in generated_code

# Test error handling
with pytest.raises(ValueError, match="expected error"):
    orchestrator.generate_pipeline("invalid", "dev")
```

## Performance Benchmarks

Current benchmarks (target values):
- Generate 100 flowgroups: < 10 seconds
- Memory usage (large project): < 500MB
- Complex dependency resolution: < 2 seconds
- 1000 actions in flowgroup: < 30 seconds

## Known Issues

- The `show` command is not yet implemented
- Some Phase 1 CLI tests may be outdated
- Performance tests require `psutil` package 