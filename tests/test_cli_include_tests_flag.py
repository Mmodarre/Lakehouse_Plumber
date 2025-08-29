"""Tests for CLI --include-tests flag functionality."""

import pytest
from click.testing import CliRunner
from pathlib import Path
import tempfile
from lhp.cli.main import cli


class TestCLIIncludeTestsFlag:
    """Test CLI --include-tests flag functionality."""
    
    @pytest.fixture
    def runner(self):
        """Create CLI runner."""
        return CliRunner()
    
    @pytest.fixture
    def temp_project(self):
        """Create a temporary project directory with full structure."""
        with tempfile.TemporaryDirectory() as tmpdir:
            project_root = Path(tmpdir)
            
            # Create full project structure matching working tests
            directories = [
                'presets', 'templates', 'pipelines', 'substitutions',
                'schemas', 'expectations', 'generated'
            ]
            
            for dir_name in directories:
                (project_root / dir_name).mkdir(parents=True)
            
            # Create project config (flat structure like working tests)
            (project_root / "lhp.yaml").write_text("""
name: test_cli_project
version: "1.0"
description: "Test project for CLI include-tests flag"
""")
            
            # Create substitutions matching working pattern
            (project_root / "substitutions" / "test.yaml").write_text("""
test:
  env: test
  catalog: test_catalog
  bronze_schema: bronze
  silver_schema: silver

secrets:
  default_scope: test_secrets
""")
            
            # Create pipeline directory structure
            pipeline_dir = project_root / "pipelines" / "test_pipeline"
            pipeline_dir.mkdir(parents=True)
            
            # Create mixed flowgroup (has both test and non-test actions)
            (pipeline_dir / "mixed_flowgroup.yaml").write_text("""
pipeline: test_pipeline
flowgroup: mixed_flowgroup

actions:
  - name: load_data
    type: load
    source:
      type: sql
      sql: "SELECT 1 as id"
    target: v_data

  - name: write_data
    type: write
    source: v_data
    write_target:
      type: streaming_table
      database: "{catalog}.{bronze_schema}"
      table: test_table
      create_table: true

  - name: test_data_quality
    type: test
    test_type: row_count
    source: ["v_data", "v_data"]
    tolerance: 0
    on_violation: fail
    description: "Test data quality"
""")
            
            # Create test-only pipeline
            test_pipeline_dir = project_root / "pipelines" / "test_only_pipeline"
            test_pipeline_dir.mkdir(parents=True)
            
            (test_pipeline_dir / "test_only_flowgroup.yaml").write_text("""
pipeline: test_only_pipeline  
flowgroup: test_only_flowgroup

actions:
  - name: test_data_quality_only
    type: test
    test_type: uniqueness
    source: some_table
    columns: ["id"]
    on_violation: fail
    description: "Test only pipeline"
""")
            
            yield project_root
    
    def test_cli_help_shows_include_tests_flag(self, runner):
        """Test that CLI help shows --include-tests flag."""
        result = runner.invoke(cli, ['generate', '--help'])
        
        # Should show the include-tests flag in help
        assert '--include-tests' in result.output
        assert 'Include test actions in generation' in result.output
    
    def test_cli_accepts_include_tests_flag(self, runner, temp_project):
        """Test that CLI generate command accepts --include-tests flag."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            # Test that the flag is accepted without error
            result = runner.invoke(cli, [
                'generate', 
                '--env', 'test',
                '--include-tests',
                '--dry-run'
            ])
            
            # Should not fail due to unknown flag
            assert result.exit_code == 0 or "No such option" not in result.output
    
    def test_cli_default_behavior_skips_tests(self, runner, temp_project):
        """Test that CLI generate skips tests by default (no flag)."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            # Run without --include-tests flag
            result = runner.invoke(cli, [
                'generate', 
                '--env', 'test',
                '--dry-run'
            ])
            
            # Should succeed
            assert result.exit_code == 0, f"CLI failed: {result.output}"
            
            # Should not mention test-only pipeline files in output
            # (test_only_pipeline should be skipped entirely)
            assert "test_only_pipeline" not in result.output or "Would generate 0 file(s)" in result.output
    
    def test_cli_with_flag_includes_tests(self, runner, temp_project):
        """Test that CLI generate includes tests when --include-tests flag is present."""
        with runner.isolated_filesystem():
            import os
            os.chdir(str(temp_project))
            
            # Run with --include-tests flag
            result = runner.invoke(cli, [
                'generate', 
                '--env', 'test',
                '--include-tests',
                '--dry-run'
            ])
            
            # Should succeed
            assert result.exit_code == 0, f"CLI failed: {result.output}"
            
            # Should mention test-related content in output (test_only_pipeline should be included)
            assert "test_only_pipeline" in result.output or "DATA QUALITY TESTS" in result.output
