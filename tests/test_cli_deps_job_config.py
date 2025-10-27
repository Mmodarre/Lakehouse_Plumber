"""Integration tests for deps command with custom job config."""

import pytest
from pathlib import Path
from click.testing import CliRunner
import yaml

from lhp.cli.main import cli


def test_deps_command_with_default_config(tmp_path):
    """Test lhp deps --format job uses defaults."""
    runner = CliRunner()
    
    with runner.isolated_filesystem(temp_dir=tmp_path):
        # Create minimal project structure
        project_root = Path.cwd()
        
        # Create lhp.yaml
        (project_root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        
        # Create a simple pipeline YAML
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        pipeline_yaml = pipelines_dir / "test.yaml"
        pipeline_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source_type: delta
    source: test_table
  - name: test_write
    type: write
    write_target:
      target_type: streaming_table
      target: output_table
""")
        
        # Run deps command
        result = runner.invoke(cli, ["deps", "--format", "job", "--job-name", "test_job"])
        
        # Check command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"
        
        # Check job file was created in default location
        job_file = project_root / ".lhp" / "dependencies" / "test_job.job.yml"
        assert job_file.exists(), f"Job file not found at {job_file}"
        
        # Verify default config values are in the job
        with open(job_file) as f:
            job_data = yaml.safe_load(f)
        
        assert job_data["resources"]["jobs"]["test_job"]["max_concurrent_runs"] == 1
        assert job_data["resources"]["jobs"]["test_job"]["performance_target"] == "STANDARD"


def test_deps_command_with_custom_config_file(tmp_path):
    """Test lhp deps --format job --job-config loads custom config."""
    runner = CliRunner()
    
    with runner.isolated_filesystem(temp_dir=tmp_path):
        project_root = Path.cwd()
        
        # Create lhp.yaml
        (project_root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        
        # Create custom job config
        custom_config = project_root / "custom_job_config.yaml"
        custom_config.write_text("""
max_concurrent_runs: 5
performance_target: PERFORMANCE_OPTIMIZED
timeout_seconds: 7200
""")
        
        # Create a simple pipeline YAML
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        pipeline_yaml = pipelines_dir / "test.yaml"
        pipeline_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source_type: delta
    source: test_table
  - name: test_write
    type: write
    write_target:
      target_type: streaming_table
      target: output_table
""")
        
        # Run deps command with custom config
        result = runner.invoke(cli, [
            "deps", "--format", "job", 
            "--job-name", "test_job",
            "--job-config", "custom_job_config.yaml"
        ])
        
        # Check command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"
        
        # Check job file was created
        job_file = project_root / ".lhp" / "dependencies" / "test_job.job.yml"
        assert job_file.exists()
        
        # Verify custom config values are in the job
        with open(job_file) as f:
            job_data = yaml.safe_load(f)
        
        assert job_data["resources"]["jobs"]["test_job"]["max_concurrent_runs"] == 5
        assert job_data["resources"]["jobs"]["test_job"]["performance_target"] == "PERFORMANCE_OPTIMIZED"
        assert job_data["resources"]["jobs"]["test_job"]["timeout_seconds"] == 7200


def test_deps_command_with_bundle_output(tmp_path):
    """Test lhp deps --format job --bundle-output saves to resources/."""
    runner = CliRunner()
    
    with runner.isolated_filesystem(temp_dir=tmp_path):
        project_root = Path.cwd()
        
        # Create lhp.yaml
        (project_root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        
        # Create a simple pipeline YAML
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        pipeline_yaml = pipelines_dir / "test.yaml"
        pipeline_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source_type: delta
    source: test_table
  - name: test_write
    type: write
    write_target:
      target_type: streaming_table
      target: output_table
""")
        
        # Run deps command with bundle output
        result = runner.invoke(cli, [
            "deps", "--format", "job",
            "--job-name", "test_job",
            "--bundle-output"
        ])
        
        # Check command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"
        
        # Check job file was created in resources/ directory
        job_file = project_root / "resources" / "test_job.job.yml"
        assert job_file.exists(), f"Job file not found at {job_file}"
        
        # Verify it's a valid job YAML
        with open(job_file) as f:
            job_data = yaml.safe_load(f)
        
        assert "resources" in job_data
        assert "jobs" in job_data["resources"]


def test_deps_command_with_both_options(tmp_path):
    """Test lhp deps with both --job-config and --bundle-output."""
    runner = CliRunner()
    
    with runner.isolated_filesystem(temp_dir=tmp_path):
        project_root = Path.cwd()
        
        # Create lhp.yaml
        (project_root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        
        # Create custom job config
        custom_config = project_root / "my_config.yaml"
        custom_config.write_text("""
max_concurrent_runs: 10
performance_target: PERFORMANCE_OPTIMIZED
tags:
  environment: production
""")
        
        # Create a simple pipeline YAML
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        pipeline_yaml = pipelines_dir / "test.yaml"
        pipeline_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source_type: delta
    source: test_table
  - name: test_write
    type: write
    write_target:
      target_type: streaming_table
      target: output_table
""")
        
        # Run deps command with both options
        result = runner.invoke(cli, [
            "deps", "--format", "job",
            "--job-name", "prod_job",
            "--job-config", "my_config.yaml",
            "--bundle-output"
        ])
        
        # Check command succeeded
        assert result.exit_code == 0, f"Command failed: {result.output}"
        
        # Check job file was created in resources/
        job_file = project_root / "resources" / "prod_job.job.yml"
        assert job_file.exists()
        
        # Verify custom config was applied
        with open(job_file) as f:
            job_data = yaml.safe_load(f)
        
        assert job_data["resources"]["jobs"]["prod_job"]["max_concurrent_runs"] == 10
        assert job_data["resources"]["jobs"]["prod_job"]["tags"]["environment"] == "production"


def test_deps_command_fails_with_missing_config_file(tmp_path):
    """Test clear error when specified config file doesn't exist."""
    runner = CliRunner()
    
    with runner.isolated_filesystem(temp_dir=tmp_path):
        project_root = Path.cwd()
        
        # Create lhp.yaml
        (project_root / "lhp.yaml").write_text("name: test_project\nversion: '1.0'\n")
        
        # Create a simple pipeline YAML
        pipelines_dir = project_root / "pipelines"
        pipelines_dir.mkdir()
        pipeline_yaml = pipelines_dir / "test.yaml"
        pipeline_yaml.write_text("""
flowgroup: test_flowgroup
pipeline: test_pipeline
actions:
  - name: test_action
    type: load
    source_type: delta
    source: test_table
  - name: test_write
    type: write
    write_target:
      target_type: streaming_table
      target: output_table
""")
        
        # Run deps command with non-existent config file
        result = runner.invoke(cli, [
            "deps", "--format", "job",
            "--job-config", "nonexistent.yaml"
        ])
        
        # Should fail
        assert result.exit_code != 0
        assert "nonexistent.yaml" in result.output or "not found" in result.output.lower()

