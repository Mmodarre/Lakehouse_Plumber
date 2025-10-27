"""Tests for JobGenerator service."""

import pytest
from pathlib import Path
import yaml
from lhp.core.services.job_generator import JobGenerator
from lhp.models.dependencies import (
    DependencyAnalysisResult,
    DependencyGraphs,
    PipelineDependency,
)
import networkx as nx


# ============================================================================
# Config Loading Tests
# ============================================================================


def test_load_default_config_when_no_user_config(tmp_path):
    """Should use DEFAULT_JOB_CONFIG when no user config file exists."""
    # Create a temp project without config file
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    generator = JobGenerator(project_root=project_root)
    
    # Should have default values
    assert generator.job_config["max_concurrent_runs"] == 1
    assert generator.job_config["queue"]["enabled"] is True
    assert generator.job_config["performance_target"] == "STANDARD"


def test_load_user_config_merges_with_defaults(tmp_path):
    """Should merge user config with defaults, user values override."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    # Create user config that overrides some defaults
    config_content = """
max_concurrent_runs: 3
performance_target: PERFORMANCE_OPTIMIZED
"""
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text(config_content)
    
    generator = JobGenerator(project_root=project_root)
    
    # User values should override
    assert generator.job_config["max_concurrent_runs"] == 3
    assert generator.job_config["performance_target"] == "PERFORMANCE_OPTIMIZED"
    # Defaults should still be present for non-overridden values
    assert generator.job_config["queue"]["enabled"] is True


def test_load_user_config_adds_new_keys(tmp_path):
    """User can add keys not in defaults (email_notifications, schedule, etc.)."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    # Create user config with optional fields
    config_content = """
max_concurrent_runs: 2
timeout_seconds: 7200
tags:
  environment: production
email_notifications:
  on_failure:
    - team@example.com
"""
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text(config_content)
    
    generator = JobGenerator(project_root=project_root)
    
    # Should have optional fields
    assert "timeout_seconds" in generator.job_config
    assert generator.job_config["timeout_seconds"] == 7200
    assert "tags" in generator.job_config
    assert generator.job_config["tags"]["environment"] == "production"
    assert "email_notifications" in generator.job_config


def test_load_config_raises_error_when_specified_file_not_found(tmp_path):
    """Should raise clear error when --job-config points to non-existent file."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    with pytest.raises(FileNotFoundError) as exc_info:
        JobGenerator(project_root=project_root, config_file_path="nonexistent.yaml")
    
    assert "nonexistent.yaml" in str(exc_info.value)


def test_load_config_raises_error_on_invalid_yaml(tmp_path):
    """Should raise clear error when config file has invalid YAML syntax."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    # Create invalid YAML
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("invalid: yaml:\n  - wrong indentation")
    
    with pytest.raises(yaml.YAMLError):
        JobGenerator(project_root=project_root)


def test_load_config_with_empty_file(tmp_path):
    """Empty config file should return defaults only."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    # Create empty config
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("# Empty config\n")
    
    generator = JobGenerator(project_root=project_root)
    
    # Should have default values
    assert generator.job_config["max_concurrent_runs"] == 1
    assert generator.job_config["queue"]["enabled"] is True
    assert generator.job_config["performance_target"] == "STANDARD"


def test_load_config_with_custom_path(tmp_path):
    """Should load config from custom path specified via config_file_path."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    # Create config in custom location
    custom_config = project_root / "custom_job_config.yaml"
    custom_config.write_text("max_concurrent_runs: 10\n")
    
    generator = JobGenerator(
        project_root=project_root,
        config_file_path="custom_job_config.yaml"
    )
    
    assert generator.job_config["max_concurrent_runs"] == 10


# ============================================================================
# Job Generation Tests
# ============================================================================


def create_test_dependency_result():
    """Helper to create a minimal DependencyAnalysisResult for testing."""
    # Create minimal graphs
    action_graph = nx.DiGraph()
    flowgroup_graph = nx.DiGraph()
    pipeline_graph = nx.DiGraph()
    pipeline_graph.add_node("test_pipeline")
    
    graphs = DependencyGraphs(
        action_graph=action_graph,
        flowgroup_graph=flowgroup_graph,
        pipeline_graph=pipeline_graph,
        metadata={}
    )
    
    # Create pipeline dependency
    pipeline_dep = PipelineDependency(
        pipeline="test_pipeline",
        depends_on=[],
        flowgroup_count=1,
        action_count=1,
        external_sources=[],
        stage=1
    )
    
    return DependencyAnalysisResult(
        graphs=graphs,
        pipeline_dependencies={"test_pipeline": pipeline_dep},
        execution_stages=[["test_pipeline"]],
        circular_dependencies=[],
        external_sources=[]
    )


def test_generate_job_with_default_config(tmp_path):
    """Generated job should have default values when no custom config."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    
    # Parse the generated YAML
    job_data = yaml.safe_load(job_yaml)
    
    # Check default values
    assert job_data["resources"]["jobs"]["test_job"]["max_concurrent_runs"] == 1
    assert job_data["resources"]["jobs"]["test_job"]["performance_target"] == "STANDARD"
    assert job_data["resources"]["jobs"]["test_job"]["queue"]["enabled"] is True


def test_generate_job_with_custom_max_concurrent_runs(tmp_path):
    """Custom max_concurrent_runs value appears in generated YAML."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("max_concurrent_runs: 5\n")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert job_data["resources"]["jobs"]["test_job"]["max_concurrent_runs"] == 5


def test_generate_job_with_custom_performance_target(tmp_path):
    """Custom performance_target value appears in generated YAML."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("performance_target: PERFORMANCE_OPTIMIZED\n")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert job_data["resources"]["jobs"]["test_job"]["performance_target"] == "PERFORMANCE_OPTIMIZED"


def test_generate_job_with_queue_disabled(tmp_path):
    """Can disable queue via config."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("queue:\n  enabled: false\n")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert job_data["resources"]["jobs"]["test_job"]["queue"]["enabled"] is False


def test_generate_job_with_optional_timeout(tmp_path):
    """Optional timeout_seconds appears when specified."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("timeout_seconds: 3600\n")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert "timeout_seconds" in job_data["resources"]["jobs"]["test_job"]
    assert job_data["resources"]["jobs"]["test_job"]["timeout_seconds"] == 3600


def test_generate_job_with_tags(tmp_path):
    """Tags section appears correctly when specified."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("""
tags:
  environment: production
  team: data-platform
""")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert "tags" in job_data["resources"]["jobs"]["test_job"]
    assert job_data["resources"]["jobs"]["test_job"]["tags"]["environment"] == "production"
    assert job_data["resources"]["jobs"]["test_job"]["tags"]["team"] == "data-platform"


def test_generate_job_with_email_notifications(tmp_path):
    """Email notifications appear correctly when specified."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("""
email_notifications:
  on_failure:
    - team@example.com
    - oncall@example.com
""")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert "email_notifications" in job_data["resources"]["jobs"]["test_job"]
    assert "team@example.com" in job_data["resources"]["jobs"]["test_job"]["email_notifications"]["on_failure"]


def test_generate_job_with_schedule(tmp_path):
    """Schedule configuration appears correctly when specified."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    
    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("""
schedule:
  quartz_cron_expression: "0 0 8 * * ?"
  timezone_id: "America/New_York"
  pause_status: UNPAUSED
""")
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    job_data = yaml.safe_load(job_yaml)
    
    assert "schedule" in job_data["resources"]["jobs"]["test_job"]
    assert job_data["resources"]["jobs"]["test_job"]["schedule"]["quartz_cron_expression"] == "0 0 8 * * ?"
    assert job_data["resources"]["jobs"]["test_job"]["schedule"]["timezone_id"] == "America/New_York"


def test_generate_job_preserves_commented_examples(tmp_path):
    """Commented example section remains in output."""
    project_root = tmp_path / "project"
    project_root.mkdir()
    
    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()
    
    job_yaml = generator.generate_job(result, job_name="test_job", project_name="test_project")
    
    # Check that commented examples are present
    assert "# Additional job configuration options" in job_yaml or "# Enable job-level timeout" in job_yaml

