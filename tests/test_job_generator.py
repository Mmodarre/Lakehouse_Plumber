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
        project_root=project_root, config_file_path="custom_job_config.yaml"
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
        metadata={},
    )

    # Create pipeline dependency
    pipeline_dep = PipelineDependency(
        pipeline="test_pipeline",
        depends_on=[],
        flowgroup_count=1,
        action_count=1,
        external_sources=[],
        stage=1,
    )

    return DependencyAnalysisResult(
        graphs=graphs,
        pipeline_dependencies={"test_pipeline": pipeline_dep},
        execution_stages=[["test_pipeline"]],
        circular_dependencies=[],
        external_sources=[],
    )


def test_generate_job_with_default_config(tmp_path):
    """Generated job should have default values when no custom config."""
    project_root = tmp_path / "project"
    project_root.mkdir()

    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )

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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
    job_data = yaml.safe_load(job_yaml)

    assert (
        job_data["resources"]["jobs"]["test_job"]["performance_target"]
        == "PERFORMANCE_OPTIMIZED"
    )


def test_generate_job_with_queue_disabled(tmp_path):
    """Can disable queue via config."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)

    config_file = templates_dir / "job_config.yaml"
    config_file.write_text("queue:\n  enabled: false\n")

    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
    job_data = yaml.safe_load(job_yaml)

    assert "tags" in job_data["resources"]["jobs"]["test_job"]
    assert (
        job_data["resources"]["jobs"]["test_job"]["tags"]["environment"] == "production"
    )
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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
    job_data = yaml.safe_load(job_yaml)

    assert "email_notifications" in job_data["resources"]["jobs"]["test_job"]
    assert (
        "team@example.com"
        in job_data["resources"]["jobs"]["test_job"]["email_notifications"][
            "on_failure"
        ]
    )


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

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )
    job_data = yaml.safe_load(job_yaml)

    assert "schedule" in job_data["resources"]["jobs"]["test_job"]
    assert (
        job_data["resources"]["jobs"]["test_job"]["schedule"]["quartz_cron_expression"]
        == "0 0 8 * * ?"
    )
    assert (
        job_data["resources"]["jobs"]["test_job"]["schedule"]["timezone_id"]
        == "America/New_York"
    )


def test_generate_job_preserves_commented_examples(tmp_path):
    """Commented example section remains in output."""
    project_root = tmp_path / "project"
    project_root.mkdir()

    generator = JobGenerator(project_root=project_root)
    result = create_test_dependency_result()

    job_yaml = generator.generate_job(
        result, job_name="test_job", project_name="test_project"
    )

    # Check that commented examples are present
    assert (
        "# Additional job configuration options" in job_yaml
        or "# Enable job-level timeout" in job_yaml
    )


# =====================================================================
# Pass-through tests: unknown job_config keys render as YAML verbatim.
#
# This is the V0.8.7 escape hatch — users can specify any Databricks
# Jobs API field (trigger.file_arrival, continuous, run_as, git_source,
# health, environments, parameters, …) without waiting for LHP to
# explicitly support the key, so LHP never blocks a user from using
# newly-released Databricks job features.
# =====================================================================


def _write_config(tmp_path, body: str):
    """Helper: scaffold a project + job_config.yaml at the default path."""
    project_root = tmp_path / "project"
    templates_dir = project_root / "templates" / "bundle"
    templates_dir.mkdir(parents=True)
    (templates_dir / "job_config.yaml").write_text(body)
    return project_root


def test_passthrough_trigger_file_arrival_renders_in_job_yaml(tmp_path):
    """The user's reported bug: trigger.file_arrival now lands in output."""
    project_root = _write_config(
        tmp_path,
        """
trigger:
  file_arrival:
    url: "s3://my-bucket/landing/"
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 30
  pause_status: UNPAUSED
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )
    job_data = yaml.safe_load(job_yaml)
    trigger = job_data["resources"]["jobs"]["test_job"]["trigger"]

    assert trigger["file_arrival"]["url"] == "s3://my-bucket/landing/"
    assert trigger["file_arrival"]["min_time_between_triggers_seconds"] == 60
    assert trigger["file_arrival"]["wait_after_last_change_seconds"] == 30
    assert trigger["pause_status"] == "UNPAUSED"


def test_passthrough_continuous_mode(tmp_path):
    """Databricks `continuous` mode is a top-level dict — must pass through."""
    project_root = _write_config(
        tmp_path,
        """
continuous:
  pause_status: PAUSED
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )
    job_data = yaml.safe_load(job_yaml)

    assert (
        job_data["resources"]["jobs"]["test_job"]["continuous"]["pause_status"]
        == "PAUSED"
    )


def test_passthrough_run_as_service_principal(tmp_path):
    """run_as also passes through (common security requirement)."""
    project_root = _write_config(
        tmp_path,
        """
run_as:
  service_principal_name: "my-sp-uuid"
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )
    job_data = yaml.safe_load(job_yaml)

    assert (
        job_data["resources"]["jobs"]["test_job"]["run_as"]["service_principal_name"]
        == "my-sp-uuid"
    )


def test_passthrough_unknown_scalar_key(tmp_path):
    """Scalar-valued unknown keys (ints, strings, bools) also pass through."""
    project_root = _write_config(
        tmp_path,
        """
budget_policy_id: "policy-42"
max_tasks_per_worker: 4
edit_mode: EDITABLE
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )
    job_data = yaml.safe_load(job_yaml)
    job = job_data["resources"]["jobs"]["test_job"]

    assert job["budget_policy_id"] == "policy-42"
    assert job["max_tasks_per_worker"] == 4
    assert job["edit_mode"] == "EDITABLE"


def test_passthrough_does_not_duplicate_known_keys(tmp_path):
    """Keys already rendered explicitly by the template (schedule, tags,
    queue, timeout_seconds, permissions, email_notifications,
    webhook_notifications, performance_target, max_concurrent_runs) must
    NOT be emitted a second time by the pass-through loop."""
    project_root = _write_config(
        tmp_path,
        """
schedule:
  quartz_cron_expression: "0 0 8 * * ?"
  timezone_id: "America/New_York"
  pause_status: UNPAUSED
tags:
  env: prod
timeout_seconds: 3600
trigger:
  file_arrival:
    url: "s3://bucket/prefix/"
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )

    # Count literal key occurrences in the rendered text. Each known key
    # should appear exactly once as a top-level block. `trigger` appears
    # once via pass-through. Count "schedule:" lines to catch duplicates.
    for known_key in ("schedule:", "tags:", "timeout_seconds:"):
        count = job_yaml.count(f"\n      {known_key}")
        assert count == 1, f"{known_key!r} rendered {count} times (expected 1)"

    # Pass-through emits trigger once.
    assert job_yaml.count("\n      trigger:") == 1


def test_passthrough_does_not_emit_internal_control_keys(tmp_path):
    """LHP-internal control keys (generate_master_job, master_job_name) must
    never appear in the rendered YAML, even though they live on job_config."""
    project_root = _write_config(
        tmp_path,
        """
generate_master_job: false
master_job_name: some_custom_master
trigger:
  file_arrival:
    url: "s3://bucket/"
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )

    assert "generate_master_job" not in job_yaml
    assert "master_job_name" not in job_yaml
    # trigger still renders via pass-through.
    assert "trigger:" in job_yaml


def test_passthrough_preserves_author_key_order(tmp_path):
    """YAML dump with sort_keys=False keeps the order the user wrote."""
    project_root = _write_config(
        tmp_path,
        """
trigger:
  file_arrival:
    url: "s3://bucket/"
    min_time_between_triggers_seconds: 60
    wait_after_last_change_seconds: 30
  pause_status: UNPAUSED
""",
    )

    generator = JobGenerator(project_root=project_root)
    job_yaml = generator.generate_job(
        create_test_dependency_result(),
        job_name="test_job",
        project_name="test_project",
    )

    # The three file_arrival subkeys must appear in the order the user wrote.
    url_idx = job_yaml.index("url:")
    min_idx = job_yaml.index("min_time_between_triggers_seconds:")
    wait_idx = job_yaml.index("wait_after_last_change_seconds:")
    assert url_idx < min_idx < wait_idx


def test_dict_to_yaml_filter_directly():
    """Sanity check the _dict_to_yaml helper in isolation."""
    from lhp.core.services.job_generator import _dict_to_yaml

    result = _dict_to_yaml({"trigger": {"file_arrival": {"url": "s3://x/"}}})

    # No trailing newline (so indent filter composes cleanly).
    assert not result.endswith("\n")
    # Standard 2-space block indent.
    assert result == "trigger:\n  file_arrival:\n    url: s3://x/"
