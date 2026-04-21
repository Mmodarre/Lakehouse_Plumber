"""Unit tests for the simplified JobGenerator monitoring-job API.

Covers:
  * ``JobGenerator.resolve_monitoring_job_config`` — deep-merges raw dict over
    ``DEFAULT_JOB_CONFIG`` without touching callers.
  * ``JobGenerator.generate_monitoring_job`` — renders the monitoring-job Jinja
    template against the caller-supplied ``job_config`` dict (no alias lookup,
    no monitoring_job_name member state).
"""

import pytest
import yaml

from lhp.core.services.job_generator import JobGenerator


@pytest.mark.unit
class TestResolveMonitoringJobConfig:
    """Tests for JobGenerator.resolve_monitoring_job_config classmethod."""

    def test_empty_input_returns_defaults(self):
        resolved = JobGenerator.resolve_monitoring_job_config({})
        assert resolved["max_concurrent_runs"] == 1
        assert resolved["performance_target"] == "STANDARD"
        assert resolved["queue"] == {"enabled": True}

    def test_none_input_returns_defaults(self):
        """None is tolerated (empty file case)."""
        resolved = JobGenerator.resolve_monitoring_job_config(None)
        assert resolved["max_concurrent_runs"] == 1

    def test_scalar_override(self):
        resolved = JobGenerator.resolve_monitoring_job_config(
            {"max_concurrent_runs": 4, "performance_target": "PERFORMANCE_OPTIMIZED"}
        )
        assert resolved["max_concurrent_runs"] == 4
        assert resolved["performance_target"] == "PERFORMANCE_OPTIMIZED"

    def test_nested_deep_merge_tags(self):
        """Nested dicts (tags, queue) merge recursively."""
        resolved = JobGenerator.resolve_monitoring_job_config(
            {"tags": {"team": "data-platform"}, "queue": {"enabled": False}}
        )
        assert resolved["queue"] == {"enabled": False}
        assert resolved["tags"] == {"team": "data-platform"}

    def test_new_keys_preserved(self):
        resolved = JobGenerator.resolve_monitoring_job_config(
            {"timeout_seconds": 3600, "schedule": {"quartz_cron_expression": "0 * * * * ?"}}
        )
        assert resolved["timeout_seconds"] == 3600
        assert resolved["schedule"]["quartz_cron_expression"] == "0 * * * * ?"

    def test_does_not_mutate_default(self):
        JobGenerator.resolve_monitoring_job_config({"max_concurrent_runs": 9})
        # DEFAULT_JOB_CONFIG must still be pristine
        assert JobGenerator.DEFAULT_JOB_CONFIG["max_concurrent_runs"] == 1


@pytest.mark.unit
class TestGenerateMonitoringJobSimplified:
    """Tests for the simplified ``generate_monitoring_job`` signature."""

    def test_renders_with_minimal_job_config(self):
        """New signature: ``job_config`` passed explicitly, no alias lookup."""
        gen = JobGenerator()
        job_cfg = JobGenerator.resolve_monitoring_job_config(
            {"tags": {"purpose": "monitor"}}
        )
        rendered = gen.generate_monitoring_job(
            pipeline_name="my_monitor",
            notebook_path="/Workspace/x/union_event_logs",
            job_name="my_monitor_job",
            job_config=job_cfg,
            has_pipeline=True,
        )
        assert "my_monitor_job" in rendered
        assert "union_event_logs" in rendered
        assert "purpose" in rendered  # tag flowed through
        # Parses as valid YAML
        parsed = yaml.safe_load(rendered)
        assert parsed["resources"]["jobs"]["my_monitor_job"]["name"] == "my_monitor_job"
        assert (
            parsed["resources"]["jobs"]["my_monitor_job"]["max_concurrent_runs"] == 1
        )

    def test_has_pipeline_false_emits_notebook_only_job(self):
        gen = JobGenerator()
        job_cfg = JobGenerator.resolve_monitoring_job_config({})
        rendered = gen.generate_monitoring_job(
            pipeline_name="my_monitor",
            notebook_path="/Workspace/x/union_event_logs",
            job_name="my_monitor_job",
            job_config=job_cfg,
            has_pipeline=False,
        )
        parsed = yaml.safe_load(rendered)
        tasks = parsed["resources"]["jobs"]["my_monitor_job"]["tasks"]
        assert len(tasks) == 1
        assert tasks[0]["task_key"] == "union_event_logs"

    def test_has_pipeline_true_emits_pipeline_task(self):
        gen = JobGenerator()
        job_cfg = JobGenerator.resolve_monitoring_job_config({})
        rendered = gen.generate_monitoring_job(
            pipeline_name="my_monitor",
            notebook_path="/Workspace/x/union_event_logs",
            job_name="my_monitor_job",
            job_config=job_cfg,
            has_pipeline=True,
        )
        parsed = yaml.safe_load(rendered)
        tasks = parsed["resources"]["jobs"]["my_monitor_job"]["tasks"]
        assert len(tasks) == 2
        assert {t["task_key"] for t in tasks} == {
            "union_event_logs",
            "my_monitor_pipeline",
        }

    def test_caller_controls_everything_no_alias_lookup(self):
        """Regression guard: JobGenerator must no longer pull monitoring config
        out of templates/bundle/job_config.yaml via the __eventlog_monitoring
        alias. The job_config argument is the sole source of truth."""
        gen = JobGenerator()
        # Intentionally pass an override for performance_target. The rendered
        # output must reflect the caller's value — not DEFAULT or some loaded
        # file — confirming the caller-owned config path.
        job_cfg = JobGenerator.resolve_monitoring_job_config(
            {"performance_target": "PERFORMANCE_OPTIMIZED"}
        )
        rendered = gen.generate_monitoring_job(
            pipeline_name="m",
            notebook_path="/p",
            job_name="m_job",
            job_config=job_cfg,
            has_pipeline=False,
        )
        assert "performance_target: PERFORMANCE_OPTIMIZED" in rendered


@pytest.mark.unit
class TestJobGeneratorNoMonitoringAliasAttrs:
    """Regression guards: the monitoring-alias surface is gone from JobGenerator."""

    def test_no_monitoring_job_name_param(self):
        """Constructor no longer accepts ``monitoring_job_name``."""
        with pytest.raises(TypeError):
            JobGenerator(monitoring_job_name="some_job")  # type: ignore[call-arg]

    def test_no_monitoring_job_alias_constant(self):
        assert not hasattr(JobGenerator, "MONITORING_JOB_ALIAS")

    def test_no_resolve_monitoring_alias_method(self):
        assert not hasattr(JobGenerator, "_resolve_monitoring_alias")
