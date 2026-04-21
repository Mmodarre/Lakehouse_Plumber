"""Unit tests for monitoring config parsing and validation in ProjectConfigLoader."""

import pytest
from pathlib import Path
from unittest.mock import patch

from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.models.config import EventLogConfig, MonitoringConfig
from lhp.utils.error_formatter import LHPError


# Path (relative to project root / tmp_path) used across tests as the monitoring
# job config location. The `loader` fixture creates this file so validation passes.
JOB_CFG_REL = "config/monitoring_job_config.yaml"


@pytest.fixture
def loader(tmp_path):
    """Create a ProjectConfigLoader with a minimal lhp.yaml and a valid
    monitoring job config file on disk so the required-file validation passes.
    """
    lhp_yaml = tmp_path / "lhp.yaml"
    lhp_yaml.write_text("name: test_project\n")
    job_cfg_file = tmp_path / JOB_CFG_REL
    job_cfg_file.parent.mkdir(parents=True, exist_ok=True)
    job_cfg_file.write_text(
        "max_concurrent_runs: 1\n"
        "performance_target: STANDARD\n"
        "queue:\n"
        "  enabled: true\n"
        "tags:\n"
        "  managed_by: lakehouse_plumber\n"
    )
    return ProjectConfigLoader(tmp_path)


@pytest.mark.unit
class TestParseMonitoringConfig:
    """Tests for _parse_monitoring_config method."""

    def test_empty_dict_all_defaults(self, loader):
        """monitoring: {} should produce all-default MonitoringConfig."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {"checkpoint_path": "/mnt/cp", "job_config_path": JOB_CFG_REL}, event_log
        )
        assert isinstance(config, MonitoringConfig)
        assert config.enabled is True
        assert config.pipeline_name is None
        assert config.streaming_table == "all_pipelines_event_log"
        assert config.checkpoint_path == "/mnt/cp"
        assert config.job_config_path == JOB_CFG_REL

    def test_none_treated_as_empty_dict(self, loader):
        """monitoring: (null) should also produce defaults."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {"checkpoint_path": "/mnt/cp", "job_config_path": JOB_CFG_REL}, event_log
        )
        assert config.enabled is True

    def test_disabled(self, loader):
        """monitoring: { enabled: false } should parse successfully."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config({"enabled": False}, event_log)
        assert config.enabled is False
        assert config.job_config_path is None

    def test_custom_fields(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {
                "pipeline_name": "my_monitor",
                "catalog": "override_cat",
                "schema": "_analytics",
                "streaming_table": "unified_events",
                "checkpoint_path": "/mnt/cp",
                "job_config_path": JOB_CFG_REL,
            },
            event_log,
        )
        assert config.pipeline_name == "my_monitor"
        assert config.catalog == "override_cat"
        assert config.schema_ == "_analytics"
        assert config.streaming_table == "unified_events"
        assert config.job_config_path == JOB_CFG_REL

    def test_with_materialized_views(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {
                "checkpoint_path": "/mnt/cp",
                "job_config_path": JOB_CFG_REL,
                "materialized_views": [
                    {"name": "summary", "sql": "SELECT 1"},
                    {"name": "errors", "sql_path": "queries/errors.sql"},
                ],
            },
            event_log,
        )
        assert len(config.materialized_views) == 2
        assert config.materialized_views[0].name == "summary"
        assert config.materialized_views[1].sql_path == "queries/errors.sql"

    def test_empty_materialized_views_list(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {
                "checkpoint_path": "/mnt/cp",
                "job_config_path": JOB_CFG_REL,
                "materialized_views": [],
            },
            event_log,
        )
        assert config.materialized_views == []

    def test_substitution_tokens_preserved(self, loader):
        event_log = EventLogConfig(
            enabled=True, catalog="${catalog}", schema="${schema}"
        )
        config = loader._parse_monitoring_config(
            {
                "catalog": "${catalog}",
                "schema": "${schema}",
                "checkpoint_path": "/mnt/cp",
                "job_config_path": JOB_CFG_REL,
            },
            event_log,
        )
        assert config.catalog == "${catalog}"
        assert config.schema_ == "${schema}"

    def test_invalid_type_raises_error(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="must be a mapping"):
            loader._parse_monitoring_config("invalid", event_log)

    def test_invalid_materialized_views_type(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="must be a list"):
            loader._parse_monitoring_config(
                {"materialized_views": "not_a_list"}, event_log
            )

    def test_invalid_mv_entry_type(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="must be a mapping"):
            loader._parse_monitoring_config(
                {"materialized_views": ["not_a_dict"]}, event_log
            )


@pytest.mark.unit
class TestValidateMonitoringConfig:
    """Tests for _validate_monitoring_config method."""

    def test_disabled_skips_validation(self, loader):
        """Disabled monitoring should not raise even without event_log or job_config_path."""
        config = MonitoringConfig(enabled=False)
        loader._validate_monitoring_config(config, None)

    def test_disabled_without_job_config_path_passes(self, loader):
        """enabled=False + missing job_config_path should NOT raise (required only when enabled)."""
        config = MonitoringConfig(
            enabled=False, checkpoint_path="/mnt/cp", job_config_path=None
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)

    def test_enabled_without_event_log_raises(self, loader):
        config = MonitoringConfig(enabled=True)
        with pytest.raises(LHPError, match="requires event_log"):
            loader._validate_monitoring_config(config, None)

    def test_enabled_with_disabled_event_log_raises(self, loader):
        config = MonitoringConfig(enabled=True)
        event_log = EventLogConfig(enabled=False)
        with pytest.raises(LHPError, match="requires event_log"):
            loader._validate_monitoring_config(config, event_log)

    def test_enabled_with_event_log_passes(self, loader):
        config = MonitoringConfig(
            enabled=True, checkpoint_path="/mnt/cp", job_config_path=JOB_CFG_REL
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)

    def test_missing_checkpoint_path_raises(self, loader):
        config = MonitoringConfig(enabled=True, job_config_path=JOB_CFG_REL)
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="checkpoint_path is required"):
            loader._validate_monitoring_config(config, event_log)

    def test_missing_job_config_path_raises(self, loader):
        """enabled=True + checkpoint_path set but job_config_path missing → error."""
        config = MonitoringConfig(
            enabled=True, checkpoint_path="/mnt/cp", job_config_path=None
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="job_config_path is required"):
            loader._validate_monitoring_config(config, event_log)

    def test_empty_job_config_path_raises(self, loader):
        """enabled=True + empty string job_config_path → error (treated same as missing)."""
        config = MonitoringConfig(
            enabled=True, checkpoint_path="/mnt/cp", job_config_path=""
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="job_config_path is required"):
            loader._validate_monitoring_config(config, event_log)

    def test_job_config_path_file_not_found_raises(self, loader):
        """enabled=True + job_config_path points to non-existent file → error."""
        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path="config/does_not_exist.yaml",
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="job_config file not found"):
            loader._validate_monitoring_config(config, event_log)

    def test_job_config_path_resolved_relative_to_project_root(self, loader):
        """Existing file at project_root / job_config_path should pass."""
        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path=JOB_CFG_REL,  # created by fixture under project_root
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)

    def test_tokenized_job_config_path_defers_existence_check(self, loader):
        """Paths containing ${...} tokens are validated later (once env is known)."""
        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path="config/monitoring_job_config_${env}.yaml",
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)

    def test_duplicate_mv_names_raises(self, loader):
        from lhp.models.config import MonitoringMaterializedViewConfig

        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path=JOB_CFG_REL,
            materialized_views=[
                MonitoringMaterializedViewConfig(name="summary", sql="SELECT 1"),
                MonitoringMaterializedViewConfig(name="summary", sql="SELECT 2"),
            ],
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="Duplicate materialized view name"):
            loader._validate_monitoring_config(config, event_log)

    def test_mv_with_both_sql_and_sql_path_raises(self, loader):
        from lhp.models.config import MonitoringMaterializedViewConfig

        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path=JOB_CFG_REL,
            materialized_views=[
                MonitoringMaterializedViewConfig(
                    name="summary", sql="SELECT 1", sql_path="q.sql"
                ),
            ],
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="Ambiguous materialized view SQL source"):
            loader._validate_monitoring_config(config, event_log)

    def test_mv_missing_name_raises(self, loader):
        from lhp.models.config import MonitoringMaterializedViewConfig

        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path=JOB_CFG_REL,
            materialized_views=[
                MonitoringMaterializedViewConfig(name="", sql="SELECT 1"),
            ],
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        with pytest.raises(LHPError, match="missing name"):
            loader._validate_monitoring_config(config, event_log)

    def test_unique_mv_names_pass(self, loader):
        from lhp.models.config import MonitoringMaterializedViewConfig

        config = MonitoringConfig(
            enabled=True,
            checkpoint_path="/mnt/cp",
            job_config_path=JOB_CFG_REL,
            materialized_views=[
                MonitoringMaterializedViewConfig(name="summary", sql="SELECT 1"),
                MonitoringMaterializedViewConfig(name="errors", sql="SELECT 2"),
            ],
        )
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)


@pytest.mark.unit
class TestParseProjectConfigWithMonitoring:
    """Integration: parsing monitoring from full lhp.yaml config data."""

    def test_monitoring_absent(self, loader):
        config = loader._parse_project_config({"name": "test_project"})
        assert config.monitoring is None

    def test_monitoring_empty_dict(self, loader):
        config = loader._parse_project_config(
            {
                "name": "test_project",
                "event_log": {"catalog": "cat", "schema": "_meta"},
                "monitoring": {
                    "checkpoint_path": "/mnt/cp",
                    "job_config_path": JOB_CFG_REL,
                },
            }
        )
        assert config.monitoring is not None
        assert config.monitoring.enabled is True
        assert config.monitoring.job_config_path == JOB_CFG_REL

    def test_monitoring_disabled(self, loader):
        config = loader._parse_project_config(
            {
                "name": "test_project",
                "event_log": {"catalog": "cat", "schema": "_meta"},
                "monitoring": {"enabled": False},
            }
        )
        assert config.monitoring is not None
        assert config.monitoring.enabled is False

    def test_monitoring_without_event_log_raises(self, loader):
        with pytest.raises(LHPError, match="requires event_log"):
            loader._parse_project_config(
                {
                    "name": "test_project",
                    "monitoring": {"enabled": True},
                }
            )

    def test_monitoring_enabled_without_job_config_path_raises(self, loader):
        """Parsing a full project config with monitoring enabled but no
        job_config_path should surface the new validation error."""
        with pytest.raises(LHPError, match="job_config_path is required"):
            loader._parse_project_config(
                {
                    "name": "test_project",
                    "event_log": {"catalog": "cat", "schema": "_meta"},
                    "monitoring": {
                        "enabled": True,
                        "checkpoint_path": "/mnt/cp",
                    },
                }
            )

    def test_monitoring_with_full_config(self, loader):
        config = loader._parse_project_config(
            {
                "name": "test_project",
                "event_log": {"catalog": "cat", "schema": "_meta"},
                "monitoring": {
                    "pipeline_name": "my_monitor",
                    "catalog": "override",
                    "streaming_table": "unified",
                    "checkpoint_path": "/mnt/cp",
                    "job_config_path": JOB_CFG_REL,
                    "materialized_views": [
                        {"name": "summary", "sql": "SELECT 1"},
                    ],
                },
            }
        )
        assert config.monitoring.pipeline_name == "my_monitor"
        assert config.monitoring.catalog == "override"
        assert config.monitoring.streaming_table == "unified"
        assert config.monitoring.job_config_path == JOB_CFG_REL
        assert len(config.monitoring.materialized_views) == 1
