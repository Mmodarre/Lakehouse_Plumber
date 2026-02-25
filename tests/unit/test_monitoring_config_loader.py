"""Unit tests for monitoring config parsing and validation in ProjectConfigLoader."""

import pytest
from pathlib import Path
from unittest.mock import patch

from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.models.config import EventLogConfig, MonitoringConfig
from lhp.utils.error_formatter import LHPError


@pytest.fixture
def loader(tmp_path):
    """Create a ProjectConfigLoader with a minimal lhp.yaml."""
    lhp_yaml = tmp_path / "lhp.yaml"
    lhp_yaml.write_text("name: test_project\n")
    return ProjectConfigLoader(tmp_path)


@pytest.mark.unit
class TestParseMonitoringConfig:
    """Tests for _parse_monitoring_config method."""

    def test_empty_dict_all_defaults(self, loader):
        """monitoring: {} should produce all-default MonitoringConfig."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config({}, event_log)
        assert isinstance(config, MonitoringConfig)
        assert config.enabled is True
        assert config.pipeline_name is None
        assert config.streaming_table == "all_pipelines_event_log"

    def test_none_treated_as_empty_dict(self, loader):
        """monitoring: (null) should also produce defaults."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(None, event_log)
        assert config.enabled is True

    def test_disabled(self, loader):
        """monitoring: { enabled: false } should parse successfully."""
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config({"enabled": False}, event_log)
        assert config.enabled is False

    def test_custom_fields(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {
                "pipeline_name": "my_monitor",
                "catalog": "override_cat",
                "schema": "_analytics",
                "streaming_table": "unified_events",
            },
            event_log,
        )
        assert config.pipeline_name == "my_monitor"
        assert config.catalog == "override_cat"
        assert config.schema_ == "_analytics"
        assert config.streaming_table == "unified_events"

    def test_with_materialized_views(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {
                "materialized_views": [
                    {"name": "summary", "sql": "SELECT 1"},
                    {"name": "errors", "sql_path": "queries/errors.sql"},
                ]
            },
            event_log,
        )
        assert len(config.materialized_views) == 2
        assert config.materialized_views[0].name == "summary"
        assert config.materialized_views[1].sql_path == "queries/errors.sql"

    def test_empty_materialized_views_list(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        config = loader._parse_monitoring_config(
            {"materialized_views": []}, event_log
        )
        assert config.materialized_views == []

    def test_substitution_tokens_preserved(self, loader):
        event_log = EventLogConfig(enabled=True, catalog="{catalog}", schema="{schema}")
        config = loader._parse_monitoring_config(
            {"catalog": "{catalog}", "schema": "{schema}"}, event_log
        )
        assert config.catalog == "{catalog}"
        assert config.schema_ == "{schema}"

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
        """Disabled monitoring should not raise even without event_log."""
        config = MonitoringConfig(enabled=False)
        loader._validate_monitoring_config(config, None)

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
        config = MonitoringConfig(enabled=True)
        event_log = EventLogConfig(enabled=True, catalog="cat", schema="_meta")
        loader._validate_monitoring_config(config, event_log)

    def test_duplicate_mv_names_raises(self, loader):
        from lhp.models.config import MonitoringMaterializedViewConfig

        config = MonitoringConfig(
            enabled=True,
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
                "monitoring": {},
            }
        )
        assert config.monitoring is not None
        assert config.monitoring.enabled is True

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

    def test_monitoring_with_full_config(self, loader):
        config = loader._parse_project_config(
            {
                "name": "test_project",
                "event_log": {"catalog": "cat", "schema": "_meta"},
                "monitoring": {
                    "pipeline_name": "my_monitor",
                    "catalog": "override",
                    "streaming_table": "unified",
                    "materialized_views": [
                        {"name": "summary", "sql": "SELECT 1"},
                    ],
                },
            }
        )
        assert config.monitoring.pipeline_name == "my_monitor"
        assert config.monitoring.catalog == "override"
        assert config.monitoring.streaming_table == "unified"
        assert len(config.monitoring.materialized_views) == 1
