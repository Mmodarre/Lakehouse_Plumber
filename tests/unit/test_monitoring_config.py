"""Unit tests for monitoring configuration models."""

import pytest
from pydantic import ValidationError

from lhp.models.config import (
    EventLogConfig,
    MonitoringConfig,
    MonitoringMaterializedViewConfig,
    ProjectConfig,
)


@pytest.mark.unit
class TestMonitoringMaterializedViewConfig:
    """Tests for MonitoringMaterializedViewConfig model."""

    def test_name_only(self):
        mv = MonitoringMaterializedViewConfig(name="events_summary")
        assert mv.name == "events_summary"
        assert mv.sql is None
        assert mv.sql_path is None

    def test_with_sql(self):
        mv = MonitoringMaterializedViewConfig(
            name="events_summary", sql="SELECT * FROM t"
        )
        assert mv.sql == "SELECT * FROM t"
        assert mv.sql_path is None

    def test_with_sql_path(self):
        mv = MonitoringMaterializedViewConfig(
            name="events_summary", sql_path="queries/summary.sql"
        )
        assert mv.sql is None
        assert mv.sql_path == "queries/summary.sql"

    def test_with_both_sql_and_sql_path(self):
        """Model allows both; validation catches this at loader level."""
        mv = MonitoringMaterializedViewConfig(
            name="events_summary",
            sql="SELECT 1",
            sql_path="queries/summary.sql",
        )
        assert mv.sql is not None
        assert mv.sql_path is not None

    def test_name_required(self):
        with pytest.raises(ValidationError):
            MonitoringMaterializedViewConfig()


@pytest.mark.unit
class TestMonitoringConfig:
    """Tests for MonitoringConfig model."""

    def test_all_defaults(self):
        config = MonitoringConfig()
        assert config.enabled is True
        assert config.pipeline_name is None
        assert config.catalog is None
        assert config.schema_ is None
        assert config.streaming_table == "all_pipelines_event_log"
        assert config.materialized_views is None

    def test_disabled(self):
        config = MonitoringConfig(enabled=False)
        assert config.enabled is False

    def test_custom_pipeline_name(self):
        config = MonitoringConfig(pipeline_name="my_monitor")
        assert config.pipeline_name == "my_monitor"

    def test_custom_catalog_schema(self):
        config = MonitoringConfig(catalog="my_catalog", schema="_analytics")
        assert config.catalog == "my_catalog"
        assert config.schema_ == "_analytics"

    def test_schema_alias(self):
        """Verify 'schema' alias works for schema_ field."""
        config = MonitoringConfig(**{"schema": "_meta"})
        assert config.schema_ == "_meta"

    def test_custom_streaming_table(self):
        config = MonitoringConfig(streaming_table="unified_events")
        assert config.streaming_table == "unified_events"

    def test_with_materialized_views(self):
        mvs = [
            MonitoringMaterializedViewConfig(name="summary", sql="SELECT 1"),
            MonitoringMaterializedViewConfig(name="errors", sql_path="q.sql"),
        ]
        config = MonitoringConfig(materialized_views=mvs)
        assert len(config.materialized_views) == 2
        assert config.materialized_views[0].name == "summary"
        assert config.materialized_views[1].name == "errors"

    def test_empty_materialized_views_list(self):
        config = MonitoringConfig(materialized_views=[])
        assert config.materialized_views == []

    def test_substitution_tokens_preserved(self):
        """Substitution tokens like ${catalog} should pass through the model."""
        config = MonitoringConfig(catalog="${catalog}", schema="${schema}")
        assert config.catalog == "${catalog}"
        assert config.schema_ == "${schema}"


@pytest.mark.unit
class TestProjectConfigWithMonitoring:
    """Tests for ProjectConfig.monitoring field."""

    def test_project_config_accepts_monitoring(self):
        config = ProjectConfig(
            name="test_project",
            monitoring=MonitoringConfig(enabled=True),
        )
        assert config.monitoring is not None
        assert config.monitoring.enabled is True

    def test_project_config_monitoring_none_by_default(self):
        config = ProjectConfig(name="test_project")
        assert config.monitoring is None

    def test_project_config_with_event_log_and_monitoring(self):
        config = ProjectConfig(
            name="test_project",
            event_log=EventLogConfig(enabled=True, catalog="cat", schema="_meta"),
            monitoring=MonitoringConfig(enabled=True),
        )
        assert config.event_log is not None
        assert config.monitoring is not None
