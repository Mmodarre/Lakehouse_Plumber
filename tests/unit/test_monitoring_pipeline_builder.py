"""Unit tests for MonitoringPipelineBuilder."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from lhp.core.services.monitoring_pipeline_builder import (
    DEFAULT_MV_SQL,
    JOBS_STATS_FUNCTION_NAME,
    JOBS_STATS_MODULE_PATH,
    JOBS_STATS_TABLE_NAME,
    JOBS_STATS_VIEW_NAME,
    MonitoringPipelineBuilder,
)
from lhp.models.config import (
    ActionType,
    EventLogConfig,
    FlowGroup,
    MonitoringConfig,
    MonitoringMaterializedViewConfig,
    ProjectConfig,
)


def _make_project_config(
    name="test_project",
    event_log_enabled=True,
    event_log_catalog="my_catalog",
    event_log_schema="_meta",
    event_log_prefix="",
    event_log_suffix="_event_log",
    monitoring_enabled=True,
    monitoring_pipeline_name=None,
    monitoring_catalog=None,
    monitoring_schema=None,
    monitoring_streaming_table="all_pipelines_event_log",
    monitoring_mvs=None,
    monitoring_enable_job_monitoring=False,
):
    """Helper to build a ProjectConfig with event_log and monitoring."""
    event_log = EventLogConfig(
        enabled=event_log_enabled,
        catalog=event_log_catalog,
        schema=event_log_schema,
        name_prefix=event_log_prefix,
        name_suffix=event_log_suffix,
    )
    monitoring = MonitoringConfig(
        enabled=monitoring_enabled,
        pipeline_name=monitoring_pipeline_name,
        catalog=monitoring_catalog,
        schema=monitoring_schema,
        streaming_table=monitoring_streaming_table,
        materialized_views=monitoring_mvs,
        enable_job_monitoring=monitoring_enable_job_monitoring,
    )
    return ProjectConfig(
        name=name,
        event_log=event_log,
        monitoring=monitoring,
    )


def _make_pipeline_config_loader(opt_outs=None, custom_dicts=None):
    """Create a mock PipelineConfigLoader.

    Args:
        opt_outs: Set of pipeline names that have event_log: false
        custom_dicts: Dict of pipeline_name -> custom event_log dict
    """
    opt_outs = opt_outs or set()
    custom_dicts = custom_dicts or {}
    loader = MagicMock()

    def get_config(name):
        if name in opt_outs:
            return {"event_log": False}
        if name in custom_dicts:
            return {"event_log": custom_dicts[name]}
        return {}

    loader.get_pipeline_config = MagicMock(side_effect=get_config)
    return loader


@pytest.mark.unit
class TestShouldBuild:
    """Tests for MonitoringPipelineBuilder.should_build()."""

    def test_monitoring_enabled_with_event_log(self):
        config = _make_project_config()
        builder = MonitoringPipelineBuilder(config)
        assert builder.should_build() is True

    def test_monitoring_disabled(self):
        config = _make_project_config(monitoring_enabled=False)
        builder = MonitoringPipelineBuilder(config)
        assert builder.should_build() is False

    def test_no_monitoring_config(self):
        config = ProjectConfig(
            name="test",
            event_log=EventLogConfig(enabled=True, catalog="c", schema="s"),
        )
        builder = MonitoringPipelineBuilder(config)
        assert builder.should_build() is False

    def test_event_log_disabled(self):
        config = _make_project_config(event_log_enabled=False)
        builder = MonitoringPipelineBuilder(config)
        assert builder.should_build() is False

    def test_no_event_log(self):
        config = ProjectConfig(
            name="test",
            monitoring=MonitoringConfig(enabled=True),
        )
        builder = MonitoringPipelineBuilder(config)
        assert builder.should_build() is False


@pytest.mark.unit
class TestPipelineName:
    """Tests for pipeline name resolution."""

    def test_default_name(self):
        config = _make_project_config(name="acme_edw")
        builder = MonitoringPipelineBuilder(config)
        assert builder.pipeline_name == "acme_edw_event_log_monitoring"

    def test_custom_name(self):
        config = _make_project_config(monitoring_pipeline_name="my_monitor")
        builder = MonitoringPipelineBuilder(config)
        assert builder.pipeline_name == "my_monitor"


@pytest.mark.unit
class TestGetEventLogPipelineNames:
    """Tests for pipeline filtering logic."""

    def test_all_pipelines_included(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(
            ["bronze", "silver", "gold"]
        )
        assert result == ["bronze", "silver", "gold"]

    def test_opt_out_excluded(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(opt_outs={"silver"})
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(
            ["bronze", "silver", "gold"]
        )
        assert result == ["bronze", "gold"]

    def test_monitoring_pipeline_excluded(self):
        config = _make_project_config(
            name="test", monitoring_pipeline_name="test_event_log_monitoring"
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(
            ["bronze", "test_event_log_monitoring", "silver"]
        )
        assert result == ["bronze", "silver"]

    def test_custom_event_log_dict_included_with_warning(self, caplog):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(
            custom_dicts={"silver": {"name": "custom_log", "catalog": "c", "schema": "s"}}
        )
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(
            ["bronze", "silver", "gold"]
        )
        assert "silver" in result
        assert "custom event_log config" in caplog.text

    def test_no_pipeline_config_loader(self):
        """Without a pipeline config loader, all pipelines are included."""
        config = _make_project_config()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=None)
        result = builder.get_event_log_pipeline_names(["bronze", "silver"])
        assert result == ["bronze", "silver"]

    def test_empty_pipeline_list(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names([])
        assert result == []

    def test_all_opted_out(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(opt_outs={"bronze", "silver"})
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(["bronze", "silver"])
        assert result == []


@pytest.mark.unit
class TestBuildUnionSql:
    """Tests for SQL generation."""

    def test_single_pipeline(self):
        config = _make_project_config()
        builder = MonitoringPipelineBuilder(config)
        sql = builder.build_union_sql(["bronze"])
        assert "stream(my_catalog._meta.bronze_event_log)" in sql
        assert "'bronze' as _source_pipeline" in sql
        assert "UNION ALL" not in sql

    def test_multiple_pipelines(self):
        config = _make_project_config()
        builder = MonitoringPipelineBuilder(config)
        sql = builder.build_union_sql(["bronze", "silver", "gold"])
        assert sql.count("UNION ALL") == 2
        assert "stream(my_catalog._meta.bronze_event_log)" in sql
        assert "stream(my_catalog._meta.silver_event_log)" in sql
        assert "stream(my_catalog._meta.gold_event_log)" in sql
        assert "'bronze' as _source_pipeline" in sql
        assert "'silver' as _source_pipeline" in sql
        assert "'gold' as _source_pipeline" in sql

    def test_empty_pipeline_list(self):
        config = _make_project_config()
        builder = MonitoringPipelineBuilder(config)
        sql = builder.build_union_sql([])
        assert sql == ""

    def test_tokens_preserved(self):
        """Substitution tokens should pass through to SQL."""
        config = _make_project_config(
            event_log_catalog="{catalog}", event_log_schema="{schema}"
        )
        builder = MonitoringPipelineBuilder(config)
        sql = builder.build_union_sql(["bronze"])
        assert "stream({catalog}.{schema}.bronze_event_log)" in sql

    def test_prefix_and_suffix(self):
        config = _make_project_config(
            event_log_prefix="dl_", event_log_suffix="_log"
        )
        builder = MonitoringPipelineBuilder(config)
        sql = builder.build_union_sql(["bronze"])
        assert "stream(my_catalog._meta.dl_bronze_log)" in sql


@pytest.mark.unit
class TestBuildFlowgroup:
    """Tests for the complete FlowGroup construction."""

    def test_returns_none_when_disabled(self):
        config = _make_project_config(monitoring_enabled=False)
        builder = MonitoringPipelineBuilder(config)
        assert builder.build_flowgroup(["bronze"]) is None

    def test_returns_none_when_no_eligible_pipelines(self, caplog):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(opt_outs={"bronze", "silver"})
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build_flowgroup(["bronze", "silver"])
        assert result is None
        assert "no pipelines have event_log" in caplog.text

    def test_synthetic_flag_set(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze", "silver"])
        assert fg is not None
        assert fg._synthetic is True

    def test_pipeline_and_flowgroup_names(self):
        config = _make_project_config(name="acme")
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])
        assert fg.pipeline == "acme_event_log_monitoring"
        assert fg.flowgroup == "monitoring"

    def test_three_actions_with_default_mv(self):
        """Default config: load + write + 1 default MV = 3 actions."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze", "silver"])
        assert len(fg.actions) == 3

        # Action 1: SQL Load
        load = fg.actions[0]
        assert load.type == ActionType.LOAD
        assert load.name == "load_all_event_logs"
        assert load.target == "v_all_event_logs"
        assert "UNION ALL" in load.source["sql"]

        # Action 2: Streaming Table Write
        write = fg.actions[1]
        assert write.type == ActionType.WRITE
        assert write.readMode == "stream"
        assert write.write_target["type"] == "streaming_table"
        assert write.write_target["table"] == "all_pipelines_event_log"

        # Action 3: Default MV
        mv = fg.actions[2]
        assert mv.type == ActionType.WRITE
        assert mv.write_target["type"] == "materialized_view"
        assert mv.write_target["table"] == "events_summary"

    def test_custom_mvs(self):
        """User-specified materialized views."""
        mvs = [
            MonitoringMaterializedViewConfig(name="summary", sql="SELECT 1"),
            MonitoringMaterializedViewConfig(name="errors", sql="SELECT 2"),
        ]
        config = _make_project_config(monitoring_mvs=mvs)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write + 2 MVs = 4 actions
        assert len(fg.actions) == 4
        assert fg.actions[2].write_target["table"] == "summary"
        assert fg.actions[2].write_target["sql"] == "SELECT 1"
        assert fg.actions[3].write_target["table"] == "errors"

    def test_empty_mvs_list_no_mv_actions(self):
        """materialized_views: [] means no MVs."""
        config = _make_project_config(monitoring_mvs=[])
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write only = 2 actions
        assert len(fg.actions) == 2

    def test_catalog_schema_override(self):
        """Monitoring-level catalog/schema override event_log defaults."""
        config = _make_project_config(
            monitoring_catalog="override_cat",
            monitoring_schema="_analytics",
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # Write action should use overridden catalog/schema
        write = fg.actions[1]
        assert write.write_target["database"] == "override_cat._analytics"

    def test_write_action_create_table_true(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])
        write = fg.actions[1]
        assert write.write_target["create_table"] is True

    def test_load_source_is_sql_type(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])
        load = fg.actions[0]
        assert load.source["type"] == "sql"
        assert "sql" in load.source

    def test_mv_sql_path_resolution(self, tmp_path):
        """MV with sql_path should load from file."""
        sql_file = tmp_path / "queries" / "custom.sql"
        sql_file.parent.mkdir(parents=True)
        sql_file.write_text("SELECT count(*) FROM tbl")

        mvs = [
            MonitoringMaterializedViewConfig(
                name="custom", sql_path="queries/custom.sql"
            ),
        ]
        config = _make_project_config(monitoring_mvs=mvs)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(
            config, pipeline_config_loader=loader, project_root=tmp_path
        )
        fg = builder.build_flowgroup(["bronze"])

        mv_action = fg.actions[2]
        assert mv_action.write_target["sql"] == "SELECT count(*) FROM tbl"


@pytest.mark.unit
class TestDefaultMvSql:
    """Tests for the default MV SQL template."""

    def test_contains_expected_columns(self):
        assert "pipeline_name" in DEFAULT_MV_SQL
        assert "run_status" in DEFAULT_MV_SQL
        assert "duration_minutes" in DEFAULT_MV_SQL

    def test_has_streaming_table_placeholder(self):
        assert "{streaming_table}" in DEFAULT_MV_SQL

    def test_format_substitution(self):
        sql = DEFAULT_MV_SQL.format(streaming_table="cat.schema.my_table")
        assert "cat.schema.my_table" in sql
        assert "{streaming_table}" not in sql


@pytest.mark.unit
class TestJobMonitoring:
    """Tests for enable_job_monitoring feature (jobs stats via Databricks SDK)."""

    def test_job_monitoring_disabled_by_default(self):
        """Default config has no python load actions."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze", "silver"])
        assert fg is not None
        # Default: load + write + default MV = 3 actions
        assert len(fg.actions) == 3
        # No python load actions
        python_loads = [a for a in fg.actions if a.source and isinstance(a.source, dict) and a.source.get("type") == "python"]
        assert len(python_loads) == 0

    def test_job_monitoring_enabled_adds_actions(self):
        """enable_job_monitoring: true adds Python load + write actions (5 total vs 3)."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze", "silver"])
        assert fg is not None
        # load + write + job_monitoring_load + job_monitoring_write + default MV = 5 actions
        assert len(fg.actions) == 5

    def test_job_monitoring_action_structure(self):
        """Verify python load action source dict, target view, function name."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        python_load = fg.actions[2]
        assert python_load.type == ActionType.LOAD
        assert python_load.name == "load_jobs_stats"
        assert python_load.source["type"] == "python"
        assert python_load.source["module_path"] == JOBS_STATS_MODULE_PATH
        assert python_load.source["function_name"] == JOBS_STATS_FUNCTION_NAME
        assert python_load.target == JOBS_STATS_VIEW_NAME

    def test_job_monitoring_write_uses_same_catalog_schema(self):
        """Jobs stats write uses same catalog/schema as event log write."""
        config = _make_project_config(
            monitoring_catalog="override_cat",
            monitoring_schema="_analytics",
            monitoring_enable_job_monitoring=True,
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # Event log write action
        event_log_write = fg.actions[1]
        # Jobs stats write action
        jobs_stats_write = fg.actions[3]

        assert jobs_stats_write.type == ActionType.WRITE
        assert jobs_stats_write.write_target["type"] == "materialized_view"
        assert jobs_stats_write.write_target["table"] == JOBS_STATS_TABLE_NAME
        assert jobs_stats_write.write_target["sql"] == f"SELECT * FROM {JOBS_STATS_VIEW_NAME}"
        # Same catalog/schema as event log write
        assert jobs_stats_write.write_target["database"] == event_log_write.write_target["database"]

    def test_job_monitoring_populates_auxiliary_files(self):
        """_auxiliary_files contains package resource content when enable_job_monitoring enabled."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        assert JOBS_STATS_MODULE_PATH in fg._auxiliary_files
        content = fg._auxiliary_files[JOBS_STATS_MODULE_PATH]
        assert "def get_jobs_stats" in content
        assert "WorkspaceClient" in content
        # Verify it matches the package resource file
        from importlib.resources import files

        expected = (
            files("lhp.templates.monitoring") / "jobs_stats_loader.py"
        ).read_text(encoding="utf-8")
        assert content == expected

    def test_job_monitoring_disabled_no_auxiliary_files(self):
        """_auxiliary_files empty when enable_job_monitoring disabled."""
        config = _make_project_config(monitoring_enable_job_monitoring=False)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        assert len(fg._auxiliary_files) == 0
