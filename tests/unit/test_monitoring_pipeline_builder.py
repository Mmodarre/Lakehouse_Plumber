"""Unit tests for MonitoringPipelineBuilder."""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from lhp.core.services.monitoring_pipeline_builder import (
    DEFAULT_MV_SQL,
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
class TestCustomActions:
    """Tests for custom_actions support in monitoring pipeline."""

    def test_custom_actions_appear_in_flowgroup(self):
        """Custom actions from config should appear between write and MVs."""
        config = _make_project_config()
        config.monitoring.custom_actions = [
            {
                "name": "load_custom",
                "type": "load",
                "source": {"type": "python", "module_path": "src/my_source.py", "function_name": "my_func"},
                "target": "v_custom",
            },
            {
                "name": "write_custom",
                "type": "write",
                "source": "v_custom",
                "write_target": {"type": "materialized_view", "database": "cat.schema", "table": "my_mv"},
            },
        ]
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write + 2 custom + 1 default MV = 5 actions
        assert len(fg.actions) == 5
        assert fg.actions[0].name == "load_all_event_logs"
        assert fg.actions[1].name == "write_all_event_logs"
        assert fg.actions[2].name == "load_custom"
        assert fg.actions[2].type == ActionType.LOAD
        assert fg.actions[3].name == "write_custom"
        assert fg.actions[3].type == ActionType.WRITE
        # Default MV comes last
        assert fg.actions[4].write_target["table"] == "events_summary"

    def test_custom_actions_before_user_mvs(self):
        """Custom actions should appear before user-specified MVs."""
        mvs = [MonitoringMaterializedViewConfig(name="my_summary", sql="SELECT 1")]
        config = _make_project_config(monitoring_mvs=mvs)
        config.monitoring.custom_actions = [
            {"name": "load_extra", "type": "load", "source": {"type": "sql", "sql": "SELECT 1"}, "target": "v_extra"},
        ]
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write + 1 custom + 1 user MV = 4 actions
        assert len(fg.actions) == 4
        assert fg.actions[2].name == "load_extra"
        assert fg.actions[3].write_target["table"] == "my_summary"

    def test_empty_custom_actions_no_effect(self):
        """Empty custom_actions list should not add any extra actions."""
        config = _make_project_config()
        config.monitoring.custom_actions = []
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write + 1 default MV = 3 actions (same as no custom_actions)
        assert len(fg.actions) == 3

    def test_missing_custom_actions_backward_compat(self):
        """None custom_actions (default) should work - backward compatibility."""
        config = _make_project_config()
        assert config.monitoring.custom_actions is None
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])

        # load + write + 1 default MV = 3 actions
        assert len(fg.actions) == 3


@pytest.mark.unit
class TestDefaultMvSql:
    """Tests for the default MV SQL template."""

    def test_contains_expected_columns(self):
        assert "_source_pipeline" in DEFAULT_MV_SQL
        assert "event_type" in DEFAULT_MV_SQL
        assert "event_count" in DEFAULT_MV_SQL

    def test_has_streaming_table_placeholder(self):
        assert "{streaming_table}" in DEFAULT_MV_SQL

    def test_format_substitution(self):
        sql = DEFAULT_MV_SQL.format(streaming_table="cat.schema.my_table")
        assert "cat.schema.my_table" in sql
        assert "{streaming_table}" not in sql
