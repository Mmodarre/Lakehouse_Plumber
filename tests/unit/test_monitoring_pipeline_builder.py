"""Unit tests for MonitoringPipelineBuilder."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.core.services.monitoring_pipeline_builder import (
    DEFAULT_MV_SQL,
    JOBS_STATS_FUNCTION_NAME,
    JOBS_STATS_MODULE_PATH,
    JOBS_STATS_TABLE_NAME,
    JOBS_STATS_VIEW_NAME,
    MonitoringBuildResult,
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
    monitoring_checkpoint_path="/mnt/checkpoints/event_logs",
    monitoring_max_concurrent_streams=10,
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
        checkpoint_path=monitoring_checkpoint_path,
        max_concurrent_streams=monitoring_max_concurrent_streams,
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
        result = builder.get_event_log_pipeline_names(["bronze", "silver", "gold"])
        assert result == ["bronze", "silver", "gold"]

    def test_opt_out_excluded(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(opt_outs={"silver"})
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(["bronze", "silver", "gold"])
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
            custom_dicts={
                "silver": {"name": "custom_log", "catalog": "c", "schema": "s"}
            }
        )
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.get_event_log_pipeline_names(["bronze", "silver", "gold"])
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
class TestBuild:
    """Tests for the complete build() returning MonitoringBuildResult."""

    def test_returns_none_when_disabled(self):
        config = _make_project_config(monitoring_enabled=False)
        builder = MonitoringPipelineBuilder(config)
        assert builder.build(["bronze"]) is None

    def test_returns_none_when_no_eligible_pipelines(self, caplog):
        config = _make_project_config()
        loader = _make_pipeline_config_loader(opt_outs={"bronze", "silver"})
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])
        assert result is None
        assert "no pipelines have event_log" in caplog.text

    def test_returns_monitoring_build_result(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])
        assert isinstance(result, MonitoringBuildResult)
        assert isinstance(result.flowgroup, FlowGroup)
        assert isinstance(result.template_context, dict)
        assert result.eligible_pipelines == ["bronze", "silver"]
        assert result.pipeline_name == "test_project_event_log_monitoring"

    def test_synthetic_flag_set(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])
        assert result.flowgroup._synthetic is True

    def test_pipeline_and_flowgroup_names(self):
        config = _make_project_config(name="acme")
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])
        assert result.flowgroup.pipeline == "acme_event_log_monitoring"
        assert result.flowgroup.flowgroup == "monitoring"

    def test_mv_only_with_default_mv(self):
        """Default config: only 1 default MV action (no LOAD, no WRITE streaming_table)."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])
        fg = result.flowgroup

        # Only 1 MV action (no SQL load, no streaming table write)
        assert len(fg.actions) == 1
        mv = fg.actions[0]
        assert mv.type == ActionType.WRITE
        assert mv.write_target["type"] == "materialized_view"
        assert mv.write_target["table"] == "events_summary"

    def test_no_load_or_streaming_write_actions(self):
        """Verify the old LOAD and streaming table WRITE actions are gone."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])
        fg = result.flowgroup

        load_actions = [a for a in fg.actions if a.type == ActionType.LOAD]
        st_writes = [
            a
            for a in fg.actions
            if a.type == ActionType.WRITE
            and isinstance(a.write_target, dict)
            and a.write_target.get("type") == "streaming_table"
        ]
        assert len(load_actions) == 0
        assert len(st_writes) == 0

    def test_custom_mvs(self):
        """User-specified materialized views."""
        mvs = [
            MonitoringMaterializedViewConfig(name="summary", sql="SELECT 1"),
            MonitoringMaterializedViewConfig(name="errors", sql="SELECT 2"),
        ]
        config = _make_project_config(monitoring_mvs=mvs)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        # 2 custom MVs only
        assert len(result.flowgroup.actions) == 2
        assert result.flowgroup.actions[0].write_target["table"] == "summary"
        assert result.flowgroup.actions[0].write_target["sql"] == "SELECT 1"
        assert result.flowgroup.actions[1].write_target["table"] == "errors"

    def test_empty_mvs_list_flowgroup_is_none(self):
        """materialized_views: [] means no DLT FlowGroup (notebook-only)."""
        config = _make_project_config(monitoring_mvs=[])
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        assert result.flowgroup is None
        assert result.pipeline_name == "test_project_event_log_monitoring"
        assert result.template_context is not None

    def test_catalog_schema_override(self):
        """Monitoring-level catalog/schema override event_log defaults."""
        config = _make_project_config(
            monitoring_catalog="override_cat",
            monitoring_schema="_analytics",
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        # MV action should use overridden catalog/schema
        mv = result.flowgroup.actions[0]
        assert mv.write_target["catalog"] == "override_cat"
        assert mv.write_target["schema"] == "_analytics"

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
        result = builder.build(["bronze"])

        mv_action = result.flowgroup.actions[0]
        assert mv_action.write_target["sql"] == "SELECT count(*) FROM tbl"

    def test_eligible_pipelines_sorted(self):
        """eligible_pipelines should be sorted."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["gold", "bronze", "silver"])
        assert result.eligible_pipelines == ["bronze", "gold", "silver"]


@pytest.mark.unit
class TestBuildFlowgroupCompat:
    """Tests for backward-compatible build_flowgroup() wrapper."""

    def test_returns_flowgroup_directly(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        fg = builder.build_flowgroup(["bronze"])
        assert isinstance(fg, FlowGroup)
        assert fg._synthetic is True

    def test_returns_none_when_disabled(self):
        config = _make_project_config(monitoring_enabled=False)
        builder = MonitoringPipelineBuilder(config)
        assert builder.build_flowgroup(["bronze"]) is None


@pytest.mark.unit
class TestTemplateContext:
    """Tests for the deferred template context (rendering happens in orchestrator)."""

    def test_context_contains_sources(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])

        ctx = result.template_context
        assert "sources" in ctx
        sources = ctx["sources"]
        assert len(sources) == 2
        # Sources stored as lists (not tuples) for substitution compatibility
        assert sources[0] == ["bronze", "my_catalog._meta.bronze_event_log"]
        assert sources[1] == ["silver", "my_catalog._meta.silver_event_log"]

    def test_context_contains_target_fqn(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        assert result.template_context["target_fqn"] == (
            "my_catalog._meta.all_pipelines_event_log"
        )

    def test_context_contains_checkpoint_path(self):
        config = _make_project_config(
            monitoring_checkpoint_path="/mnt/checkpoints/monitoring"
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        assert result.template_context["checkpoint_path"] == (
            "/mnt/checkpoints/monitoring"
        )

    def test_context_contains_max_concurrent_streams(self):
        config = _make_project_config(monitoring_max_concurrent_streams=5)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        assert result.template_context["max_concurrent_streams"] == 5

    def test_context_has_all_expected_keys(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        expected_keys = {"sources", "target_fqn", "checkpoint_path", "max_concurrent_streams"}
        assert set(result.template_context.keys()) == expected_keys

    def test_context_single_pipeline(self):
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])

        sources = result.template_context["sources"]
        assert len(sources) == 1
        assert sources[0][0] == "bronze"
        assert sources[0][1] == "my_catalog._meta.bronze_event_log"


class TestNotebookRendering:
    """Rendered-notebook assertions (beyond context dict checks)."""

    def test_rendered_notebook_contains_ensure_target_exists_prologue(self):
        """The rendered notebook must pre-create the target table before the
        executor pool starts — this guards against a parallel-CREATE race on
        the first run (TABLE_OR_VIEW_ALREADY_EXISTS)."""
        config = _make_project_config()
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])

        rendered = builder._render_notebook(
            sources=result.template_context["sources"],
            target_fqn=result.template_context["target_fqn"],
        )

        assert "def _ensure_target_exists()" in rendered
        assert "_ensure_target_exists()" in rendered
        assert "from pyspark.sql.utils import AnalysisException" in rendered

        ensure_call_idx = rendered.index("\n    _ensure_target_exists()\n")
        executor_idx = rendered.index("with ThreadPoolExecutor")
        assert ensure_call_idx < executor_idx, (
            "_ensure_target_exists() must be invoked before the "
            "ThreadPoolExecutor block"
        )


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
        result = builder.build(["bronze", "silver"])
        fg = result.flowgroup
        # Default: 1 default MV only
        assert len(fg.actions) == 1
        python_loads = [
            a
            for a in fg.actions
            if a.source
            and isinstance(a.source, dict)
            and a.source.get("type") == "python"
        ]
        assert len(python_loads) == 0

    def test_job_monitoring_enabled_adds_actions(self):
        """enable_job_monitoring: true adds Python load + MV write (3 total with default MV)."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze", "silver"])
        fg = result.flowgroup
        # python_load + jobs_stats_write + default MV = 3 actions
        assert len(fg.actions) == 3

    def test_job_monitoring_action_structure(self):
        """Verify python load action source dict, target view, function name."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])
        fg = result.flowgroup

        python_load = fg.actions[0]
        assert python_load.type == ActionType.LOAD
        assert python_load.name == "load_jobs_stats"
        assert python_load.source["type"] == "python"
        assert python_load.source["module_path"] == JOBS_STATS_MODULE_PATH
        assert python_load.source["function_name"] == JOBS_STATS_FUNCTION_NAME
        assert python_load.target == JOBS_STATS_VIEW_NAME

    def test_job_monitoring_write_uses_same_catalog_schema(self):
        """Jobs stats write uses same catalog/schema as monitoring config."""
        config = _make_project_config(
            monitoring_catalog="override_cat",
            monitoring_schema="_analytics",
            monitoring_enable_job_monitoring=True,
        )
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])
        fg = result.flowgroup

        # Jobs stats write action (index 1)
        jobs_stats_write = fg.actions[1]
        assert jobs_stats_write.type == ActionType.WRITE
        assert jobs_stats_write.write_target["type"] == "materialized_view"
        assert jobs_stats_write.write_target["table"] == JOBS_STATS_TABLE_NAME
        assert (
            jobs_stats_write.write_target["sql"]
            == f"SELECT * FROM {JOBS_STATS_VIEW_NAME}"
        )
        assert jobs_stats_write.write_target["catalog"] == "override_cat"
        assert jobs_stats_write.write_target["schema"] == "_analytics"

    def test_job_monitoring_populates_auxiliary_files(self):
        """_auxiliary_files contains package resource content when enable_job_monitoring enabled."""
        config = _make_project_config(monitoring_enable_job_monitoring=True)
        loader = _make_pipeline_config_loader()
        builder = MonitoringPipelineBuilder(config, pipeline_config_loader=loader)
        result = builder.build(["bronze"])
        fg = result.flowgroup

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
        result = builder.build(["bronze"])
        fg = result.flowgroup

        assert len(fg._auxiliary_files) == 0


@pytest.mark.unit
class TestMonitoringConfigModel:
    """Tests for MonitoringConfig model fields."""

    def test_checkpoint_path_defaults_empty(self):
        mc = MonitoringConfig(enabled=True)
        assert mc.checkpoint_path == ""

    def test_checkpoint_path_set(self):
        mc = MonitoringConfig(enabled=True, checkpoint_path="/mnt/cp")
        assert mc.checkpoint_path == "/mnt/cp"

    def test_max_concurrent_streams_default(self):
        mc = MonitoringConfig(enabled=True)
        assert mc.max_concurrent_streams == 10

    def test_max_concurrent_streams_custom(self):
        mc = MonitoringConfig(enabled=True, max_concurrent_streams=5)
        assert mc.max_concurrent_streams == 5
