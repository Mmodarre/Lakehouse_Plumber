"""Builder for synthetic event log monitoring pipeline.

Constructs a FlowGroup that UNIONs all pipeline event log tables into a single
streaming table, with optional materialized views for analysis.
"""

import logging
from pathlib import Path
from typing import Any, Dict, List, Optional

from ...models.config import (
    Action,
    ActionType,
    FlowGroup,
    MonitoringConfig,
    ProjectConfig,
)
from ...utils.error_formatter import ErrorCategory, LHPError
from ...utils.external_file_loader import load_external_file_text

logger = logging.getLogger(__name__)

# Default MV SQL: summarizes event counts by pipeline, event type, and hour
# DEFAULT_MV_SQL = """\
# SELECT
#   _source_pipeline,
#   event_type,
#   date_trunc('HOUR', timestamp) AS event_hour,
#   count(*) AS event_count,
#   max(timestamp) AS latest_event
# FROM {streaming_table}
# GROUP BY _source_pipeline, event_type, date_trunc('HOUR', timestamp)\
# """

# Default MV SQL: pipeline run summary with status, duration, and row metrics
DEFAULT_MV_SQL = """\
WITH run_info AS (
    SELECT
        origin.pipeline_name,
        origin.pipeline_id,
        origin.update_id,
        MIN(`timestamp`) AS run_start_time,
        MAX(`timestamp`) AS run_end_time,
        MAX_BY(
            CASE WHEN event_type = 'update_progress'
                THEN details:update_progress:state::STRING END,
            CASE WHEN event_type = 'update_progress'
                THEN `timestamp` END
        ) AS run_status
    FROM {streaming_table}
    GROUP BY origin.pipeline_name, origin.pipeline_id, origin.update_id
),
run_metrics AS (
    SELECT
        origin.pipeline_name,
        origin.update_id,
        SUM(COALESCE(details:flow_progress:metrics:num_upserted_rows::BIGINT, 0))
          AS total_upserted_rows,
        SUM(COALESCE(details:flow_progress:metrics:num_deleted_rows::BIGINT, 0))
          AS total_deleted_rows,
        SUM(COALESCE(details:flow_progress:data_quality:dropped_records::BIGINT, 0))
          AS total_dropped_records,
        COUNT(DISTINCT origin.flow_name) AS tables_processed
    FROM {streaming_table}
    WHERE event_type = 'flow_progress'
      AND details:flow_progress:metrics IS NOT NULL
    GROUP BY origin.pipeline_name, origin.update_id
),
run_config AS (
    SELECT
        origin.pipeline_name,
        origin.update_id,
        MAX(details:create_update:runtime_version:dbr_version::STRING) AS dbr_version,
        MAX(CASE WHEN details:create_update:config:serverless::BOOLEAN
            THEN 'Serverless' ELSE 'Classic' END) AS compute_type,
        MAX(details:create_update:cause::STRING) AS trigger_cause,
        MAX(details:create_update:full_refresh::BOOLEAN) AS is_full_refresh
    FROM {streaming_table}
    WHERE event_type = 'create_update'
    GROUP BY origin.pipeline_name, origin.update_id
)
SELECT
    ri.pipeline_name,
    ri.pipeline_id,
    ri.update_id,
    ri.run_status,
    rc.trigger_cause,
    rc.is_full_refresh,
    rc.dbr_version,
    rc.compute_type,
    ri.run_start_time,
    ri.run_end_time,
    ROUND((unix_timestamp(ri.run_end_time) - unix_timestamp(ri.run_start_time)) / 60, 2)
      AS duration_minutes,
    COALESCE(rm.tables_processed, 0) AS tables_processed,
    COALESCE(rm.total_upserted_rows, 0) AS total_upserted_rows,
    COALESCE(rm.total_deleted_rows, 0) AS total_deleted_rows,
    COALESCE(rm.total_upserted_rows, 0) + COALESCE(rm.total_deleted_rows, 0)
      AS total_rows_affected,
    COALESCE(rm.total_dropped_records, 0) AS total_dropped_records
FROM run_info ri
LEFT JOIN run_metrics rm
  ON ri.pipeline_name = rm.pipeline_name AND ri.update_id = rm.update_id
LEFT JOIN run_config rc
  ON ri.pipeline_name = rc.pipeline_name AND ri.update_id = rc.update_id
ORDER BY ri.run_start_time DESC\
"""

# Python load constants for jobs stats
JOBS_STATS_MODULE_PATH = "jobs_stats_loader.py"
JOBS_STATS_FUNCTION_NAME = "get_jobs_stats"
JOBS_STATS_VIEW_NAME = "v_jobs_stats"
JOBS_STATS_TABLE_NAME = "jobs_stats"

try:
    from importlib.resources import files
except ImportError:
    import importlib_resources

    files = importlib_resources.files


def _load_jobs_stats_source() -> str:
    """Load jobs_stats_loader.py source from package resources."""
    resource = files("lhp.templates.monitoring") / "jobs_stats_loader.py"
    return resource.read_text(encoding="utf-8")


class MonitoringPipelineBuilder:
    """Builds a synthetic FlowGroup for the event log monitoring pipeline.

    The monitoring pipeline:
    1. Creates a temporary SQL view that UNIONs all pipeline event_log tables
    2. Writes the unioned stream to a single streaming table
    3. Creates materialized views for analysis (configurable or default)
    """

    def __init__(
        self,
        project_config: ProjectConfig,
        pipeline_config_loader: Optional[Any] = None,
        project_root: Optional[Path] = None,
    ) -> None:
        self.project_config = project_config
        self.pipeline_config_loader = pipeline_config_loader
        self.project_root = project_root

    @property
    def monitoring_config(self) -> Optional[MonitoringConfig]:
        return self.project_config.monitoring

    @property
    def pipeline_name(self) -> str:
        """Resolved monitoring pipeline name."""
        if self.monitoring_config and self.monitoring_config.pipeline_name:
            return self.monitoring_config.pipeline_name
        return f"{self.project_config.name}_event_log_monitoring"

    def should_build(self) -> bool:
        """True if monitoring is present, enabled, and event_log is enabled."""
        if not self.monitoring_config:
            return False
        if not self.monitoring_config.enabled:
            return False
        event_log = self.project_config.event_log
        if not event_log or not event_log.enabled:
            return False
        return True

    def get_event_log_pipeline_names(
        self, all_pipeline_names: List[str]
    ) -> List[str]:
        """Filter pipelines to those that actually have event_log enabled.

        Replicates BundleManager._inject_project_event_log logic:
        - Exclude pipelines with event_log: false in pipeline_config
        - Exclude the monitoring pipeline itself
        - Include all others (project-level injection applies)

        For pipelines with custom event_log dicts: include but use project-level
        naming convention + emit warning (V1 simplification).

        Args:
            all_pipeline_names: All discovered pipeline names

        Returns:
            Pipeline names that will have event_log tables
        """
        monitoring_name = self.pipeline_name
        eligible: List[str] = []

        for name in all_pipeline_names:
            # Skip the monitoring pipeline itself
            if name == monitoring_name:
                continue

            # Check pipeline-level config for opt-outs
            if self.pipeline_config_loader:
                pipeline_cfg = self.pipeline_config_loader.get_pipeline_config(name)
                pipeline_event_log = pipeline_cfg.get("event_log")

                if pipeline_event_log is False:
                    logger.debug(
                        f"Pipeline '{name}' opted out of event_log, "
                        f"excluding from monitoring"
                    )
                    continue

                if isinstance(pipeline_event_log, dict):
                    logger.warning(
                        f"Pipeline '{name}' has custom event_log config. "
                        f"V1 monitoring uses project-level naming convention."
                    )

            eligible.append(name)

        return eligible

    def _get_event_log_table_ref(self, pipeline_name: str) -> str:
        """Build the fully-qualified event log table reference for a pipeline.

        Uses project-level event_log config for catalog, schema, prefix, suffix.

        Args:
            pipeline_name: Pipeline name

        Returns:
            Fully-qualified table reference (e.g. catalog.schema.prefix_name_suffix)
        """
        event_log = self.project_config.event_log
        assert event_log is not None  # Caller ensures this

        catalog = event_log.catalog or ""
        schema = event_log.schema_ or ""
        name = f"{event_log.name_prefix}{pipeline_name}{event_log.name_suffix}"

        return f"{catalog}.{schema}.{name}"

    def build_union_sql(self, pipeline_names: List[str]) -> str:
        """Build UNION ALL SQL with stream() wrappers.

        Single pipeline: simple SELECT (no UNION).
        Multiple pipelines: UNION ALL with _source_pipeline column.
        Table references preserve substitution tokens for later resolution.

        Args:
            pipeline_names: Pipeline names with active event_log

        Returns:
            SQL string for the UNION ALL query
        """
        if not pipeline_names:
            return ""

        parts: List[str] = []
        for name in pipeline_names:
            table_ref = self._get_event_log_table_ref(name)
            parts.append(
                f"SELECT *, '{name}' as _source_pipeline\n"
                f"FROM stream({table_ref})"
            )

        return "\nUNION ALL\n".join(parts)

    def _resolve_catalog_schema(self) -> tuple:
        """Resolve catalog and schema for monitoring tables.

        Priority: monitoring config overrides > event_log config defaults.

        Returns:
            (catalog, schema) tuple
        """
        event_log = self.project_config.event_log
        assert event_log is not None

        catalog = event_log.catalog or ""
        schema = event_log.schema_ or ""

        if self.monitoring_config:
            if self.monitoring_config.catalog:
                catalog = self.monitoring_config.catalog
            if self.monitoring_config.schema_:
                schema = self.monitoring_config.schema_

        return catalog, schema

    def _build_load_action(self, union_sql: str) -> Action:
        """Build the SQL load action (temporary view) for the UNION ALL query."""
        return Action(
            name="load_all_event_logs",
            type=ActionType.LOAD,
            source={"type": "sql", "sql": union_sql},
            target="v_all_event_logs",
            description="SQL source: load_all_event_logs",
        )

    def _build_write_action(self, catalog: str, schema: str) -> Action:
        """Build the streaming table write action."""
        assert self.monitoring_config is not None

        st_name = self.monitoring_config.streaming_table

        return Action(
            name="write_all_event_logs",
            type=ActionType.WRITE,
            source="v_all_event_logs",
            readMode="stream",
            write_target={
                "type": "streaming_table",
                "catalog": catalog or "",
                "schema": schema or "",
                "table": st_name,
                "create_table": True,
            },
        )

    def _build_python_load_action(self) -> Action:
        """Build the Python load action for jobs stats."""
        return Action(
            name="load_jobs_stats",
            type=ActionType.LOAD,
            source={
                "type": "python",
                "module_path": JOBS_STATS_MODULE_PATH,
                "function_name": JOBS_STATS_FUNCTION_NAME,
            },
            target=JOBS_STATS_VIEW_NAME,
            description="Python source: load_jobs_stats",
        )

    def _build_jobs_stats_write_action(
        self, catalog: str, schema: str
    ) -> Action:
        """Build the materialized view write action for jobs stats.

        Uses a materialized view (not streaming table) because the Python
        SDK source returns batch data, not a streaming DataFrame.
        """
        return Action(
            name="write_jobs_stats",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": catalog or "",
                "schema": schema or "",
                "table": JOBS_STATS_TABLE_NAME,
                "sql": f"SELECT * FROM {JOBS_STATS_VIEW_NAME}",
            },
        )

    def _build_mv_action(
        self, mv_name: str, sql: str, catalog: str, schema: str
    ) -> Action:
        """Build a materialized view action."""
        return Action(
            name=f"mv_{mv_name}",
            type=ActionType.WRITE,
            write_target={
                "type": "materialized_view",
                "catalog": catalog or "",
                "schema": schema or "",
                "table": mv_name,
                "sql": sql,
            },
        )

    def _resolve_mv_sql(
        self, mv_name: str, sql: Optional[str], sql_path: Optional[str]
    ) -> str:
        """Resolve SQL for a materialized view from inline or file.

        Args:
            mv_name: MV name (for error messages)
            sql: Inline SQL string
            sql_path: Path to external SQL file

        Returns:
            Resolved SQL string
        """
        if sql:
            return sql

        if sql_path:
            if not self.project_root:
                raise LHPError(
                    category=ErrorCategory.CONFIG,
                    code_number="008",
                    title="Cannot resolve sql_path without project root",
                    details=(
                        f"Materialized view '{mv_name}' uses sql_path but "
                        f"project_root was not provided to the builder."
                    ),
                    suggestions=["This is an internal error; please report it."],
                )
            return load_external_file_text(
                sql_path, self.project_root, f"monitoring MV '{mv_name}' SQL file"
            )

        # Neither sql nor sql_path — should not happen after validation
        return ""

    def _get_default_mv_sql(self, streaming_table_fqn: str) -> str:
        """Get default MV SQL with the streaming table name substituted."""
        return DEFAULT_MV_SQL.format(streaming_table=streaming_table_fqn)

    def build_flowgroup(
        self, all_pipeline_names: List[str]
    ) -> Optional[FlowGroup]:
        """Build complete monitoring FlowGroup. Returns None if not applicable.

        Actions:
        1. SQL Load -> temporary view (UNION ALL of event_log tables)
        2. Streaming Table Write (readMode: stream, create_table: true)
        3. Materialized View(s) - from config or default

        Args:
            all_pipeline_names: All discovered pipeline names

        Returns:
            Synthetic FlowGroup or None if monitoring should not be built
        """
        if not self.should_build():
            return None

        assert self.monitoring_config is not None

        eligible_pipelines = sorted(
            self.get_event_log_pipeline_names(all_pipeline_names)
        )

        if not eligible_pipelines:
            logger.warning(
                "Monitoring enabled but no pipelines have event_log. "
                "Skipping monitoring pipeline generation."
            )
            return None

        logger.info(
            f"Building monitoring pipeline '{self.pipeline_name}' "
            f"for {len(eligible_pipelines)} pipeline(s): {eligible_pipelines}"
        )

        # Build SQL
        union_sql = self.build_union_sql(eligible_pipelines)

        # Resolve catalog/schema
        catalog, schema = self._resolve_catalog_schema()

        # Build actions
        actions: List[Action] = []

        # 1. SQL Load (temporary view)
        actions.append(self._build_load_action(union_sql))

        # 2. Streaming Table Write
        actions.append(self._build_write_action(catalog, schema))

        # 3. Python Load (optional — jobs stats via Databricks SDK)
        if self.monitoring_config.enable_job_monitoring:
            actions.append(self._build_python_load_action())
            actions.append(
                self._build_jobs_stats_write_action(catalog, schema)
            )

        # 4. Materialized Views
        st_fqn = f"{catalog}.{schema}.{self.monitoring_config.streaming_table}"

        if self.monitoring_config.materialized_views is not None:
            # User-specified MVs (can be empty list = no MVs)
            for mv_config in self.monitoring_config.materialized_views:
                mv_sql = self._resolve_mv_sql(
                    mv_config.name, mv_config.sql, mv_config.sql_path
                )
                actions.append(
                    self._build_mv_action(mv_config.name, mv_sql, catalog, schema)
                )
        else:
            # Default: events summary + pipeline run summary MVs
            default_sql = self._get_default_mv_sql(st_fqn)
            actions.append(
                self._build_mv_action(
                    "events_summary", default_sql, catalog, schema
                )
            )

        # Build FlowGroup
        fg = FlowGroup(
            pipeline=self.pipeline_name,
            flowgroup="monitoring",
            actions=actions,
        )
        fg._synthetic = True

        if self.monitoring_config.enable_job_monitoring:
            fg._auxiliary_files[JOBS_STATS_MODULE_PATH] = _load_jobs_stats_source()

        return fg
