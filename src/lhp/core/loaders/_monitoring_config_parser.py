"""Parse and validate ``monitoring`` section of lhp.yaml."""

import logging
import re
from pathlib import Path
from typing import Any, Dict, Optional

from lhp.errors import ErrorFactory, codes
from lhp.models import (
    EventLogConfig,
    MonitoringConfig,
    MonitoringMaterializedViewConfig,
)

logger = logging.getLogger(__name__)

_SUBSTITUTION_TOKEN_PATTERN = re.compile(r"\$\{[^}]+\}")


def parse_monitoring_config(
    monitoring_data: Any,
    event_log_config: Optional[EventLogConfig],
    project_root: Path,
) -> MonitoringConfig:
    """Parse the ``monitoring`` mapping; ``monitoring: {}`` yields all defaults."""
    if monitoring_data is None:
        monitoring_data = {}

    if not isinstance(monitoring_data, dict):
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Invalid monitoring configuration",
            details=f"monitoring must be a mapping, got {type(monitoring_data).__name__}",
            suggestions=[
                "Define monitoring as a YAML mapping",
                "Example: monitoring:\n  pipeline_name: my_monitor",
                "Use 'monitoring: {}' for all defaults",
            ],
        )

    mv_configs = None
    raw_mvs = monitoring_data.get("materialized_views")
    if raw_mvs is not None:
        if not isinstance(raw_mvs, list):
            raise ErrorFactory.config_error(
                codes.CFG_008,
                title="Invalid monitoring materialized_views",
                details="materialized_views must be a list of view definitions",
                suggestions=[
                    "Define materialized_views as a YAML list",
                    "Example:\n  materialized_views:\n    - name: events_summary\n      sql: 'SELECT ...'",
                ],
            )
        mv_configs = []
        for mv_data in raw_mvs:
            if not isinstance(mv_data, dict):
                raise ErrorFactory.config_error(
                    codes.CFG_008,
                    title="Invalid materialized view entry",
                    details=f"Each materialized_view must be a mapping, got {type(mv_data).__name__}",
                    suggestions=[
                        "Define each view with at least a 'name' field",
                        "Example:\n  - name: events_summary\n    sql: 'SELECT ...'",
                    ],
                )
            mv_configs.append(
                MonitoringMaterializedViewConfig(
                    name=mv_data.get("name", ""),
                    sql=mv_data.get("sql"),
                    sql_path=mv_data.get("sql_path"),
                )
            )

    max_streams = monitoring_data.get("max_concurrent_streams", 10)
    if isinstance(max_streams, bool) or not isinstance(max_streams, int):
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Invalid monitoring max_concurrent_streams",
            details=(
                f"max_concurrent_streams must be an integer in the range 1..20, "
                f"got {type(max_streams).__name__}: {max_streams!r}"
            ),
            suggestions=[
                "Set max_concurrent_streams to an integer between 1 and 20",
                "Example:\n  monitoring:\n    max_concurrent_streams: 10",
            ],
        )
    if max_streams < 1 or max_streams > 20:
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Invalid monitoring max_concurrent_streams",
            details=(
                f"max_concurrent_streams must be in the range 1..20, got {max_streams}"
            ),
            suggestions=[
                "Set max_concurrent_streams to an integer between 1 and 20",
                "The upper bound matches the databricks-sdk default connection-pool "
                "size; beyond it, extra worker threads only block",
                "Example:\n  monitoring:\n    max_concurrent_streams: 10",
            ],
        )

    try:
        config = MonitoringConfig(
            enabled=monitoring_data.get("enabled", True),
            pipeline_name=monitoring_data.get("pipeline_name"),
            catalog=monitoring_data.get("catalog"),
            schema=monitoring_data.get("schema"),
            streaming_table=monitoring_data.get(
                "streaming_table", "all_pipelines_event_log"
            ),
            checkpoint_path=monitoring_data.get("checkpoint_path", ""),
            job_config_path=monitoring_data.get("job_config_path"),
            max_concurrent_streams=monitoring_data.get("max_concurrent_streams", 10),
            materialized_views=mv_configs,
            enable_job_monitoring=monitoring_data.get("enable_job_monitoring", False),
        )
    except Exception as e:
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Error parsing monitoring configuration",
            details=f"Failed to parse monitoring configuration: {e}",
            suggestions=[
                "Check monitoring field types: enabled (bool), pipeline_name (string)",
                "catalog and schema must be strings",
            ],
        ) from e

    _validate_monitoring_config(config, event_log_config, project_root)
    return config


def _validate_monitoring_config(
    config: MonitoringConfig,
    event_log_config: Optional[EventLogConfig],
    project_root: Path,
) -> None:
    """When enabled: event_log must be present and enabled, checkpoint_path and job_config_path must be set (job_config_path existence check is deferred when it contains substitution tokens). Materialized view names must be unique; ``sql`` and ``sql_path`` are mutually exclusive."""
    if not config.enabled:
        return

    if not event_log_config or not event_log_config.enabled:
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Monitoring requires event_log",
            details=(
                "monitoring is enabled but event_log is either missing or disabled. "
                "The monitoring pipeline needs event_log tables to function."
            ),
            suggestions=[
                "Add an event_log section to lhp.yaml with catalog and schema",
                "Or set 'monitoring: { enabled: false }' to disable monitoring",
                "Example:\n  event_log:\n    catalog: my_catalog\n    schema: _meta",
            ],
        )

    if not config.checkpoint_path:
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Monitoring checkpoint_path is required",
            details=(
                "monitoring.checkpoint_path must be set when monitoring is enabled. "
                "Each streaming query needs a unique checkpoint directory."
            ),
            suggestions=[
                "Add checkpoint_path to your monitoring config in lhp.yaml",
                "Example:\n  monitoring:\n    checkpoint_path: "
                "/Volumes/catalog/schema/checkpoints/event_logs",
            ],
        )

    if not config.job_config_path:
        raise ErrorFactory.config_error(
            codes.CFG_008,
            title="Monitoring job_config_path is required",
            details=(
                "monitoring.job_config_path must be set when monitoring is enabled. "
                "It points to a dedicated YAML file describing the monitoring "
                "workflow job (cluster, tags, notifications, schedule, etc.)."
            ),
            suggestions=[
                "Add job_config_path to your monitoring config in lhp.yaml",
                "Example:\n  monitoring:\n    job_config_path: "
                "config/monitoring_job_config.yaml",
                "Run 'lhp init <project>' to scaffold a starter config file",
            ],
        )

    # Defer the existence check when the path contains substitution tokens
    # (e.g. ${env}). Tokens can only be resolved once the CLI selects an
    # environment — orchestrator.finalize_monitoring_artifacts re-checks
    # existence after substitution.
    if not _SUBSTITUTION_TOKEN_PATTERN.search(config.job_config_path):
        job_config_file = project_root / config.job_config_path
        if not job_config_file.is_file():
            raise ErrorFactory.config_error(
                codes.CFG_008,
                title="Monitoring job_config file not found",
                details=(
                    f"monitoring.job_config_path points to '{config.job_config_path}', "
                    f"but no file exists at {job_config_file}."
                ),
                suggestions=[
                    "Create the file at the configured path",
                    "Or update monitoring.job_config_path to a valid location",
                    "Paths are resolved relative to the project root",
                ],
            )

    if config.materialized_views:
        seen_names: Dict[str, int] = {}
        for i, mv in enumerate(config.materialized_views):
            if not mv.name:
                raise ErrorFactory.config_error(
                    codes.CFG_008,
                    title="Materialized view missing name",
                    details=f"Materialized view at index {i} has no 'name' field",
                    suggestions=[
                        "Add a 'name' field to each materialized view",
                        "Example:\n  - name: events_summary\n    sql: 'SELECT ...'",
                    ],
                )

            if mv.name in seen_names:
                raise ErrorFactory.config_error(
                    codes.CFG_008,
                    title="Duplicate materialized view name",
                    details=(
                        f"Materialized view name '{mv.name}' appears at "
                        f"index {seen_names[mv.name]} and {i}"
                    ),
                    suggestions=[
                        "Each materialized view must have a unique name",
                    ],
                )
            seen_names[mv.name] = i

            if mv.sql and mv.sql_path:
                raise ErrorFactory.config_error(
                    codes.CFG_008,
                    title="Ambiguous materialized view SQL source",
                    details=(
                        f"Materialized view '{mv.name}' specifies both 'sql' and 'sql_path'. "
                        f"Only one is allowed."
                    ),
                    suggestions=[
                        "Use 'sql' for inline SQL or 'sql_path' for external file, not both",
                    ],
                )
