"""Parse and validate ``event_log`` section of lhp.yaml."""

import logging
from typing import Any

from ...errors import ErrorCategory, LHPError
from lhp.models import EventLogConfig

logger = logging.getLogger(__name__)


def parse_event_log_config(event_log_data: Any) -> EventLogConfig:
    """Parse and validate the ``event_log`` mapping into an :class:`EventLogConfig`."""
    if not isinstance(event_log_data, dict):
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="006",
            title="Invalid event_log configuration",
            details=f"event_log must be a mapping, got {type(event_log_data).__name__}",
            suggestions=[
                "Define event_log as a YAML mapping with keys: enabled, catalog, schema, name_prefix, name_suffix",
                "Example: event_log:\\n  catalog: my_catalog\\n  schema: _meta",
            ],
        )

    try:
        config = EventLogConfig(
            enabled=event_log_data.get("enabled", True),
            catalog=event_log_data.get("catalog"),
            schema=event_log_data.get("schema"),
            name_prefix=event_log_data.get("name_prefix", ""),
            name_suffix=event_log_data.get("name_suffix", ""),
        )
    except Exception as e:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="006",
            title="Error parsing event_log configuration",
            details=f"Failed to parse event_log configuration: {e}",
            suggestions=[
                "Check event_log field types: enabled (bool), catalog (string), schema (string)",
                "name_prefix and name_suffix must be strings",
            ],
        ) from e

    _validate_event_log_config(config)
    return config


def _validate_event_log_config(config: EventLogConfig) -> None:
    """When enabled, both ``catalog`` and ``schema`` must be provided."""
    if not config.enabled:
        return

    if not config.catalog or not config.schema_:
        missing = []
        if not config.catalog:
            missing.append("catalog")
        if not config.schema_:
            missing.append("schema")
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="007",
            title="Incomplete event_log configuration",
            details=(
                f"event_log is enabled but missing required fields: {', '.join(missing)}. "
                f"Both 'catalog' and 'schema' are required when event_log is enabled."
            ),
            suggestions=[
                "Add the missing fields to event_log in lhp.yaml",
                "Or set 'enabled: false' to disable project-level event logging",
                "Example: event_log:\\n  catalog: my_catalog\\n  schema: _meta",
            ],
        )
