"""Webapp runtime settings, resolved from environment variables.

The ``lhp web`` CLI sets ``LHP_WEBAPP_*`` env vars before launching uvicorn with
a factory string; uvicorn re-imports the app in a fresh process and may pass no
arguments, so env vars are the only handoff channel for these settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path

# Host is intentionally NOT configurable. Binding the local IDE to loopback is a
# security property (no exposure beyond the developer's machine), not a setting.
HOST = "127.0.0.1"

_DEFAULT_PORT = 8000
_DEFAULT_LOG_LEVEL = "info"

_ENV_PROJECT_ROOT = "LHP_WEBAPP_PROJECT_ROOT"
_ENV_PORT = "LHP_WEBAPP_PORT"
_ENV_LOG_LEVEL = "LHP_WEBAPP_LOG_LEVEL"


@dataclass(frozen=True)
class WebappSettings:
    """Immutable webapp configuration."""

    project_root: Path = field(default_factory=lambda: Path.cwd().resolve())
    port: int = _DEFAULT_PORT
    log_level: str = _DEFAULT_LOG_LEVEL


def get_settings() -> WebappSettings:
    """Build :class:`WebappSettings` from the ``LHP_WEBAPP_*`` environment.

    Raises:
        ValueError: if ``LHP_WEBAPP_PORT`` is set but not a valid integer.
    """
    project_root_env = os.environ.get(_ENV_PROJECT_ROOT)
    project_root = (
        Path(project_root_env).resolve()
        if project_root_env is not None
        else Path.cwd().resolve()
    )

    port_env = os.environ.get(_ENV_PORT)
    if port_env is not None:
        try:
            port = int(port_env)
        except ValueError as exc:
            raise ValueError(
                f"{_ENV_PORT} must be an integer, got {port_env!r}"
            ) from exc
    else:
        port = _DEFAULT_PORT

    log_level = os.environ.get(_ENV_LOG_LEVEL, _DEFAULT_LOG_LEVEL)

    return WebappSettings(
        project_root=project_root,
        port=port,
        log_level=log_level,
    )
