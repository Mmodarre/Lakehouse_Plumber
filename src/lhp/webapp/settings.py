"""Webapp runtime settings, resolved from environment variables.

The ``lhp web`` CLI sets ``LHP_WEBAPP_*`` env vars before launching uvicorn with
a factory string; uvicorn re-imports the app in a fresh process and may pass no
arguments, so env vars are the only handoff channel for these settings.
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional
from urllib.parse import urlsplit

# Host is intentionally NOT configurable. Binding the local IDE to loopback is a
# security property (no exposure beyond the developer's machine), not a setting.
HOST = "127.0.0.1"

_DEFAULT_PORT = 8000
_DEFAULT_LOG_LEVEL = "info"

_ENV_PROJECT_ROOT = "LHP_WEBAPP_PROJECT_ROOT"
_ENV_PORT = "LHP_WEBAPP_PORT"
_ENV_LOG_LEVEL = "LHP_WEBAPP_LOG_LEVEL"
_ENV_TOKEN = "LHP_WEBAPP_TOKEN"
_ENV_ASSISTANT_URL = "LHP_WEBAPP_ASSISTANT_URL"

# The Omnigent REST API is unauthenticated; a non-loopback assistant URL would
# send project content to another machine, so only these hosts are accepted.
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


@dataclass(frozen=True)
class WebappSettings:
    """Immutable webapp configuration.

    ``token`` is the per-session secret minted by the ``lhp web`` command; when
    ``None`` (unset or empty env var) the token guard middleware is a no-op.

    ``assistant_url`` is the base URL of the user-managed local Omnigent
    daemon; when ``None`` callers default to probing ``http://127.0.0.1:6767``.
    """

    project_root: Path = field(default_factory=lambda: Path.cwd().resolve())
    port: int = _DEFAULT_PORT
    log_level: str = _DEFAULT_LOG_LEVEL
    token: Optional[str] = None
    assistant_url: Optional[str] = None


def get_settings() -> WebappSettings:
    """Build :class:`WebappSettings` from the ``LHP_WEBAPP_*`` environment.

    Raises:
        ValueError: if ``LHP_WEBAPP_PORT`` is set but not a valid integer, or
            if ``LHP_WEBAPP_ASSISTANT_URL`` is set but its host is not
            loopback.
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

    # Empty string means "no token" so an accidentally blank env var cannot
    # create an app whose guard compares against "".
    token = os.environ.get(_ENV_TOKEN) or None

    # Empty string means "not configured", mirroring the token handling.
    assistant_url = os.environ.get(_ENV_ASSISTANT_URL) or None
    if assistant_url is not None and urlsplit(assistant_url).hostname not in (
        _LOOPBACK_HOSTS
    ):
        raise ValueError(
            f"{_ENV_ASSISTANT_URL} must point at a loopback host (127.0.0.1, "
            f"localhost, or ::1) because the Omnigent REST API is "
            f"unauthenticated; got {assistant_url!r}"
        )

    return WebappSettings(
        project_root=project_root,
        port=port,
        log_level=log_level,
        token=token,
        assistant_url=assistant_url,
    )
