"""Where the client reads, writes and sends: config directory, files, endpoint.

Pure resolution over an injected environment mapping: nothing here touches
the filesystem or the network, so importing or calling any of these
functions never creates a directory or opens a connection. Directory
creation happens lazily in the store, on the first write.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Callable, Mapping
from urllib.parse import urlsplit

logger = logging.getLogger(__name__)

# Placeholder until the LHP-owned hostname exists, tracked by the merge-blocker
# issue "replace placeholder telemetry hostname". The ``.invalid`` TLD never
# resolves, so a build that ships with it fails closed into the spool.
DEFAULT_ENDPOINT = "https://telemetry.lakehouse-plumber.invalid/v1/events"

_DIR_NAME = "lhp"
_STATE_FILE = "telemetry.json"
_SPOOL_DIR = "telemetry"
_SPOOL_FILE = "spool.jsonl"
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})


def config_dir(
    environ: Mapping[str, str],
    *,
    os_name: str = os.name,
    home: Callable[[], Path] = Path.home,
) -> Path:
    """Resolve the directory holding the telemetry state file and spool.

    Resolution order: ``LHP_CONFIG_DIR`` verbatim (the test and
    power-user escape hatch), then the platform convention — ``%APPDATA%\\lhp``
    on Windows, ``$XDG_CONFIG_HOME/lhp`` elsewhere, falling back to
    ``~/.config/lhp``.

    ``os_name`` and ``home`` are injected so the Windows branch is reachable
    from a POSIX host without touching the real home directory.
    """
    override = environ.get("LHP_CONFIG_DIR")
    if override:
        return Path(override)

    if os_name == "nt":
        appdata = environ.get("APPDATA")
        if appdata:
            return Path(appdata) / _DIR_NAME
        return home() / "AppData" / "Roaming" / _DIR_NAME

    xdg = environ.get("XDG_CONFIG_HOME")
    # The XDG base-directory spec requires an absolute path and says a
    # relative one must be ignored as invalid.
    if xdg and Path(xdg).is_absolute():
        return Path(xdg) / _DIR_NAME

    return home() / ".config" / _DIR_NAME


def state_path(cfg: Path) -> Path:
    """Path of the JSON state file inside ``cfg``."""
    return cfg / _STATE_FILE


def spool_path(cfg: Path) -> Path:
    """Path of the JSONL spool inside ``cfg``.

    The spool lives one level down so a half-written inflight file never sits
    beside the state file the CLI reports to the user.
    """
    return cfg / _SPOOL_DIR / _SPOOL_FILE


def endpoint_allowed(endpoint: str) -> bool:
    """``https://`` to any host, or ``http://`` to a loopback host only."""
    try:
        parts = urlsplit(endpoint)
    except ValueError:  # not a URL at all
        return False
    if parts.scheme == "https":
        return bool(parts.hostname)
    return parts.scheme == "http" and parts.hostname in _LOOPBACK_HOSTS


def resolve_endpoint(environ: Mapping[str, str]) -> str:
    """The endpoint to post to: a guarded ``LHP_TELEMETRY_ENDPOINT`` or the default."""
    override = environ.get("LHP_TELEMETRY_ENDPOINT")
    if not override:
        return DEFAULT_ENDPOINT
    if endpoint_allowed(override):
        return override
    logger.debug("Ignoring LHP_TELEMETRY_ENDPOINT: only https:// or loopback http://")
    return DEFAULT_ENDPOINT
