"""Config-directory and file-path resolution for the telemetry client.

Pure path arithmetic: nothing here touches the filesystem, so importing or
calling any of these functions never creates a directory. Creation happens
lazily in the store, on the first write.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Callable, Mapping

_DIR_NAME = "lhp"
_STATE_FILE = "telemetry.json"
_SPOOL_DIR = "telemetry"
_SPOOL_FILE = "spool.jsonl"


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
