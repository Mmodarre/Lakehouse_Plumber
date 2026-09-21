"""The JSON state file: the install id, the user's flag and the version stamps.

Every handle is opened and closed inside the call that needs it, the config
directory is created on the first write only, and every failure is logged at
DEBUG and swallowed — except the strict write behind ``lhp telemetry on|off``,
whose ``OSError`` must reach the user. ``write_private`` is the one atomic,
private-mode file write in the package; the spool reuses it.

Timestamps are minted here because every stamp the state file carries must
parse back with ``datetime.fromisoformat``: one formatter keeps the read and
write sides in agreement.
"""

from __future__ import annotations

import json
import logging
import os
import uuid
from dataclasses import asdict, dataclass, fields, replace
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Tuple

from lhp.telemetry._paths import state_path

logger = logging.getLogger(__name__)

_DIR_MODE = 0o700
_FILE_MODE = 0o600
_FIELD_TYPES: Dict[str, type] = {"enabled": bool, "schema_version": int}

InstallEvents = List[Tuple[str, Dict[str, Any]]]


@dataclass(frozen=True)
class StateFile:
    """The persisted per-user, per-machine telemetry record (ISO stamps)."""

    schema_version: int = 1
    install_id: Optional[str] = None
    enabled: bool = True
    created_at: Optional[str] = None
    last_version_seen: Optional[str] = None
    latest_known_version: Optional[str] = None
    latest_checked_at: Optional[str] = None
    update_hint_shown_at: Optional[str] = None
    server_disabled_until: Optional[str] = None


def utc_now_iso(offset: Optional[timedelta] = None) -> str:
    """The current UTC time, plus ``offset``, as ``YYYY-MM-DDTHH:MM:SS.mmmZ``."""
    stamp = datetime.now(timezone.utc) + (offset or timedelta(0))
    return stamp.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _state_from_document(document: Mapping[str, Any]) -> StateFile:
    """Unknown keys and wrongly typed values fall back to the defaults, so a
    hand-edited or future-schema file degrades instead of failing."""
    values = {
        f.name: document[f.name]
        for f in fields(StateFile)
        if isinstance(document.get(f.name), _FIELD_TYPES.get(f.name, str))
    }
    return StateFile(**values)


def read_state(cfg: Path) -> Optional[StateFile]:
    """The state file, or ``None`` when absent, unreadable or not a JSON object."""
    path = state_path(cfg)
    try:
        if not path.is_file():
            return None
        document = json.loads(path.read_text("utf-8"))
    except (OSError, ValueError):  # unreadable or corrupt counts as absent
        logger.debug("Could not read the telemetry state file", exc_info=True)
        return None
    if not isinstance(document, dict):
        logger.debug("The telemetry state file is not a JSON object; ignoring it")
        return None
    return _state_from_document(document)


def write_private(cfg: Path, path: Path, data: bytes) -> None:
    """Replace ``path`` under ``cfg`` atomically, with private modes; may raise.

    Directories are created one level at a time because ``mkdir(parents=True)``
    applies ``mode`` to the leaf only, and both levels hold private data. The
    temp file is removed on every exit; after a successful rename it is gone.
    """
    cfg.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
    path.parent.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, _FILE_MODE)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(data)
        os.replace(tmp, path)
    finally:
        tmp.unlink(missing_ok=True)


def write_state(cfg: Path, state: StateFile, *, strict: bool = False) -> bool:
    """Persist ``state`` atomically; ``True`` on success.

    Inert by default. ``strict`` lets the ``OSError`` propagate untouched —
    errno and filename intact — for the one caller that must tell the user a
    write did not take.
    """
    try:
        document = json.dumps(asdict(state), indent=2).encode("utf-8")
        write_private(cfg, state_path(cfg), document)
    except OSError:
        if strict:
            raise
        logger.debug("Could not write the telemetry state file", exc_info=True)
        return False
    return True


def ensure_install(
    cfg: Path, state: Optional[StateFile], version: str, now: str
) -> Tuple[Optional[StateFile], InstallEvents]:
    """Mint the install id or note an upgrade, persisting the change.

    Returns the state to record under and the install events — the state
    file's own lifecycle — to spool ahead of the triggering event. A ``None``
    state means the write failed: the config directory is unusable and the
    run must stay inert.
    """
    events: InstallEvents = []
    if state is None or state.install_id is None:
        base = state or StateFile(created_at=now)
        state = replace(base, install_id=str(uuid.uuid4()), last_version_seen=version)
        events.append(("install.first_seen", {}))
    elif state.last_version_seen != version:
        if state.last_version_seen:
            events.append(
                ("install.upgraded", {"previous_version": state.last_version_seen})
            )
        state = replace(state, last_version_seen=version)
    else:
        return state, events
    return (state if write_state(cfg, state) else None), events
