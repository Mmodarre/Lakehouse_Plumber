"""The stored preference and the inspection surface behind ``lhp telemetry``.

``set_user_enabled`` is the write behind ``lhp telemetry on|off`` and the
package's ONE entry point allowed to raise. The rest read the state file or
the spool for ``status``/``show`` and the update hint, and never raise.
State-file writes take the recording lock from ``_client`` so a concurrent
``record`` cannot lose the user's choice to a read-modify-write race.
"""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import Any, Dict, List, Optional

from lhp.telemetry import _spool, _store
from lhp.telemetry._client import _STATE
from lhp.telemetry._environment import (
    Environ,
    ci_vendor,
    lhp_version,
    resolve_environ,
)
from lhp.telemetry._paths import config_dir, spool_path, state_path
from lhp.telemetry._update_check import is_newer, update_check_disabled

logger = logging.getLogger(__name__)


def set_user_enabled(enabled: bool, environ: Environ = None) -> Path:
    """Persist the choice behind ``lhp telemetry on|off``; returns the state path.

    Writes ``enabled`` only: an existing install id is kept, the spool is
    left alone, and no id is minted. A failed write propagates as the plain
    ``OSError`` so the command can tell the user it did not take.
    """
    cfg = config_dir(resolve_environ(environ))
    with _STATE.lock:
        state = _store.read_state(cfg) or _store.StateFile(
            created_at=_store.utc_now_iso()
        )
        _store.write_state(cfg, replace(state, enabled=enabled), strict=True)
    return state_path(cfg)


def mark_update_hint_shown(environ: Environ = None) -> None:
    """Stamp the state file so the hint waits another day; never creates it."""
    try:
        resolved = resolve_environ(environ)
        cfg = config_dir(resolved)
        with _STATE.lock:
            state = _store.read_state(cfg)
            if state is not None and ci_vendor(resolved) == "none":
                stamped = replace(state, update_hint_shown_at=_store.utc_now_iso())
                _store.write_state(cfg, stamped)
    except Exception:  # an unstamped hint is shown once more, nothing worse
        logger.debug("Telemetry mark_update_hint_shown failed", exc_info=True)


def newer_version_available(environ: Environ = None) -> Optional[str]:
    """The newer release the Worker last reported, or ``None``.

    Honours ``LHP_UPDATE_CHECK``; costs one state-file read; never raises.
    Consent is not consulted: the value is only there because telemetry was
    on when it arrived.
    """
    try:
        resolved = resolve_environ(environ)
        if update_check_disabled(resolved):
            return None
        state = _store.read_state(config_dir(resolved))
        latest = state.latest_known_version if state is not None else None
        return latest if latest and is_newer(latest, lhp_version()) else None
    except Exception:  # no hint is better than a wrong one
        logger.debug("Telemetry newer_version_available failed", exc_info=True)
        return None


def spool_count(environ: Environ = None) -> int:
    """How many envelopes wait in the spool."""
    try:
        return _spool.spool_count(config_dir(resolve_environ(environ)))
    except Exception:  # an uncountable spool reads as empty
        logger.debug("Telemetry spool_count failed", exc_info=True)
        return 0


def _parse_line(line: str) -> Optional[Dict[str, Any]]:
    try:
        return json.loads(line)  # type: ignore[no-any-return]
    except ValueError:  # a corrupt line is not worth failing the listing
        logger.debug("Skipping an unparseable spooled telemetry line")
        return None


def spooled_events(limit: int, environ: Environ = None) -> List[Dict[str, Any]]:
    """The newest ``limit`` spooled envelopes as dicts; corrupt lines are skipped."""
    try:
        lines = _spool.read_lines(spool_path(config_dir(resolve_environ(environ))))
    except Exception:  # an unreadable spool shows as empty
        logger.debug("Telemetry spooled_events failed", exc_info=True)
        return []
    newest = lines[-limit:] if limit > 0 else []
    return [event for event in map(_parse_line, newest) if event is not None]
