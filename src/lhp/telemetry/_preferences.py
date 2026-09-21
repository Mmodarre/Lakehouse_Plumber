"""The stored preference and the inspection surface behind ``lhp telemetry``.

``set_user_enabled`` is the write behind ``lhp telemetry on|off`` and the
package's ONE entry point allowed to raise. The rest read the state file or
the spool for ``status``/``show`` and the update hint, and never raise.
State-file writes take the recording lock from ``_client`` — as do the
recording path and the sender thread's settle step — so no read-modify-write
can lose the user's choice.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, replace
from pathlib import Path
from typing import Any, Dict, List, Optional

from lhp.telemetry import _spool, _store
from lhp.telemetry._client import _STATE, _inert
from lhp.telemetry._environment import (
    Environ,
    ci_vendor,
    interactive,
    lhp_version,
    resolve_environ,
)
from lhp.telemetry._paths import config_dir, spool_path, state_path
from lhp.telemetry._update_check import (
    is_newer,
    pending_update_hint,
    update_check_disabled,
)

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


@_inert()
def mark_update_hint_shown(environ: Environ = None) -> None:
    """Stamp the state file so the hint waits another day; never creates it."""
    resolved = resolve_environ(environ)
    cfg = config_dir(resolved)
    with _STATE.lock:
        state = _store.read_state(cfg)
        if state is not None and ci_vendor(resolved) == "none":
            stamped = replace(state, update_hint_shown_at=_store.utc_now_iso())
            _store.write_state(cfg, stamped)


@_inert()
def newer_version_available(environ: Environ = None) -> Optional[str]:
    """The newer release the Worker last reported, or ``None``.

    Honours ``LHP_UPDATE_CHECK``; costs one state-file read; never raises.
    Consent is not consulted: the value is only there because telemetry was
    on when it arrived.
    """
    resolved = resolve_environ(environ)
    if update_check_disabled(resolved):
        return None
    state = _store.read_state(config_dir(resolved))
    latest = state.latest_known_version if state is not None else None
    return latest if latest and is_newer(latest, lhp_version()) else None


@_inert()
def due_update_hint(environ: Environ = None) -> Optional[str]:
    """The newer release the CLI should mention on this run, or ``None``.

    Every rule of the hint in one state read: consent must resolve to
    ``send``, the run must be interactive and outside CI, ``LHP_UPDATE_CHECK``
    must not opt out, the Worker's last ``latest`` must be newer than the
    installed version, and a day must have passed since the last hint.
    """
    resolved = resolve_environ(environ)
    now = _store.utc_now_iso()
    consent, state = _store.load_consent(resolved, config_dir(resolved), now)
    if consent.mode != "send" or state is None:
        return None
    return pending_update_hint(
        asdict(state),
        current_version=lhp_version(),
        now_iso=now,
        environ=resolved,
        interactive=interactive(),
        in_ci=ci_vendor(resolved) != "none",
    )


@_inert(int)
def spool_count(environ: Environ = None) -> int:
    """How many envelopes wait in the spool."""
    return _spool.spool_count(config_dir(resolve_environ(environ)))


@_inert(list)
def spooled_events(limit: int, environ: Environ = None) -> List[Dict[str, Any]]:
    """The newest ``limit`` spooled envelopes as dicts; corrupt lines are skipped."""
    lines = _spool.read_lines(spool_path(config_dir(resolve_environ(environ))))
    events: List[Dict[str, Any]] = []
    for line in lines[-limit:] if limit > 0 else []:
        try:
            events.append(json.loads(line))
        except ValueError:  # a corrupt line is not worth failing the listing
            logger.debug("Skipping an unparseable spooled telemetry line")
    return events
