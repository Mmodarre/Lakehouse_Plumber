"""Recording and sending: ``record``, ``new_event``, ``flush``, ``effective_state``.

One data flow: resolve consent → return before any disk access when off →
read the state → capture environment and identity → build the envelope →
print it (``log``) or spool it (``send``). ``flush`` claims the spool and
hands it to the sender thread. Every entry point resolves ``environ`` to
``os.environ`` once and never raises: failures go to DEBUG.

Process state is confined to ``_SenderState``: the in-flight sender thread
and the lock that serialises state-file writes and spool claims across the
threads that record. ``_preferences`` shares the lock for its own writes.
"""

from __future__ import annotations

import json
import logging
import sys
import threading
import uuid
from dataclasses import asdict
from pathlib import Path
from typing import Any, List, Mapping, Optional, Tuple

from lhp.telemetry import _events, _sender, _spool, _store
from lhp.telemetry._consent import TelemetryState, resolve_consent
from lhp.telemetry._environment import (
    Environ,
    EnvironmentFacts,
    capture,
    lhp_version,
    resolve_environ,
)
from lhp.telemetry._identity import ProjectIdentity, read_project_identity
from lhp.telemetry._paths import config_dir

logger = logging.getLogger(__name__)

Props = Mapping[str, Any]
_Env = Mapping[str, str]
_COMPACT = (",", ":")


class _SenderState:
    """The package's one piece of process state: the in-flight sender and its lock."""

    def __init__(self) -> None:
        self.lock = threading.Lock()
        self.thread: Optional[threading.Thread] = None


_STATE = _SenderState()


def _reset_for_tests() -> None:
    """Forget the in-flight thread so a test starts without one."""
    with _STATE.lock:
        _STATE.thread = None


def load_consent(
    environ: _Env, cfg: Path, now: str
) -> Tuple[TelemetryState, Optional[_store.StateFile]]:
    """Resolve consent, reading the state file only if the environment allows."""
    loaded: List[Optional[_store.StateFile]] = []

    def reader() -> Optional[_store.StateFile]:
        loaded.append(_store.read_state(cfg))
        return loaded[0]

    consent = resolve_consent(environ, state=reader, now_iso=now)
    return consent, (loaded[0] if loaded else None)


def _envelope(
    name: str,
    props: Props,
    facts: EnvironmentFacts,
    identity: ProjectIdentity,
    install_id: Optional[str],
    now: str,
) -> _events.TelemetryEnvelope:
    """One envelope; ``EnvironmentFacts`` field names match the wire's by design."""
    return _events.TelemetryEnvelope(
        schema_version=_events.SCHEMA_VERSION,
        event_id=str(uuid.uuid4()),
        event=name,
        ts=now,
        install_id=install_id,
        project_id=identity.project_id,
        project_id_source=identity.source,
        props=props,
        **asdict(facts),
    )


def _record(name: str, root: Optional[Path], props: Props, env: _Env) -> None:
    if name not in _events.EVENT_NAMES:
        logger.debug("Refusing to record an event outside the schema's names")
        return
    now = _store.utc_now_iso()
    cfg = config_dir(env)
    with _STATE.lock:
        consent, state = load_consent(env, cfg, now)
        if not consent.enabled:
            return
        facts = capture(env)
        in_ci = facts.ci_vendor != "none"
        pending: _store.InstallEvents = []
        if consent.mode == "send" and not in_ci:
            state, pending = _store.ensure_install(cfg, state, facts.lhp_version, now)
            if state is None:
                return
        install_id = None if in_ci or state is None else state.install_id
        identity = read_project_identity(root)
        pending.append((name, props))
        built = [_envelope(n, p, facts, identity, install_id, now) for n, p in pending]
        lines = [
            json.dumps(_events.to_json_dict(e), separators=_COMPACT) for e in built
        ]
        if consent.mode == "log":
            sys.stderr.write(lines[-1] + "\n")
            return
        for line in lines:
            _spool.append_spool(cfg, line)


def record(
    name: str, *, project_root: Optional[Path], props: Props, environ: Environ = None
) -> None:
    """Record one event: print it in ``log`` mode, otherwise spool it.

    Safe to call from any thread; never raises; never touches the network —
    sending is ``flush``'s job.
    """
    try:
        _record(name, project_root, dict(props), resolve_environ(environ))
    except Exception:  # telemetry must never reach the command or the request
        logger.debug("Telemetry record failed", exc_info=True)


def new_event(
    name: str, *, project_root: Optional[Path], props: Props, environ: Environ = None
) -> Optional[_events.TelemetryEnvelope]:
    """Build the envelope ``record`` would produce, writing nothing.

    ``None`` when consent is off. An install id is reported only if the
    state file already holds one; it is never minted here.
    """
    try:
        resolved = resolve_environ(environ)
        now = _store.utc_now_iso()
        consent, state = load_consent(resolved, config_dir(resolved), now)
        if not consent.enabled:
            return None
        facts = capture(resolved)
        in_ci = facts.ci_vendor != "none"
        install_id = None if in_ci or state is None else state.install_id
        identity = read_project_identity(project_root)
        return _envelope(name, props, facts, identity, install_id, now)
    except Exception:  # a preview that cannot be built is simply absent
        logger.debug("Telemetry new_event failed", exc_info=True)
        return None


def flush(join_s: float = 0.0, environ: Environ = None) -> Optional[threading.Thread]:
    """Send the spool on a daemon thread and return that thread.

    ``None`` when consent is not ``send``, when nothing is spooled, or when a
    send is already in flight (idempotent). ``join_s > 0`` waits up to that
    long for whichever sender is running — the CLI's bound on exit latency.
    """
    try:
        resolved = resolve_environ(environ)
        cfg = config_dir(resolved)
        endpoint, version = _sender.resolve_endpoint(resolved), lhp_version()
        started: Optional[threading.Thread] = None
        with _STATE.lock:
            if load_consent(resolved, cfg, _store.utc_now_iso())[0].mode != "send":
                return None
            running = _STATE.thread
            if running is None or not running.is_alive():
                started = _sender.start_sender(cfg, endpoint=endpoint, version=version)
                _STATE.thread = running = started
        if running is not None and join_s > 0:
            running.join(join_s)
        return started
    except Exception:  # an unsent batch stays spooled for the next run
        logger.debug("Telemetry flush failed", exc_info=True)
        return None


def effective_state(environ: Environ = None) -> TelemetryState:
    """The consent ``record`` would apply right now."""
    try:
        resolved = resolve_environ(environ)
        return load_consent(resolved, config_dir(resolved), _store.utc_now_iso())[0]
    except Exception:  # undecidable consent is reported as off
        logger.debug("Telemetry effective_state failed", exc_info=True)
        return TelemetryState(enabled=False, mode="off", reason="error")
