"""Per-tab accounting for the web IDE's anonymous ``web.session`` events.

One :class:`WebSession` accumulates what a browser tab did — request
families, file mutations by kind, run buckets, assistant use and UI-surface
counts — and the :class:`WebSessionRegistry` holds every live accumulator
until the tab ends (SSE disconnect past the grace period, idle sweep or
process shutdown), at which point the record becomes one ``web.session``
event handed to the injected sink.

Every count is keyed by a bounded enum — route families, file kinds, run
buckets and ``surface.action[.via]`` labels — never by a URL, a path, a file
name or anything the user typed. The wire shape is the frozen
``lhp.telemetry.WebSessionProps`` dataclass: :func:`build_web_session_props`
constructs one and serialises it with ``dataclasses.asdict``, so the key set
cannot drift from the schema. :data:`WEB_SESSION_PROP_KEYS` restates that key
list as a literal for the drift test.

Threading: the hooks that feed the registry run on the event-loop thread
(middleware, SSE lifecycle, run recorder) and on Starlette's worker threads
(sync file handlers), so every public method takes the registry's single
lock. The sink and flush callables are always invoked OUTSIDE the lock — a
session is popped and rendered under the lock, then delivered — so a sink
that re-enters the registry cannot deadlock and a slow sink never stalls
other tabs' accounting. :meth:`WebSessionRegistry.sse_connected` cancels a
pending grace task and therefore runs on the event-loop thread, as the SSE
handler does.

The registry exposes thirteen public methods (§3.2 justification): it is the
single accounting surface for six hook sites, and each method is one counter
or one lifecycle transition. Folding them into fewer, mode-flagged methods
would hide which hook may call what.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
import re
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Optional

from lhp import telemetry
from lhp.telemetry import WebSessionProps

logger = logging.getLogger(__name__)

#: Request header carrying the per-tab session id, set by the SPA on every call.
SESSION_HEADER = "X-LHP-Session"
#: Query parameter carrying the same id where a header cannot be set (EventSource).
SESSION_QUERY_PARAM = "session"
#: Lowercase RFC 4122 text form; any other spelling counts as "no session".
SESSION_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
#: How long a tab may stay without an SSE connection before its session ends.
SSE_GRACE_SECONDS: float = 30
#: No activity for this long, with no live SSE connection, ends a session as ``idle``.
IDLE_SECONDS: float = 1800
#: Minimum spacing between opportunistic idle sweeps run by the middleware.
SWEEP_INTERVAL_SECONDS: float = 60
#: Live sessions one process tracks at most; further tabs are simply not counted.
MAX_SESSIONS = 64

#: ``web.session`` prop keys in wire order (DESIGN §E2).
WEB_SESSION_PROP_KEYS: tuple[str, ...] = (
    "session_id",
    "duration_s",
    "end_reason",
    "sse_seen",
    "requests_by_family",
    "files_created",
    "files_updated",
    "files_deleted",
    "runs",
    "dag_views",
    "lineage_views",
    "sandbox_toggles",
    "assistant_used",
    "assistant_provider",
    "assistant_mode",
    "ui",
)

#: The ``runs`` buckets, always all present so the column shape is fixed.
RUN_BUCKETS: tuple[str, ...] = (
    "validate_manual",
    "validate_auto",
    "generate",
    "sandbox",
)

#: ``telemetry.record``'s keyword-only shape: ``sink(name, *, project_root, props)``.
SessionSink = Callable[..., None]
FlushFn = Callable[[], object]

# A session shorter than this with no counter moved is noise (a tab opened and
# closed, a reload), not usage, and is dropped rather than emitted.
_EMPTY_SESSION_SECONDS = 5
_SESSION_ID_RE = re.compile(SESSION_ID_PATTERN)


def is_session_id(value: str) -> bool:
    """``True`` for a lowercase RFC 4122 text-form uuid, the only accepted spelling."""
    return _SESSION_ID_RE.fullmatch(value) is not None


@dataclass
class WebSession:
    """In-process accumulator for one browser tab — NOT a DTO.

    Mutable by design: the registry updates it in place under its lock for
    the tab's whole lifetime, and nothing here is emitted directly. The wire
    record is the frozen ``WebSessionProps`` that
    :func:`build_web_session_props` renders when the session ends. Timestamps
    are in the registry clock's timebase (monotonic seconds), never wall
    clock. ``last_activity`` alone drives the idle sweep; ``last_sse_close``
    only bounds ``duration_s``, so closing a tab never restarts its idle clock.
    """

    session_id: str
    started: float
    last_activity: float
    last_sse_close: Optional[float] = None
    sse_connections: int = 0
    sse_seen: bool = False
    grace_task: Optional[asyncio.Task[None]] = None
    requests_by_family: dict[str, int] = field(default_factory=dict)
    files_created: dict[str, int] = field(default_factory=dict)
    files_updated: dict[str, int] = field(default_factory=dict)
    files_deleted: dict[str, int] = field(default_factory=dict)
    runs: dict[str, int] = field(default_factory=lambda: dict.fromkeys(RUN_BUCKETS, 0))
    assistant_used: bool = False
    assistant_provider: Optional[str] = None
    assistant_mode: Optional[str] = None
    ui: dict[str, int] = field(default_factory=dict)

    def has_activity(self) -> bool:
        """``True`` once any counter moved; a bare SSE connection is not activity."""
        return bool(
            self.requests_by_family
            or self.files_created
            or self.files_updated
            or self.files_deleted
            or self.ui
            or self.assistant_used
            or any(self.runs.values())
        )


def build_web_session_props(
    session: WebSession, *, duration_s: int, end_reason: str
) -> dict[str, Any]:
    """Render ``session`` as ``web.session`` props through the frozen wire type.

    The derived view counts are sums over ``ui`` labels, so a surface counted
    there is never double-reported. ``dataclasses.asdict`` copies every
    mapping, so the returned props share nothing with the accumulator.
    """
    ui = session.ui
    props = WebSessionProps(
        session_id=session.session_id,
        duration_s=duration_s,
        end_reason=end_reason,
        sse_seen=session.sse_seen,
        requests_by_family=session.requests_by_family,
        files_created=session.files_created,
        files_updated=session.files_updated,
        files_deleted=session.files_deleted,
        runs=session.runs,
        dag_views=ui.get("project_map.opened", 0) + ui.get("pipeline_dag.opened", 0),
        lineage_views=ui.get("table_detail.opened", 0),
        sandbox_toggles=ui.get("sandbox_control.toggled", 0),
        assistant_used=session.assistant_used,
        assistant_provider=session.assistant_provider,
        assistant_mode=session.assistant_mode,
        ui=ui,
    )
    return dataclasses.asdict(props)


def deliver_event(
    sink: SessionSink,
    flush: FlushFn,
    name: str,
    *,
    project_root: Optional[Path],
    props: dict[str, Any],
) -> None:
    """Hand one event to ``sink`` and then ``flush``, swallowing both failures.

    Always called outside the registry lock. The production callables never
    raise, but the hook sites' contract — telemetry never reaches the request
    that triggered it — is enforced here rather than trusted.
    """
    _sink_quietly(sink, name, project_root=project_root, props=props)
    _flush_quietly(flush, name)


def _sink_quietly(
    sink: SessionSink,
    name: str,
    *,
    project_root: Optional[Path],
    props: dict[str, Any],
) -> None:
    try:
        sink(name, project_root=project_root, props=props)
    except Exception:  # telemetry must never surface in the request that caused it
        logger.debug(f"telemetry: {name} sink failed", exc_info=True)


def _flush_quietly(flush: FlushFn, name: str) -> None:
    try:
        flush()
    except Exception:  # same contract as the sink
        logger.debug(f"telemetry: flush after {name} failed", exc_info=True)


def _bump(counter: dict[str, int], key: str) -> None:
    counter[key] = counter.get(key, 0) + 1


def _run_bucket(kind: str, trigger: str) -> Optional[str]:
    """The ``runs`` bucket a run of ``kind`` falls into; ``None`` for unknown kinds."""
    if kind == "generate":
        return "generate"
    if kind == "validate":
        return "validate_auto" if trigger == "auto" else "validate_manual"
    return None


def _file_counter(session: WebSession, op: str) -> Optional[dict[str, int]]:
    if op == "created":
        return session.files_created
    if op == "updated":
        return session.files_updated
    if op == "deleted":
        return session.files_deleted
    return None


class WebSessionRegistry:
    """Live :class:`WebSession` records, keyed by session id.

    Construct once per app process with the telemetry client's ``record`` and
    ``flush`` (the defaults) and keep it on ``app.state.web_sessions``; tests
    inject a list sink and a fake clock. ``project_root`` is attached to every
    ``web.session`` event and must be the served project only when one is
    actually loaded — ``None`` otherwise, so an unloaded directory is never
    hashed into a project id.
    """

    def __init__(
        self,
        sink: Optional[SessionSink] = None,
        flush: Optional[FlushFn] = None,
        clock: Callable[[], float] = time.monotonic,
        project_root: Optional[Path] = None,
    ) -> None:
        self.sink: SessionSink = telemetry.record if sink is None else sink
        self.flush: FlushFn = telemetry.flush if flush is None else flush
        self.project_root = project_root
        self._clock = clock
        self._lock = threading.Lock()
        self._sessions: dict[str, WebSession] = {}

    # -- accounting ---------------------------------------------------------

    def touch(self, sid: str) -> bool:
        """Ensure a record exists for ``sid``; ``False`` if it cannot be tracked."""
        with self._lock:
            return self._get(sid) is not None

    def count_request(self, sid: str, family: str) -> None:
        with self._lock:
            session = self._get(sid)
            if session is not None:
                _bump(session.requests_by_family, family)

    def count_file(self, sid: str, kind: str, op: str) -> None:
        """Count one ``created`` / ``updated`` / ``deleted`` file of ``kind``."""
        with self._lock:
            session = self._get(sid)
            counter = None if session is None else _file_counter(session, op)
            if counter is not None:
                _bump(counter, kind)

    def count_run(self, sid: str, kind: str, trigger: str, sandbox: bool) -> None:
        """Count a run in its kind bucket, and in ``sandbox`` too when it was one."""
        bucket = _run_bucket(kind, trigger)
        with self._lock:
            session = self._get(sid)
            if session is None or bucket is None:
                return
            session.runs[bucket] += 1
            if sandbox:
                session.runs["sandbox"] += 1

    def mark_assistant(self, sid: str, provider: str, mode: str) -> None:
        """Record that the assistant was used; the last provider/mode wins."""
        with self._lock:
            session = self._get(sid)
            if session is None:
                return
            session.assistant_used = True
            session.assistant_provider = provider
            session.assistant_mode = mode

    def count_ui(self, sid: str, key: str) -> None:
        """Count one ``surface.action[.via]`` label."""
        with self._lock:
            session = self._get(sid)
            if session is not None:
                _bump(session.ui, key)

    # -- SSE lifecycle ------------------------------------------------------

    def sse_connected(self, sid: str) -> bool:
        """Count one more live SSE connection, cancelling any pending grace task.

        Runs on the event-loop thread (the SSE handler), which is what makes
        the cancel safe: ``asyncio.Task.cancel`` is not thread-safe.
        """
        with self._lock:
            session = self._get(sid)
            if session is None:
                return False
            session.sse_connections += 1
            session.sse_seen = True
            pending, session.grace_task = session.grace_task, None
        if pending is not None and not pending.done():
            pending.cancel()
        return True

    def sse_disconnected(self, sid: str) -> bool:
        """Count one SSE connection gone; ``True`` when it was the last one.

        ``True`` is the caller's cue to schedule the grace task. The last close
        is stamped so the session's duration ends there, not after the grace wait.
        """
        with self._lock:
            session = self._sessions.get(sid)
            if session is None or session.sse_connections == 0:
                return False
            session.sse_connections -= 1
            if session.sse_connections:
                return False
            session.last_sse_close = self._clock()
            return True

    def grace_started(self, sid: str, task: asyncio.Task[None]) -> None:
        """Remember the grace task so a reconnect or shutdown can cancel it."""
        with self._lock:
            session = self._sessions.get(sid)
            if session is not None:
                session.grace_task = task

    def pending_grace_tasks(self) -> tuple[asyncio.Task[None], ...]:
        """Grace tasks still sleeping — what shutdown must cancel first."""
        with self._lock:
            return tuple(
                session.grace_task
                for session in self._sessions.values()
                if session.grace_task is not None and not session.grace_task.done()
            )

    # -- emission -----------------------------------------------------------

    def emit(self, sid: str, reason: str) -> bool:
        """End ``sid`` and deliver its ``web.session``; ``True`` if one was sent.

        A session with a live SSE connection is never ended here — only
        :meth:`emit_all` (shutdown) does that — so a grace task that lost the
        race against a reconnect is harmless. Empty sessions are dropped.
        """
        with self._lock:
            session = self._sessions.get(sid)
            if session is None or session.sse_connections > 0:
                return False
            del self._sessions[sid]
            props = self._render(session, reason)
        if props is None:
            return False
        self._deliver(props)
        return True

    def sweep_idle(self, now: Optional[float] = None) -> int:
        """End every session idle for over ``IDLE_SECONDS`` with no live SSE.

        ``now`` is in the registry clock's timebase and defaults to the clock;
        returns how many ``web.session`` events were sent.
        """
        cutoff = (self._clock() if now is None else now) - IDLE_SECONDS
        with self._lock:
            idle = [
                sid
                for sid, session in self._sessions.items()
                if session.sse_connections == 0 and session.last_activity < cutoff
            ]
            rendered = [self._render(self._sessions.pop(sid), "idle") for sid in idle]
        return self._deliver_all(rendered)

    def emit_all(self, reason: str) -> int:
        """End every session, live or not; returns how many events were sent."""
        with self._lock:
            ended = list(self._sessions.values())
            self._sessions.clear()
            rendered = [self._render(session, reason) for session in ended]
        return self._deliver_all(rendered)

    # -- internals (lock held unless stated) --------------------------------

    def _get(self, sid: str) -> Optional[WebSession]:
        """The live record for ``sid``, created on first sight, with activity bumped.

        Refuses malformed ids and, at ``MAX_SESSIONS``, unknown ids. Nothing
        is evicted: an evicted tab would re-create itself on its next request
        and fragment its own session.
        """
        now = self._clock()
        session = self._sessions.get(sid)
        if session is not None:
            session.last_activity = now
            return session
        if not is_session_id(sid):
            return None
        if len(self._sessions) >= MAX_SESSIONS:
            logger.debug("telemetry: web session cap reached, tab not counted")
            return None
        session = WebSession(session_id=sid, started=now, last_activity=now)
        self._sessions[sid] = session
        return session

    def _render(self, session: WebSession, reason: str) -> Optional[dict[str, Any]]:
        """Props for an ended session, or ``None`` when it is dropped as empty.

        Duration runs to now for a live session, otherwise to the later of its
        last activity and its last SSE close — the grace and idle waits that
        follow are not usage.
        """
        ended = self._clock() if session.sse_connections else session.last_activity
        if session.last_sse_close is not None:
            ended = max(ended, session.last_sse_close)
        duration_s = int(ended - session.started)
        if duration_s < _EMPTY_SESSION_SECONDS and not session.has_activity():
            logger.debug(f"telemetry: dropping an empty web session ({reason})")
            return None
        return build_web_session_props(
            session, duration_s=duration_s, end_reason=reason
        )

    def _deliver(self, props: dict[str, Any]) -> None:
        """Deliver one rendered session; called with the lock released."""
        deliver_event(
            self.sink,
            self.flush,
            "web.session",
            project_root=self.project_root,
            props=props,
        )

    def _deliver_all(self, rendered: list[Optional[dict[str, Any]]]) -> int:
        """Sink every rendered session, then flush once (lock released).

        One flush hands the whole batch to a single sender thread, where a
        flush per session would find the first send in flight and skip.
        """
        delivered = 0
        for props in rendered:
            if props is not None:
                _sink_quietly(
                    self.sink,
                    "web.session",
                    project_root=self.project_root,
                    props=props,
                )
                delivered += 1
        if delivered:
            _flush_quietly(self.flush, "web.session")
        return delivered
