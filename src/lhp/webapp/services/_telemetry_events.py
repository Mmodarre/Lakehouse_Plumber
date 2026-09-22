"""Web-IDE hook helpers: session attribution and the ``web.run`` builder.

The routers, middleware and run recorder call these thin functions instead
of touching :class:`~lhp.webapp.services.telemetry_sessions.WebSessionRegistry`
directly, so the three gates every hook must pass live in one place:
telemetry is enabled for this process (``app.state.telemetry_enabled``), the
registry is wired (``app.state.web_sessions``), and the request carries a
well-formed session id. Any gate failing makes the hook a no-op; no helper
here raises into a request.

The session id is read from the ``X-LHP-Session`` header, or from the
``session`` query parameter for ``EventSource`` requests that cannot set
headers. A value that is not a lowercase uuid is treated as absent and is
never logged, so a malformed or hostile header cannot reach the logs.

``web.run`` props are the frozen ``lhp.telemetry.WebRunProps`` built from the
recorder's terminal outcome; ``error_code`` is forwarded only when
``lhp.telemetry.is_lhp_code`` recognises it, never a message.
"""

from __future__ import annotations

import asyncio
import dataclasses
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Literal, Optional

from lhp import telemetry
from lhp.telemetry import WebRunProps
from lhp.webapp.schemas.assistant import _PROVIDER_MODES
from lhp.webapp.services import telemetry_sessions
from lhp.webapp.services.file_kinds import classify_path
from lhp.webapp.services.telemetry_sessions import WebSessionRegistry, deliver_event

if TYPE_CHECKING:
    from fastapi import FastAPI
    from starlette.requests import Request

    # Type-only: the recorder imports this module at runtime to emit, so the
    # reverse edge must never be a runtime import.
    from lhp.webapp.services.run_recorder import _TerminalOutcome

logger = logging.getLogger(__name__)

#: ``web.run`` prop keys in wire order (DESIGN §E5).
WEB_RUN_PROP_KEYS: tuple[str, ...] = (
    "session_id",
    "kind",
    "trigger",
    "env_class",
    "sandbox",
    "pipeline_filter",
    "bundle_enabled",
    "duration_ms",
    "success",
    "aborted",
    "error_code",
    "error_count",
    "warning_count",
    "files_written",
)

FileOp = Literal["created", "updated", "deleted"]

# Assistant provider/mode values are bounded to the executor schema's own
# vocabulary; anything else collapses to ``other`` so a hand-edited config
# can never put a free-form string on the wire.
_ASSISTANT_PROVIDERS = frozenset(_PROVIDER_MODES)
_ASSISTANT_MODES = frozenset(
    mode for modes in _PROVIDER_MODES.values() for mode in modes
)
_OTHER = "other"


@dataclass(frozen=True)
class RunTelemetryContext:
    """What the router knew about a run that the recorder's ``finally`` does not.

    Built by :func:`run_context` when a run is accepted and threaded through
    the recorder unchanged. ``pipeline_filter`` records only THAT a filter was
    applied. ``registry`` is carried here because the recorder holds no
    request or app handle to look it up from.
    """

    session_id: str
    trigger: str
    sandbox: bool
    pipeline_filter: bool
    bundle_enabled: Optional[bool]
    registry: WebSessionRegistry


def build_web_run_props(
    ctx: RunTelemetryContext,
    kind: str,
    env_class: str,
    outcome: Optional[_TerminalOutcome],
    duration_ms: int,
) -> dict[str, Any]:
    """Render one run as ``web.run`` props through the frozen wire type.

    ``outcome is None`` means the stream ended without a terminal frame
    (client disconnect or upstream crash) and is reported as ``aborted`` with
    zero counts. Otherwise a total the terminal reported wins; a terminal
    without one (a generate result, an error frame) falls back to the
    stream's ``PipelineFailed`` / ``WarningEmitted`` tallies, and a failed
    run with no tallied failure counts as one error.
    """
    summary: Mapping[str, Any] = {} if outcome is None else outcome.summary
    success = bool(summary.get("success", False))
    props = WebRunProps(
        session_id=ctx.session_id,
        kind=kind,
        trigger=ctx.trigger,
        env_class=env_class,
        sandbox=ctx.sandbox,
        pipeline_filter=ctx.pipeline_filter,
        bundle_enabled=ctx.bundle_enabled,
        duration_ms=duration_ms,
        success=success,
        aborted=outcome is None,
        error_code=_lhp_error_code(summary.get("error_code")),
        error_count=_error_count(summary, outcome, success),
        warning_count=_warning_count(summary, outcome),
        files_written=_optional_count(summary.get("total_files_written")),
    )
    return dataclasses.asdict(props)


def _lhp_error_code(value: object) -> Optional[str]:
    return value if telemetry.is_lhp_code(value) else None


def _optional_count(value: object) -> Optional[int]:
    # ``bool`` is excluded explicitly: it is an ``int`` subclass.
    if isinstance(value, int) and not isinstance(value, bool):
        return value
    return None


def _error_count(
    summary: Mapping[str, Any], outcome: Optional[_TerminalOutcome], success: bool
) -> int:
    # An absent, ``None`` or non-integer total is no report at all; it must
    # not mask the tallied count.
    total = _optional_count(summary.get("total_errors"))
    if total is not None:
        return total
    if outcome is None:
        return 0
    if outcome.failed_pipelines:
        return outcome.failed_pipelines
    return 0 if success else 1


def _warning_count(
    summary: Mapping[str, Any], outcome: Optional[_TerminalOutcome]
) -> int:
    total = _optional_count(summary.get("total_warnings"))
    if total is not None:
        return total
    return 0 if outcome is None else outcome.warnings_seen


# -- request attribution ------------------------------------------------------


def session_id_from(request: Request) -> Optional[str]:
    """The request's session id, or ``None`` when absent or malformed.

    The header wins over the query parameter. The raw value is validated
    before anything else touches it and is never logged.
    """
    raw = request.headers.get(telemetry_sessions.SESSION_HEADER)
    if raw is None:
        raw = request.query_params.get(telemetry_sessions.SESSION_QUERY_PARAM)
    if raw is None or not telemetry_sessions.is_session_id(raw):
        return None
    return raw


def session_for(request: Request) -> Optional[tuple[WebSessionRegistry, str]]:
    """The registry and session id a hook should count against, if any.

    ``None`` when telemetry is off for this process, the registry is not
    wired, the request carries no valid id, or the registry refuses the id
    (capacity). A returned id is already touched into existence.
    """
    state = request.app.state
    if not getattr(state, "telemetry_enabled", False):
        return None
    registry: Optional[WebSessionRegistry] = getattr(state, "web_sessions", None)
    if registry is None:
        return None
    sid = session_id_from(request)
    if sid is None or not registry.touch(sid):
        return None
    return registry, sid


def run_context(
    request: Request,
    *,
    trigger: str,
    sandbox: bool,
    pipeline_filter: bool,
    bundle_enabled: Optional[bool],
) -> Optional[RunTelemetryContext]:
    """Attribute a run to the request's session; ``None`` when not counted.

    The router passes ``pipeline_filter=body.pipeline is not None`` — the
    flag, never the pipeline name.
    """
    attributed = session_for(request)
    if attributed is None:
        return None
    registry, sid = attributed
    return RunTelemetryContext(
        session_id=sid,
        trigger=trigger,
        sandbox=sandbox,
        pipeline_filter=pipeline_filter,
        bundle_enabled=bundle_enabled,
        registry=registry,
    )


def count_file_mutation(request: Request, path: str, op: FileOp) -> None:
    """Count a file write or delete by its :class:`FileKind`, never by path."""
    attributed = session_for(request)
    if attributed is None:
        return
    registry, sid = attributed
    registry.count_file(sid, classify_path(path).value, op)


def record_run(
    ctx: RunTelemetryContext,
    project_root: Path,
    kind: str,
    env: str,
    outcome: Optional[_TerminalOutcome],
    duration_ms: int,
) -> None:
    """Count the run on its session and deliver one ``web.run`` event.

    Runs on a worker thread from the recorder's ``finally``
    (``asyncio.to_thread``): ``env_class`` reads ``databricks.yml`` from disk.
    """
    env_class = telemetry.env_class(project_root, env)
    ctx.registry.count_run(ctx.session_id, kind, ctx.trigger, ctx.sandbox)
    props = build_web_run_props(ctx, kind, env_class, outcome, duration_ms)
    deliver_event(
        ctx.registry.sink,
        ctx.registry.flush,
        "web.run",
        project_root=project_root,
        props=props,
    )


def mark_assistant(request: Request, provider: object, mode: object) -> None:
    """Mark the session as having used the assistant, with bounded provider/mode.

    The values come from a stored config that may hold any JSON value, so a
    non-string is ``other`` before any membership test can hash it.
    """
    attributed = session_for(request)
    if attributed is None:
        return
    registry, sid = attributed
    registry.mark_assistant(
        sid, _bounded(provider, _ASSISTANT_PROVIDERS), _bounded(mode, _ASSISTANT_MODES)
    )


def _bounded(value: object, vocabulary: frozenset[str]) -> str:
    return value if isinstance(value, str) and value in vocabulary else _OTHER


async def on_sse_disconnect(app: FastAPI, sid: str) -> None:
    """Grace task: end ``sid`` as ``disconnect`` unless the tab reconnects first.

    Scheduled by the SSE handler when
    :meth:`WebSessionRegistry.sse_disconnected` reports the last connection
    gone. The task registers itself so a reconnect (``sse_connected``) or
    shutdown can cancel the sleep; the final ``emit`` re-checks for a live
    connection under the lock, so a reconnect that slipped past the cancel is
    still honoured.
    """
    registry: Optional[WebSessionRegistry] = getattr(app.state, "web_sessions", None)
    if registry is None:
        return
    task = asyncio.current_task()
    if task is not None:
        registry.grace_started(sid, task)
    await asyncio.sleep(telemetry_sessions.SSE_GRACE_SECONDS)
    await asyncio.to_thread(registry.emit, sid, "disconnect")
