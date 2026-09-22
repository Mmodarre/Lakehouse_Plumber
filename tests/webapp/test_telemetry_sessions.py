"""Tests for the web-IDE telemetry session registry and its event builders.

The registry is driven with a fake monotonic clock and a list sink, so no
test reaches ``lhp.telemetry.record``, the spool or the network, and none
relies on the suite-wide ``LHP_TELEMETRY=off`` guard. Request-facing helpers
get a real ``starlette.requests.Request`` built from a bare ASGI scope whose
``app`` is a stand-in carrying only the ``app.state`` attributes the helpers
read. Async scenarios run under ``asyncio.run`` (no pytest-asyncio), as
elsewhere in this suite.
"""

from __future__ import annotations

import asyncio
import dataclasses
import threading
from dataclasses import fields
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from starlette.requests import Request

from lhp import telemetry
from lhp.telemetry import WebRunProps, WebSessionProps
from lhp.webapp.services import _telemetry_events as events
from lhp.webapp.services import telemetry_sessions as sessions
from lhp.webapp.services.run_recorder import _TerminalOutcome
from lhp.webapp.services.telemetry_sessions import WebSession, WebSessionRegistry

pytestmark = pytest.mark.webapp

SID = "0f1e2d3c-4b5a-4978-8a9b-0c1d2e3f4a5b"
SID_2 = "11111111-2222-4333-8444-555555555555"
SID_3 = "aaaaaaaa-bbbb-4ccc-8ddd-eeeeeeeeeeee"
INVALID_IDS = ("", "not-a-uuid", SID.upper(), SID + "0", "../" + SID[3:], "0" * 36)
PROJECT_ROOT = Path("/projects/example")

# The wire key lists of DESIGN §E2 and §E5, spelled out so a drift in either
# the frozen dataclasses or the module tuples is caught against a literal.
E2_KEYS = (
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
E5_KEYS = (
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


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.now = start

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class ListSink:
    """Records every ``(name, project_root, props)`` triple and counts flushes."""

    def __init__(self) -> None:
        self.events: list[tuple[str, Path | None, dict[str, Any]]] = []
        self.flushes = 0

    def __call__(
        self, name: str, *, project_root: Path | None, props: dict[str, Any]
    ) -> None:
        self.events.append((name, project_root, props))

    def flush(self) -> None:
        self.flushes += 1


def _registry(
    project_root: Path | None = PROJECT_ROOT,
) -> tuple[WebSessionRegistry, ListSink, FakeClock]:
    sink = ListSink()
    clock = FakeClock()
    registry = WebSessionRegistry(
        sink=sink, flush=sink.flush, clock=clock, project_root=project_root
    )
    return registry, sink, clock


def _app(registry: WebSessionRegistry | None, enabled: bool = True) -> SimpleNamespace:
    state = SimpleNamespace(telemetry_enabled=enabled)
    if registry is not None:
        state.web_sessions = registry
    return SimpleNamespace(state=state)


def _request(app: object, *, header: str | None = None, query: str = "") -> Request:
    headers = [(b"host", b"127.0.0.1")]
    if header is not None:
        headers.append((b"x-lhp-session", header.encode()))
    scope = {
        "type": "http",
        "method": "GET",
        "path": "/api/project",
        "headers": headers,
        "query_string": query.encode(),
        "app": app,
    }
    return Request(scope)


def _busy_session(registry: WebSessionRegistry, sid: str = SID) -> None:
    """Exercise every counter once so the emitted props are fully populated."""
    assert registry.touch(sid) is True
    registry.count_request(sid, "files.read")
    registry.count_request(sid, "files.read")
    registry.count_request(sid, "runs.validate")
    registry.count_file(sid, "flowgroup", "created")
    registry.count_file(sid, "flowgroup", "created")
    registry.count_file(sid, "preset", "updated")
    registry.count_file(sid, "other", "deleted")
    registry.count_run(sid, "validate", "manual", False)
    registry.count_run(sid, "validate", "auto", False)
    registry.count_run(sid, "generate", "manual", True)
    registry.mark_assistant(sid, "claude_sdk", "claude_subscription")
    for key in (
        "project_map.opened",
        "pipeline_dag.opened",
        "pipeline_dag.opened",
        "table_detail.opened",
        "sandbox_control.toggled",
        "sandbox_control.toggled",
        "create_flowgroup_dialog.created.template",
    ):
        registry.count_ui(sid, key)


# --- wire shape -------------------------------------------------------------


def test_web_session_prop_keys_are_exactly_the_frozen_dataclass_fields() -> None:
    assert sessions.WEB_SESSION_PROP_KEYS == E2_KEYS
    assert sessions.WEB_SESSION_PROP_KEYS == tuple(
        f.name for f in fields(WebSessionProps)
    )


def test_web_run_prop_keys_are_exactly_the_frozen_dataclass_fields() -> None:
    assert events.WEB_RUN_PROP_KEYS == E5_KEYS
    assert events.WEB_RUN_PROP_KEYS == tuple(f.name for f in fields(WebRunProps))


def test_web_session_is_a_mutable_accumulator_and_context_is_frozen() -> None:
    assert WebSession.__dataclass_params__.frozen is False
    registry, _, _ = _registry()
    ctx = events.RunTelemetryContext(
        session_id=SID,
        trigger="manual",
        sandbox=False,
        pipeline_filter=False,
        bundle_enabled=None,
        registry=registry,
    )
    with pytest.raises(dataclasses.FrozenInstanceError):
        ctx.trigger = "auto"  # type: ignore[misc]


# --- web.session emission ---------------------------------------------------


def test_emit_builds_exact_e2_props_then_sinks_and_flushes() -> None:
    registry, sink, clock = _registry()
    _busy_session(registry)
    assert registry.sse_connected(SID) is True
    clock.advance(42.7)
    assert registry.sse_disconnected(SID) is True

    assert registry.emit(SID, "disconnect") is True

    ((name, project_root, props),) = sink.events
    assert name == "web.session"
    assert project_root == PROJECT_ROOT
    assert tuple(props) == sessions.WEB_SESSION_PROP_KEYS
    assert props == {
        "session_id": SID,
        "duration_s": 42,
        "end_reason": "disconnect",
        "sse_seen": True,
        "requests_by_family": {"files.read": 2, "runs.validate": 1},
        "files_created": {"flowgroup": 2},
        "files_updated": {"preset": 1},
        "files_deleted": {"other": 1},
        "runs": {"validate_manual": 1, "validate_auto": 1, "generate": 1, "sandbox": 1},
        "dag_views": 3,
        "lineage_views": 1,
        "sandbox_toggles": 2,
        "assistant_used": True,
        "assistant_provider": "claude_sdk",
        "assistant_mode": "claude_subscription",
        "ui": {
            "project_map.opened": 1,
            "pipeline_dag.opened": 2,
            "table_detail.opened": 1,
            "sandbox_control.toggled": 2,
            "create_flowgroup_dialog.created.template": 1,
        },
    }
    assert sink.flushes == 1
    # The session is gone once emitted.
    assert registry.emit(SID, "disconnect") is False
    assert len(sink.events) == 1


def test_registry_without_a_project_root_emits_none() -> None:
    registry, sink, _ = _registry(project_root=None)
    registry.touch(SID)
    registry.count_request(SID, "project.read")

    assert registry.emit(SID, "idle") is True

    assert sink.events[0][1] is None


def test_short_sessions_without_activity_are_dropped() -> None:
    registry, sink, clock = _registry()
    # ``sse_seen`` alone is not activity.
    assert registry.sse_connected(SID) is True
    assert registry.sse_disconnected(SID) is True
    clock.advance(4.9)

    assert registry.emit(SID, "disconnect") is False

    assert sink.events == []
    assert sink.flushes == 0
    # Dropped, not retained: a second emit finds nothing.
    assert registry.emit(SID, "disconnect") is False


def test_a_session_older_than_five_seconds_is_emitted_even_without_activity() -> None:
    registry, sink, clock = _registry()
    assert registry.sse_connected(SID) is True
    clock.advance(5)
    assert registry.sse_disconnected(SID) is True

    assert registry.emit(SID, "disconnect") is True

    props = sink.events[0][2]
    assert props["duration_s"] == 5
    assert props["requests_by_family"] == {}
    assert props["runs"] == {
        "validate_manual": 0,
        "validate_auto": 0,
        "generate": 0,
        "sandbox": 0,
    }
    assert props["assistant_used"] is False
    assert props["assistant_provider"] is None


def test_a_short_session_with_any_counter_is_emitted() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)
    registry.count_ui(SID, "init_wizard.opened")

    assert registry.emit(SID, "idle") is True

    assert sink.events[0][2]["ui"] == {"init_wizard.opened": 1}


def test_emit_never_ends_a_session_with_a_live_sse_connection() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    registry.count_request(SID, "project.read")
    clock.advance(sessions.IDLE_SECONDS + 1)

    assert registry.emit(SID, "disconnect") is False
    assert registry.sweep_idle() == 0
    assert sink.events == []

    # Only shutdown ends a live session.
    assert registry.emit_all("shutdown") == 1
    assert sink.events[0][2]["end_reason"] == "shutdown"


def test_emit_all_ends_every_session_with_the_given_reason() -> None:
    registry, sink, _ = _registry()
    for sid in (SID, SID_2):
        registry.touch(sid)
        registry.count_request(sid, "project.read")

    assert registry.emit_all("shutdown") == 2

    assert sorted(
        (e[0], e[2]["session_id"], e[2]["end_reason"]) for e in sink.events
    ) == [
        ("web.session", SID, "shutdown"),
        ("web.session", SID_2, "shutdown"),
    ]
    assert sink.flushes == 1
    assert registry.emit_all("shutdown") == 0


def test_emit_all_sinks_every_session_then_flushes_once() -> None:
    registry, sink, _ = _registry()
    for sid in (SID, SID_2, SID_3):
        registry.touch(sid)
        registry.count_request(sid, "project.read")

    assert registry.emit_all("shutdown") == 3

    assert len(sink.events) == 3
    assert sink.flushes == 1


def test_a_single_emission_still_flushes_its_own_event() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)
    registry.count_request(SID, "project.read")

    assert registry.emit(SID, "idle") is True

    assert len(sink.events) == 1
    assert sink.flushes == 1


def test_sweep_idle_emits_only_idle_sessions_without_live_sse() -> None:
    registry, sink, clock = _registry()
    registry.touch(SID)
    registry.count_request(SID, "project.read")
    registry.sse_connected(SID_2)
    registry.count_request(SID_2, "project.read")
    clock.advance(sessions.IDLE_SECONDS + 1)
    registry.touch(SID_3)
    registry.count_request(SID_3, "project.read")

    assert registry.sweep_idle(clock()) == 1

    assert [(e[2]["session_id"], e[2]["end_reason"]) for e in sink.events] == [
        (SID, "idle")
    ]
    # The live and the recent sessions are still tracked.
    assert registry.emit_all("shutdown") == 2


def test_sweep_idle_defaults_to_the_registry_clock() -> None:
    registry, sink, clock = _registry()
    registry.touch(SID)
    registry.count_request(SID, "project.read")
    assert registry.sweep_idle() == 0
    clock.advance(sessions.IDLE_SECONDS + 1)

    assert registry.sweep_idle() == 1


def test_activity_resets_the_idle_clock() -> None:
    registry, _, clock = _registry()
    registry.touch(SID)
    clock.advance(sessions.IDLE_SECONDS)
    registry.count_ui(SID, "problems.opened")
    clock.advance(2)

    assert registry.sweep_idle() == 0


def test_an_sse_close_does_not_reset_the_idle_clock() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    registry.count_request(SID, "project.read")
    clock.advance(sessions.IDLE_SECONDS + 1)
    assert registry.sse_disconnected(SID) is True

    assert registry.sweep_idle() == 1

    props = sink.events[0][2]
    assert props["end_reason"] == "idle"
    assert props["duration_s"] == sessions.IDLE_SECONDS + 1


# --- session duration ----------------------------------------------------------


def test_duration_stops_at_the_last_disconnect_not_after_the_grace_wait() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    registry.count_request(SID, "project.read")
    clock.advance(8)
    assert registry.sse_disconnected(SID) is True
    clock.advance(sessions.SSE_GRACE_SECONDS)

    assert registry.emit(SID, "disconnect") is True

    assert sink.events[0][2]["duration_s"] == 8


def test_duration_of_an_idle_session_stops_at_its_last_activity() -> None:
    registry, sink, clock = _registry()
    registry.touch(SID)
    clock.advance(10)
    registry.count_request(SID, "project.read")
    clock.advance(sessions.IDLE_SECONDS + 1)

    assert registry.sweep_idle() == 1

    props = sink.events[0][2]
    assert props["end_reason"] == "idle"
    assert props["duration_s"] == 10


def test_a_session_still_connected_at_shutdown_counts_to_shutdown() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    registry.count_request(SID, "project.read")
    clock.advance(120)

    assert registry.emit_all("shutdown") == 1

    assert sink.events[0][2]["duration_s"] == 120


def test_a_request_after_the_last_disconnect_extends_the_session() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    clock.advance(5)
    assert registry.sse_disconnected(SID) is True
    clock.advance(7)
    registry.count_request(SID, "project.read")
    clock.advance(sessions.SSE_GRACE_SECONDS)

    assert registry.emit(SID, "disconnect") is True

    assert sink.events[0][2]["duration_s"] == 12


def test_a_two_second_open_and_close_tab_is_dropped() -> None:
    registry, sink, clock = _registry()
    registry.sse_connected(SID)
    clock.advance(2)
    assert registry.sse_disconnected(SID) is True
    clock.advance(sessions.SSE_GRACE_SECONDS)

    assert registry.emit(SID, "disconnect") is False

    assert sink.events == []
    assert sink.flushes == 0


# --- capacity and validation ---------------------------------------------------


def test_max_sessions_cap_refuses_new_sessions_without_evicting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sessions, "MAX_SESSIONS", 2)
    registry, sink, _ = _registry()
    for sid in (SID, SID_2):
        assert registry.touch(sid) is True
        registry.count_request(sid, "project.read")

    assert registry.touch(SID_3) is False
    registry.count_request(SID_3, "project.read")
    assert registry.sse_connected(SID_3) is False
    # Known ids keep working at the cap.
    assert registry.touch(SID) is True

    assert registry.emit(SID, "idle") is True
    # A freed slot admits the next tab.
    assert registry.touch(SID_3) is True
    registry.count_request(SID_3, "project.read")
    assert registry.emit_all("shutdown") == 2
    assert {e[2]["session_id"] for e in sink.events} == {SID, SID_2, SID_3}


@pytest.mark.parametrize("bad", INVALID_IDS)
def test_invalid_session_ids_are_ignored_everywhere(bad: str) -> None:
    registry, sink, _ = _registry()

    assert registry.touch(bad) is False
    registry.count_request(bad, "project.read")
    registry.count_file(bad, "flowgroup", "created")
    registry.count_run(bad, "validate", "manual", False)
    registry.mark_assistant(bad, "claude_sdk", "databricks")
    registry.count_ui(bad, "problems.opened")
    assert registry.sse_connected(bad) is False
    assert registry.sse_disconnected(bad) is False
    assert registry.emit(bad, "idle") is False

    assert registry.emit_all("shutdown") == 0
    assert sink.events == []


def test_count_run_maps_kind_and_trigger_onto_the_four_buckets() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)
    registry.count_run(SID, "validate", "manual", False)
    registry.count_run(SID, "validate", "auto", False)
    registry.count_run(SID, "validate", "auto", True)
    registry.count_run(SID, "generate", "manual", False)
    registry.count_run(SID, "generate", "manual", True)
    registry.count_run(SID, "deploy", "manual", True)

    registry.emit_all("shutdown")

    assert sink.events[0][2]["runs"] == {
        "validate_manual": 1,
        "validate_auto": 2,
        "generate": 2,
        "sandbox": 2,
    }


def test_count_file_ignores_unknown_operations() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)
    registry.count_file(SID, "flowgroup", "renamed")
    registry.count_file(SID, "flowgroup", "created")

    registry.emit_all("shutdown")

    props = sink.events[0][2]
    assert props["files_created"] == {"flowgroup": 1}
    assert props["files_updated"] == {}
    assert props["files_deleted"] == {}


def test_sse_connection_count_never_underflows() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)
    assert registry.sse_disconnected(SID) is False
    assert registry.sse_disconnected(SID_2) is False
    # Two connections (StrictMode double-subscribe) need two disconnects.
    assert registry.sse_connected(SID) is True
    assert registry.sse_connected(SID) is True
    assert registry.sse_disconnected(SID) is False
    assert registry.sse_disconnected(SID) is True

    registry.count_request(SID, "project.read")
    registry.emit_all("shutdown")

    assert sink.events[0][2]["sse_seen"] is True


# --- grace period ------------------------------------------------------------------


def test_reconnect_within_grace_cancels_the_pending_emission(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(sessions, "SSE_GRACE_SECONDS", 0.05)
    registry, sink, _ = _registry()
    app = _app(registry)

    async def scenario() -> None:
        assert registry.sse_connected(SID) is True
        registry.count_request(SID, "project.read")
        assert registry.sse_disconnected(SID) is True
        task = asyncio.create_task(events.on_sse_disconnect(app, SID))
        await asyncio.sleep(0)  # let the grace task register itself
        assert registry.pending_grace_tasks() == (task,)

        assert registry.sse_connected(SID) is True  # the tab came back

        with pytest.raises(asyncio.CancelledError):
            await task
        assert registry.pending_grace_tasks() == ()
        await asyncio.sleep(0.1)
        assert sink.events == []

        # The same tab closing for good ends the session after the grace.
        assert registry.sse_disconnected(SID) is True
        await events.on_sse_disconnect(app, SID)
        assert [e[0] for e in sink.events] == ["web.session"]
        assert sink.events[0][2]["end_reason"] == "disconnect"
        assert registry.pending_grace_tasks() == ()

    asyncio.run(scenario())


def test_grace_task_without_a_registry_is_a_noop() -> None:
    app = SimpleNamespace(state=SimpleNamespace(telemetry_enabled=False))
    asyncio.run(events.on_sse_disconnect(app, SID))


# --- web.run -------------------------------------------------------------------------


def test_record_run_counts_the_run_and_emits_exact_e5_props(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    seen: list[tuple[Path, str]] = []

    def fake_env_class(project_root: Path, env: str) -> str:
        seen.append((project_root, env))
        return "development"

    monkeypatch.setattr(telemetry, "env_class", fake_env_class)
    registry, sink, _ = _registry()
    ctx = events.RunTelemetryContext(
        session_id=SID,
        trigger="auto",
        sandbox=True,
        pipeline_filter=False,
        bundle_enabled=True,
        registry=registry,
    )
    outcome = _TerminalOutcome(
        status="completed",
        summary={
            "success": True,
            "total_errors": 0,
            "total_warnings": 3,
            "pipeline_count": 2,
        },
        issues=(),
    )

    events.record_run(ctx, PROJECT_ROOT, "validate", "dev", outcome, 1234)

    assert seen == [(PROJECT_ROOT, "dev")]
    ((name, project_root, props),) = sink.events
    assert name == "web.run"
    assert project_root == PROJECT_ROOT
    assert tuple(props) == events.WEB_RUN_PROP_KEYS
    assert props == {
        "session_id": SID,
        "kind": "validate",
        "trigger": "auto",
        "env_class": "development",
        "sandbox": True,
        "pipeline_filter": False,
        "bundle_enabled": True,
        "duration_ms": 1234,
        "success": True,
        "aborted": False,
        "error_code": None,
        "error_count": 0,
        "warning_count": 3,
        "files_written": None,
    }
    assert sink.flushes == 1

    registry.emit_all("shutdown")
    assert sink.events[1][2]["runs"] == {
        "validate_manual": 0,
        "validate_auto": 1,
        "generate": 0,
        "sandbox": 1,
    }


def test_record_run_generate_reports_files_written(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "env_class", lambda root, env: "none")
    registry, sink, _ = _registry()
    ctx = events.RunTelemetryContext(
        session_id=SID,
        trigger="manual",
        sandbox=False,
        pipeline_filter=True,
        bundle_enabled=False,
        registry=registry,
    )
    outcome = _TerminalOutcome(
        status="completed",
        summary={"success": True, "total_files_written": 7, "pipeline_count": 1},
        issues=(),
    )

    events.record_run(ctx, PROJECT_ROOT, "generate", "dev", outcome, 50)

    props = sink.events[0][2]
    assert props["kind"] == "generate"
    assert props["files_written"] == 7
    assert props["error_count"] == 0
    assert props["warning_count"] == 0


def test_record_run_without_an_outcome_is_aborted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(telemetry, "env_class", lambda root, env: "none")
    registry, sink, _ = _registry()
    ctx = events.RunTelemetryContext(
        session_id=SID,
        trigger="manual",
        sandbox=False,
        pipeline_filter=False,
        bundle_enabled=None,
        registry=registry,
    )

    events.record_run(ctx, PROJECT_ROOT, "validate", "dev", None, 9)

    props = sink.events[0][2]
    assert props["aborted"] is True
    assert props["success"] is False
    assert props["error_code"] is None
    assert props["error_count"] == 0
    assert props["warning_count"] == 0
    assert props["files_written"] is None
    assert props["bundle_enabled"] is None


@pytest.mark.parametrize(
    ("code", "expected"),
    [
        ("LHP-ACT-001", "LHP-ACT-001"),
        ("LHP-IO-028", "LHP-IO-028"),
        ("LHP-VAL-DUPFG", "LHP-VAL-DUPFG"),
        ("LHP-EVT-SOFT-CAP", "LHP-EVT-SOFT-CAP"),
        ("Unknown action type 'nope'", None),
        ("", None),
        (None, None),
    ],
)
def test_record_run_forwards_only_lhp_error_codes(
    monkeypatch: pytest.MonkeyPatch, code: str | None, expected: str | None
) -> None:
    monkeypatch.setattr(telemetry, "env_class", lambda root, env: "none")
    registry, sink, _ = _registry()
    ctx = events.RunTelemetryContext(
        session_id=SID,
        trigger="manual",
        sandbox=False,
        pipeline_filter=False,
        bundle_enabled=None,
        registry=registry,
    )
    outcome = _TerminalOutcome(
        status="failed", summary={"success": False, "error_code": code}, issues=()
    )

    events.record_run(ctx, PROJECT_ROOT, "generate", "dev", outcome, 1)

    props = sink.events[0][2]
    assert props["error_code"] == expected
    assert props["success"] is False
    assert props["aborted"] is False


# --- request attribution -------------------------------------------------------------


def test_session_id_from_prefers_the_header_over_the_query_param() -> None:
    app = _app(None)
    assert (
        events.session_id_from(_request(app, header=SID, query=f"session={SID_2}"))
        == SID
    )
    assert events.session_id_from(_request(app, query=f"session={SID_2}")) == SID_2
    assert events.session_id_from(_request(app)) is None


@pytest.mark.parametrize("bad", INVALID_IDS)
def test_session_id_from_rejects_malformed_ids(bad: str) -> None:
    app = _app(None)
    assert events.session_id_from(_request(app, header=bad)) is None
    assert events.session_id_from(_request(app, query=f"session={bad}")) is None


def test_session_for_requires_enabled_state_registry_and_valid_id() -> None:
    registry, _, _ = _registry()
    assert (
        events.session_for(_request(_app(registry, enabled=False), header=SID)) is None
    )
    assert events.session_for(_request(_app(None), header=SID)) is None
    assert events.session_for(_request(_app(registry), header="nope")) is None
    assert events.session_for(_request(_app(registry))) is None
    unwired = SimpleNamespace(state=SimpleNamespace())
    assert events.session_for(_request(unwired, header=SID)) is None

    assert events.session_for(_request(_app(registry), header=SID)) == (registry, SID)
    # ``session_for`` touched the record into existence.
    assert registry.emit_all("shutdown") == 0  # empty and young: dropped
    registry.touch(SID)
    registry.count_request(SID, "project.read")
    assert registry.emit_all("shutdown") == 1


def test_run_context_carries_the_registry_and_only_flags() -> None:
    registry, _, _ = _registry()
    request = _request(_app(registry), header=SID)

    ctx = events.run_context(
        request,
        trigger="auto",
        sandbox=False,
        pipeline_filter=True,
        bundle_enabled=None,
    )

    assert ctx == events.RunTelemetryContext(
        session_id=SID,
        trigger="auto",
        sandbox=False,
        pipeline_filter=True,
        bundle_enabled=None,
        registry=registry,
    )
    assert ctx is not None and ctx.registry is registry
    assert (
        events.run_context(
            _request(_app(registry, enabled=False), header=SID),
            trigger="manual",
            sandbox=False,
            pipeline_filter=False,
            bundle_enabled=None,
        )
        is None
    )
    assert (
        events.run_context(
            _request(_app(registry)),
            trigger="manual",
            sandbox=False,
            pipeline_filter=False,
            bundle_enabled=None,
        )
        is None
    )


def test_count_file_mutation_counts_by_kind_never_by_path() -> None:
    registry, sink, _ = _registry()
    request = _request(_app(registry), header=SID)

    events.count_file_mutation(request, "pipelines/bronze/orders.yaml", "created")
    events.count_file_mutation(request, "presets\\bronze.yaml", "updated")
    events.count_file_mutation(request, "notes.txt", "deleted")
    events.count_file_mutation(
        _request(_app(registry, enabled=False), header=SID), "lhp.yaml", "updated"
    )

    registry.emit_all("shutdown")
    props = sink.events[0][2]
    assert props["files_created"] == {"flowgroup": 1}
    assert props["files_updated"] == {"preset": 1}
    assert props["files_deleted"] == {"other": 1}
    assert "orders" not in repr(props)
    assert "bronze" not in repr(props)


@pytest.mark.parametrize(
    ("provider", "mode", "expected"),
    [
        ("claude_sdk", "claude_subscription", ("claude_sdk", "claude_subscription")),
        ("claude_sdk", "databricks", ("claude_sdk", "databricks")),
        ("omnigent", "omnigent_defaults", ("omnigent", "omnigent_defaults")),
        ("omnigent", "api_key_env", ("omnigent", "api_key_env")),
        ("acme-llm", "turbo", ("other", "other")),
        ("claude_sdk", "my-secret-mode", ("claude_sdk", "other")),
        (None, None, ("other", "other")),
        ("claude_sdk", ["databricks"], ("claude_sdk", "other")),
        ("claude_sdk", {"mode": "databricks"}, ("claude_sdk", "other")),
        (["claude_sdk"], "databricks", ("other", "databricks")),
    ],
)
def test_mark_assistant_normalises_provider_and_mode(
    provider: Any, mode: Any, expected: tuple[str, str]
) -> None:
    registry, sink, _ = _registry()
    request = _request(_app(registry), header=SID)

    events.mark_assistant(request, provider, mode)

    registry.emit_all("shutdown")
    props = sink.events[0][2]
    assert props["assistant_used"] is True
    assert (props["assistant_provider"], props["assistant_mode"]) == expected


def test_mark_assistant_is_a_noop_when_disabled() -> None:
    registry, sink, _ = _registry()
    events.mark_assistant(
        _request(_app(registry, enabled=False), header=SID), "claude_sdk", "databricks"
    )
    assert registry.emit_all("shutdown") == 0
    assert sink.events == []


# --- concurrency and delivery discipline ---------------------------------------------


def test_counting_from_many_threads_loses_nothing() -> None:
    registry, sink, _ = _registry()
    registry.touch(SID)

    def work() -> None:
        for _ in range(500):
            registry.count_request(SID, "files.read")
            registry.count_ui(SID, "project_map.opened")

    threads = [threading.Thread(target=work) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    registry.emit_all("shutdown")
    props = sink.events[0][2]
    assert props["requests_by_family"] == {"files.read": 4000}
    assert props["dag_views"] == 4000


def test_sink_is_called_outside_the_registry_lock() -> None:
    clock = FakeClock()

    class ReentrantSink(ListSink):
        registry: WebSessionRegistry

        def __call__(
            self, name: str, *, project_root: Path | None, props: dict[str, Any]
        ) -> None:
            super().__call__(name, project_root=project_root, props=props)
            assert self.registry.touch(SID_2) is True
            self.registry.count_request(SID_2, "project.read")

    sink = ReentrantSink()
    registry = WebSessionRegistry(sink=sink, flush=sink.flush, clock=clock)
    sink.registry = registry
    registry.touch(SID)
    registry.count_request(SID, "project.read")

    worker = threading.Thread(target=registry.emit, args=(SID, "idle"), daemon=True)
    worker.start()
    worker.join(timeout=2)

    assert not worker.is_alive(), "emit held the registry lock while calling the sink"
    assert [name for name, _, _ in sink.events] == ["web.session"]
    assert registry.emit_all("shutdown") == 1


def test_sink_and_flush_failures_never_propagate() -> None:
    def bad_sink(
        name: str, *, project_root: Path | None, props: dict[str, Any]
    ) -> None:
        raise RuntimeError("sink down")

    def bad_flush() -> None:
        raise RuntimeError("flush down")

    registry = WebSessionRegistry(sink=bad_sink, flush=bad_flush, clock=FakeClock())
    registry.touch(SID)
    registry.count_request(SID, "project.read")

    assert registry.emit(SID, "idle") is True
    assert registry.emit(SID, "idle") is False


def test_registry_defaults_to_the_telemetry_client() -> None:
    registry = WebSessionRegistry()
    assert registry.sink is telemetry.record
    assert registry.flush is telemetry.flush
    assert registry.project_root is None
