"""Tests for the route-family telemetry middleware and its ``create_app`` wiring.

The middleware is exercised two ways. A minimal inline app (the pattern of
``test_middleware.py``) mounts one route per family under ``/api`` so every
family in the table is hit exactly once and the skip/``other``/404/disabled
rules are proven against a registry with a list sink. ``create_app()`` is
then used for the wiring facts: the state attributes, the middleware order,
that every real route resolves to a family, and the lifespan shutdown
sequence. No test reaches ``lhp.telemetry.record``: every registry here is
built on a list sink and a no-op flush.
"""

from __future__ import annotations

import asyncio
import re
import time
from pathlib import Path
from typing import Any

import pytest
from fastapi import APIRouter, FastAPI, Request
from fastapi.responses import Response
from fastapi.routing import APIRoute
from fastapi.testclient import TestClient

from lhp import telemetry
from lhp.telemetry import TelemetryState
from lhp.webapp.app import create_app, lifespan
from lhp.webapp.middleware.request_logging import RequestLoggingMiddleware
from lhp.webapp.middleware.telemetry_session import (
    COUNTED_METHODS,
    OTHER_FAMILY,
    PREFIX_FAMILIES,
    ROUTE_FAMILIES,
    SKIPPED_TEMPLATES,
    TelemetrySessionMiddleware,
    route_family,
)
from lhp.webapp.services import telemetry_sessions
from lhp.webapp.services._telemetry_events import on_sse_disconnect
from lhp.webapp.services.telemetry_sessions import SESSION_HEADER, WebSessionRegistry

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp

SID = "0f4a2c6e-1b3d-4e5f-8a9b-0c1d2e3f4a5b"
SID_2 = "1a2b3c4d-5e6f-4a7b-8c9d-0e1f2a3b4c5d"

#: The families DESIGN §E3 names, plus ``other``; the table must produce
#: exactly this set so a typo in a family label cannot ship.
E3_FAMILIES = frozenset(
    {
        "project.read",
        "project.stats",
        "project.init",
        "pipelines.read",
        "flowgroups.read",
        "tables.read",
        "lineage.read",
        "presets.read",
        "templates.read",
        "templates.preview",
        "blueprints.read",
        "environments.read",
        "metadata.read",
        "dependencies.read",
        "dependencies.refresh",
        "sandbox.read",
        "schemas.read",
        "config_templates.read",
        "configuration.preview",
        "help.read",
        "files.tree",
        "files.read",
        "files.write",
        "files.delete",
        "runs.validate",
        "runs.generate",
        "runs.history",
        "assistant.status",
        "assistant.chat",
        "assistant.control",
        "assistant.daemon",
        "assistant.config_read",
        "assistant.config_write",
        "assistant.skill_install",
        "assistant.sessions_read",
        "assistant.sessions_write",
    }
)

#: Real routes allowed to resolve to ``other``: the SPA shell entry points
#: that ``static_app`` registers as ``APIRoute``s (``/`` and, with a built
#: bundle, each top-level static file). They are browser navigations, which
#: never carry the session header, so nothing is ever counted for them.
_OTHER_ROUTES: frozenset[tuple[str, str]] = frozenset({("GET", "/")})

_PLACEHOLDER = re.compile(r"\{(\w+)(?::path)?\}")


def _concrete(template: str) -> str:
    """A concrete URL for ``template``; the value is irrelevant to the family."""
    return _PLACEHOLDER.sub("x", template)


def _one_route_per_family() -> dict[str, tuple[str, str]]:
    first: dict[str, tuple[str, str]] = {}
    for key, family in ROUTE_FAMILIES.items():
        first.setdefault(family, key)
    return first


class _ListSinkRegistry(WebSessionRegistry):
    """A registry whose events land in ``self.events`` instead of the client."""

    def __init__(self, project_root: Path | None = None) -> None:
        self.events: list[tuple[str, Path | None, dict[str, Any]]] = []
        super().__init__(sink=self._sink, flush=lambda: None, project_root=project_root)

    def _sink(self, name: str, *, project_root: Path | None, props: Any) -> None:
        self.events.append((name, project_root, props))


class _ExplodingRegistry(_ListSinkRegistry):
    def count_request(self, sid: str, family: str) -> None:
        raise RuntimeError("counting failed")


async def _ok(request: Request) -> dict[str, bool]:
    return {"ok": True}


async def _server_error(request: Request) -> Response:
    return Response(status_code=500)


def _build_app(
    *, enabled: bool = True, registry: _ListSinkRegistry | None = None
) -> tuple[FastAPI, _ListSinkRegistry]:
    """Inline app: one route per family mounted under ``/api``, plus edge routes.

    Family routes are declared router-locally and included with the ``/api``
    prefix, exactly as ``create_app`` mounts its routers, so whichever
    template spelling FastAPI hands the middleware is the one it sees in
    production. ``/api/unmapped`` is registered with the prefix baked in to
    prove the other spelling resolves too.
    """
    app = FastAPI()
    app.add_middleware(TelemetrySessionMiddleware)
    registry = _ListSinkRegistry() if registry is None else registry
    app.state.telemetry_enabled = enabled
    app.state.web_sessions = registry

    api = APIRouter()
    for method, template in _one_route_per_family().values():
        api.add_api_route(template, _ok, methods=[method])
    api.add_api_route("/health", _ok, methods=["GET"])
    api.add_api_route("/events", _ok, methods=["GET"])
    api.add_api_route("/telemetry/ui", _ok, methods=["POST"])
    api.add_api_route("/failing", _server_error, methods=["GET"])
    app.include_router(api, prefix="/api")
    app.add_api_route("/api/unmapped", _ok, methods=["GET"])
    return app, registry


def _client(app: FastAPI) -> TestClient:
    return TestClient(app, base_url=LOOPBACK_BASE_URL)


def _wait_for_events(registry: _ListSinkRegistry, count: int) -> None:
    """Block until the executor-thread sweep has delivered ``count`` events."""
    deadline = time.monotonic() + 2.0
    while len(registry.events) < count and time.monotonic() < deadline:
        time.sleep(0.01)


# -- route_family: the pure table lookup ------------------------------------


class TestRouteFamily:
    def test_accepts_router_local_and_mounted_spellings(self) -> None:
        """FastAPI hands back either template spelling depending on version."""
        assert route_family("GET", "/pipelines") == "pipelines.read"
        assert route_family("GET", "/api/pipelines") == "pipelines.read"
        assert route_family("PUT", "/api/files/{path:path}") == "files.write"

    @pytest.mark.parametrize(
        ("method", "template", "family"),
        [
            ("GET", "/project", "project.read"),
            ("GET", "/project/stats", "project.stats"),
            ("POST", "/project/init", "project.init"),
            ("GET", "/pipelines/{name}/flowgroups", "pipelines.read"),
            ("GET", "/flowgroups/{name}/resolved", "flowgroups.read"),
            ("GET", "/operational-metadata", "metadata.read"),
            ("GET", "/dependencies/graph/action", "dependencies.read"),
            ("POST", "/dependencies/refresh", "dependencies.refresh"),
            ("GET", "/config-templates/{kind}", "config_templates.read"),
            ("GET", "/files", "files.tree"),
            ("GET", "/files/{path:path}", "files.read"),
            ("PUT", "/files/{path:path}", "files.write"),
            ("DELETE", "/files/{path:path}", "files.delete"),
            ("POST", "/validate/stream", "runs.validate"),
            ("POST", "/generate/stream", "runs.generate"),
            ("GET", "/runs/{run_id}", "runs.history"),
            ("GET", "/assistant/status", "assistant.status"),
            ("POST", "/assistant/chat", "assistant.chat"),
            ("POST", "/assistant/approval", "assistant.control"),
            ("POST", "/assistant/interrupt", "assistant.control"),
            ("POST", "/assistant/daemon/start", "assistant.daemon"),
            ("GET", "/assistant/databricks-profiles", "assistant.config_read"),
            ("PUT", "/assistant/permissions", "assistant.config_write"),
            ("POST", "/assistant/skill", "assistant.skill_install"),
            ("GET", "/assistant/sessions", "assistant.sessions_read"),
            ("POST", "/assistant/session/archive", "assistant.sessions_write"),
        ],
    )
    def test_explicit_rows(self, method: str, template: str, family: str) -> None:
        assert route_family(method, template) == family

    @pytest.mark.parametrize(
        "template",
        ["/health", "/events", "/telemetry/ui", "/docs", "/redoc", "/openapi.json"],
    )
    def test_skipped_templates_resolve_to_none(self, template: str) -> None:
        assert route_family("GET", template) is None
        assert route_family("GET", "/api" + template) is None
        assert route_family("POST", template) is None

    def test_skip_set_is_exact(self) -> None:
        assert SKIPPED_TEMPLATES == frozenset(
            {"/health", "/events", "/telemetry/ui", "/docs", "/redoc", "/openapi.json"}
        )

    @pytest.mark.parametrize("method", ["HEAD", "OPTIONS", "PATCH"])
    def test_uncounted_methods_resolve_to_none(self, method: str) -> None:
        assert COUNTED_METHODS == frozenset({"GET", "POST", "PUT", "DELETE"})
        assert route_family(method, "/pipelines") is None

    def test_unknown_template_falls_back_to_other(self) -> None:
        assert route_family("GET", "/something/new") == OTHER_FAMILY
        assert route_family("POST", "/pipelines") == OTHER_FAMILY

    def test_families_are_exactly_the_design_set(self) -> None:
        listed = set(ROUTE_FAMILIES.values()) | {f for _, _, f in PREFIX_FAMILIES}
        assert listed == E3_FAMILIES
        assert OTHER_FAMILY not in E3_FAMILIES

    @pytest.mark.parametrize(
        ("method", "template", "family"),
        [
            ("GET", "/files/{path:path}/history", "files.read"),
            ("PUT", "/files/{path:path}/rename", "files.write"),
            ("DELETE", "/files/{path:path}/all", "files.delete"),
            ("GET", "/assistant/session/{id}", "assistant.sessions_read"),
            ("POST", "/assistant/session/rename", "assistant.sessions_write"),
            ("GET", "/assistant/config/defaults", "assistant.config_read"),
            ("PUT", "/assistant/config/defaults", "assistant.config_write"),
        ],
    )
    def test_prefix_rules_cover_sub_routes(
        self, method: str, template: str, family: str
    ) -> None:
        assert route_family(method, template) == family


# -- the middleware over an inline app --------------------------------------


class TestTelemetrySessionMiddleware:
    def test_every_family_is_counted_once(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            for family, (method, template) in _one_route_per_family().items():
                response = client.request(
                    method, "/api" + _concrete(template), headers={SESSION_HEADER: SID}
                )
                assert response.status_code == 200, family
        assert registry.emit_all("shutdown") == 1
        (name, _root, props), *rest = registry.events
        assert not rest
        assert name == "web.session"
        assert props["session_id"] == SID
        assert props["requests_by_family"] == dict.fromkeys(E3_FAMILIES, 1)

    def test_no_session_header_counts_nothing(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            assert client.get("/api/pipelines").status_code == 200
            assert client.get("/api/unmapped").status_code == 200
        assert registry.emit_all("shutdown") == 0
        assert registry.events == []

    def test_malformed_session_header_counts_nothing(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            client.get("/api/pipelines", headers={SESSION_HEADER: "not-a-uuid"})
            client.get("/api/pipelines", headers={SESSION_HEADER: SID.upper()})
        assert registry.emit_all("shutdown") == 0

    def test_skipped_templates_do_not_touch_the_session(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            headers = {SESSION_HEADER: SID}
            assert client.get("/api/health", headers=headers).status_code == 200
            assert client.get("/api/events", headers=headers).status_code == 200
            assert client.post("/api/telemetry/ui", headers=headers).status_code == 200
        # Not even an empty record: a health poll must never keep a tab alive.
        assert registry.emit_all("shutdown") == 0

    def test_unmapped_api_route_counts_as_other(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            client.get("/api/unmapped", headers={SESSION_HEADER: SID})
        assert registry.emit_all("shutdown") == 1
        assert registry.events[0][2]["requests_by_family"] == {OTHER_FAMILY: 1}

    def test_unmatched_404_is_not_counted(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            response = client.get("/api/does-not-exist", headers={SESSION_HEADER: SID})
            assert response.status_code == 404
        assert registry.emit_all("shutdown") == 0

    def test_counting_is_status_agnostic(self) -> None:
        app, registry = _build_app()
        with _client(app) as client:
            assert (
                client.get("/api/failing", headers={SESSION_HEADER: SID}).status_code
                == 500
            )
        assert registry.emit_all("shutdown") == 1
        assert registry.events[0][2]["requests_by_family"] == {OTHER_FAMILY: 1}

    def test_disabled_counts_nothing_and_never_sweeps(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(telemetry_sessions, "IDLE_SECONDS", 0)
        monkeypatch.setattr(telemetry_sessions, "SWEEP_INTERVAL_SECONDS", 0)
        app, registry = _build_app(enabled=False)
        # A record placed directly must survive: the sweep never runs when off.
        registry.touch(SID_2)
        registry.count_request(SID_2, "pipelines.read")
        with _client(app) as client:
            client.get("/api/pipelines", headers={SESSION_HEADER: SID})
            time.sleep(0.05)
            client.get("/api/unmapped", headers={SESSION_HEADER: SID})
        time.sleep(0.05)
        assert registry.events == []
        assert not hasattr(app.state, "telemetry_last_sweep")
        assert registry.emit_all("shutdown") == 1
        assert registry.events[0][2]["session_id"] == SID_2

    def test_idle_sweep_runs_opportunistically(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(telemetry_sessions, "IDLE_SECONDS", 0)
        monkeypatch.setattr(telemetry_sessions, "SWEEP_INTERVAL_SECONDS", 0)
        app, registry = _build_app()
        with _client(app) as client:
            client.get("/api/pipelines", headers={SESSION_HEADER: SID})
            time.sleep(0.02)
            client.get("/api/tables", headers={SESSION_HEADER: SID_2})
            _wait_for_events(registry, 1)
        idle = [
            props for name, _, props in registry.events if props["session_id"] == SID
        ]
        assert len(idle) == 1
        assert idle[0]["end_reason"] == "idle"
        assert idle[0]["requests_by_family"] == {"pipelines.read": 1}

    def test_sweep_is_rate_limited(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setattr(telemetry_sessions, "IDLE_SECONDS", 0)
        monkeypatch.setattr(telemetry_sessions, "SWEEP_INTERVAL_SECONDS", 3600)
        app, registry = _build_app()
        app.state.telemetry_last_sweep = time.monotonic()
        with _client(app) as client:
            client.get("/api/pipelines", headers={SESSION_HEADER: SID})
            time.sleep(0.02)
            client.get("/api/tables", headers={SESSION_HEADER: SID_2})
            time.sleep(0.05)
        assert registry.events == []
        assert registry.emit_all("shutdown") == 2

    def test_counting_failure_never_reaches_the_response(self) -> None:
        app, _registry = _build_app(registry=_ExplodingRegistry())
        with _client(app) as client:
            response = client.get("/api/pipelines", headers={SESSION_HEADER: SID})
        assert response.status_code == 200
        assert response.json() == {"ok": True}


# -- create_app wiring -----------------------------------------------------


def _create_app_at(monkeypatch: pytest.MonkeyPatch, project_root: Path) -> FastAPI:
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(project_root))
    for var in ("LHP_WEBAPP_PORT", "LHP_WEBAPP_LOG_LEVEL", "LHP_WEBAPP_TOKEN"):
        monkeypatch.delenv(var, raising=False)
    return create_app()


def _api_routes(app: FastAPI) -> list[APIRoute]:
    """Every ``APIRoute`` the app can match, whatever FastAPI's nesting is.

    Recent FastAPI keeps included routers nested (``original_router``) rather
    than cloning their routes onto the app; older versions clone. Both are
    walked, and the OpenAPI cross-check below proves the walk found them all.
    """
    found: list[APIRoute] = []
    pending: list[Any] = list(app.routes)
    while pending:
        item = pending.pop()
        if isinstance(item, APIRoute):
            found.append(item)
            continue
        nested = getattr(item, "original_router", None)
        if nested is not None:
            pending.extend(nested.routes)
    return found


def _without_converters(template: str) -> str:
    return _PLACEHOLDER.sub(r"{\1}", template)


class TestCreateAppWiring:
    def test_sets_telemetry_state_from_the_client(
        self, e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr(
            telemetry,
            "effective_state",
            lambda: TelemetryState(enabled=True, mode="send", reason="default"),
        )
        app = _create_app_at(monkeypatch, e2e_project_path)
        assert app.state.telemetry_enabled is True
        registry = app.state.web_sessions
        assert isinstance(registry, WebSessionRegistry)
        assert registry.project_root == e2e_project_path
        assert registry.sink is telemetry.record

    def test_is_off_under_pytest_and_registry_has_no_root_without_a_project(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        app = _create_app_at(monkeypatch, tmp_path)
        assert app.state.telemetry_enabled is False
        assert isinstance(app.state.web_sessions, WebSessionRegistry)
        assert app.state.web_sessions.project_root is None

    def test_middleware_is_innermost(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        """Starlette lists user middleware outermost first."""
        app = _create_app_at(monkeypatch, tmp_path)
        classes = [entry.cls for entry in app.user_middleware]
        assert classes[-1] is TelemetrySessionMiddleware
        assert classes[-2] is RequestLoggingMiddleware

    def test_every_real_route_has_a_family(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        monkeypatch.setattr("lhp.webapp.static_app.resolve_static_dir", lambda: None)
        app = _create_app_at(monkeypatch, tmp_path)
        routes = _api_routes(app)

        # Every operation OpenAPI documents must be among the walked routes,
        # or the walk missed a nesting level and the assertions below are
        # vacuous. Both sides are compared in mount-stripped, converter-free
        # form because OpenAPI drops ``:path`` converters.
        documented = {
            (method.upper(), path.removeprefix("/api"))
            for path, operations in app.openapi()["paths"].items()
            for method in operations
        }
        walked = {
            (method, _without_converters(route.path.removeprefix("/api")))
            for route in routes
            for method in route.methods
        }
        assert documented <= walked, documented - walked

        others: set[tuple[str, str]] = set()
        for route in routes:
            for method in route.methods:
                assert method in COUNTED_METHODS | {"HEAD"}, (method, route.path)
                if method not in COUNTED_METHODS:
                    continue
                family = route_family(method, route.path)
                template = route.path.removeprefix("/api")
                if template in SKIPPED_TEMPLATES:
                    assert family is None, (method, template)
                elif family == OTHER_FAMILY:
                    others.add((method, template))
                else:
                    assert family in E3_FAMILIES, (method, template, family)
        assert others == _OTHER_ROUTES


# -- lifespan shutdown -----------------------------------------------------


def _run(scenario: Any) -> None:
    asyncio.run(scenario())


class TestLifespanShutdown:
    def test_emits_every_session_as_shutdown_and_cancels_grace_tasks(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flushes: list[float] = []
        monkeypatch.setattr(
            telemetry, "flush", lambda join_s=0.0: flushes.append(join_s)
        )
        app = _create_app_at(monkeypatch, tmp_path)
        app.state.telemetry_enabled = True
        registry = _ListSinkRegistry()
        app.state.web_sessions = registry

        async def scenario() -> None:
            async with lifespan(app):
                registry.touch(SID)
                registry.count_request(SID, "pipelines.read")
                grace = asyncio.create_task(on_sse_disconnect(app, SID))
                await asyncio.sleep(0)
                await asyncio.sleep(0)
                assert registry.pending_grace_tasks() == (grace,)
            assert grace.cancelled()

        _run(scenario)
        assert [name for name, _, _ in registry.events] == ["web.session"]
        assert registry.events[0][2]["end_reason"] == "shutdown"
        assert registry.events[0][2]["session_id"] == SID
        assert flushes == [1.0]

    def test_emits_nothing_when_disabled(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        flushes: list[float] = []
        monkeypatch.setattr(
            telemetry, "flush", lambda join_s=0.0: flushes.append(join_s)
        )
        app = _create_app_at(monkeypatch, tmp_path)
        registry = _ListSinkRegistry()
        app.state.web_sessions = registry
        registry.touch(SID)
        registry.count_request(SID, "pipelines.read")

        async def scenario() -> None:
            async with lifespan(app):
                pass

        _run(scenario)
        assert registry.events == []
        assert flushes == []

    def test_never_fails_shutdown(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        class _FailingRegistry(_ListSinkRegistry):
            def emit_all(self, reason: str) -> int:
                raise RuntimeError("emit failed")

        app = _create_app_at(monkeypatch, tmp_path)
        app.state.telemetry_enabled = True
        app.state.web_sessions = _FailingRegistry()

        async def scenario() -> None:
            async with lifespan(app):
                pass

        _run(scenario)
