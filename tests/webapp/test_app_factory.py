"""Tests for the webapp application factory (``lhp.webapp.app.create_app``).

Self-sufficient: sets ``LHP_WEBAPP_PROJECT_ROOT`` to a tmp dir via a fixture
and exercises the app through a ``TestClient``. The router registry is
tolerant, so these tests pass whether or not the individual router modules
are present.
"""

import importlib

import pytest
from fastapi.testclient import TestClient

from lhp.errors import ErrorCategory, LHPError
from lhp.webapp.app import create_app

pytestmark = pytest.mark.webapp


@pytest.fixture
def project_root_env(tmp_path, monkeypatch):
    """Point the webapp at an isolated tmp project root via env var."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(tmp_path))
    return tmp_path


def test_create_app_boots(project_root_env):
    """create_app() boots regardless of which routers are present."""
    app = create_app()
    assert app is not None
    # TestClient triggers the lifespan (logging-only); must not raise.
    with TestClient(app, raise_server_exceptions=False):
        pass


def test_root_serves_plaintext_fallback_without_static(project_root_env):
    """Without built static assets, GET / returns a 200 plain-text page.

    The dev tree ships no static/index.html (assets are gitignored), so the
    fallback route should be active and mention the build script.
    """
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        assert "scripts/build_webapp.sh" in resp.text


def test_api_health_does_not_crash_app(project_root_env):
    """GET /api/health is either 200 (router present) or 404 — never a crash."""
    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/api/health")
        assert resp.status_code in (200, 404)


def test_lhp_error_handler_registered(project_root_env):
    """An LHPError raised in a route is mapped to a structured JSON body.

    Adds a probe route to the built app to confirm the exception handler is
    registered by the factory.
    """
    app = create_app()

    @app.get("/__probe_lhp_error")
    async def _probe() -> dict:
        raise LHPError(
            category=ErrorCategory.CONFIG,
            code_number="999",
            title="Probe error",
            details="probe details",
        )

    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/__probe_lhp_error")
        assert resp.status_code == 422
        body = resp.json()
        assert body["error"]["code"] == "LHP-CFG-999"
        assert body["error"]["message"] == "Probe error"


def test_router_registry_is_tolerant(project_root_env, monkeypatch):
    """A ModuleNotFoundError for a pinned router is skipped, not fatal.

    Force every pinned router to be absent by making import_module raise, and
    assert the app still boots and the plain-text fallback still serves.
    """
    import lhp.webapp.app as app_module

    real_import = importlib.import_module

    def _fake_import(name, *args, **kwargs):
        if name.startswith("lhp.webapp.routers."):
            raise ModuleNotFoundError(name)
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(app_module.importlib, "import_module", _fake_import)

    app = create_app()
    with TestClient(app, raise_server_exceptions=False) as client:
        resp = client.get("/")
        assert resp.status_code == 200
        assert "scripts/build_webapp.sh" in resp.text
