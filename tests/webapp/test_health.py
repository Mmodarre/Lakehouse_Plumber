"""Tests for the health endpoint (``GET /api/health``).

The contract under test:

- ``/api/health`` is the only endpoint; there is no ``/version`` or ``/me``
  (neither is consumed by the local SPA).
- No auth: it is exempt from the session-token guard so the SPA can always
  render guidance and the launch readiness-poll needs no credential.
- :class:`HealthResponse` is ``{status, version, project_state, root}``:
  ``project_state`` is ``"ok"`` when the project root holds an ``lhp.yaml``
  and ``"no_project"`` otherwise (fail-closed, resolved by the lifespan), and
  ``root`` is the project-root path as a string.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp


class TestHealthEndpoint:
    """Tests for GET /api/health."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_status_is_healthy(self, client: TestClient) -> None:
        resp = client.get("/api/health")
        assert resp.json()["status"] == "healthy"

    def test_includes_version(self, client: TestClient) -> None:
        data = client.get("/api/health").json()
        assert "version" in data
        assert isinstance(data["version"], str)

    def test_project_state_ok_for_real_project(self, client: TestClient) -> None:
        """The fixture project has an lhp.yaml, so the state resolves to ok."""
        data = client.get("/api/health").json()
        assert data["project_state"] == "ok"

    def test_root_is_project_root_path(
        self, client: TestClient, e2e_project_path: Path
    ) -> None:
        data = client.get("/api/health").json()
        assert data["root"] == str(e2e_project_path.resolve())


class TestHealthNoProject:
    """Fail-closed project root: empty dir -> no_project, but health still 200s."""

    @pytest.fixture
    def no_project_client(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch
    ) -> TestClient:
        """Client over an EMPTY project root (no lhp.yaml anywhere above it)."""
        monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(tmp_path))
        monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
        monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
        monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
        app = create_app()
        # ``with`` runs the lifespan, which resolves project_state.
        with TestClient(app, base_url=LOOPBACK_BASE_URL) as test_client:
            yield test_client

    def test_health_still_200(self, no_project_client: TestClient) -> None:
        assert no_project_client.get("/api/health").status_code == 200

    def test_project_state_is_no_project(self, no_project_client: TestClient) -> None:
        data = no_project_client.get("/api/health").json()
        assert data["project_state"] == "no_project"

    def test_root_points_at_empty_dir(
        self, no_project_client: TestClient, tmp_path: Path
    ) -> None:
        data = no_project_client.get("/api/health").json()
        assert data["root"] == str(tmp_path.resolve())
