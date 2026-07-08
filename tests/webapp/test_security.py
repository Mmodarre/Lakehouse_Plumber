"""Security tests for the web IDE backend: Host, Origin/Referer, session token.

Three independent layers, in effective middleware order:

- ``TrustedHostMiddleware`` (DNS-rebinding defense): loopback Hosts only, 400
  otherwise.
- ``OriginGuardMiddleware`` (CSRF defense): state-changing methods carrying a
  foreign ``Origin``/``Referer`` get 403; absent headers pass (curl).
- ``TokenGuardMiddleware``: with a token configured, ``/api/`` paths (except
  ``/api/health``) require it via the ``X-LHP-Token`` header or the ``token``
  query param; without a configured token the guard is a no-op.

Origin/token probes use ``/api/nonexistent`` where possible so outcomes do not
depend on any specific business router: passing the guards yields the JSON 404,
being blocked yields the guard's own 403/401.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp

_TEST_TOKEN = "test-session-token-not-a-secret"


@pytest.fixture
def token_client(
    e2e_project_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Client over ``create_app()`` with the session token armed via env."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(e2e_project_path))
    monkeypatch.setenv("LHP_WEBAPP_TOKEN", _TEST_TOKEN)
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    app = create_app()
    with TestClient(app, base_url=LOOPBACK_BASE_URL) as test_client:
        yield test_client


class TestTrustedHost:
    """DNS-rebinding defense: only loopback Host headers are accepted."""

    def test_foreign_host_rejected_with_400(self, client: TestClient) -> None:
        resp = client.get("/api/health", headers={"Host": "evil.com"})
        assert resp.status_code == 400

    def test_loopback_host_accepted(self, client: TestClient) -> None:
        # The fixture client's base_url already presents Host: 127.0.0.1.
        assert client.get("/api/health").status_code == 200

    def test_localhost_host_accepted(self, client: TestClient) -> None:
        resp = client.get("/api/health", headers={"Host": "localhost:8000"})
        assert resp.status_code == 200


class TestOriginGuard:
    """CSRF defense: foreign Origin/Referer on write methods -> 403."""

    def test_foreign_origin_put_rejected(self, client: TestClient) -> None:
        resp = client.put("/api/nonexistent", headers={"Origin": "http://evil.com"})
        assert resp.status_code == 403
        assert resp.json() == {"detail": "cross-origin request rejected"}

    def test_foreign_referer_post_rejected(self, client: TestClient) -> None:
        resp = client.post(
            "/api/nonexistent", headers={"Referer": "https://evil.com/attack.html"}
        )
        assert resp.status_code == 403
        assert resp.json() == {"detail": "cross-origin request rejected"}

    def test_loopback_origin_put_passes_through(self, client: TestClient) -> None:
        # Not 403: the unmatched path falls through to the JSON 404 handler.
        resp = client.put(
            "/api/nonexistent", headers={"Origin": "http://127.0.0.1:8000"}
        )
        assert resp.status_code == 404

    def test_localhost_origin_delete_passes_through(self, client: TestClient) -> None:
        resp = client.delete(
            "/api/nonexistent", headers={"Origin": "http://localhost:8000"}
        )
        assert resp.status_code == 404

    def test_absent_origin_and_referer_put_allowed(self, client: TestClient) -> None:
        # curl compatibility: no Origin/Referer means no rejection.
        resp = client.put("/api/nonexistent")
        assert resp.status_code == 404

    def test_get_never_blocked_by_origin_guard(self, client: TestClient) -> None:
        resp = client.get("/api/health", headers={"Origin": "http://evil.com"})
        assert resp.status_code == 200


class TestTokenGuard:
    """Session token: required on /api/ (except health) when configured."""

    def test_missing_token_is_401(self, token_client: TestClient) -> None:
        resp = token_client.get("/api/project")
        assert resp.status_code == 401
        assert resp.json() == {"detail": "invalid or missing session token"}

    def test_wrong_header_token_is_401(self, token_client: TestClient) -> None:
        resp = token_client.get("/api/project", headers={"X-LHP-Token": "wrong-token"})
        assert resp.status_code == 401

    def test_correct_header_token_is_200(self, token_client: TestClient) -> None:
        resp = token_client.get("/api/project", headers={"X-LHP-Token": _TEST_TOKEN})
        assert resp.status_code == 200

    def test_correct_query_token_is_200(self, token_client: TestClient) -> None:
        resp = token_client.get(f"/api/project?token={_TEST_TOKEN}")
        assert resp.status_code == 200

    def test_health_is_exempt(self, token_client: TestClient) -> None:
        assert token_client.get("/api/health").status_code == 200

    def test_non_api_paths_are_exempt(self, token_client: TestClient) -> None:
        # The SPA shell (or its not-built fallback) must load without a token —
        # the token arrives via the URL fragment only after the shell renders.
        resp = token_client.get("/")
        assert resp.status_code == 200

    def test_unknown_api_path_with_token_is_404_not_401(
        self, token_client: TestClient
    ) -> None:
        resp = token_client.get(
            "/api/nonexistent", headers={"X-LHP-Token": _TEST_TOKEN}
        )
        assert resp.status_code == 404

    def test_no_token_configured_is_noop(self, client: TestClient) -> None:
        # The plain fixture client has no token in its settings.
        assert client.get("/api/project").status_code == 200
