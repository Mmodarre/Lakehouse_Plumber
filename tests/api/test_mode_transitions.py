"""Tests for dev mode vs production mode behavior transitions.

Validates that the API behaves correctly in both dev_mode=True and
dev_mode=False configurations, covering:

- Dev mode: no auth required, no workspace setup needed for CRUD
- Production mode: auth headers required, workspace must exist for Phase 2 CRUD
- Phase 1 read-only endpoints work identically in both modes
- get_project_root was NOT modified by Phase 2 (still delegates to workspace-aware)

TDD note: Phase 2 CRUD endpoints (environments, workspace) don't fully exist yet.
Tests targeting those paths will fail until the Phase 2 router is implemented.
Phase 1 GET endpoints already work and those tests should pass today.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# TestDevModeBehavior
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDevModeBehavior:
    """Dev mode (dev_mode=True): no auth, no workspace setup required."""

    def test_phase2_endpoints_work_without_workspace(self, client: TestClient):
        """GET /environments returns 200 in dev mode without PUT /workspace first.

        In dev mode, get_workspace_project_root falls back to settings.project_root
        so Phase 2 CRUD endpoints should resolve the project root directly — no
        workspace creation step is needed.
        """
        resp = client.get("/api/environments")
        assert resp.status_code == 200

    def test_dev_mode_no_auth_required(self, client: TestClient):
        """Requests work without auth headers in dev mode.

        get_current_user returns DEV_USER when headers are absent and
        dev_mode=True, so all endpoints that depend on user context should
        succeed without X-Forwarded-* headers.
        """
        resp = client.get("/api/me")
        assert resp.status_code == 200
        data = resp.json()
        assert data["email"] == "dev@localhost"
        assert data["username"] == "dev"


# ---------------------------------------------------------------------------
# TestProductionModeBehavior
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestProductionModeBehavior:
    """Production mode (dev_mode=False): auth required, workspace required for CRUD."""

    def test_phase2_crud_requires_workspace(
        self, prod_client: TestClient, auth_headers: dict
    ):
        """Phase 2 write endpoint without workspace returns 409.

        In production mode, get_workspace_project_root must resolve through
        the user's workspace directory. Without a prior PUT /workspace, the
        workspace does not exist, so CRUD endpoints should fail with 409
        (Conflict — no active workspace).

        NOTE: This test encodes the *desired* Phase 2 behavior. Until
        get_workspace_project_root is updated to check workspace state in
        prod mode, this test will fail. That is correct TDD.
        """
        resp = prod_client.get("/api/environments", headers=auth_headers)
        assert resp.status_code == 409

    def test_missing_auth_returns_401(self, prod_client: TestClient):
        """Requests without auth headers in production mode return 401.

        get_current_user raises HTTPException(401) when X-Forwarded-*
        headers are absent and dev_mode=False.
        """
        resp = prod_client.get("/api/me")
        assert resp.status_code == 401


# ---------------------------------------------------------------------------
# TestPhase1Compatibility
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestPhase1Compatibility:
    """Phase 1 read-only endpoints work identically in both modes.

    These endpoints (flowgroups, presets, templates) existed before Phase 2
    and must continue to work in both dev and production configurations.
    get_project_root was NOT modified by Phase 2 — it still delegates to
    get_workspace_project_root, which in dev mode falls back to
    settings.project_root.
    """

    def test_flowgroups_list_unchanged_dev(self, client: TestClient):
        """GET /flowgroups works in dev mode."""
        resp = client.get("/api/flowgroups")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_flowgroups_list_unchanged_prod(
        self, prod_client: TestClient, auth_headers: dict
    ):
        """GET /flowgroups works in production mode with auth headers."""
        resp = prod_client.get("/api/flowgroups", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_presets_list_unchanged_dev(self, client: TestClient):
        """GET /presets works in dev mode."""
        resp = client.get("/api/presets")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_presets_list_unchanged_prod(
        self, prod_client: TestClient, auth_headers: dict
    ):
        """GET /presets works in production mode with auth headers."""
        resp = prod_client.get("/api/presets", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_templates_list_unchanged_dev(self, client: TestClient):
        """GET /templates works in dev mode."""
        resp = client.get("/api/templates")
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0

    def test_templates_list_unchanged_prod(
        self, prod_client: TestClient, auth_headers: dict
    ):
        """GET /templates works in production mode with auth headers."""
        resp = prod_client.get("/api/templates", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] > 0


# ---------------------------------------------------------------------------
# TestDevModeWorkspaceMapping
# ---------------------------------------------------------------------------


@pytest.mark.integration
class TestDevModeWorkspaceMapping:
    """In dev mode, all requests map to a single DEV_USER workspace."""

    def test_all_requests_map_to_dev_user(self, client: TestClient):
        """Two PUT /workspace calls return the same branch (dev/workspace).

        In dev mode, get_current_user always returns DEV_USER when auth
        headers are absent. Consecutive PUT /workspace calls for the same
        user must be idempotent and return the same branch name.

        NOTE: PUT /workspace does not exist yet (Phase 2). This test will
        fail until the workspace router is implemented.
        """
        first = client.put("/api/workspace")
        second = client.put("/api/workspace")

        assert first.status_code == 200
        assert second.status_code == 200

        assert first.json()["branch"] == second.json()["branch"]
