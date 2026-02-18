"""TDD tests for workspace router endpoints.

These tests exercise the full workspace HTTP API lifecycle: creation, status,
heartbeat, stop, delete, manual commit, push/pull, and git status/log.  They
use REAL git repositories (via ``mock_git_remote``) and the filesystem (via
``mock_workspace_root``) so there is no mocking.

The production router (``lhp.api.routers.workspace``) and its DI dependencies
(``get_workspace_manager``, ``get_git_service``) do not exist yet.  These tests
are written first to drive implementation -- they will fail at import time
until the Phase 2 services and router are built.  That is correct TDD behavior.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.api.app import create_app
from lhp.api.config import APISettings


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Local fixtures: workspace-aware FastAPI app
# ---------------------------------------------------------------------------


@pytest.fixture
def workspace_settings(
    mock_workspace_root: Path,
    mock_git_remote: Path,
) -> APISettings:
    """APISettings configured for workspace mode.

    Points workspace_root at the temp directory and uses the bare git repo
    as the source_repo.  dev_mode is False so auth headers are required —
    this matches production behavior.
    """
    return APISettings(
        project_root=mock_workspace_root,  # not a real LHP project; that's OK
        workspace_root=mock_workspace_root,
        source_repo=str(mock_git_remote),
        dev_mode=False,
        log_level="DEBUG",
    )


@pytest.fixture
def workspace_app(workspace_settings: APISettings):
    """FastAPI app with workspace router for testing.

    This will fail until Phase 2 code is implemented -- that is TDD.
    """
    return create_app(settings=workspace_settings)


@pytest.fixture
def ws_client(workspace_app) -> TestClient:
    """Test client for workspace HTTP requests."""
    return TestClient(workspace_app)


@pytest.fixture
def workspace_settings_dev(
    mock_workspace_root: Path,
    mock_git_remote: Path,
) -> APISettings:
    """APISettings in dev_mode for workspace operations (no auth required)."""
    return APISettings(
        project_root=mock_workspace_root,
        workspace_root=mock_workspace_root,
        source_repo=str(mock_git_remote),
        dev_mode=True,
        log_level="DEBUG",
    )


@pytest.fixture
def workspace_app_dev(workspace_settings_dev: APISettings):
    """FastAPI app in dev mode with workspace router."""
    return create_app(settings=workspace_settings_dev)


@pytest.fixture
def ws_client_dev(workspace_app_dev) -> TestClient:
    """Test client in dev mode (no auth headers needed)."""
    return TestClient(workspace_app_dev)


# ---------------------------------------------------------------------------
# TestEnsureWorkspace
# ---------------------------------------------------------------------------


class TestEnsureWorkspace:
    """PUT /api/workspace -- create or resume (idempotent)."""

    def test_creates_workspace_returns_200(
        self, ws_client: TestClient, auth_headers: dict
    ):
        resp = ws_client.put("/api/workspace", headers=auth_headers)
        assert resp.status_code == 200

    def test_response_has_required_fields(
        self, ws_client: TestClient, auth_headers: dict
    ):
        resp = ws_client.put("/api/workspace", headers=auth_headers)
        data = resp.json()
        for field in ("state", "branch", "created_at", "last_activity", "has_uncommitted_changes"):
            assert field in data, f"Missing field: {field}"

    def test_idempotent_returns_same_branch(
        self, ws_client: TestClient, auth_headers: dict
    ):
        first = ws_client.put("/api/workspace", headers=auth_headers).json()
        second = ws_client.put("/api/workspace", headers=auth_headers).json()
        assert first["branch"] == second["branch"]

    def test_state_is_active(
        self, ws_client: TestClient, auth_headers: dict
    ):
        data = ws_client.put("/api/workspace", headers=auth_headers).json()
        assert data["state"] == "active"


# ---------------------------------------------------------------------------
# TestGetWorkspaceStatus
# ---------------------------------------------------------------------------


class TestGetWorkspaceStatus:
    """GET /api/workspace -- current state, branch, last activity."""

    def test_returns_status_after_creation(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.get("/api/workspace", headers=auth_headers)
        assert resp.status_code == 200

    def test_returns_404_without_workspace(
        self, ws_client: TestClient, auth_headers: dict
    ):
        resp = ws_client.get("/api/workspace", headers=auth_headers)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestHeartbeat
# ---------------------------------------------------------------------------


class TestHeartbeat:
    """POST /api/workspace/heartbeat -- keep-alive signal."""

    def test_heartbeat_returns_ok(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post("/api/workspace/heartbeat", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["ok"] is True


# ---------------------------------------------------------------------------
# TestStopWorkspace
# ---------------------------------------------------------------------------


class TestStopWorkspace:
    """POST /api/workspace/stop -- soft-stop (keep files, deactivate)."""

    def test_stop_returns_stopped(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post("/api/workspace/stop", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["stopped"] is True


# ---------------------------------------------------------------------------
# TestDeleteWorkspace
# ---------------------------------------------------------------------------


class TestDeleteWorkspace:
    """DELETE /api/workspace -- hard-delete (flush auto-commit, remove disk)."""

    def test_delete_returns_deleted(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.delete("/api/workspace", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["deleted"] is True

    def test_get_returns_404_after_delete(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        ws_client.delete("/api/workspace", headers=auth_headers)
        resp = ws_client.get("/api/workspace", headers=auth_headers)
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestManualCommit
# ---------------------------------------------------------------------------


class TestManualCommit:
    """POST /api/workspace/commit -- manual commit with user message."""

    def test_commit_without_changes_returns_committed_false(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post(
            "/api/workspace/commit",
            headers=auth_headers,
            json={"message": "My manual commit"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["committed"] is False
        assert data["sha"] is None

    def test_commit_requires_message(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post(
            "/api/workspace/commit",
            headers=auth_headers,
            json={},
        )
        assert resp.status_code == 422


# ---------------------------------------------------------------------------
# TestPushPull
# ---------------------------------------------------------------------------


class TestPushPull:
    """POST /api/workspace/push and POST /api/workspace/pull."""

    def test_push_returns_pushed(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post("/api/workspace/push", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["pushed"] is True

    def test_pull_returns_pulled(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.post("/api/workspace/pull", headers=auth_headers)
        assert resp.status_code == 200
        assert resp.json()["pulled"] is True


# ---------------------------------------------------------------------------
# TestGitStatus
# ---------------------------------------------------------------------------


class TestGitStatus:
    """GET /api/workspace/git/status -- modified/staged/untracked."""

    def test_git_status_returns_all_fields(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.get("/api/workspace/git/status", headers=auth_headers)
        assert resp.status_code == 200
        data = resp.json()
        for field in ("branch", "modified", "staged", "untracked", "ahead", "behind"):
            assert field in data, f"Missing field: {field}"

    def test_git_status_branch_matches_workspace(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_data = ws_client.put("/api/workspace", headers=auth_headers).json()
        status_data = ws_client.get(
            "/api/workspace/git/status", headers=auth_headers
        ).json()
        # The git status branch should match the workspace branch
        assert status_data["branch"] == ws_data["branch"]


# ---------------------------------------------------------------------------
# TestGitLog
# ---------------------------------------------------------------------------


class TestGitLog:
    """GET /api/workspace/git/log -- recent commit history."""

    def test_git_log_returns_entries(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.get("/api/workspace/git/log", headers=auth_headers)
        assert resp.status_code == 200
        entries = resp.json()["entries"]
        # At least the initial commit from mock_git_remote
        assert len(entries) >= 1

    def test_git_log_respects_max_count(
        self, ws_client: TestClient, auth_headers: dict
    ):
        ws_client.put("/api/workspace", headers=auth_headers)
        resp = ws_client.get(
            "/api/workspace/git/log",
            headers=auth_headers,
            params={"max_count": 1},
        )
        assert resp.status_code == 200
        entries = resp.json()["entries"]
        assert len(entries) <= 1


# ---------------------------------------------------------------------------
# TestWorkspaceAuthRequired
# ---------------------------------------------------------------------------


class TestWorkspaceAuthRequired:
    """All workspace endpoints require authentication in production mode."""

    def test_put_requires_auth_in_prod_mode(
        self, ws_client: TestClient
    ):
        resp = ws_client.put("/api/workspace")
        assert resp.status_code == 401

    def test_get_requires_auth_in_prod_mode(
        self, ws_client: TestClient
    ):
        resp = ws_client.get("/api/workspace")
        assert resp.status_code == 401
