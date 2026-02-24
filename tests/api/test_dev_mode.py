"""Tests for dev_mode adaptive dependencies.

Verifies the three mode boundaries:
1. Read endpoints work WITHOUT auth in dev_mode
2. Write endpoints return 403 (dev_mode_restricted) in dev_mode
3. Production mode still requires auth (401) for all Phase 2 endpoints
"""

import pytest


# ---------------------------------------------------------------------------
# 1. Dev-mode read access — no auth headers needed
# ---------------------------------------------------------------------------


class TestDevModeReadAccess:
    """Phase 2 read endpoints should work without auth in dev_mode."""

    def test_list_files_no_auth(self, client):
        resp = client.get("/api/files")
        assert resp.status_code == 200
        data = resp.json()
        assert "items" in data
        assert "total" in data

    def test_read_file_no_auth(self, client):
        resp = client.get("/api/files/lhp.yaml")
        assert resp.status_code == 200
        data = resp.json()
        assert data["type"] == "file"
        assert "content" in data

    def test_list_environments_no_auth(self, client):
        resp = client.get("/api/environments")
        assert resp.status_code == 200
        data = resp.json()
        assert "environments" in data

    def test_get_environment_no_auth(self, client):
        # The testing project has a 'dev' environment
        resp = client.get("/api/environments/dev")
        assert resp.status_code == 200

    def test_git_status_no_auth(self, client_with_git):
        resp = client_with_git.get("/api/workspace/git/status")
        assert resp.status_code == 200
        data = resp.json()
        assert "branch" in data

    def test_git_log_no_auth(self, client_with_git):
        resp = client_with_git.get("/api/workspace/git/log")
        assert resp.status_code == 200
        data = resp.json()
        assert "entries" in data
        assert len(data["entries"]) >= 1

    def test_health_includes_dev_mode(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["dev_mode"] is True


# ---------------------------------------------------------------------------
# 2. Dev-mode write restrictions — 403 with dev_mode_restricted error
# ---------------------------------------------------------------------------


class TestDevModeWriteRestrictions:
    """Write endpoints should return 403 in dev_mode."""

    def _assert_dev_mode_restricted(self, resp):
        assert resp.status_code == 403
        detail = resp.json()["detail"]
        assert detail["error"] == "dev_mode_restricted"

    def test_write_file_blocked(self, client):
        resp = client.put(
            "/api/files/test.txt",
            json={"content": "hello"},
        )
        self._assert_dev_mode_restricted(resp)

    def test_delete_file_blocked(self, client):
        resp = client.delete("/api/files/test.txt")
        self._assert_dev_mode_restricted(resp)

    def test_commit_blocked(self, client):
        resp = client.post(
            "/api/workspace/commit",
            json={"message": "test"},
        )
        self._assert_dev_mode_restricted(resp)

    def test_push_blocked(self, client):
        resp = client.post("/api/workspace/push")
        self._assert_dev_mode_restricted(resp)

    def test_pull_blocked(self, client):
        resp = client.post("/api/workspace/pull")
        self._assert_dev_mode_restricted(resp)

    def test_ensure_workspace_blocked(self, client):
        resp = client.put("/api/workspace")
        self._assert_dev_mode_restricted(resp)

    def test_get_workspace_status_blocked(self, client):
        resp = client.get("/api/workspace")
        self._assert_dev_mode_restricted(resp)

    def test_heartbeat_blocked(self, client):
        resp = client.post("/api/workspace/heartbeat")
        self._assert_dev_mode_restricted(resp)

    def test_stop_workspace_blocked(self, client):
        resp = client.post("/api/workspace/stop")
        self._assert_dev_mode_restricted(resp)

    def test_delete_workspace_blocked(self, client):
        resp = client.delete("/api/workspace")
        self._assert_dev_mode_restricted(resp)

    def test_create_environment_blocked(self, client):
        resp = client.post(
            "/api/environments",
            json={"name": "staging", "tokens": {"key": "val"}},
        )
        self._assert_dev_mode_restricted(resp)

    def test_update_environment_blocked(self, client):
        resp = client.put(
            "/api/environments/dev",
            json={"tokens": {"key": "val"}},
        )
        self._assert_dev_mode_restricted(resp)

    def test_delete_environment_blocked(self, client):
        resp = client.delete("/api/environments/dev")
        self._assert_dev_mode_restricted(resp)


# ---------------------------------------------------------------------------
# 3. Production mode — auth still required
# ---------------------------------------------------------------------------


class TestProdModeUnchanged:
    """Production mode (dev_mode=False) should still require auth headers."""

    def test_list_files_requires_auth(self, prod_client):
        resp = prod_client.get("/api/files")
        assert resp.status_code == 401

    def test_git_status_requires_auth(self, prod_client):
        resp = prod_client.get("/api/workspace/git/status")
        assert resp.status_code == 401

    def test_list_environments_requires_auth(self, prod_client):
        resp = prod_client.get("/api/environments")
        assert resp.status_code == 401

    def test_health_shows_prod_mode(self, prod_client):
        resp = prod_client.get("/api/health")
        assert resp.status_code == 200
        assert resp.json()["dev_mode"] is False
