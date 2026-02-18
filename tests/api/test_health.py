"""Tests for health, version, and me endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestHealthEndpoint:
    """Tests for GET /api/health."""

    def test_returns_200(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200

    def test_status_is_healthy(self, client):
        resp = client.get("/api/health")
        assert resp.json()["status"] == "healthy"

    def test_includes_version(self, client):
        data = client.get("/api/health").json()
        assert "version" in data
        assert isinstance(data["version"], str)

    def test_includes_python_version(self, client):
        data = client.get("/api/health").json()
        assert "python_version" in data

    def test_no_auth_required(self, prod_client):
        """Health check works even without auth headers in prod mode."""
        resp = prod_client.get("/api/health")
        assert resp.status_code == 200


class TestVersionEndpoint:
    """Tests for GET /api/version."""

    def test_returns_200(self, client):
        resp = client.get("/api/version")
        assert resp.status_code == 200

    def test_includes_lhp_version(self, client):
        data = client.get("/api/version").json()
        assert "lhp_version" in data

    def test_includes_dependency_versions(self, client):
        data = client.get("/api/version").json()
        deps = data["dependencies"]
        for pkg in ("fastapi", "pydantic", "click", "jinja2", "pyyaml", "networkx"):
            assert pkg in deps, f"Missing dependency version for {pkg}"

    def test_no_auth_required(self, prod_client):
        resp = prod_client.get("/api/version")
        assert resp.status_code == 200


class TestMeEndpoint:
    """Tests for GET /api/me (basic smoke test; auth details in test_auth.py)."""

    def test_returns_200_in_dev_mode(self, client):
        resp = client.get("/api/me")
        assert resp.status_code == 200

    def test_response_has_user_fields(self, client):
        data = client.get("/api/me").json()
        assert "email" in data
        assert "username" in data
        assert "user_id" in data
