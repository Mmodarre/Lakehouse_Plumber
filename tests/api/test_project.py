"""Tests for project info, config, and stats endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestProjectInfo:
    """Tests for GET /api/project."""

    def test_returns_200(self, client):
        resp = client.get("/api/project")
        assert resp.status_code == 200

    def test_project_name(self, client):
        data = client.get("/api/project").json()
        assert data["name"] == "acme_edw"

    def test_project_version(self, client):
        data = client.get("/api/project").json()
        assert data["version"] == "1.0"

    def test_resource_counts_has_all_keys(self, client):
        counts = client.get("/api/project").json()["resource_counts"]
        for key in ("pipelines", "flowgroups", "presets", "templates", "environments"):
            assert key in counts

    def test_resource_counts_are_positive(self, client):
        counts = client.get("/api/project").json()["resource_counts"]
        assert counts["pipelines"] > 0
        assert counts["flowgroups"] > 0
        assert counts["presets"] > 0
        assert counts["templates"] > 0
        assert counts["environments"] > 0


class TestProjectConfig:
    """Tests for GET /api/project/config."""

    def test_returns_200(self, client):
        resp = client.get("/api/project/config")
        assert resp.status_code == 200

    def test_config_has_project_keys(self, client):
        config = client.get("/api/project/config").json()["config"]
        assert "name" in config
        assert "version" in config


class TestProjectConfigUpdate:
    """Tests for PUT /api/project/config (uses mutable_client)."""

    def test_valid_config_update_returns_200(self, mutable_client):
        # First get current config
        resp = mutable_client.get("/api/project/config")
        assert resp.status_code == 200
        current = resp.json()["config"]

        # Update with same config (no-op but tests the endpoint)
        resp = mutable_client.put("/api/project/config", json=current)
        assert resp.status_code == 200
        assert resp.json()["message"] == "Project config updated"


class TestProjectStats:
    """Tests for GET /api/project/stats."""

    def test_returns_200(self, client):
        resp = client.get("/api/project/stats")
        assert resp.status_code == 200

    def test_totals_are_positive(self, client):
        data = client.get("/api/project/stats").json()
        assert data["total_pipelines"] > 0
        assert data["total_flowgroups"] > 0
        assert data["total_actions"] > 0

    def test_actions_by_type_includes_core_types(self, client):
        data = client.get("/api/project/stats").json()
        types = data["actions_by_type"]
        assert "load" in types
        assert "transform" in types
        assert "write" in types

    def test_pipelines_list_has_expected_fields(self, client):
        data = client.get("/api/project/stats").json()
        assert len(data["pipelines"]) > 0
        first = data["pipelines"][0]
        assert "name" in first
        assert "flowgroup_count" in first
        assert "action_count" in first
