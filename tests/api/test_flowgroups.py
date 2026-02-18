"""Tests for flowgroup list, detail, and resolved endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestListFlowgroups:
    """Tests for GET /api/flowgroups."""

    def test_returns_200(self, client):
        resp = client.get("/api/flowgroups")
        assert resp.status_code == 200

    def test_flowgroups_list_non_empty(self, client):
        data = client.get("/api/flowgroups").json()
        assert data["total"] > 0
        assert len(data["flowgroups"]) > 0

    def test_total_matches_list_length(self, client):
        data = client.get("/api/flowgroups").json()
        assert data["total"] == len(data["flowgroups"])

    def test_each_flowgroup_has_required_fields(self, client):
        fgs = client.get("/api/flowgroups").json()["flowgroups"]
        for fg in fgs:
            assert "name" in fg
            assert "pipeline" in fg
            assert "action_count" in fg
            assert "action_types" in fg

    def test_filter_by_pipeline(self, client):
        resp = client.get("/api/flowgroups", params={"pipeline": "acmi_edw_bronze"})
        assert resp.status_code == 200
        data = resp.json()
        for fg in data["flowgroups"]:
            assert fg["pipeline"] == "acmi_edw_bronze"

    def test_filter_by_nonexistent_pipeline_returns_empty(self, client):
        resp = client.get("/api/flowgroups", params={"pipeline": "nonexistent"})
        assert resp.status_code == 200
        data = resp.json()
        assert data["total"] == 0
        assert data["flowgroups"] == []


class TestGetFlowgroup:
    """Tests for GET /api/flowgroups/{name}."""

    def test_known_flowgroup_returns_200(self, client):
        resp = client.get("/api/flowgroups/customer_bronze")
        assert resp.status_code == 200

    def test_flowgroup_name_matches(self, client):
        data = client.get("/api/flowgroups/customer_bronze").json()
        assert data["flowgroup"]["flowgroup"] == "customer_bronze"

    def test_has_source_file_field(self, client):
        data = client.get("/api/flowgroups/customer_bronze").json()
        assert "source_file" in data

    def test_nonexistent_flowgroup_returns_404(self, client):
        resp = client.get("/api/flowgroups/nonexistent_flowgroup")
        assert resp.status_code == 404


class TestGetResolvedFlowgroup:
    """Tests for GET /api/flowgroups/{name}/resolved."""

    def test_returns_200_with_dev_env(self, client):
        resp = client.get(
            "/api/flowgroups/customer_bronze/resolved", params={"env": "dev"}
        )
        assert resp.status_code == 200

    def test_environment_matches_query(self, client):
        data = client.get(
            "/api/flowgroups/customer_bronze/resolved", params={"env": "dev"}
        ).json()
        assert data["environment"] == "dev"

    def test_has_applied_presets(self, client):
        data = client.get(
            "/api/flowgroups/customer_bronze/resolved", params={"env": "dev"}
        ).json()
        assert "applied_presets" in data

    def test_resolved_flowgroup_has_actions(self, client):
        data = client.get(
            "/api/flowgroups/customer_bronze/resolved", params={"env": "dev"}
        ).json()
        fg = data["flowgroup"]
        assert "actions" in fg
        assert len(fg["actions"]) > 0

    def test_different_environment(self, client):
        resp = client.get(
            "/api/flowgroups/customer_bronze/resolved", params={"env": "tst"}
        )
        assert resp.status_code == 200
        assert resp.json()["environment"] == "tst"

    def test_nonexistent_flowgroup_returns_404(self, client):
        resp = client.get("/api/flowgroups/nonexistent/resolved")
        assert resp.status_code == 404
