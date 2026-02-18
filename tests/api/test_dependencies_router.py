"""Tests for dependency analysis endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestGetDependencies:
    """Tests for GET /api/dependencies."""

    def test_returns_200(self, client):
        resp = client.get("/api/dependencies")
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/dependencies").json()
        assert "total_pipelines" in data
        assert "total_external_sources" in data
        assert "execution_stages" in data
        assert "pipeline_dependencies" in data

    def test_total_pipelines_is_positive(self, client):
        data = client.get("/api/dependencies").json()
        assert data["total_pipelines"] > 0

    def test_filter_by_pipeline(self, client):
        resp = client.get(
            "/api/dependencies", params={"pipeline": "acmi_edw_bronze"}
        )
        assert resp.status_code == 200


class TestGetDependencyGraph:
    """Tests for GET /api/dependencies/graph/{level}."""

    def test_action_level_returns_200(self, client):
        resp = client.get("/api/dependencies/graph/action")
        assert resp.status_code == 200

    def test_flowgroup_level_returns_200(self, client):
        resp = client.get("/api/dependencies/graph/flowgroup")
        assert resp.status_code == 200

    def test_pipeline_level_returns_200(self, client):
        resp = client.get("/api/dependencies/graph/pipeline")
        assert resp.status_code == 200

    def test_invalid_level_returns_400(self, client):
        resp = client.get("/api/dependencies/graph/invalid")
        assert resp.status_code == 400

    def test_action_graph_has_nodes_and_edges(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        assert "nodes" in data
        assert "edges" in data
        assert "metadata" in data

    def test_metadata_level_matches_request(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        assert data["metadata"]["level"] == "action"

    def test_nodes_have_expected_fields(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        if data["nodes"]:
            node = data["nodes"][0]
            assert "id" in node
            assert "label" in node
            assert "type" in node

    def test_edges_have_expected_fields(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        if data["edges"]:
            edge = data["edges"][0]
            assert "source" in edge
            assert "target" in edge
            assert "type" in edge

    def test_filter_by_pipeline(self, client):
        resp = client.get(
            "/api/dependencies/graph/action",
            params={"pipeline": "acmi_edw_bronze"},
        )
        assert resp.status_code == 200


class TestExecutionOrder:
    """Tests for GET /api/dependencies/execution-order."""

    def test_returns_200(self, client):
        resp = client.get("/api/dependencies/execution-order")
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/dependencies/execution-order").json()
        assert "stages" in data
        assert "total_stages" in data
        assert "flat_order" in data

    def test_total_stages_matches_list(self, client):
        data = client.get("/api/dependencies/execution-order").json()
        assert data["total_stages"] == len(data["stages"])


class TestCircularDependencies:
    """Tests for GET /api/dependencies/circular."""

    def test_returns_200(self, client):
        resp = client.get("/api/dependencies/circular")
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/dependencies/circular").json()
        assert "has_circular" in data
        assert "cycles" in data
        assert "total_cycles" in data

    def test_total_cycles_matches_list(self, client):
        data = client.get("/api/dependencies/circular").json()
        assert data["total_cycles"] == len(data["cycles"])


class TestExternalSources:
    """Tests for GET /api/dependencies/external-sources."""

    def test_returns_200(self, client):
        resp = client.get("/api/dependencies/external-sources")
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/dependencies/external-sources").json()
        assert "sources" in data
        assert "total" in data
        assert isinstance(data["sources"], list)


class TestExportDependencies:
    """Tests for GET /api/dependencies/export/{fmt}."""

    def test_dot_format(self, client):
        resp = client.get("/api/dependencies/export/dot")
        assert resp.status_code == 200
        data = resp.json()
        assert data["format"] == "dot"
        assert isinstance(data["content"], str)

    def test_json_format(self, client):
        resp = client.get("/api/dependencies/export/json")
        assert resp.status_code == 200
        data = resp.json()
        assert data["format"] == "json"

    def test_text_format(self, client):
        resp = client.get("/api/dependencies/export/text")
        assert resp.status_code == 200
        data = resp.json()
        assert data["format"] == "text"

    def test_invalid_format_returns_400(self, client):
        resp = client.get("/api/dependencies/export/invalid")
        assert resp.status_code == 400
