"""Tests for the webapp dependency-analysis endpoints.

Only ``/graph/pipeline`` is covered — there is no ``/export/{fmt}`` endpoint
in v1 and no per-action / per-flowgroup graph levels. The ``client`` fixture is
the read-only :class:`~fastapi.testclient.TestClient` over the shared E2E
fixture project (see ``tests/webapp/conftest.py``).
"""

import pytest

pytestmark = pytest.mark.webapp


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
        resp = client.get("/api/dependencies", params={"pipeline": "acmi_edw_bronze"})
        assert resp.status_code == 200


class TestGetPipelineGraph:
    """Tests for GET /api/dependencies/graph/pipeline."""

    def test_pipeline_level_returns_200(self, client):
        resp = client.get("/api/dependencies/graph/pipeline")
        assert resp.status_code == 200

    def test_unknown_level_returns_404(self, client):
        # Per-action / per-flowgroup levels are a v2 feature; only the literal
        # ``/graph/pipeline`` route exists, so any other level is an unmatched
        # path and FastAPI returns 404.
        resp = client.get("/api/dependencies/graph/action")
        assert resp.status_code == 404

    def test_pipeline_graph_has_nodes_and_edges(self, client):
        data = client.get("/api/dependencies/graph/pipeline").json()
        assert "nodes" in data
        assert "edges" in data
        assert "metadata" in data

    def test_metadata_level_is_pipeline(self, client):
        data = client.get("/api/dependencies/graph/pipeline").json()
        assert data["metadata"]["level"] == "pipeline"

    def test_nodes_have_expected_fields(self, client):
        data = client.get("/api/dependencies/graph/pipeline").json()
        if data["nodes"]:
            node = data["nodes"][0]
            assert "id" in node
            assert "label" in node
            assert "type" in node

    def test_edges_have_expected_fields(self, client):
        data = client.get("/api/dependencies/graph/pipeline").json()
        if data["edges"]:
            edge = data["edges"][0]
            assert "source" in edge
            assert "target" in edge
            assert "type" in edge

    def test_filter_by_pipeline(self, client):
        resp = client.get(
            "/api/dependencies/graph/pipeline",
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
