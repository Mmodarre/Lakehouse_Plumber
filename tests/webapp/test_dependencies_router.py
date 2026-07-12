"""Tests for the webapp dependency-analysis endpoints.

All three graph levels are covered (``/graph/pipeline``, ``/graph/flowgroup``,
``/graph/action``); there is no ``/export/{fmt}`` endpoint in v1. The
``client`` fixture is the read-only
:class:`~fastapi.testclient.TestClient` over the shared E2E fixture project
(see ``tests/webapp/conftest.py``).
"""

import pytest

from lhp.webapp.schemas.dependency import GraphResponse

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
        # Only the three literal level routes exist; any other level is an
        # unmatched path and FastAPI returns 404.
        resp = client.get("/api/dependencies/graph/bogus")
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


class TestGetFlowgroupGraph:
    """Tests for GET /api/dependencies/graph/flowgroup (pipeline drill modal)."""

    def test_returns_200_with_nodes_and_edges(self, client):
        resp = client.get("/api/dependencies/graph/flowgroup")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) > 0
        assert len(data["edges"]) > 0

    def test_metadata_level_is_flowgroup(self, client):
        data = client.get("/api/dependencies/graph/flowgroup").json()
        assert data["metadata"]["level"] == "flowgroup"

    def test_validates_against_graph_response_schema(self, client):
        data = client.get("/api/dependencies/graph/flowgroup").json()
        response = GraphResponse.model_validate(data)
        assert response.metadata.total_nodes == len(response.nodes)
        assert response.metadata.total_edges == len(response.edges)

    def test_node_types_are_flowgroup_or_external(self, client):
        data = client.get("/api/dependencies/graph/flowgroup").json()
        assert {n["type"] for n in data["nodes"]} <= {"flowgroup", "external"}

    def test_flowgroup_nodes_carry_pipeline_and_flowgroup(self, client):
        data = client.get("/api/dependencies/graph/flowgroup").json()
        flowgroup_nodes = [n for n in data["nodes"] if n["type"] == "flowgroup"]
        assert flowgroup_nodes
        for node in flowgroup_nodes:
            assert node["pipeline"]
            assert node["flowgroup"] == node["id"]

    def test_edge_endpoints_reference_existing_nodes(self, client):
        data = client.get("/api/dependencies/graph/flowgroup").json()
        node_ids = {n["id"] for n in data["nodes"]}
        for edge in data["edges"]:
            assert edge["source"] in node_ids
            assert edge["target"] in node_ids

    def test_filter_by_pipeline_narrows_nodes(self, client):
        full = client.get("/api/dependencies/graph/flowgroup").json()
        filtered = client.get(
            "/api/dependencies/graph/flowgroup",
            params={"pipeline": "acmi_edw_bronze"},
        ).json()
        assert 0 < len(filtered["nodes"]) < len(full["nodes"])
        for node in filtered["nodes"]:
            if node["type"] == "flowgroup":
                assert node["pipeline"] == "acmi_edw_bronze"

    def test_unknown_pipeline_matches_pipeline_level_behavior(self, client):
        # Parity contract: an unknown pipeline filter yields the same shape
        # (200 + empty graph) at every level.
        pipeline_level = client.get(
            "/api/dependencies/graph/pipeline", params={"pipeline": "does_not_exist"}
        )
        flowgroup_level = client.get(
            "/api/dependencies/graph/flowgroup", params={"pipeline": "does_not_exist"}
        )
        assert flowgroup_level.status_code == pipeline_level.status_code == 200
        assert flowgroup_level.json()["nodes"] == pipeline_level.json()["nodes"] == []
        assert flowgroup_level.json()["edges"] == pipeline_level.json()["edges"] == []


class TestGetActionGraph:
    """Tests for GET /api/dependencies/graph/action (flowgroup drill modal)."""

    def test_returns_200_with_nodes_and_edges(self, client):
        resp = client.get("/api/dependencies/graph/action")
        assert resp.status_code == 200
        data = resp.json()
        assert len(data["nodes"]) > 0
        assert len(data["edges"]) > 0

    def test_metadata_level_is_action(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        assert data["metadata"]["level"] == "action"

    def test_validates_against_graph_response_schema(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        response = GraphResponse.model_validate(data)
        assert response.metadata.total_nodes == len(response.nodes)
        assert response.metadata.total_edges == len(response.edges)

    def test_node_types_are_action_kinds_or_external(self, client):
        data = client.get("/api/dependencies/graph/action").json()
        assert {n["type"] for n in data["nodes"]} <= {
            "load",
            "transform",
            "write",
            "test",
            "external",
        }

    def test_action_nodes_are_keyed_flowgroup_dot_action(self, client):
        # The frontend narrows the graph to one flowgroup client-side via the
        # node's ``flowgroup`` field, so both the key format and the field
        # must hold for every action node.
        data = client.get("/api/dependencies/graph/action").json()
        action_nodes = [n for n in data["nodes"] if n["type"] != "external"]
        assert action_nodes
        for node in action_nodes:
            assert node["flowgroup"]
            assert node["id"].startswith(f"{node['flowgroup']}.")
            assert node["label"] == node["id"].removeprefix(f"{node['flowgroup']}.")

    def test_filter_by_pipeline_narrows_nodes(self, client):
        full = client.get("/api/dependencies/graph/action").json()
        filtered = client.get(
            "/api/dependencies/graph/action",
            params={"pipeline": "acmi_edw_bronze"},
        ).json()
        assert 0 < len(filtered["nodes"]) < len(full["nodes"])
        for node in filtered["nodes"]:
            if node["type"] != "external":
                assert node["pipeline"] == "acmi_edw_bronze"

    def test_unknown_pipeline_matches_pipeline_level_behavior(self, client):
        pipeline_level = client.get(
            "/api/dependencies/graph/pipeline", params={"pipeline": "does_not_exist"}
        )
        action_level = client.get(
            "/api/dependencies/graph/action", params={"pipeline": "does_not_exist"}
        )
        assert action_level.status_code == pipeline_level.status_code == 200
        assert action_level.json()["nodes"] == pipeline_level.json()["nodes"] == []
        assert action_level.json()["edges"] == pipeline_level.json()["edges"] == []


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
