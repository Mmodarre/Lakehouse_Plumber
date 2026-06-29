"""Tests for the pipeline list / detail / flowgroups endpoints.

The contract under test:

- There is no ``GET``/``PUT /api/pipelines/{name}/config``: per-pipeline merged
  config is not exposed by the public inspection API.
- Expected values track the shared E2E fixture project
  (``acmi_edw_bronze`` is a known pipeline).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


class TestListPipelines:
    """Tests for GET /api/pipelines."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/pipelines")
        assert resp.status_code == 200

    def test_pipelines_list_non_empty(self, client: TestClient) -> None:
        data = client.get("/api/pipelines").json()
        assert len(data["pipelines"]) > 0

    def test_total_matches_list_length(self, client: TestClient) -> None:
        data = client.get("/api/pipelines").json()
        assert data["total"] == len(data["pipelines"])

    def test_each_pipeline_has_expected_fields(self, client: TestClient) -> None:
        pipelines = client.get("/api/pipelines").json()["pipelines"]
        for p in pipelines:
            assert "name" in p
            assert "flowgroup_count" in p
            assert "action_count" in p

    def test_known_pipeline_is_present(self, client: TestClient) -> None:
        pipelines = client.get("/api/pipelines").json()["pipelines"]
        names = [p["name"] for p in pipelines]
        assert "acmi_edw_bronze" in names


class TestGetPipeline:
    """Tests for GET /api/pipelines/{name}."""

    def test_known_pipeline_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/pipelines/acmi_edw_bronze")
        assert resp.status_code == 200

    def test_pipeline_name_matches(self, client: TestClient) -> None:
        data = client.get("/api/pipelines/acmi_edw_bronze").json()
        assert data["name"] == "acmi_edw_bronze"

    def test_pipeline_has_flowgroups_list(self, client: TestClient) -> None:
        data = client.get("/api/pipelines/acmi_edw_bronze").json()
        assert len(data["flowgroups"]) > 0

    def test_nonexistent_pipeline_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/pipelines/nonexistent_pipeline")
        assert resp.status_code == 404


class TestGetPipelineFlowgroups:
    """Tests for GET /api/pipelines/{name}/flowgroups."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/pipelines/acmi_edw_bronze/flowgroups")
        assert resp.status_code == 200

    def test_flowgroups_list_non_empty(self, client: TestClient) -> None:
        data = client.get("/api/pipelines/acmi_edw_bronze/flowgroups").json()
        assert len(data["flowgroups"]) > 0

    def test_each_flowgroup_has_expected_fields(self, client: TestClient) -> None:
        fgs = client.get("/api/pipelines/acmi_edw_bronze/flowgroups").json()[
            "flowgroups"
        ]
        for fg in fgs:
            assert "name" in fg
            assert "pipeline" in fg
            assert "action_count" in fg
            assert "action_types" in fg

    def test_all_flowgroups_belong_to_pipeline(self, client: TestClient) -> None:
        fgs = client.get("/api/pipelines/acmi_edw_bronze/flowgroups").json()[
            "flowgroups"
        ]
        for fg in fgs:
            assert fg["pipeline"] == "acmi_edw_bronze"

    def test_nonexistent_pipeline_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/pipelines/nonexistent_pipeline/flowgroups")
        assert resp.status_code == 404
