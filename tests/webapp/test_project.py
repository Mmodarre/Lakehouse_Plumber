"""Tests for the project info / stats endpoints.

The contract under test:

- There is no ``GET``/``PUT /api/project/config``: the structured config
  read/write endpoints are not part of the local IDE.
- Expected values track the shared E2E fixture project (``acme_edw``, v1.0).
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


class TestProjectInfo:
    """Tests for GET /api/project."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/project")
        assert resp.status_code == 200

    def test_project_name(self, client: TestClient) -> None:
        data = client.get("/api/project").json()
        assert data["name"] == "acme_edw"

    def test_project_version(self, client: TestClient) -> None:
        data = client.get("/api/project").json()
        assert data["version"] == "1.0"

    def test_resource_counts_has_all_keys(self, client: TestClient) -> None:
        counts = client.get("/api/project").json()["resource_counts"]
        for key in (
            "pipelines",
            "flowgroups",
            "presets",
            "templates",
            "environments",
        ):
            assert key in counts

    def test_resource_counts_are_positive(self, client: TestClient) -> None:
        counts = client.get("/api/project").json()["resource_counts"]
        assert counts["pipelines"] > 0
        assert counts["flowgroups"] > 0
        assert counts["presets"] > 0
        assert counts["templates"] > 0
        assert counts["environments"] > 0


class TestProjectStats:
    """Tests for GET /api/project/stats."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/project/stats")
        assert resp.status_code == 200

    def test_totals_are_positive(self, client: TestClient) -> None:
        data = client.get("/api/project/stats").json()
        assert data["total_pipelines"] > 0
        assert data["total_flowgroups"] > 0
        assert data["total_actions"] > 0

    def test_actions_by_type_includes_core_types(self, client: TestClient) -> None:
        data = client.get("/api/project/stats").json()
        types = data["actions_by_type"]
        assert "load" in types
        assert "transform" in types
        assert "write" in types

    def test_pipelines_list_has_expected_fields(self, client: TestClient) -> None:
        data = client.get("/api/project/stats").json()
        assert len(data["pipelines"]) > 0
        first = data["pipelines"][0]
        assert "name" in first
        assert "flowgroup_count" in first
        assert "action_count" in first
