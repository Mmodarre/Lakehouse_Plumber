"""Tests for the blueprint list endpoint (``GET /api/blueprints``).

The contract under test:

- Backed by the public :meth:`InspectionFacade.list_blueprints`.
- Default listing returns summaries with empty ``instances``;
  ``include_instances=true`` populates per-instance flowgroup counts and
  pipeline names.
- Subjects are the shared E2E fixture project, which declares one blueprint
  (``blueprints/medallion_demo.yaml``: 2 parameters, 3 flowgroup specs) with
  two instance files under ``pipelines/10_blueprint_demo/sites/``.
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp

# The pipelines every fixture instance expands into (site_alpha and site_beta
# both instantiate the same 3-layer medallion blueprint).
EXPECTED_PIPELINES = {"acme_edw_bp_raw", "acme_edw_bp_bronze", "acme_edw_bp_silver"}


def _medallion(data: dict) -> dict:
    """Pick the fixture's ``medallion_demo`` blueprint out of a list payload."""
    matches = [b for b in data["blueprints"] if b["name"] == "medallion_demo"]
    assert matches, f"medallion_demo not in {[b['name'] for b in data['blueprints']]}"
    return matches[0]


class TestListBlueprints:
    """Tests for GET /api/blueprints."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/blueprints")
        assert resp.status_code == 200

    def test_total_matches_list_length(self, client: TestClient) -> None:
        data = client.get("/api/blueprints").json()
        assert data["total"] == len(data["blueprints"])
        assert data["total"] >= 1

    def test_summary_shape(self, client: TestClient) -> None:
        bp = _medallion(client.get("/api/blueprints").json())
        assert bp["version"] == "1.0"
        assert isinstance(bp["description"], str)
        assert bp["parameter_count"] == 2
        assert bp["flowgroup_count"] == 3
        assert bp["instance_count"] == 2

    def test_default_listing_omits_instances(self, client: TestClient) -> None:
        bp = _medallion(client.get("/api/blueprints").json())
        assert bp["instances"] == []

    def test_instance_count_present_without_instances(self, client: TestClient) -> None:
        """The cheap listing still reports how many instances exist."""
        bp = _medallion(
            client.get("/api/blueprints", params={"include_instances": False}).json()
        )
        assert bp["instance_count"] == 2
        assert bp["instances"] == []


class TestListBlueprintsWithInstances:
    """Tests for GET /api/blueprints?include_instances=true."""

    def test_instances_populated(self, client: TestClient) -> None:
        bp = _medallion(
            client.get("/api/blueprints", params={"include_instances": True}).json()
        )
        assert len(bp["instances"]) == 2

    def test_instance_shape(self, client: TestClient) -> None:
        bp = _medallion(
            client.get("/api/blueprints", params={"include_instances": True}).json()
        )
        for inst in bp["instances"]:
            assert inst["flowgroup_count"] == 3
            assert set(inst["pipelines"]) == EXPECTED_PIPELINES

    def test_instance_paths_relative_to_project_root(self, client: TestClient) -> None:
        bp = _medallion(
            client.get("/api/blueprints", params={"include_instances": True}).json()
        )
        paths = sorted(i["instance_file_path"] for i in bp["instances"])
        assert paths == [
            "pipelines/10_blueprint_demo/sites/site_alpha.yaml",
            "pipelines/10_blueprint_demo/sites/site_beta.yaml",
        ]
