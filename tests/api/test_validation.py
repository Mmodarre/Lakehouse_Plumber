"""Tests for validation endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestValidateAll:
    """Tests for POST /api/validate."""

    def test_returns_200(self, client):
        resp = client.post("/api/validate", json={"environment": "dev"})
        assert resp.status_code == 200

    def test_success_for_valid_project(self, client):
        data = client.post("/api/validate", json={"environment": "dev"}).json()
        assert data["success"] is True

    def test_validated_pipelines_non_empty(self, client):
        data = client.post("/api/validate", json={"environment": "dev"}).json()
        assert len(data["validated_pipelines"]) > 0

    def test_errors_is_list(self, client):
        data = client.post("/api/validate", json={"environment": "dev"}).json()
        assert isinstance(data["errors"], list)

    def test_different_environment(self, client):
        resp = client.post("/api/validate", json={"environment": "tst"})
        assert resp.status_code == 200


class TestValidatePipeline:
    """Tests for POST /api/validate/pipeline/{name}."""

    def test_known_pipeline_returns_200(self, client):
        resp = client.post(
            "/api/validate/pipeline/acmi_edw_bronze",
            json={"environment": "dev"},
        )
        assert resp.status_code == 200

    def test_pipeline_in_validated_list(self, client):
        data = client.post(
            "/api/validate/pipeline/acmi_edw_bronze",
            json={"environment": "dev"},
        ).json()
        assert "acmi_edw_bronze" in data["validated_pipelines"]


class TestValidateIdempotent:
    """Validation must be idempotent — calling twice returns same result.

    Regression test for cache-corruption bug where CachingYAMLParser
    returned shared FlowGroup references and process_flowgroup mutated
    them in-place via actions.extend(), causing duplicate action errors
    on every subsequent call.
    """

    def test_validate_all_twice_returns_success_both_times(self, client):
        """Validate all pipelines twice — both must succeed."""
        data1 = client.post("/api/validate", json={"environment": "dev"}).json()
        data2 = client.post("/api/validate", json={"environment": "dev"}).json()
        assert data1["success"] is True, f"1st call failed: {data1['errors'][:2]}"
        assert data2["success"] is True, f"2nd call failed: {data2['errors'][:2]}"

    def test_validate_pipeline_twice_returns_success_both_times(self, client):
        """Validate a specific pipeline twice — both must succeed."""
        payload = {"environment": "dev"}
        data1 = client.post(
            "/api/validate/pipeline/acmi_edw_bronze", json=payload
        ).json()
        data2 = client.post(
            "/api/validate/pipeline/acmi_edw_bronze", json=payload
        ).json()
        assert data1["success"] is True
        assert data2["success"] is True


class TestValidateFlowgroup:
    """Tests for POST /api/validate/flowgroup/{name}."""

    def test_known_flowgroup_returns_200(self, client):
        resp = client.post("/api/validate/flowgroup/customer_bronze?env=dev")
        assert resp.status_code == 200

    def test_validated_pipelines_includes_flowgroup(self, client):
        data = client.post("/api/validate/flowgroup/customer_bronze?env=dev").json()
        assert "customer_bronze" in data["validated_pipelines"]


class TestValidateYAML:
    """Tests for POST /api/validate/yaml."""

    def test_valid_yaml_returns_success(self, client):
        yaml_content = """
pipeline: test_pipeline
flowgroup: test_fg
actions:
  - name: test_load
    type: load
    source:
      type: cloudfiles
      format: csv
      path: /mnt/data/test
"""
        resp = client.post(
            "/api/validate/yaml",
            json={"content": yaml_content},
        )
        assert resp.status_code == 200

    def test_invalid_yaml_returns_errors(self, client):
        yaml_content = """
flowgroup: missing_pipeline
actions: not_a_list
"""
        resp = client.post(
            "/api/validate/yaml",
            json={"content": yaml_content},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        assert len(data["errors"]) > 0

    def test_non_parseable_yaml_returns_errors(self, client):
        resp = client.post(
            "/api/validate/yaml",
            json={"content": "{{{{not yaml at all::::"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
