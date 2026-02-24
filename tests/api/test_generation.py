"""Tests for code generation and preview endpoints.

All tests use mutable_client since generation writes files to disk.
"""

import pytest


pytestmark = pytest.mark.api


class TestGeneratePreview:
    """Tests for POST /api/generate/preview (dry run)."""

    def test_returns_200(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        )
        assert resp.status_code == 200

    def test_preview_success(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        ).json()
        assert data["success"] is True

    def test_would_generate_is_non_empty(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        ).json()
        assert len(data["would_generate"]) > 0

    def test_total_flowgroups_is_positive(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        ).json()
        assert data["total_flowgroups"] > 0

    def test_default_generation_mode_is_smart(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        ).json()
        assert data["generation_mode"] == "smart"

    def test_force_mode(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev", "force": True},
        ).json()
        assert data["generation_mode"] == "force"

    def test_specific_pipeline_preview(self, mutable_client):
        data = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "dev", "pipeline": "acmi_edw_bronze", "force": True},
        ).json()
        assert data["success"] is True
        assert len(data["would_generate"]) > 0

    def test_different_environment(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate/preview",
            json={"environment": "tst"},
        )
        assert resp.status_code == 200


class TestGenerate:
    """Tests for POST /api/generate (actual code generation).

    These tests write files to the mutable project copy.
    """

    def test_returns_200(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate",
            json={"environment": "dev", "force": True},
        )
        assert resp.status_code == 200

    def test_generate_success(self, mutable_client):
        data = mutable_client.post(
            "/api/generate",
            json={"environment": "dev", "force": True},
        ).json()
        assert data["success"] is True

    def test_files_written_is_positive(self, mutable_client):
        data = mutable_client.post(
            "/api/generate",
            json={"environment": "dev", "force": True},
        ).json()
        assert data["files_written"] > 0

    def test_generated_files_dict_non_empty(self, mutable_client):
        data = mutable_client.post(
            "/api/generate",
            json={"environment": "dev", "force": True},
        ).json()
        assert len(data["generated_files"]) > 0

    def test_output_location_contains_env(self, mutable_client):
        data = mutable_client.post(
            "/api/generate",
            json={"environment": "dev", "force": True},
        ).json()
        assert "generated" in data["output_location"]
        assert "dev" in data["output_location"]

    def test_specific_pipeline_generation(self, mutable_client):
        resp = mutable_client.post(
            "/api/generate",
            json={
                "environment": "dev",
                "pipeline": "acmi_edw_bronze",
                "force": True,
            },
        )
        assert resp.status_code == 200
        assert resp.json()["success"] is True
