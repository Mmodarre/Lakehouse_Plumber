"""Tests for preset list and detail endpoints."""

import pytest


pytestmark = pytest.mark.api

# Known presets in the E2E fixture project
KNOWN_PRESETS = [
    "default_delta_properties",
    "cloudfiles_defaults",
    "bronze_layer",
    "write_defaults",
]


class TestListPresets:
    """Tests for GET /api/presets."""

    def test_returns_200(self, client):
        resp = client.get("/api/presets")
        assert resp.status_code == 200

    def test_total_matches_list_length(self, client):
        data = client.get("/api/presets").json()
        assert data["total"] == len(data["presets"])

    def test_known_presets_are_present(self, client):
        presets = client.get("/api/presets").json()["presets"]
        for name in KNOWN_PRESETS:
            assert name in presets, f"Expected preset '{name}' not found"


class TestGetPreset:
    """Tests for GET /api/presets/{name}."""

    def test_returns_200(self, client):
        resp = client.get("/api/presets/default_delta_properties")
        assert resp.status_code == 200

    def test_name_matches(self, client):
        data = client.get("/api/presets/default_delta_properties").json()
        assert data["name"] == "default_delta_properties"

    def test_has_raw_and_resolved(self, client):
        data = client.get("/api/presets/default_delta_properties").json()
        assert "raw" in data
        assert "resolved" in data
        assert isinstance(data["raw"], dict)
        assert isinstance(data["resolved"], dict)

    def test_bronze_layer_resolved_has_write_actions(self, client):
        data = client.get("/api/presets/bronze_layer").json()
        assert "write_actions" in data["resolved"]

    def test_nonexistent_preset_returns_404(self, client):
        resp = client.get("/api/presets/nonexistent_preset")
        assert resp.status_code == 404
