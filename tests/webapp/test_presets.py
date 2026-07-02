"""Tests for preset list and detail read endpoints.

Reads only — no auth; subjects are the shared E2E fixture project. The detail
endpoint returns the RAW preset YAML only (the ``resolved`` inheritance chain
is deferred — see ``routers/presets.py``).
"""

from __future__ import annotations

import pytest

pytestmark = pytest.mark.webapp

# Known presets in the E2E fixture project (substitutions-independent).
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

    def test_detail_returns_summaries(self, client):
        data = client.get("/api/presets", params={"detail": True}).json()
        assert data["total"] == len(data["presets"])
        names = {p["name"] for p in data["presets"]}
        for name in KNOWN_PRESETS:
            assert name in names


class TestGetPreset:
    """Tests for GET /api/presets/{name}."""

    def test_returns_200(self, client):
        resp = client.get("/api/presets/default_delta_properties")
        assert resp.status_code == 200

    def test_name_matches(self, client):
        data = client.get("/api/presets/default_delta_properties").json()
        assert data["name"] == "default_delta_properties"

    def test_has_raw_dict(self, client):
        data = client.get("/api/presets/default_delta_properties").json()
        assert "raw" in data
        assert isinstance(data["raw"], dict)

    def test_raw_carries_preset_fields(self, client):
        # The raw body is the literal preset YAML, so the preset's own
        # ``name`` key round-trips through the file content.
        data = client.get("/api/presets/default_delta_properties").json()
        assert data["raw"].get("name") == "default_delta_properties"

    def test_resolved_is_deferred(self, client):
        # The resolved inheritance chain is deferred; with
        # response_model_exclude_none the field is omitted entirely.
        data = client.get("/api/presets/bronze_layer").json()
        assert "resolved" not in data

    def test_nonexistent_preset_returns_404(self, client):
        resp = client.get("/api/presets/nonexistent_preset")
        assert resp.status_code == 404
