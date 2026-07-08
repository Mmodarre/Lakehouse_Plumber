"""Tests for preset list and detail read endpoints.

Reads only — no auth; subjects are the shared E2E fixture project. The detail
endpoint pairs the RAW preset YAML with the ``resolved`` inheritance-merged
config and base→leaf ``chain`` from ``InspectionFacade.resolve_preset``. The
fixture presets carry no ``extends``, so chains here are single-level;
multi-level chain / merge / error behavior is covered by
``tests/api/test_resolve_preset.py``. The broken-chain → 422 route test
uses the mutable (deep-copied) project fixtures, never the shared one.
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

    def test_resolved_is_populated(self, client):
        # ``resolved`` carries the inheritance-merged defaults from
        # InspectionFacade.resolve_preset.
        data = client.get("/api/presets/bronze_layer").json()
        assert isinstance(data["resolved"], dict)
        # bronze_layer's defaults declare write_actions table properties;
        # the merge must surface them.
        assert "write_actions" in data["resolved"]
        props = data["resolved"]["write_actions"]["streaming_table"]["table_properties"]
        assert props["delta.enableChangeDataFeed"] == "true"

    def test_chain_is_populated(self, client):
        # The fixture presets have no ``extends``, so the chain is the
        # preset itself (base→leaf, requested preset last).
        data = client.get("/api/presets/bronze_layer").json()
        assert data["chain"] == ["bronze_layer"]

    def test_resolved_and_chain_for_preset_without_extends(self, client):
        data = client.get("/api/presets/default_delta_properties").json()
        assert data["chain"] == ["default_delta_properties"]
        assert isinstance(data["resolved"], dict)

    def test_nonexistent_preset_returns_404(self, client):
        resp = client.get("/api/presets/nonexistent_preset")
        assert resp.status_code == 404


class TestGetPresetBrokenChain:
    """GET /api/presets/{name} when the ``extends`` chain is broken.

    Exercises ``resolve_preset`` raising ``LHP-ACT-001`` through
    ``asyncio.to_thread`` inside the router, mapped to a structured 422
    by the app-level ``lhp_error_handler`` (the preset FILE exists, so
    the router's plain-404 guard does not fire).
    """

    def test_missing_extends_target_returns_422(self, mutable_project, mutable_client):
        # Written before the first request: the application facade (and
        # its preset manager) is built lazily on first use, so it sees
        # this file. The mutable deep-copy keeps the shared fixture
        # project pristine.
        preset_file = mutable_project / "presets" / "orphan_preset.yaml"
        preset_file.write_text(
            'name: orphan_preset\nversion: "1.0"\nextends: ghost_that_does_not_exist\n',
            encoding="utf-8",
        )
        resp = mutable_client.get("/api/presets/orphan_preset")
        assert resp.status_code == 422
        error = resp.json()["error"]
        assert error["code"] == "LHP-ACT-001"
        assert error["http_status"] == 422
