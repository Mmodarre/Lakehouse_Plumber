"""Tests for preset create, update, and delete endpoints.

All tests use mutable_client since these endpoints write files to disk.
Each test uses unique preset names to avoid interference (session-scoped client).
"""

import pytest


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MINIMAL_PRESET = {
    "name": "test_crud_preset_create",
    "version": "1.0",
    "defaults": {
        "write_actions": {
            "streaming_table": {
                "table_properties": {
                    "delta.enableRowTracking": "true",
                }
            }
        }
    },
}


def _make_preset(name: str) -> dict:
    """Return a minimal preset payload with the given name."""
    return {
        "name": name,
        "version": "1.0",
        "defaults": {
            "load_actions": {},
            "write_actions": {
                "streaming_table": {
                    "table_properties": {
                        "delta.enableRowTracking": "true",
                    }
                }
            },
        },
    }


# ---------------------------------------------------------------------------
# TestCreatePreset
# ---------------------------------------------------------------------------


class TestCreatePreset:
    """Tests for POST /api/presets."""

    def test_creates_preset_returns_201(self, mutable_client):
        payload = _make_preset("test_crud_create_201")
        resp = mutable_client.post("/api/presets", json=payload)
        assert resp.status_code == 201

    def test_response_has_path_and_success(self, mutable_client):
        payload = _make_preset("test_crud_create_fields")
        resp = mutable_client.post("/api/presets", json=payload)
        data = resp.json()
        assert data["success"] is True
        assert "path" in data
        assert "test_crud_create_fields" in data["path"]

    def test_created_preset_is_readable(self, mutable_client):
        payload = _make_preset("test_crud_create_readable")
        create_resp = mutable_client.post("/api/presets", json=payload)
        assert create_resp.status_code == 201

        get_resp = mutable_client.get("/api/presets/test_crud_create_readable")
        assert get_resp.status_code == 200
        data = get_resp.json()
        assert data["name"] == "test_crud_create_readable"


# ---------------------------------------------------------------------------
# TestUpdatePreset
# ---------------------------------------------------------------------------


class TestUpdatePreset:
    """Tests for PUT /api/presets/{name}."""

    def test_updates_preset_returns_200(self, mutable_client):
        # Create first
        payload = _make_preset("test_crud_update_200")
        mutable_client.post("/api/presets", json=payload)

        # Update
        payload["version"] = "2.0"
        resp = mutable_client.put(
            "/api/presets/test_crud_update_200", json=payload
        )
        assert resp.status_code == 200

    def test_if_match_with_correct_etag_succeeds(self, mutable_client):
        # Create
        payload = _make_preset("test_crud_update_etag_ok")
        mutable_client.post("/api/presets", json=payload)

        # GET to obtain ETag
        get_resp = mutable_client.get("/api/presets/test_crud_update_etag_ok")
        assert get_resp.status_code == 200
        etag = get_resp.headers["ETag"]

        # PUT with valid If-Match
        payload["version"] = "2.0"
        resp = mutable_client.put(
            "/api/presets/test_crud_update_etag_ok",
            json=payload,
            headers={"If-Match": etag},
        )
        assert resp.status_code == 200

    def test_stale_etag_returns_412(self, mutable_client):
        # Create
        payload = _make_preset("test_crud_update_stale_etag")
        mutable_client.post("/api/presets", json=payload)

        # PUT with a fabricated stale ETag
        payload["version"] = "2.0"
        resp = mutable_client.put(
            "/api/presets/test_crud_update_stale_etag",
            json=payload,
            headers={"If-Match": "stale_etag_value_000"},
        )
        assert resp.status_code == 412

    def test_update_nonexistent_returns_404(self, mutable_client):
        payload = _make_preset("test_crud_update_ghost")
        resp = mutable_client.put(
            "/api/presets/test_crud_update_ghost", json=payload
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestDeletePreset
# ---------------------------------------------------------------------------


class TestDeletePreset:
    """Tests for DELETE /api/presets/{name}."""

    def test_deletes_preset(self, mutable_client):
        # Create first
        payload = _make_preset("test_crud_delete_ok")
        mutable_client.post("/api/presets", json=payload)

        resp = mutable_client.delete("/api/presets/test_crud_delete_ok")
        assert resp.status_code == 200

    def test_delete_nonexistent_returns_404(self, mutable_client):
        resp = mutable_client.delete("/api/presets/test_crud_delete_ghost")
        assert resp.status_code == 404

    def test_preset_gone_after_delete(self, mutable_client):
        # Create
        payload = _make_preset("test_crud_delete_gone")
        create_resp = mutable_client.post("/api/presets", json=payload)
        assert create_resp.status_code == 201

        # Delete
        del_resp = mutable_client.delete("/api/presets/test_crud_delete_gone")
        assert del_resp.status_code == 200

        # Verify gone
        get_resp = mutable_client.get("/api/presets/test_crud_delete_gone")
        assert get_resp.status_code == 404
