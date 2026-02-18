"""Tests for template Create, Update, and Delete endpoints.

TDD tests -- the production endpoints (POST/PUT/DELETE /api/templates) do not
exist yet.  These tests are written first to drive the implementation.

All tests use ``mutable_client`` (session-scoped) because they write to disk.
Each test uses a unique template name to avoid collisions with sibling tests.
"""

import pytest


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TEMPLATE_YAML = {
    "pipeline": "{{ pipeline_name }}",
    "catalog": "{{ catalog }}",
    "actions": [
        {
            "name": "load_{{ table_name }}",
            "type": "load",
            "source": {"type": "cloudfiles", "path": "/data/{{ table_name }}"},
            "target": "v_{{ table_name }}",
        }
    ],
}


def _unique_template_yaml(suffix: str) -> dict:
    """Return a template payload with a unique description for differentiation."""
    return {**_TEMPLATE_YAML, "description": f"Auto-generated for {suffix}"}


# ---------------------------------------------------------------------------
# TestCreateTemplate
# ---------------------------------------------------------------------------


class TestCreateTemplate:
    """Tests for POST /api/templates."""

    def test_creates_template_returns_201(self, mutable_client):
        resp = mutable_client.post(
            "/api/templates",
            json={
                "name": "test_crud_template_create_201",
                "content": _unique_template_yaml("create_201"),
            },
        )
        assert resp.status_code == 201

    def test_response_has_path_and_success(self, mutable_client):
        resp = mutable_client.post(
            "/api/templates",
            json={
                "name": "test_crud_template_create_fields",
                "content": _unique_template_yaml("create_fields"),
            },
        )
        data = resp.json()
        assert data["success"] is True
        assert "path" in data

    def test_created_template_is_readable(self, mutable_client):
        name = "test_crud_template_create_readable"
        mutable_client.post(
            "/api/templates",
            json={
                "name": name,
                "content": _unique_template_yaml("create_readable"),
            },
        )

        resp = mutable_client.get(f"/api/templates/{name}")
        assert resp.status_code == 200


# ---------------------------------------------------------------------------
# TestUpdateTemplate
# ---------------------------------------------------------------------------


class TestUpdateTemplate:
    """Tests for PUT /api/templates/{name}."""

    def test_updates_template_returns_200(self, mutable_client):
        name = "test_crud_template_update_200"
        # Create first
        mutable_client.post(
            "/api/templates",
            json={"name": name, "content": _unique_template_yaml("update_200_v1")},
        )

        # Update
        resp = mutable_client.put(
            f"/api/templates/{name}",
            json={"content": _unique_template_yaml("update_200_v2")},
        )
        assert resp.status_code == 200

    def test_if_match_with_correct_etag_succeeds(self, mutable_client):
        name = "test_crud_template_update_etag_ok"
        # Create
        mutable_client.post(
            "/api/templates",
            json={"name": name, "content": _unique_template_yaml("etag_ok_v1")},
        )

        # GET to obtain current ETag
        get_resp = mutable_client.get(f"/api/templates/{name}")
        etag = get_resp.headers["ETag"]

        # PUT with matching If-Match
        resp = mutable_client.put(
            f"/api/templates/{name}",
            json={"content": _unique_template_yaml("etag_ok_v2")},
            headers={"If-Match": etag},
        )
        assert resp.status_code == 200

    def test_stale_etag_returns_412(self, mutable_client):
        name = "test_crud_template_update_stale"
        # Create
        mutable_client.post(
            "/api/templates",
            json={"name": name, "content": _unique_template_yaml("stale_v1")},
        )

        # PUT with a deliberately wrong ETag
        resp = mutable_client.put(
            f"/api/templates/{name}",
            json={"content": _unique_template_yaml("stale_v2")},
            headers={"If-Match": "stale_etag_value_000"},
        )
        assert resp.status_code == 412

    def test_update_nonexistent_returns_404(self, mutable_client):
        resp = mutable_client.put(
            "/api/templates/nonexistent_crud_template",
            json={"content": _unique_template_yaml("nonexistent")},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestDeleteTemplate
# ---------------------------------------------------------------------------


class TestDeleteTemplate:
    """Tests for DELETE /api/templates/{name}."""

    def test_deletes_template(self, mutable_client):
        name = "test_crud_template_delete_ok"
        mutable_client.post(
            "/api/templates",
            json={"name": name, "content": _unique_template_yaml("delete_ok")},
        )

        resp = mutable_client.delete(f"/api/templates/{name}")
        assert resp.status_code == 200

    def test_delete_nonexistent_returns_404(self, mutable_client):
        resp = mutable_client.delete("/api/templates/nonexistent_crud_template_del")
        assert resp.status_code == 404

    def test_template_gone_after_delete(self, mutable_client):
        name = "test_crud_template_delete_gone"
        mutable_client.post(
            "/api/templates",
            json={"name": name, "content": _unique_template_yaml("delete_gone")},
        )

        mutable_client.delete(f"/api/templates/{name}")

        resp = mutable_client.get(f"/api/templates/{name}")
        assert resp.status_code == 404
