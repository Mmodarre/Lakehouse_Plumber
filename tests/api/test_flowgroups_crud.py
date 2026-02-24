"""TDD tests for flowgroup CUD endpoints and source retrieval.

These tests exercise the Create / Update / Delete / Source endpoints
on the ``/api/flowgroups`` router.  The production handlers do **not exist
yet** -- tests are written first to drive the implementation.

All tests use ``mutable_client`` (session-scoped, writes to disk).
Create tests use unique flowgroup names to avoid cross-test collisions.
"""

import uuid

import pytest


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _unique_fg_name(prefix: str = "test_crud_fg") -> str:
    """Return a unique flowgroup name to avoid session-scoped collisions."""
    return f"{prefix}_{uuid.uuid4().hex[:8]}"


def _create_flowgroup(client, *, pipeline: str, flowgroup: str, config: dict | None = None):
    """POST a new flowgroup and return the response."""
    payload = {
        "pipeline": pipeline,
        "flowgroup": flowgroup,
        "config": config or {"catalog": "main", "target": "schema.test_table"},
    }
    return client.post("/api/flowgroups", json=payload)


# ---------------------------------------------------------------------------
# TestCreateFlowgroup
# ---------------------------------------------------------------------------


class TestCreateFlowgroup:
    """Tests for POST /api/flowgroups — create a new flowgroup YAML file."""

    def test_creates_flowgroup_returns_201(self, mutable_client):
        name = _unique_fg_name("create_201")
        resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        assert resp.status_code == 201

    def test_response_has_success_and_path(self, mutable_client):
        name = _unique_fg_name("create_path")
        resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        data = resp.json()
        assert data["success"] is True
        assert "test_crud_sandbox" in data["path"]
        assert name in data["path"]

    def test_response_includes_etag(self, mutable_client):
        name = _unique_fg_name("create_etag")
        resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        data = resp.json()
        assert data["etag"] is not None
        assert isinstance(data["etag"], str)
        assert len(data["etag"]) > 0

    def test_creates_yaml_file_on_disk(self, mutable_client):
        """Verify indirectly via GET /api/flowgroups/{name}."""
        name = _unique_fg_name("create_disk")
        create_resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        assert create_resp.status_code == 201

        # The newly created flowgroup should be discoverable via the detail endpoint
        get_resp = mutable_client.get(f"/api/flowgroups/{name}")
        assert get_resp.status_code == 200
        assert get_resp.json()["flowgroup"]["flowgroup"] == name


# ---------------------------------------------------------------------------
# TestGetFlowgroupSource
# ---------------------------------------------------------------------------


class TestGetFlowgroupSource:
    """Tests for GET /api/flowgroups/{name}/source — raw YAML content + ETag."""

    def test_returns_source_yaml(self, mutable_client):
        # Create a flowgroup first so we have a known source
        name = _unique_fg_name("source_200")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        resp = mutable_client.get(f"/api/flowgroups/{name}/source")
        assert resp.status_code == 200

    def test_response_has_content_and_path(self, mutable_client):
        name = _unique_fg_name("source_content")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        data = mutable_client.get(f"/api/flowgroups/{name}/source").json()
        # content should be a YAML string (not parsed dict)
        assert isinstance(data["content"], str)
        assert len(data["content"]) > 0
        # path should be a relative path to the YAML file
        assert isinstance(data["path"], str)
        assert name in data["path"]

    def test_response_includes_etag_header(self, mutable_client):
        name = _unique_fg_name("source_etag")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        resp = mutable_client.get(f"/api/flowgroups/{name}/source")
        etag = resp.headers.get("ETag")
        assert etag is not None
        assert len(etag) > 0

    def test_source_not_found_returns_404(self, mutable_client):
        resp = mutable_client.get("/api/flowgroups/nonexistent_fg_xyz_999/source")
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestUpdateFlowgroup
# ---------------------------------------------------------------------------


class TestUpdateFlowgroup:
    """Tests for PUT /api/flowgroups/{name} — update flowgroup config."""

    def test_updates_flowgroup_returns_200(self, mutable_client):
        name = _unique_fg_name("update_200")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        resp = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "updated_catalog", "target": "schema.updated"}},
        )
        assert resp.status_code == 200

    def test_returns_new_etag(self, mutable_client):
        name = _unique_fg_name("update_etag")
        create_resp = _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )
        original_etag = create_resp.json()["etag"]

        update_resp = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "changed_catalog"}},
        )
        data = update_resp.json()
        assert data["etag"] is not None
        assert isinstance(data["etag"], str)
        # The etag should change after an update
        assert data["etag"] != original_etag

    def test_if_match_with_correct_etag_succeeds(self, mutable_client):
        name = _unique_fg_name("update_ifmatch_ok")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        # Get the current ETag from source endpoint
        source_resp = mutable_client.get(f"/api/flowgroups/{name}/source")
        current_etag = source_resp.headers["ETag"]

        # PUT with matching If-Match should succeed
        resp = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "with_etag"}},
            headers={"If-Match": current_etag},
        )
        assert resp.status_code == 200

    def test_stale_etag_returns_412(self, mutable_client):
        name = _unique_fg_name("update_stale")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        # Get the current ETag
        source_resp = mutable_client.get(f"/api/flowgroups/{name}/source")
        original_etag = source_resp.headers["ETag"]

        # Perform an update (changes the file, invalidating the old ETag)
        mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "first_update"}},
        )

        # Now try to update with the stale ETag
        resp = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "second_update"}},
            headers={"If-Match": original_etag},
        )
        assert resp.status_code == 412

    def test_last_write_wins_without_if_match(self, mutable_client):
        name = _unique_fg_name("update_lww")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        # Two consecutive updates without If-Match should both succeed
        resp1 = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "update_one"}},
        )
        assert resp1.status_code == 200

        resp2 = mutable_client.put(
            f"/api/flowgroups/{name}",
            json={"config": {"catalog": "update_two"}},
        )
        assert resp2.status_code == 200

    def test_update_nonexistent_returns_404(self, mutable_client):
        resp = mutable_client.put(
            "/api/flowgroups/nonexistent_fg_xyz_999",
            json={"config": {"catalog": "nope"}},
        )
        assert resp.status_code == 404


# ---------------------------------------------------------------------------
# TestDeleteFlowgroup
# ---------------------------------------------------------------------------


class TestDeleteFlowgroup:
    """Tests for DELETE /api/flowgroups/{name} — delete a flowgroup."""

    def test_deletes_flowgroup(self, mutable_client):
        name = _unique_fg_name("delete_ok")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        resp = mutable_client.delete(f"/api/flowgroups/{name}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True

        # Verify the flowgroup is no longer retrievable
        get_resp = mutable_client.get(f"/api/flowgroups/{name}")
        assert get_resp.status_code == 404

    def test_delete_nonexistent_returns_404(self, mutable_client):
        resp = mutable_client.delete("/api/flowgroups/nonexistent_fg_xyz_999")
        assert resp.status_code == 404

    def test_response_includes_file_deleted_info(self, mutable_client):
        name = _unique_fg_name("delete_info")
        _create_flowgroup(
            mutable_client, pipeline="test_crud_sandbox", flowgroup=name
        )

        resp = mutable_client.delete(f"/api/flowgroups/{name}")
        data = resp.json()
        assert "file_deleted" in data or "path" in data
        # For a single-flowgroup file, file_deleted should be True
        # and was_multi_flowgroup_file should be False
        if "file_deleted" in data:
            assert data["file_deleted"] is True
        if "was_multi_flowgroup_file" in data:
            assert data["was_multi_flowgroup_file"] is False
