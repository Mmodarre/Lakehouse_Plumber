"""TDD tests for file browser endpoints.

These tests exercise the file browser router at ``/api/files`` which provides
project file listing, reading, writing, and deletion. The router module
``lhp.api.routers.files`` does not exist yet; these tests are written first
to drive the implementation.

Endpoints:
    GET    /api/files              - Project file tree (first level)
    GET    /api/files/{path:path}  - Read file content
    PUT    /api/files/{path:path}  - Write file content + auto-commit
    DELETE /api/files/{path:path}  - Delete file + auto-commit
"""

import pytest


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# TestListFiles
# ---------------------------------------------------------------------------


class TestListFiles:
    """Tests for GET /api/files — project root file listing."""

    def test_returns_200(self, client):
        resp = client.get("/api/files")
        assert resp.status_code == 200

    def test_items_have_name_and_type(self, client):
        data = client.get("/api/files").json()
        for item in data["items"]:
            assert "name" in item
            assert isinstance(item["name"], str)
            assert item["type"] in ("file", "directory")

    def test_hides_dotfiles(self, client):
        data = client.get("/api/files").json()
        names = [item["name"] for item in data["items"]]
        assert ".git" not in names
        assert ".DS_Store" not in names
        assert ".lhp" not in names

    def test_supports_pagination(self, client):
        # Fetch first 2 items
        resp_page1 = client.get("/api/files", params={"offset": 0, "limit": 2})
        assert resp_page1.status_code == 200
        page1 = resp_page1.json()
        assert len(page1["items"]) <= 2

        # Fetch next 2 items
        resp_page2 = client.get("/api/files", params={"offset": 2, "limit": 2})
        assert resp_page2.status_code == 200
        page2 = resp_page2.json()

        # Pages should not overlap
        names_1 = {item["name"] for item in page1["items"]}
        names_2 = {item["name"] for item in page2["items"]}
        assert names_1.isdisjoint(names_2)

    def test_total_field_present(self, client):
        data = client.get("/api/files").json()
        assert "total" in data
        assert isinstance(data["total"], int)
        assert data["total"] > 0


# ---------------------------------------------------------------------------
# TestReadFile
# ---------------------------------------------------------------------------


class TestReadFile:
    """Tests for GET /api/files/{path} — read file or directory."""

    def test_reads_file_content(self, client):
        resp = client.get("/api/files/lhp.yaml")
        assert resp.status_code == 200
        data = resp.json()
        assert "content" in data
        assert len(data["content"]) > 0

    def test_response_has_type_and_content(self, client):
        data = client.get("/api/files/lhp.yaml").json()
        assert data["type"] == "file"
        assert isinstance(data["content"], str)

    def test_directory_returns_listing(self, client):
        resp = client.get("/api/files/pipelines")
        assert resp.status_code == 200
        data = resp.json()
        assert data["type"] == "directory"
        assert "items" in data
        assert len(data["items"]) > 0

    def test_not_found_returns_404(self, client):
        resp = client.get("/api/files/nonexistent_file_that_does_not_exist.txt")
        assert resp.status_code == 404

    def test_binary_file_detected(self, client):
        """Binary files should be detected and return type='binary' with a message.

        The .DS_Store file in the fixture project serves as a natural binary
        file. If it's not present, this test is skipped.
        """
        # Use .DS_Store if present, otherwise create one via the listing
        # to check for any existing binary. We'll try reading a known
        # binary-like file. If none exists we skip.
        resp = client.get("/api/files/.DS_Store")
        if resp.status_code == 404:
            pytest.skip("No binary file available in fixture project")
        data = resp.json()
        assert data["type"] == "binary"


# ---------------------------------------------------------------------------
# TestWriteFile
# ---------------------------------------------------------------------------


class TestWriteFile:
    """Tests for PUT /api/files/{path} — write file content.

    Uses mutable_client since writes modify the project on disk.
    """

    def test_writes_file_content(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/test_write_basic.yaml",
            json={"content": "key: value\n", "encoding": "utf-8"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["written"] is True

    def test_creates_parent_directories(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/nested_dir_a/nested_dir_b/test_nested_write.yaml",
            json={"content": "nested: true\n"},
        )
        assert resp.status_code == 200
        assert resp.json()["written"] is True

    def test_written_content_is_readable(self, mutable_client):
        file_path = "test_roundtrip_content.txt"
        content = "Hello, round-trip test!\nLine two.\n"

        # Write
        write_resp = mutable_client.put(
            f"/api/files/{file_path}",
            json={"content": content},
        )
        assert write_resp.status_code == 200

        # Read back
        read_resp = mutable_client.get(f"/api/files/{file_path}")
        assert read_resp.status_code == 200
        assert read_resp.json()["content"] == content


# ---------------------------------------------------------------------------
# TestWriteFileProtection
# ---------------------------------------------------------------------------


class TestWriteFileProtection:
    """Tests for write-protection rules on PUT /api/files/{path}.

    Uses mutable_client since these hit the write endpoint (even though
    they should be rejected).
    """

    def test_rejects_path_traversal(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/../../../etc/passwd",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403

    def test_rejects_git_directory_write(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/.git/config",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403

    def test_rejects_generated_directory_write(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/generated/test.py",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403

    def test_rejects_lhp_state_write(self, mutable_client):
        resp = mutable_client.put(
            "/api/files/.lhp_state.json",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# TestDeleteFile
# ---------------------------------------------------------------------------


class TestDeleteFile:
    """Tests for DELETE /api/files/{path} — delete file.

    Uses mutable_client since deletes modify the project on disk.
    """

    def test_deletes_file(self, mutable_client):
        # First create a file to delete
        file_path = "test_delete_target.txt"
        mutable_client.put(
            f"/api/files/{file_path}",
            json={"content": "to be deleted\n"},
        )

        # Now delete it
        resp = mutable_client.delete(f"/api/files/{file_path}")
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] is True

    def test_delete_not_found_returns_404(self, mutable_client):
        resp = mutable_client.delete(
            "/api/files/this_file_absolutely_does_not_exist_12345.txt"
        )
        assert resp.status_code == 404

    def test_rejects_write_protected_delete(self, mutable_client):
        resp = mutable_client.delete("/api/files/.git/config")
        assert resp.status_code == 403


# ---------------------------------------------------------------------------
# TestPathTraversal
# ---------------------------------------------------------------------------


class TestPathTraversal:
    """Tests for path traversal attack prevention across endpoints."""

    @pytest.mark.parametrize(
        "attack_path",
        [
            "../../../etc/passwd",
            "foo/../../etc/passwd",
            "pipelines/../../../etc/shadow",
            "..%2F..%2F..%2Fetc%2Fpasswd",
        ],
    )
    def test_rejects_dot_dot_in_path(self, mutable_client, attack_path):
        resp = mutable_client.put(
            f"/api/files/{attack_path}",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403

    def test_rejects_encoded_traversal(self, mutable_client):
        # URL-encoded ../ patterns
        resp = mutable_client.put(
            "/api/files/%2e%2e/%2e%2e/%2e%2e/etc/passwd",
            json={"content": "malicious"},
        )
        # Should be 403 (path traversal) or 404/422 (if framework decodes
        # and the path doesn't resolve within project root)
        assert resp.status_code in (403, 404, 422)
