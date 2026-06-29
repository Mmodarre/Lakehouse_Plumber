"""HTTP-contract tests for the files router (``/api/files``).

The router is a thin HTTP shell over
:class:`lhp.webapp.services.file_io.FileIOService`; the service-level guards
are tested in ``test_file_io_service.py``, so these tests focus on the HTTP
surface: status mapping (403 traversal / 403 write-protection / 404 missing),
the response *body shapes*, and the recursive tree contract.

The contract under test:

* GET ``/api/files`` returns the full recursive tree
  (``{"name","path","type","children"}``), not a paginated first-level
  ``items`` page.
* GET ``/api/files/{path}`` returns the file content as ``text/plain``, not a
  ``{"type","content"}`` JSON envelope, and does no binary detection or
  directory-listing-on-GET. Reads stay unrestricted (``generated/`` readable).
* PUT bad YAML returns ``200`` with a structured ``yaml_error`` (the write
  persists), NOT a 4xx.
* The protected prefixes are ``.git/`` / ``generated/`` / ``.lhp/logs/`` /
  ``.lhp/dependencies/``.

Write/delete tests use ``mutable_client`` (per-test deep copy); read/tree tests
use the read-only ``client``.
"""

from __future__ import annotations

from pathlib import Path

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.webapp


class TestFileTree:
    """GET /api/files returns the recursive project tree."""

    def test_returns_200(self, client: TestClient) -> None:
        resp = client.get("/api/files")
        assert resp.status_code == 200

    def test_root_node_shape(self, client: TestClient) -> None:
        tree = client.get("/api/files").json()
        assert tree["type"] == "directory"
        assert tree["path"] == ""
        assert isinstance(tree["children"], list)
        assert tree["children"]  # non-empty

    def test_nodes_have_name_path_type(self, client: TestClient) -> None:
        tree = client.get("/api/files").json()
        for node in tree["children"]:
            assert isinstance(node["name"], str)
            assert isinstance(node["path"], str)
            assert node["type"] in ("file", "directory")

    def test_excludes_noise_dirs(self, client: TestClient) -> None:
        tree = client.get("/api/files").json()
        top_names = {n["name"] for n in tree["children"]}
        assert ".git" not in top_names
        assert "__pycache__" not in top_names
        assert ".venv" not in top_names

    def test_real_entries_present_and_recursive(self, client: TestClient) -> None:
        tree = client.get("/api/files").json()
        by_name = {n["name"]: n for n in tree["children"]}
        assert "lhp.yaml" in by_name
        assert by_name["lhp.yaml"]["type"] == "file"
        assert "children" not in by_name["lhp.yaml"]

        pipelines = by_name["pipelines"]
        assert pipelines["type"] == "directory"
        assert pipelines["path"] == "pipelines"
        # Recursive: a nested node carries a project-relative "/"-separated path.
        nested_paths = {n["path"] for n in pipelines["children"]}
        assert any(p.startswith("pipelines/") for p in nested_paths)


class TestReadFile:
    """GET /api/files/{path} returns file content as text/plain."""

    def test_reads_file_content(self, client: TestClient) -> None:
        resp = client.get("/api/files/lhp.yaml")
        assert resp.status_code == 200
        assert resp.headers["content-type"].startswith("text/plain")
        assert len(resp.text) > 0

    def test_content_matches_disk(
        self, client: TestClient, e2e_project_path: Path
    ) -> None:
        resp = client.get("/api/files/lhp.yaml")
        assert resp.text == (e2e_project_path / "lhp.yaml").read_text()

    def test_not_found_returns_404(self, client: TestClient) -> None:
        resp = client.get("/api/files/nonexistent_file_that_does_not_exist.txt")
        assert resp.status_code == 404

    def test_read_generated_file_unrestricted(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        """Reads under ``generated/`` are allowed (only traversal-guarded).

        ``generated/`` is write-protected, so the file is seeded directly on
        disk (bypassing the API), then read back through the API to prove reads
        are unrestricted.
        """
        gen_dir = mutable_project / "generated"
        gen_dir.mkdir(parents=True, exist_ok=True)
        (gen_dir / "pipeline.py").write_text("# generated code\n")

        resp = mutable_client.get("/api/files/generated/pipeline.py")
        assert resp.status_code == 200
        assert resp.text == "# generated code\n"


class TestWriteFile:
    """PUT /api/files/{path} writes/creates a file (mutable_client)."""

    def test_writes_pipeline_yaml(self, mutable_client: TestClient) -> None:
        resp = mutable_client.put(
            "/api/files/pipelines/new.yaml",
            json={"content": "flowgroup: new\n"},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["written"] is True
        assert data["path"] == "pipelines/new.yaml"
        assert data["yaml_error"] is None

    def test_creates_parent_directories(self, mutable_client: TestClient) -> None:
        resp = mutable_client.put(
            "/api/files/pipelines/nested_a/nested_b/created.yaml",
            json={"content": "nested: true\n"},
        )
        assert resp.status_code == 200
        assert resp.json()["written"] is True

    def test_written_content_round_trips(self, mutable_client: TestClient) -> None:
        path = "pipelines/roundtrip.txt"
        content = "Hello, round-trip test!\nLine two.\n"
        assert (
            mutable_client.put(
                f"/api/files/{path}", json={"content": content}
            ).status_code
            == 200
        )
        read = mutable_client.get(f"/api/files/{path}")
        assert read.status_code == 200
        assert read.text == content


class TestWriteFileProtection:
    """Write-protection (403) on PUT for protected prefixes."""

    def test_rejects_generated_write(self, mutable_client: TestClient) -> None:
        resp = mutable_client.put(
            "/api/files/generated/x.py",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403
        # Distinct write-protection detail (not the traversal message).
        assert "write-protected" in resp.json()["detail"].lower()

    def test_rejects_git_directory_write(self, mutable_client: TestClient) -> None:
        resp = mutable_client.put(
            "/api/files/.git/config",
            json={"content": "malicious"},
        )
        assert resp.status_code == 403
        assert "write-protected" in resp.json()["detail"].lower()


class TestWriteBadYaml:
    """Bad YAML on PUT persists and returns 200 with a structured diagnostic."""

    def test_bad_yaml_returns_200_with_diagnostic(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        bad = "a:\n  b: [1, 2\n c: 3\n"  # unterminated flow sequence
        resp = mutable_client.put(
            "/api/files/pipelines/broken.yaml",
            json={"content": bad},
        )
        # PINNED: HTTP 200 even though the YAML is broken — the write persists.
        assert resp.status_code == 200
        data = resp.json()
        assert data["written"] is True

        # The structured 1-based diagnostic rides the success body.
        assert data["yaml_error"] is not None
        assert data["yaml_error"]["line"] >= 1
        assert data["yaml_error"]["column"] >= 1
        assert data["yaml_error"]["message"]

        # The bytes actually persisted despite the syntax error.
        assert (mutable_project / "pipelines" / "broken.yaml").read_text() == bad

    def test_bad_yaml_line_column_one_based(self, mutable_client: TestClient) -> None:
        bad = "a:\n  b: [1, 2\n c: 3\n"
        data = mutable_client.put(
            "/api/files/pipelines/broken2.yaml",
            json={"content": bad},
        ).json()
        # PyYAML reports 0-based (2, 2); the service/router emit 1-based (3, 3).
        assert data["yaml_error"]["line"] == 3
        assert data["yaml_error"]["column"] == 3


class TestDeleteFile:
    """DELETE /api/files/{path} (mutable_client)."""

    def test_deletes_normal_file(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        target = mutable_project / "pipelines" / "02_bronze" / "customer_bronze.yaml"
        assert target.exists()

        resp = mutable_client.delete(
            "/api/files/pipelines/02_bronze/customer_bronze.yaml"
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["deleted"] is True
        assert data["path"] == "pipelines/02_bronze/customer_bronze.yaml"
        assert not target.exists()

    def test_delete_missing_returns_404(self, mutable_client: TestClient) -> None:
        resp = mutable_client.delete(
            "/api/files/this_file_absolutely_does_not_exist_12345.txt"
        )
        assert resp.status_code == 404

    def test_rejects_write_protected_delete(self, mutable_client: TestClient) -> None:
        resp = mutable_client.delete("/api/files/.git/config")
        assert resp.status_code == 403
        assert "write-protected" in resp.json()["detail"].lower()


class TestPathTraversal:
    """Traversal attempts are denied.

    NOTE on status codes: ``..`` segments in the URL path may be normalized by
    the TestClient / httpx before the request reaches the app, in which case the
    request never matches the ``{path:path}`` route and the framework returns
    ``404``. When the literal ``..`` segments DO reach the service (e.g. an
    absolute path, or non-normalized input), the service raises
    ``PathTraversalError`` and the router maps it to ``403``. Either way the
    attack is denied; we assert the denial (403/404) rather than pinning which
    layer fires.
    """

    def test_get_traversal_denied(self, client: TestClient) -> None:
        resp = client.get("/api/files/../../etc/passwd")
        assert resp.status_code in (403, 404)

    def test_put_traversal_denied(self, mutable_client: TestClient) -> None:
        resp = mutable_client.put(
            "/api/files/../../../etc/passwd",
            json={"content": "malicious"},
        )
        assert resp.status_code in (403, 404)

    def test_absolute_path_traversal_denied_at_service(
        self, mutable_project: Path, mutable_client: TestClient
    ) -> None:
        """An absolute path is not normalized away, so the SERVICE-level guard
        fires and the router maps the ``PathTraversalError`` to ``403``."""
        # Joining an absolute path to the root discards the root in pathlib, so
        # this resolves outside and trips the traversal guard (not the 404 path).
        outside = mutable_project.parent / "outside_secret.txt"
        outside.write_text("secret\n")
        resp = mutable_client.get(f"/api/files/{outside}")
        assert resp.status_code == 403
        assert "traversal" in resp.json()["detail"].lower()
