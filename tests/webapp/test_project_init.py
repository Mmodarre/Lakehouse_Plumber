"""Tests for the project init endpoint (``POST /api/project/init``).

The contract under test:

- Only valid in ``no_project`` state (empty project root, no ``lhp.yaml``):
  a loaded project responds 409.
- Success scaffolds the project in place, flips ``project_state`` to
  ``"ok"`` (observable via ``/api/health``), runs the run-history DB
  migrations the lifespan skipped, and makes ``GET /api/project`` work
  without a restart.
- The public bootstrap never raises: a non-empty directory surfaces as
  ``success=false`` + ``LHP-IO-007`` in a 200 body, and the server stays in
  ``no_project`` state.

Every test uses an isolated ``tmp_path`` project root — never the shared
fixture project.
"""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from lhp.webapp.app import create_app

from .conftest import LOOPBACK_BASE_URL

pytestmark = pytest.mark.webapp


@pytest.fixture
def empty_root(tmp_path: Path) -> Path:
    """An existing-but-empty directory to scaffold into."""
    root = tmp_path / "fresh_project"
    root.mkdir()
    return root


@pytest.fixture
def no_project_client(
    empty_root: Path, monkeypatch: pytest.MonkeyPatch
) -> Iterator[TestClient]:
    """Client over the EMPTY project root — lifespan resolves ``no_project``."""
    monkeypatch.setenv("LHP_WEBAPP_PROJECT_ROOT", str(empty_root))
    monkeypatch.delenv("LHP_WEBAPP_PORT", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_LOG_LEVEL", raising=False)
    monkeypatch.delenv("LHP_WEBAPP_TOKEN", raising=False)
    with TestClient(create_app(), base_url=LOOPBACK_BASE_URL) as test_client:
        yield test_client


class TestInitProject:
    """POST /api/project/init in no_project state."""

    def test_init_succeeds(self, no_project_client: TestClient) -> None:
        resp = no_project_client.post("/api/project/init", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is True
        assert data["error_code"] is None
        assert data["error_message"] is None

    def test_init_scaffolds_project_on_disk(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        data = no_project_client.post("/api/project/init", json={}).json()
        assert (empty_root / "lhp.yaml").is_file()
        assert "lhp.yaml" in data["created_files"]
        assert data["created_files"]
        assert data["created_dirs"]

    def test_bundle_default_true(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        data = no_project_client.post("/api/project/init", json={}).json()
        assert data["bundle_enabled"] is True
        assert (empty_root / "databricks.yml").is_file()

    def test_bundle_false_skips_bundle_scaffolding(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        data = no_project_client.post(
            "/api/project/init", json={"bundle": False}
        ).json()
        assert data["success"] is True
        assert data["bundle_enabled"] is False
        assert not (empty_root / "databricks.yml").exists()

    def test_project_state_flips_to_ok(self, no_project_client: TestClient) -> None:
        assert (
            no_project_client.get("/api/health").json()["project_state"] == "no_project"
        )
        no_project_client.post("/api/project/init", json={})
        assert no_project_client.get("/api/health").json()["project_state"] == "ok"

    def test_project_endpoint_works_after_init(
        self, no_project_client: TestClient
    ) -> None:
        """The freshly-scaffolded project is browsable without a restart."""
        no_project_client.post("/api/project/init", json={})
        resp = no_project_client.get("/api/project")
        assert resp.status_code == 200
        assert resp.json()["name"] == "fresh_project"

    def test_project_name_override(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        no_project_client.post(
            "/api/project/init", json={"project_name": "custom_name"}
        )
        assert "name: custom_name" in (empty_root / "lhp.yaml").read_text(
            encoding="utf-8"
        )

    def test_init_runs_db_migrations(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        """The run-history DB bootstrap skipped by the lifespan runs post-init."""
        no_project_client.post("/api/project/init", json={})
        assert (empty_root / ".lhp" / "webapp.db").is_file()

    def test_second_init_conflicts(self, no_project_client: TestClient) -> None:
        no_project_client.post("/api/project/init", json={})
        resp = no_project_client.post("/api/project/init", json={})
        assert resp.status_code == 409
        assert resp.json()["detail"] == "project already initialized"


class TestInitProjectFailure:
    """Bootstrap-reported failures (never raised) and the loaded-project 409."""

    def test_non_empty_dir_reports_lhp_io_007(
        self, no_project_client: TestClient, empty_root: Path
    ) -> None:
        (empty_root / "leftover.txt").write_text("not empty\n", encoding="utf-8")
        resp = no_project_client.post("/api/project/init", json={})
        assert resp.status_code == 200
        data = resp.json()
        assert data["success"] is False
        assert data["error_code"] == "LHP-IO-007"
        assert data["created_files"] == []
        # A failed init must not flip the server out of no_project state.
        health = no_project_client.get("/api/health").json()
        assert health["project_state"] == "no_project"

    def test_loaded_project_conflicts(self, client: TestClient) -> None:
        """The shared fixture project is already initialized -> 409."""
        resp = client.post("/api/project/init", json={})
        assert resp.status_code == 409
        assert resp.json()["detail"] == "project already initialized"
