"""Tests for state management endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestStateOverviewNoGeneration:
    """Tests for GET /api/state when no generation has occurred."""

    def test_returns_200(self, client):
        resp = client.get("/api/state")
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/state").json()
        assert "version" in data
        assert "last_updated" in data
        assert "environments" in data
        assert "total_files" in data

    def test_total_files_is_zero(self, client):
        data = client.get("/api/state").json()
        assert data["total_files"] == 0


class TestStateByEnvironmentNoGeneration:
    """Tests for GET /api/state/{env} when no generation has occurred."""

    def test_returns_200(self, client):
        resp = client.get("/api/state/dev")
        assert resp.status_code == 200

    def test_environment_matches(self, client):
        data = client.get("/api/state/dev").json()
        assert data["environment"] == "dev"

    def test_total_files_is_zero(self, client):
        data = client.get("/api/state/dev").json()
        assert data["total_files"] == 0

    def test_nonexistent_env_returns_empty(self, client):
        data = client.get("/api/state/nonexistent_env").json()
        assert data["total_files"] == 0


class TestStateAfterGeneration:
    """Tests for state endpoints after code generation has populated .lhp_state.json.

    Uses mutable_client since generation writes files to disk.
    """

    def _generate(self, mutable_client):
        """Run generation for a known-good pipeline to populate state file."""
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

    def test_overview_has_files_after_generation(self, mutable_client):
        self._generate(mutable_client)
        data = mutable_client.get("/api/state").json()
        assert data["total_files"] > 0

    def test_overview_has_dev_environment(self, mutable_client):
        self._generate(mutable_client)
        data = mutable_client.get("/api/state").json()
        assert "dev" in data["environments"]

    def test_env_state_has_files_after_generation(self, mutable_client):
        self._generate(mutable_client)
        data = mutable_client.get("/api/state/dev").json()
        assert data["environment"] == "dev"
        assert data["total_files"] > 0
        assert len(data["files"]) > 0

    def test_env_file_entries_have_expected_fields(self, mutable_client):
        self._generate(mutable_client)
        data = mutable_client.get("/api/state/dev").json()
        for _path, file_state in data["files"].items():
            assert "source_yaml" in file_state
            assert "generated_path" in file_state
            assert "checksum" in file_state
            assert "environment" in file_state
            assert file_state["environment"] == "dev"

    def test_staleness_shows_up_to_date_after_generation(self, mutable_client):
        self._generate(mutable_client)
        data = mutable_client.get(
            "/api/state/staleness", params={"env": "dev"}
        ).json()
        assert data["total_up_to_date"] > 0


class TestStaleness:
    """Tests for GET /api/state/staleness."""

    def test_returns_200(self, client):
        resp = client.get("/api/state/staleness", params={"env": "dev"})
        assert resp.status_code == 200

    def test_has_expected_fields(self, client):
        data = client.get("/api/state/staleness", params={"env": "dev"}).json()
        assert "has_work_to_do" in data
        assert "total_new" in data
        assert "total_stale" in data
        assert "total_up_to_date" in data

    def test_with_include_tests(self, client):
        resp = client.get(
            "/api/state/staleness",
            params={"env": "dev", "include_tests": "true"},
        )
        assert resp.status_code == 200


class TestStateCleanup:
    """Tests for POST /api/state/cleanup (uses mutable_client)."""

    def test_returns_200(self, mutable_client):
        resp = mutable_client.post("/api/state/cleanup", params={"env": "dev"})
        assert resp.status_code == 200

    def test_has_expected_fields(self, mutable_client):
        data = mutable_client.post(
            "/api/state/cleanup", params={"env": "dev"}
        ).json()
        assert "cleaned_files" in data
        assert "total_cleaned" in data
        assert isinstance(data["cleaned_files"], list)
