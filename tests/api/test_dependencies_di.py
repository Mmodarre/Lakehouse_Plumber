"""Tests for API dependency injection providers."""

import pytest


pytestmark = pytest.mark.api


class TestGetProjectRoot:
    """Tests for the get_project_root DI provider (tested via endpoints)."""

    def test_valid_project_root_allows_request(self, client):
        """A valid project root (with lhp.yaml) allows the request through."""
        resp = client.get("/api/project")
        assert resp.status_code == 200

    def test_invalid_project_root_returns_error(self, tmp_path):
        """A project root without lhp.yaml raises LHPConfigError (422)."""
        from lhp.api.app import create_app
        from lhp.api.config import APISettings
        from fastapi.testclient import TestClient

        settings = APISettings(
            project_root=tmp_path,
            dev_mode=True,
            log_level="DEBUG",
        )
        app = create_app(settings=settings)
        test_client = TestClient(app)
        resp = test_client.get("/api/project")
        assert resp.status_code == 422
        data = resp.json()
        assert "error" in data
        assert data["error"]["code"] == "LHP-CFG-011"


class TestServiceCreation:
    """Tests that DI providers create the expected service types (via endpoints)."""

    def test_facade_is_created_for_generate_preview(self, client):
        """get_facade creates a working LakehousePlumberApplicationFacade."""
        resp = client.post(
            "/api/generate/preview",
            json={"environment": "dev"},
        )
        assert resp.status_code == 200

    def test_state_manager_is_created_for_state_endpoint(self, client):
        """get_state_manager creates a working StateManager."""
        resp = client.get("/api/state")
        assert resp.status_code == 200

    def test_dependency_analyzer_is_created_for_deps_endpoint(self, client):
        """get_dependency_analyzer creates a working DependencyAnalyzer."""
        resp = client.get("/api/dependencies")
        assert resp.status_code == 200
