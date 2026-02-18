"""Tests for template list and detail endpoints."""

import pytest


pytestmark = pytest.mark.api


class TestListTemplates:
    """Tests for GET /api/templates."""

    def test_returns_200(self, client):
        resp = client.get("/api/templates")
        assert resp.status_code == 200

    def test_total_matches_list_length(self, client):
        data = client.get("/api/templates").json()
        assert data["total"] == len(data["templates"])

    def test_known_templates_are_present(self, client):
        templates = client.get("/api/templates").json()["templates"]
        assert "parquet_ingestion_template" in templates
        assert "json_ingestion_template" in templates


class TestGetTemplate:
    """Tests for GET /api/templates/{name}."""

    def test_returns_200(self, client):
        resp = client.get("/api/templates/parquet_ingestion_template")
        assert resp.status_code == 200

    def test_name_matches(self, client):
        data = client.get("/api/templates/parquet_ingestion_template").json()
        assert data["name"] == "parquet_ingestion_template"

    def test_template_has_expected_fields(self, client):
        tmpl = client.get("/api/templates/parquet_ingestion_template").json()[
            "template"
        ]
        assert "version" in tmpl
        assert "description" in tmpl
        assert "parameters" in tmpl
        assert "action_count" in tmpl

    def test_template_parameters_is_list(self, client):
        tmpl = client.get("/api/templates/parquet_ingestion_template").json()[
            "template"
        ]
        assert isinstance(tmpl["parameters"], list)
        for param in tmpl["parameters"]:
            assert "name" in param

    def test_nonexistent_template_returns_404(self, client):
        resp = client.get("/api/templates/nonexistent_template")
        assert resp.status_code == 404
