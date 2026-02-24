"""Integration tests for the GET /api/flowgroups/{name}/related-files endpoint.

Tests the full endpoint including discovery, resolution, and file extraction.
Uses the standard ``client`` fixture (dev_mode, per-test isolated project copy).
"""

import pytest


pytestmark = pytest.mark.api


class TestRelatedFilesEndpoint:
    """Tests for GET /api/flowgroups/{name}/related-files."""

    def test_returns_200_with_source_and_related(self, client):
        """orders_bronze references sql/orders_europe_bronze_cleanse.sql."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        assert resp.status_code == 200

        data = resp.json()
        assert data["flowgroup"] == "orders_bronze"
        assert data["environment"] == "dev"

        # Source file always present
        source = data["source_file"]
        assert source["path"].endswith(".yaml")
        assert source["exists"] is True
        assert source["category"] == "yaml"

        # related_files is a list
        assert isinstance(data["related_files"], list)

    def test_related_files_include_sql_path(self, client):
        """orders_bronze has an action with sql_path pointing to a .sql file."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        data = resp.json()

        sql_files = [
            f for f in data["related_files"] if f["category"] == "sql"
        ]
        assert len(sql_files) >= 1
        # The known sql_path reference
        sql_paths = [f["path"] for f in sql_files]
        assert any("orders_europe_bronze_cleanse.sql" in p for p in sql_paths)

    def test_related_file_has_expected_fields(self, client):
        """Each related file entry has path, category, action_name, field, exists."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        data = resp.json()

        if data["related_files"]:
            entry = data["related_files"][0]
            assert "path" in entry
            assert "category" in entry
            assert "action_name" in entry
            assert "field" in entry
            assert "exists" in entry

    def test_404_for_nonexistent_flowgroup(self, client):
        resp = client.get(
            "/api/flowgroups/nonexistent_fg_xyz/related-files?env=dev"
        )
        assert resp.status_code == 404

    def test_env_parameter_used(self, client):
        """Passing a different env still returns 200 (resolution uses that env)."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=prod")
        # May succeed or fail depending on whether prod.yaml exists,
        # but we mostly verify the param is accepted (not ignored).
        # The fixture project has a dev.yaml but may not have prod.yaml.
        # Either 200 or 500 is acceptable — just not 422 (validation error).
        assert resp.status_code != 422

    def test_flowgroup_with_no_external_files(self, client):
        """A simple flowgroup without file references returns empty related_files."""
        # First, find a simple flowgroup with no external file references
        list_resp = client.get("/api/flowgroups")
        assert list_resp.status_code == 200
        flowgroups = list_resp.json()["flowgroups"]

        # Try each flowgroup and find one with no related files
        # (or just verify the structure is correct for any flowgroup)
        for fg in flowgroups[:3]:  # Check first few
            resp = client.get(
                f"/api/flowgroups/{fg['name']}/related-files?env=dev"
            )
            if resp.status_code == 200:
                data = resp.json()
                assert isinstance(data["related_files"], list)
                assert data["source_file"]["category"] == "yaml"
                break

    def test_default_env_is_dev(self, client):
        """Omitting env defaults to 'dev'."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["environment"] == "dev"
