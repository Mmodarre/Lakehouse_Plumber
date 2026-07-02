"""Integration tests for GET /api/flowgroups/{name}/related-files.

Exercises the full endpoint: locate the flowgroup's source YAML, load it raw,
and walk the file-path-bearing fields via the related-files extractor.

Test subject ``orders_bronze`` (pipeline ``acmi_edw_bronze``) has a transform
action with ``sql_path: sql/orders_europe_bronze_cleanse.sql``. No auth
(single-user local IDE).
"""

import pytest

pytestmark = pytest.mark.webapp


class TestRelatedFilesEndpoint:
    """Tests for GET /api/flowgroups/{name}/related-files."""

    def test_returns_200_with_source_and_related(self, client):
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        assert resp.status_code == 200

        data = resp.json()
        assert data["flowgroup"] == "orders_bronze"
        assert data["environment"] == "dev"

        # Source file always present.
        source = data["source_file"]
        assert source["path"].endswith(".yaml")
        assert source["exists"] is True
        assert source["category"] == "yaml"

        # related_files is a list.
        assert isinstance(data["related_files"], list)

    def test_related_files_include_sql_path(self, client):
        """orders_bronze has a transform action with a .sql sql_path."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        data = resp.json()

        sql_files = [f for f in data["related_files"] if f["category"] == "sql"]
        assert len(sql_files) >= 1
        sql_paths = [f["path"] for f in sql_files]
        assert any("orders_europe_bronze_cleanse.sql" in p for p in sql_paths)

    def test_related_file_has_expected_fields(self, client):
        """Each related file entry has path, category, action_name, field, exists."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=dev")
        data = resp.json()

        assert data["related_files"], "expected at least one related file"
        entry = data["related_files"][0]
        assert "path" in entry
        assert "category" in entry
        assert "action_name" in entry
        assert "field" in entry
        assert "exists" in entry

    def test_404_for_nonexistent_flowgroup(self, client):
        resp = client.get("/api/flowgroups/nonexistent_fg_xyz/related-files?env=dev")
        assert resp.status_code == 404

    def test_env_parameter_used(self, client):
        """A different env is accepted (not a 422 validation error)."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files?env=prod")
        assert resp.status_code != 422

    def test_flowgroup_structure_for_first_few(self, client):
        """The response structure is correct for any listed flowgroup."""
        list_resp = client.get("/api/flowgroups")
        assert list_resp.status_code == 200
        flowgroups = list_resp.json()["flowgroups"]

        checked = 0
        for fg in flowgroups[:3]:
            resp = client.get(f"/api/flowgroups/{fg['name']}/related-files?env=dev")
            if resp.status_code == 200:
                data = resp.json()
                assert isinstance(data["related_files"], list)
                assert data["source_file"]["category"] == "yaml"
                checked += 1
        assert checked > 0

    def test_default_env_is_dev(self, client):
        """Omitting env defaults to 'dev'."""
        resp = client.get("/api/flowgroups/orders_bronze/related-files")
        assert resp.status_code == 200
        data = resp.json()
        assert data["environment"] == "dev"
