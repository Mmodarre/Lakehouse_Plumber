"""Tests for the /api/tables endpoint (table-centric write target view)."""

import re

import pytest

pytestmark = pytest.mark.api


class TestListTables:
    """Core list_tables endpoint behaviour."""

    def test_env_required_returns_422(self, client):
        """Omitting the required `env` query parameter yields 422."""
        resp = client.get("/api/tables")
        assert resp.status_code == 422

    def test_returns_200_with_valid_env(self, client):
        resp = client.get("/api/tables?env=dev")
        assert resp.status_code == 200

    def test_returns_non_empty_list(self, client):
        data = client.get("/api/tables?env=dev").json()
        assert len(data["tables"]) > 0

    def test_total_matches_length(self, client):
        data = client.get("/api/tables?env=dev").json()
        assert data["total"] == len(data["tables"])

    def test_required_fields_present(self, client):
        data = client.get("/api/tables?env=dev").json()
        required = {
            "full_name",
            "target_type",
            "pipeline",
            "flowgroup",
            "source_file",
        }
        for table in data["tables"]:
            assert required.issubset(
                table.keys()
            ), f"Missing fields: {required - table.keys()}"

    def test_valid_target_types(self, client):
        valid = {"streaming_table", "materialized_view", "sink"}
        data = client.get("/api/tables?env=dev").json()
        for table in data["tables"]:
            assert (
                table["target_type"] in valid
            ), f"Unexpected target_type: {table['target_type']}"

    def test_no_unresolved_substitution_tokens(self, client):
        """Resolved table names should not contain raw ${...} or {env_...} tokens."""
        data = client.get("/api/tables?env=dev").json()
        token_pattern = re.compile(r"\$?\{[^}]+\}")
        for table in data["tables"]:
            assert not token_pattern.search(
                table["full_name"]
            ), f"Unresolved token in: {table['full_name']}"

    def test_warnings_field_exists(self, client):
        data = client.get("/api/tables?env=dev").json()
        assert "warnings" in data
        assert isinstance(data["warnings"], list)

    def test_invalid_env_returns_400(self, client):
        resp = client.get("/api/tables?env=nonexistent_env_xyz")
        assert resp.status_code == 400


class TestTablesFilterByPipeline:
    """Pipeline filter narrows results."""

    def test_pipeline_filter_works(self, client):
        all_data = client.get("/api/tables?env=dev").json()
        all_pipelines = {t["pipeline"] for t in all_data["tables"]}
        # Pick the first pipeline that has tables
        target_pipeline = next(iter(all_pipelines))

        filtered = client.get(f"/api/tables?env=dev&pipeline={target_pipeline}").json()
        assert filtered["total"] > 0
        assert all(t["pipeline"] == target_pipeline for t in filtered["tables"])

    def test_nonexistent_pipeline_returns_empty(self, client):
        data = client.get("/api/tables?env=dev&pipeline=no_such_pipeline_xyz").json()
        assert data["total"] == 0
        assert data["tables"] == []


class TestTablesTargetTypes:
    """Verify all expected target types are represented."""

    def test_has_streaming_tables(self, client):
        data = client.get("/api/tables?env=dev").json()
        types = {t["target_type"] for t in data["tables"]}
        assert "streaming_table" in types

    def test_has_materialized_views(self, client):
        data = client.get("/api/tables?env=dev").json()
        types = {t["target_type"] for t in data["tables"]}
        assert "materialized_view" in types

    def test_has_sinks(self, client):
        data = client.get("/api/tables?env=dev").json()
        types = {t["target_type"] for t in data["tables"]}
        assert "sink" in types


class TestTablesCDCMetadata:
    """CDC and SCD metadata extraction."""

    def test_cdc_tables_have_write_mode(self, client):
        data = client.get("/api/tables?env=dev").json()
        cdc_tables = [t for t in data["tables"] if t["write_mode"] is not None]
        assert len(cdc_tables) > 0, "Expected at least one CDC table"
        for t in cdc_tables:
            assert t["write_mode"] in ("cdc", "snapshot_cdc")

    def test_cdc_tables_have_scd_type(self, client):
        data = client.get("/api/tables?env=dev").json()
        cdc_tables = [t for t in data["tables"] if t["write_mode"] is not None]
        tables_with_scd = [t for t in cdc_tables if t["scd_type"] is not None]
        assert len(tables_with_scd) > 0, "Expected at least one CDC table with scd_type"
        for t in tables_with_scd:
            assert t["scd_type"] in (1, 2)


class TestTablesTemplateResolution:
    """Template-based flowgroups produce tables with resolved names."""

    def test_template_tables_have_resolved_names(self, client):
        data = client.get("/api/tables?env=dev").json()
        # Template-expanded tables should have fully resolved names
        # (no template parameter markers like {{ }})
        template_marker = re.compile(r"\{\{.*?\}\}")
        for table in data["tables"]:
            assert not template_marker.search(
                table["full_name"]
            ), f"Unresolved template in: {table['full_name']}"
