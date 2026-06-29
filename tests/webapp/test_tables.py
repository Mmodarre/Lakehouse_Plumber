"""Tests for the ``GET /api/tables`` endpoint (table-centric write-target view).

Runs over the E2E fixture project (read-only ``client`` fixture) and the
public inspection surface. The contract under test:

1. **No auth** — the local IDE is same-origin, single-user.
2. **``target_type`` is ``{"streaming_table", "sink"}``** — the public
   :class:`~lhp.api.views.ActionView` does not carry the streaming-table /
   materialized-view distinction, so the router reports ``"streaming_table"``
   for every table target and ``"sink"`` (via the ``sink:`` prefix) for sinks.
3. **``write_mode`` is populated on standard writes too** — the converter
   defaults table targets to ``write_mode="standard"``; CDC rows are identified
   by ``write_mode in ("cdc", "snapshot_cdc")``.
4. **``full_name`` is the canonical ``catalog.schema.table``** (3-part).

Concrete fixture anchors used below:

- ``customer_bronze`` (pipeline ``acmi_edw_bronze``) — STANDARD streaming-table
  writes resolving to ``acme_edw_dev.edw_bronze.customer`` under ``env=dev``.
- ``customer_silver_dim`` (pipeline ``acmi_edw_silver``) — a CDC write
  (``write_mode="cdc"``, ``scd_type=2``) resolving to
  ``acme_edw_dev.edw_silver.customer_dim``.
"""

import re

import pytest

pytestmark = pytest.mark.webapp


class TestListTables:
    """Core list_tables endpoint behaviour."""

    def test_env_required_returns_422(self, client):
        """Omitting the required ``env`` query parameter yields 422."""
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
            assert required.issubset(table.keys()), (
                f"Missing fields: {required - table.keys()}"
            )

    def test_valid_target_types(self, client):
        # Public ActionView surface only distinguishes table vs sink.
        valid = {"streaming_table", "sink"}
        data = client.get("/api/tables?env=dev").json()
        for table in data["tables"]:
            assert table["target_type"] in valid, (
                f"Unexpected target_type: {table['target_type']}"
            )

    def test_no_unresolved_substitution_tokens(self, client):
        """Resolved table names carry no raw ${...} or {token} markers."""
        data = client.get("/api/tables?env=dev").json()
        token_pattern = re.compile(r"\$?\{[^}]+\}")
        for table in data["tables"]:
            assert not token_pattern.search(table["full_name"]), (
                f"Unresolved token in: {table['full_name']}"
            )

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
        target_pipeline = next(iter(all_pipelines))

        filtered = client.get(f"/api/tables?env=dev&pipeline={target_pipeline}").json()
        assert filtered["total"] > 0
        assert all(t["pipeline"] == target_pipeline for t in filtered["tables"])

    def test_nonexistent_pipeline_returns_empty(self, client):
        data = client.get("/api/tables?env=dev&pipeline=no_such_pipeline_xyz").json()
        assert data["total"] == 0
        assert data["tables"] == []


class TestTablesTargetTypes:
    """The reachable target types are represented."""

    def test_has_streaming_tables(self, client):
        data = client.get("/api/tables?env=dev").json()
        types = {t["target_type"] for t in data["tables"]}
        assert "streaming_table" in types

    def test_has_sinks(self, client):
        data = client.get("/api/tables?env=dev").json()
        types = {t["target_type"] for t in data["tables"]}
        assert "sink" in types


class TestTablesStandardWrite:
    """A real standard write row has a resolved full name and standard mode."""

    def test_standard_write_row_present(self, client):
        data = client.get("/api/tables?env=dev").json()
        # ``customer_bronze`` writes a standard streaming table to
        # ``acme_edw_dev.edw_bronze.customer`` under env=dev.
        standard = [
            t
            for t in data["tables"]
            if t["flowgroup"] == "customer_bronze"
            and t["full_name"] == "acme_edw_dev.edw_bronze.customer"
        ]
        assert standard, "Expected a resolved standard write row for customer_bronze"
        for t in standard:
            assert t["target_type"] == "streaming_table"
            assert t["write_mode"] == "standard"
            assert t["scd_type"] is None
            # 3-part canonical name (catalog.schema.table), the planned rework.
            assert t["full_name"].count(".") == 2

    def test_at_least_one_standard_mode_row(self, client):
        data = client.get("/api/tables?env=dev").json()
        standard = [t for t in data["tables"] if t["write_mode"] == "standard"]
        assert len(standard) > 0, "Expected at least one standard-mode write row"
        for t in standard:
            assert t["full_name"]  # populated full name


class TestTablesCDCMetadata:
    """CDC and SCD metadata extraction off the public ActionView fields."""

    def test_cdc_row_present_with_scd_type(self, client):
        data = client.get("/api/tables?env=dev").json()
        # ``customer_silver_dim`` is a CDC write (scd_type 2).
        cdc = [
            t
            for t in data["tables"]
            if t["flowgroup"] == "customer_silver_dim" and t["write_mode"] == "cdc"
        ]
        assert cdc, "Expected a CDC write row for customer_silver_dim"
        for t in cdc:
            assert t["scd_type"] == 2
            assert t["full_name"] == "acme_edw_dev.edw_silver.customer_dim"

    def test_cdc_tables_have_valid_mode(self, client):
        data = client.get("/api/tables?env=dev").json()
        cdc_tables = [
            t for t in data["tables"] if t["write_mode"] in ("cdc", "snapshot_cdc")
        ]
        assert len(cdc_tables) > 0, "Expected at least one CDC table"
        for t in cdc_tables:
            assert t["write_mode"] in ("cdc", "snapshot_cdc")

    def test_cdc_tables_have_scd_type(self, client):
        data = client.get("/api/tables?env=dev").json()
        cdc_tables = [
            t for t in data["tables"] if t["write_mode"] in ("cdc", "snapshot_cdc")
        ]
        tables_with_scd = [t for t in cdc_tables if t["scd_type"] is not None]
        assert tables_with_scd, "Expected at least one CDC table with scd_type"
        for t in tables_with_scd:
            assert t["scd_type"] in (1, 2)


class TestTablesTemplateResolution:
    """Template-based flowgroups produce tables with resolved names."""

    def test_template_tables_have_resolved_names(self, client):
        data = client.get("/api/tables?env=dev").json()
        template_marker = re.compile(r"\{\{.*?\}\}")
        for table in data["tables"]:
            assert not template_marker.search(table["full_name"]), (
                f"Unresolved template in: {table['full_name']}"
            )
