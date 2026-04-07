"""Tests for the namespace normalizer.

# REMOVE_AT_V1.0.0: Delete this entire file when database field support is removed.
"""

import logging

import pytest

from lhp.core.services.namespace_normalizer import normalize_namespace_fields
from lhp.utils.error_formatter import LHPConfigError


class TestNormalizeWriteTarget:
    """Tests for write target normalization."""

    def test_database_split_into_catalog_schema(self):
        """database: 'cat.sch' → catalog: 'cat', schema: 'sch', no database."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "cat.sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "sch"
        assert "database" not in wt
        assert wt["table"] == "tbl"

    def test_database_split_first_dot_only(self):
        """database: 'cat.sch.with.dots' → catalog='cat', schema='sch.with.dots'."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "cat.sch.with.dots",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "sch.with.dots"

    def test_database_no_dot_raises(self):
        """database: 'no_dot' → raises LHPConfigError."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "no_dot",
                        "table": "tbl",
                    },
                }
            ]
        }
        with pytest.raises(LHPConfigError, match="does not contain a dot"):
            normalize_namespace_fields(fg)

    def test_already_has_catalog_schema_no_change(self):
        """Already has catalog + schema → no change."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": "sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "sch"
        assert "database" not in wt

    def test_database_and_schema_ddl_collision(self):
        """database + schema (DDL) → schema redirected to table_schema."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "cat.sch",
                        "schema": "id INT, name STRING",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "sch"
        assert wt["table_schema"] == "id INT, name STRING"
        assert "database" not in wt

    def test_schema_with_ddl_raises(self):
        """catalog + schema with DDL value → raises LHPConfigError."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": "id BIGINT, name STRING",
                        "table": "tbl",
                    },
                }
            ]
        }
        with pytest.raises(LHPConfigError, match="DDL"):
            normalize_namespace_fields(fg)

    def test_schema_with_normal_name_ok(self):
        """catalog + schema with a normal name → no error."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": "bronze_layer",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["schema"] == "bronze_layer"

    def test_materialized_view_database_split(self):
        """Materialized view database field is also normalized."""
        fg = {
            "actions": [
                {
                    "name": "write_mv",
                    "type": "write",
                    "write_target": {
                        "type": "materialized_view",
                        "database": "cat.sch",
                        "table": "mv_tbl",
                        "sql": "SELECT 1",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "sch"
        assert "database" not in wt

    def test_sink_type_not_normalized(self):
        """Sink write targets are not normalized (no table references)."""
        fg = {
            "actions": [
                {
                    "name": "write_sink",
                    "type": "write",
                    "write_target": {
                        "type": "sink",
                        "sink_type": "delta",
                        "sink_name": "my_sink",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        # Sink type should be untouched
        assert "catalog" not in wt
        assert "schema" not in wt

    def test_no_database_field_no_change(self):
        """write_target without database → no change."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        wt = result["actions"][0]["write_target"]
        assert "database" not in wt
        assert "catalog" not in wt


class TestNormalizeDeltaSource:
    """Tests for delta load source normalization."""

    def test_database_split_into_catalog_schema(self):
        """Delta source database: 'cat.sch' → catalog + schema."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "database": "cat.sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        src = result["actions"][0]["source"]
        assert src["catalog"] == "cat"
        assert src["schema"] == "sch"
        assert "database" not in src

    def test_catalog_plus_database_format_a(self):
        """catalog present + database (no dot) → database renamed to schema."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "catalog": "cat",
                        "database": "sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        src = result["actions"][0]["source"]
        assert src["catalog"] == "cat"
        assert src["schema"] == "sch"
        assert "database" not in src

    def test_database_no_dot_no_catalog_raises(self):
        """database: 'no_dot' without catalog → raises LHPConfigError."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "database": "no_dot",
                        "table": "tbl",
                    },
                }
            ]
        }
        with pytest.raises(LHPConfigError, match="does not contain a dot"):
            normalize_namespace_fields(fg)

    def test_already_has_catalog_schema_no_change(self):
        """Delta source with catalog + schema → no change."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "catalog": "cat",
                        "schema": "sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        src = result["actions"][0]["source"]
        assert src["catalog"] == "cat"
        assert src["schema"] == "sch"
        assert "database" not in src

    def test_non_delta_source_untouched(self):
        """Non-delta load sources are not normalized."""
        fg = {
            "actions": [
                {
                    "name": "load_cf",
                    "type": "load",
                    "source": {
                        "type": "cloudfiles",
                        "path": "/data",
                        "format": "csv",
                    },
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        src = result["actions"][0]["source"]
        assert "catalog" not in src
        assert "schema" not in src


class TestNonWriteNonLoadActions:
    """Tests that non-write/non-delta-load actions are untouched."""

    def test_transform_action_untouched(self):
        """Transform actions pass through unchanged."""
        fg = {
            "actions": [
                {
                    "name": "transform_test",
                    "type": "transform",
                    "transform_type": "sql",
                    "source": "v_input",
                    "target": "v_output",
                    "sql": "SELECT * FROM v_input",
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        assert result["actions"][0]["source"] == "v_input"

    def test_test_action_untouched(self):
        """Test actions pass through unchanged."""
        fg = {
            "actions": [
                {
                    "name": "test_test",
                    "type": "test",
                    "test_type": "row_count",
                    "source": ["v_a", "v_b"],
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        assert result["actions"][0]["source"] == ["v_a", "v_b"]


class TestDeprecationWarning:
    """Tests that deprecation warnings are emitted."""

    def test_write_target_deprecation_warning(self, caplog):
        """Deprecation warning emitted for write target database conversion."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "cat.sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        with caplog.at_level(logging.WARNING):
            normalize_namespace_fields(fg)
        assert any("DEPRECATION" in record.message for record in caplog.records)

    def test_delta_source_deprecation_warning(self, caplog):
        """Deprecation warning emitted for delta source database conversion."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "database": "cat.sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        with caplog.at_level(logging.WARNING):
            normalize_namespace_fields(fg)
        assert any("DEPRECATION" in record.message for record in caplog.records)

    def test_new_format_no_warning(self, caplog):
        """No deprecation warning when using new catalog/schema format."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "catalog": "cat",
                        "schema": "sch",
                        "table": "tbl",
                    },
                }
            ]
        }
        with caplog.at_level(logging.WARNING):
            normalize_namespace_fields(fg)
        assert not any("DEPRECATION" in record.message for record in caplog.records)


class TestEdgeCases:
    """Edge case tests."""

    def test_empty_actions_list(self):
        """Empty actions list → no error."""
        fg = {"actions": []}
        result = normalize_namespace_fields(fg)
        assert result["actions"] == []

    def test_no_actions_key(self):
        """Missing actions key → no error."""
        fg = {"flowgroup": "test"}
        result = normalize_namespace_fields(fg)
        assert "flowgroup" in result

    def test_write_target_not_dict(self):
        """Non-dict write_target → skipped."""
        fg = {
            "actions": [
                {
                    "name": "write_test",
                    "type": "write",
                    "write_target": "not_a_dict",
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        assert result["actions"][0]["write_target"] == "not_a_dict"

    def test_source_not_dict(self):
        """Non-dict source for load action → skipped."""
        fg = {
            "actions": [
                {
                    "name": "load_test",
                    "type": "load",
                    "source": "v_something",
                }
            ]
        }
        result = normalize_namespace_fields(fg)
        assert result["actions"][0]["source"] == "v_something"

    def test_multiple_actions_normalized(self):
        """Multiple actions in one flowgroup are all normalized."""
        fg = {
            "actions": [
                {
                    "name": "load_raw",
                    "type": "load",
                    "source": {
                        "type": "delta",
                        "database": "cat.raw",
                        "table": "orders",
                    },
                },
                {
                    "name": "write_bronze",
                    "type": "write",
                    "write_target": {
                        "type": "streaming_table",
                        "database": "cat.bronze",
                        "table": "orders",
                    },
                },
            ]
        }
        result = normalize_namespace_fields(fg)
        src = result["actions"][0]["source"]
        assert src["catalog"] == "cat"
        assert src["schema"] == "raw"
        wt = result["actions"][1]["write_target"]
        assert wt["catalog"] == "cat"
        assert wt["schema"] == "bronze"
