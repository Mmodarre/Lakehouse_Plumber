"""Tests for shared source extraction functions in lhp.utils.source_extractor."""

from unittest.mock import Mock

import pytest

from lhp.models.config import Action
from lhp.utils.source_extractor import (
    extract_action_sources,
    extract_cdc_sources,
    is_cdc_write_action,
)


class TestExtractActionSources:
    """Tests for extract_action_sources — the main entry point for dependency analysis."""

    def test_string_source_returns_single_item_list(self):
        """A plain string source should return a list containing that string."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = "v_raw_data"

        result = extract_action_sources(action)

        assert result == ["v_raw_data"]

    def test_list_of_strings_returns_all(self):
        """A list of string sources should return all of them."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = ["v_customers", "v_orders", "v_products"]

        result = extract_action_sources(action)

        assert result == ["v_customers", "v_orders", "v_products"]

    def test_dict_with_view_key_returns_view_name(self):
        """A dict source with a 'view' key should return the view name."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {"mode": "append"}
        action.source = {"view": "v_clean_data"}

        result = extract_action_sources(action)

        assert result == ["v_clean_data"]

    def test_dict_with_source_key_string(self):
        """A dict source with a 'source' key (string) should return that source."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = {"source": "v_input_view"}

        result = extract_action_sources(action)

        assert result == ["v_input_view"]

    def test_dict_with_source_key_list(self):
        """A dict source with a 'source' key (list) should return all sources."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = {"source": ["v_left", "v_right"]}

        result = extract_action_sources(action)

        assert result == ["v_left", "v_right"]

    def test_dict_with_sources_key_returns_all(self):
        """A dict source with a 'sources' key should return all listed sources."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = {"sources": ["v_dim_1", "v_dim_2", "v_fact"]}

        result = extract_action_sources(action)

        assert result == ["v_dim_1", "v_dim_2", "v_fact"]

    def test_dict_with_catalog_schema_and_table_returns_qualified_name(self):
        """A dict with 'catalog', 'schema', and 'table' keys should return 'catalog.schema.table'."""
        action = Mock(spec=Action)
        action.type = "load"
        action.write_target = None
        action.source = {"catalog": "bronze_catalog", "schema": "bronze", "table": "raw_events"}

        result = extract_action_sources(action)

        assert result == ["bronze_catalog.bronze.raw_events"]

    def test_no_source_attribute_returns_empty(self):
        """An action without a source attribute should return an empty list."""
        action = Mock(spec=[])  # No attributes at all

        result = extract_action_sources(action)

        assert result == []

    def test_none_source_returns_empty(self):
        """An action with source=None should return an empty list."""
        action = Mock(spec=Action)
        action.type = "load"
        action.write_target = None
        action.source = None

        result = extract_action_sources(action)

        assert result == []

    def test_cdc_write_action_delegates_to_extract_cdc_sources(self):
        """A CDC write action should delegate extraction to extract_cdc_sources."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {
            "mode": "cdc",
            "cdc_config": {"source": "v_cdc_source"},
        }
        action.source = "v_fallback_should_not_be_used"

        result = extract_action_sources(action)

        assert result == ["v_cdc_source"]

    def test_cdc_write_action_falls_back_to_action_source(self):
        """A CDC write action with no CDC-specific source falls back to action.source."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {
            "mode": "cdc",
            "cdc_config": {},
        }
        action.source = "v_fallback_source"

        result = extract_action_sources(action)

        assert result == ["v_fallback_source"]

    def test_dict_source_view_takes_priority_over_other_keys(self):
        """The 'view' key in a dict source should take priority."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {"mode": "append"}
        action.source = {"view": "v_priority", "source": "v_secondary"}

        result = extract_action_sources(action)

        assert result == ["v_priority"]

    def test_empty_list_source_returns_empty(self):
        """An empty list source should return an empty list."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = []

        result = extract_action_sources(action)

        assert result == []

    def test_list_with_non_string_items_ignored(self):
        """Non-string items in a list source should be ignored."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = None
        action.source = ["v_valid", 42, None, "v_also_valid"]

        result = extract_action_sources(action)

        assert result == ["v_valid", "v_also_valid"]

    def test_dict_with_empty_catalog_schema_and_table(self):
        """A dict with empty catalog, schema, and table should not produce a dotted name."""
        action = Mock(spec=Action)
        action.type = "load"
        action.write_target = None
        action.source = {"catalog": "", "schema": "", "table": ""}

        result = extract_action_sources(action)

        assert result == []

    def test_dict_with_no_recognized_keys_returns_empty(self):
        """A dict with no recognized keys should return an empty list."""
        action = Mock(spec=Action)
        action.type = "load"
        action.write_target = None
        action.source = {"path": "/mnt/data", "format": "parquet"}

        result = extract_action_sources(action)

        assert result == []


class TestIsCdcWriteAction:
    """Tests for is_cdc_write_action — predicate for CDC write detection."""

    def test_write_action_with_cdc_mode_returns_true(self):
        """A write action with mode 'cdc' should be identified as CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {"mode": "cdc", "cdc_config": {"source": "v_src"}}

        assert is_cdc_write_action(action) is True

    def test_write_action_with_snapshot_cdc_mode_returns_true(self):
        """A write action with mode 'snapshot_cdc' should be identified as CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {"source": "v_snap"},
        }

        assert is_cdc_write_action(action) is True

    def test_write_action_without_write_target_returns_false(self):
        """A write action with no write_target should not be CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = None

        assert not is_cdc_write_action(action)

    def test_write_action_with_non_cdc_mode_returns_false(self):
        """A write action with a non-CDC mode (e.g. 'append') should not be CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {"mode": "append"}

        assert not is_cdc_write_action(action)

    def test_non_write_action_returns_false(self):
        """A non-write action (e.g. 'transform') should not be CDC."""
        action = Mock(spec=Action)
        action.type = "transform"
        action.write_target = {"mode": "cdc"}

        assert not is_cdc_write_action(action)

    def test_action_without_type_attribute_returns_false(self):
        """An action without a 'type' attribute should not be CDC."""
        action = Mock(spec=[])  # No attributes at all

        assert not is_cdc_write_action(action)

    def test_action_without_write_target_attribute_returns_false(self):
        """An action missing the write_target attribute entirely should not be CDC."""
        action = Mock(spec=["type"])
        action.type = "write"

        assert not is_cdc_write_action(action)

    def test_write_target_not_a_dict_returns_false(self):
        """A write_target that is not a dict should not be CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = "not_a_dict"

        assert not is_cdc_write_action(action)

    def test_write_target_empty_dict_returns_false(self):
        """An empty write_target dict (no mode key) should not be CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {}

        assert not is_cdc_write_action(action)

    def test_write_action_mode_complete_returns_false(self):
        """A write action with mode 'complete' should not be CDC."""
        action = Mock(spec=Action)
        action.type = "write"
        action.write_target = {"mode": "complete"}

        assert not is_cdc_write_action(action)


class TestExtractCdcSources:
    """Tests for extract_cdc_sources — CDC-specific source extraction."""

    def test_cdc_mode_with_cdc_config_source(self):
        """CDC mode with cdc_config.source should return that source."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "cdc",
            "cdc_config": {"source": "v_cdc_input"},
        }

        result = extract_cdc_sources(action)

        assert result == ["v_cdc_input"]

    def test_snapshot_cdc_with_source_function_returns_empty(self):
        """snapshot_cdc with source_function should return [] (self-contained)."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {
                "source_function": "my_snapshot_func",
            },
        }

        result = extract_cdc_sources(action)

        assert result == []

    def test_snapshot_cdc_with_snapshot_cdc_config_source(self):
        """snapshot_cdc with snapshot_cdc_config.source should return that source."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {"source": "v_snapshot_input"},
        }

        result = extract_cdc_sources(action)

        assert result == ["v_snapshot_input"]

    def test_no_cdc_specific_source_returns_none(self):
        """CDC mode with no CDC-specific source should return None (fallback signal)."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "cdc",
            "cdc_config": {},
        }

        result = extract_cdc_sources(action)

        assert result is None

    def test_empty_write_target_returns_none(self):
        """An empty write_target should return None."""
        action = Mock(spec=Action)
        action.write_target = {}

        result = extract_cdc_sources(action)

        assert result is None

    def test_none_write_target_returns_none(self):
        """A None write_target should return None."""
        action = Mock(spec=Action)
        action.write_target = None

        result = extract_cdc_sources(action)

        assert result is None

    def test_write_target_not_a_dict_returns_none(self):
        """A non-dict write_target should return None."""
        action = Mock(spec=Action)
        action.write_target = "not_a_dict"

        result = extract_cdc_sources(action)

        assert result is None

    def test_snapshot_cdc_source_function_takes_priority_over_source(self):
        """source_function should take priority over source in snapshot_cdc_config."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "snapshot_cdc",
            "snapshot_cdc_config": {
                "source_function": "my_func",
                "source": "v_should_not_be_used",
            },
        }

        result = extract_cdc_sources(action)

        assert result == []

    def test_cdc_mode_without_cdc_config_key_returns_none(self):
        """CDC mode without cdc_config key at all should return None."""
        action = Mock(spec=Action)
        action.write_target = {"mode": "cdc"}

        result = extract_cdc_sources(action)

        assert result is None

    def test_snapshot_cdc_without_snapshot_cdc_config_key_returns_none(self):
        """snapshot_cdc without snapshot_cdc_config key should return None."""
        action = Mock(spec=Action)
        action.write_target = {"mode": "snapshot_cdc"}

        result = extract_cdc_sources(action)

        assert result is None

    def test_cdc_config_source_is_none_returns_none(self):
        """cdc_config with source=None should return None (falsy source)."""
        action = Mock(spec=Action)
        action.write_target = {
            "mode": "cdc",
            "cdc_config": {"source": None},
        }

        result = extract_cdc_sources(action)

        assert result is None

    def test_non_cdc_mode_returns_none(self):
        """A non-CDC mode (e.g. 'append') should return None."""
        action = Mock(spec=Action)
        action.write_target = {"mode": "append"}

        result = extract_cdc_sources(action)

        assert result is None
