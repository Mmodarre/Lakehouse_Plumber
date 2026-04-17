"""
Unit tests for declarative event_log configuration in lhp.yaml.

Tests the EventLogConfig model, ProjectConfigLoader parsing/validation,
and BundleManager injection logic.
"""

import pytest
from pathlib import Path
from unittest.mock import MagicMock

from lhp.models.config import EventLogConfig, ProjectConfig
from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.utils.error_formatter import LHPError


class TestEventLogConfigModel:
    """Test EventLogConfig Pydantic model."""

    def test_default_values(self):
        """Test model creates with sensible defaults."""
        config = EventLogConfig()
        assert config.enabled is True
        assert config.catalog is None
        assert config.schema_ is None
        assert config.name_prefix == ""
        assert config.name_suffix == ""

    def test_custom_values(self):
        """Test model accepts all custom values."""
        config = EventLogConfig(
            enabled=True,
            catalog="${catalog}",
            schema="_meta",
            name_prefix="prefix_",
            name_suffix="_event_log",
        )
        assert config.enabled is True
        assert config.catalog == "${catalog}"
        assert config.schema_ == "_meta"
        assert config.name_prefix == "prefix_"
        assert config.name_suffix == "_event_log"

    def test_disabled_state(self):
        """Test disabled config does not require catalog/schema."""
        config = EventLogConfig(enabled=False)
        assert config.enabled is False
        assert config.catalog is None
        assert config.schema_ is None

    def test_project_config_event_log_field(self):
        """Test ProjectConfig accepts event_log field."""
        event_log = EventLogConfig(catalog="my_catalog", schema="my_schema")
        project = ProjectConfig(name="test", event_log=event_log)
        assert project.event_log is not None
        assert project.event_log.catalog == "my_catalog"

    def test_project_config_event_log_none_by_default(self):
        """Test ProjectConfig.event_log is None when not provided."""
        project = ProjectConfig(name="test")
        assert project.event_log is None


class TestProjectConfigLoaderEventLog:
    """Test ProjectConfigLoader parsing and validation for event_log."""

    @pytest.fixture
    def loader(self, tmp_path):
        """Create a loader with a temp project root."""
        return ProjectConfigLoader(tmp_path)

    def test_valid_parse(self, loader):
        """Test valid event_log configuration is parsed correctly."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "enabled": True,
                "catalog": "my_catalog",
                "schema": "_meta",
                "name_prefix": "",
                "name_suffix": "_event_log",
            },
        }
        result = loader._parse_project_config(config_data)
        assert result.event_log is not None
        assert result.event_log.enabled is True
        assert result.event_log.catalog == "my_catalog"
        assert result.event_log.schema_ == "_meta"
        assert result.event_log.name_suffix == "_event_log"

    def test_missing_catalog_when_enabled_raises_error(self, loader):
        """Test that enabled=True without catalog raises validation error."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "enabled": True,
                "schema": "_meta",
            },
        }
        with pytest.raises(LHPError) as exc_info:
            loader._parse_project_config(config_data)
        assert "007" in str(exc_info.value)
        assert "catalog" in str(exc_info.value).lower()

    def test_missing_schema_when_enabled_raises_error(self, loader):
        """Test that enabled=True without schema raises validation error."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "enabled": True,
                "catalog": "my_catalog",
            },
        }
        with pytest.raises(LHPError) as exc_info:
            loader._parse_project_config(config_data)
        assert "007" in str(exc_info.value)
        assert "schema" in str(exc_info.value).lower()

    def test_missing_both_when_enabled_raises_error(self, loader):
        """Test that enabled=True without both catalog and schema raises error."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "enabled": True,
            },
        }
        with pytest.raises(LHPError) as exc_info:
            loader._parse_project_config(config_data)
        assert "007" in str(exc_info.value)

    def test_disabled_without_catalog_schema_ok(self, loader):
        """Test that disabled event_log does not require catalog/schema."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "enabled": False,
            },
        }
        result = loader._parse_project_config(config_data)
        assert result.event_log is not None
        assert result.event_log.enabled is False

    def test_prefix_suffix_parsing(self, loader):
        """Test prefix and suffix are parsed correctly."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "catalog": "cat",
                "schema": "sch",
                "name_prefix": "pre_",
                "name_suffix": "_suf",
            },
        }
        result = loader._parse_project_config(config_data)
        assert result.event_log.name_prefix == "pre_"
        assert result.event_log.name_suffix == "_suf"

    def test_substitution_tokens_stored_raw(self, loader):
        """Test that substitution tokens in catalog/schema are stored raw (not resolved)."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "catalog": "${event_log_catalog}",
                "schema": "${event_log_schema}",
            },
        }
        result = loader._parse_project_config(config_data)
        assert result.event_log.catalog == "${event_log_catalog}"
        assert result.event_log.schema_ == "${event_log_schema}"

    def test_non_dict_event_log_raises_error(self, loader):
        """Test that non-dict event_log raises parse error."""
        config_data = {
            "name": "test_project",
            "event_log": "invalid",
        }
        with pytest.raises(LHPError) as exc_info:
            loader._parse_project_config(config_data)
        assert "006" in str(exc_info.value)

    def test_no_event_log_in_config(self, loader):
        """Test that missing event_log key results in None."""
        config_data = {"name": "test_project"}
        result = loader._parse_project_config(config_data)
        assert result.event_log is None

    def test_minimal_valid_config(self, loader):
        """Test minimal valid event_log with just catalog and schema."""
        config_data = {
            "name": "test_project",
            "event_log": {
                "catalog": "cat",
                "schema": "sch",
            },
        }
        result = loader._parse_project_config(config_data)
        assert result.event_log.enabled is True
        assert result.event_log.catalog == "cat"
        assert result.event_log.schema_ == "sch"
        assert result.event_log.name_prefix == ""
        assert result.event_log.name_suffix == ""


class TestBundleManagerEventLogInjection:
    """Test BundleManager._inject_project_event_log() method."""

    def _make_project_config(self, event_log=None):
        """Helper to create a ProjectConfig with optional event_log."""
        return ProjectConfig(name="test", event_log=event_log)

    def _make_manager(self, project_config=None):
        """Helper to create a BundleManager with mocked internals."""
        from lhp.bundle.manager import BundleManager

        manager = BundleManager.__new__(BundleManager)
        manager.project_config = project_config
        manager.logger = MagicMock()
        return manager

    def test_basic_injection(self):
        """Test basic event_log injection with name generation."""
        event_log = EventLogConfig(catalog="my_cat", schema="_meta")
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {"serverless": True}
        result = manager._inject_project_event_log(pipeline_config, "acmi_edw_raw")

        assert "event_log" in result
        assert result["event_log"]["name"] == "acmi_edw_raw"
        assert result["event_log"]["catalog"] == "my_cat"
        assert result["event_log"]["schema"] == "_meta"

    def test_injection_with_prefix_suffix(self):
        """Test event_log injection with name_prefix and name_suffix."""
        event_log = EventLogConfig(
            catalog="cat",
            schema="sch",
            name_prefix="log_",
            name_suffix="_events",
        )
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {}
        result = manager._inject_project_event_log(pipeline_config, "bronze_load")

        assert result["event_log"]["name"] == "log_bronze_load_events"

    def test_pipeline_config_override_full_replace(self):
        """Test that pipeline_config event_log dict fully replaces project-level."""
        event_log = EventLogConfig(catalog="project_cat", schema="project_sch")
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {
            "event_log": {
                "name": "custom_log",
                "catalog": "pipeline_cat",
                "schema": "pipeline_sch",
            }
        }
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        # Pipeline's own event_log should be unchanged
        assert result["event_log"]["name"] == "custom_log"
        assert result["event_log"]["catalog"] == "pipeline_cat"

    def test_pipeline_config_opt_out_with_false(self):
        """Test that event_log: false in pipeline_config disables event logging."""
        event_log = EventLogConfig(catalog="cat", schema="sch")
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {"event_log": False, "serverless": True}
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        assert "event_log" not in result
        assert result["serverless"] is True

    def test_no_project_config(self):
        """Test no injection when project_config is None."""
        manager = self._make_manager(project_config=None)

        pipeline_config = {"serverless": True}
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        assert "event_log" not in result

    def test_no_event_log_in_project_config(self):
        """Test no injection when project_config has no event_log."""
        project_config = self._make_project_config(event_log=None)
        manager = self._make_manager(project_config)

        pipeline_config = {"serverless": True}
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        assert "event_log" not in result

    def test_disabled_event_log(self):
        """Test no injection when event_log is disabled."""
        event_log = EventLogConfig(enabled=False)
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {"serverless": True}
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        assert "event_log" not in result

    def test_substitution_tokens_passed_through(self):
        """Test that substitution tokens in event_log values are preserved for later resolution."""
        event_log = EventLogConfig(
            catalog="${event_log_catalog}",
            schema="${event_log_schema}",
            name_suffix="_log",
        )
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {}
        result = manager._inject_project_event_log(pipeline_config, "my_pipeline")

        assert result["event_log"]["catalog"] == "${event_log_catalog}"
        assert result["event_log"]["schema"] == "${event_log_schema}"
        assert result["event_log"]["name"] == "my_pipeline_log"

    def test_empty_prefix_suffix_defaults(self):
        """Test that empty prefix/suffix defaults result in pipeline_name as event_log name."""
        event_log = EventLogConfig(catalog="cat", schema="sch")
        project_config = self._make_project_config(event_log)
        manager = self._make_manager(project_config)

        pipeline_config = {}
        result = manager._inject_project_event_log(pipeline_config, "gold_analytics")

        assert result["event_log"]["name"] == "gold_analytics"
