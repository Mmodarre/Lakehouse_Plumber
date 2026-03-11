"""
Tests for DatabricksYAMLManager.

Tests databricks.yml reading, validation, and variable update operations
using the DatabricksYAMLManager class directly.
"""

import pytest
from pathlib import Path
from unittest.mock import patch, mock_open

import yaml

from lhp.bundle.databricks_yaml_manager import DatabricksYAMLManager
from lhp.bundle.exceptions import BundleResourceError, MissingDatabricksTargetError
from lhp.utils.error_formatter import LHPFileError


BASIC_DATABRICKS_YML = """\
targets:
  dev:
    variables:
      catalog: dev_catalog
  prod:
    variables:
      catalog: prod_catalog
"""

NO_TARGETS_YML = """\
bundle:
  name: my_bundle
"""

TARGETS_NO_VARIABLES_YML = """\
targets:
  dev:
    mode: development
  prod:
    mode: production
"""


class TestDatabricksYAMLManager:
    """Tests for DatabricksYAMLManager."""

    def _write_databricks_yml(self, path: Path, content: str) -> Path:
        """Helper to write a databricks.yml file in the given directory."""
        dbx_file = path / "databricks.yml"
        dbx_file.write_text(content)
        return dbx_file

    # --- validate_targets_exist ---

    def test_validate_targets_exist_file_missing_raises_lhp_file_error(self, tmp_path):
        """Missing databricks.yml should raise LHPFileError."""
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(LHPFileError):
            manager.validate_targets_exist(["dev"])

    def test_validate_targets_exist_missing_targets_section(self, tmp_path):
        """databricks.yml without 'targets' section should raise MissingDatabricksTargetError."""
        self._write_databricks_yml(tmp_path, NO_TARGETS_YML)
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(MissingDatabricksTargetError, match="missing 'targets' section"):
            manager.validate_targets_exist(["dev"])

    def test_validate_targets_exist_missing_specific_targets(self, tmp_path):
        """Requesting non-existent targets should raise MissingDatabricksTargetError with available list."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(MissingDatabricksTargetError, match="Missing targets") as exc_info:
            manager.validate_targets_exist(["dev", "staging"])
        assert "staging" in str(exc_info.value)
        assert "Available targets" in str(exc_info.value)

    def test_validate_targets_exist_all_present_no_error(self, tmp_path):
        """All targets present should not raise."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)
        # Should not raise
        manager.validate_targets_exist(["dev", "prod"])

    # --- update_target_variables ---

    def test_update_target_variables_happy_path(self, tmp_path):
        """Variables should be updated in the file after calling update_target_variables."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        manager.update_target_variables(["dev"], {"new_var": "new_value"})

        # Read back and verify
        result = manager.get_target_variables("dev")
        assert result["catalog"] == "dev_catalog"
        assert result["new_var"] == "new_value"

    def test_update_target_variables_multiple_targets(self, tmp_path):
        """Variables should be updated in multiple targets."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        manager.update_target_variables(["dev", "prod"], {"shared_var": "shared_val"})

        dev_vars = manager.get_target_variables("dev")
        prod_vars = manager.get_target_variables("prod")
        assert dev_vars["shared_var"] == "shared_val"
        assert prod_vars["shared_var"] == "shared_val"

    def test_update_target_variables_creates_variables_section(self, tmp_path):
        """update_target_variables should create the 'variables' section when missing."""
        self._write_databricks_yml(tmp_path, TARGETS_NO_VARIABLES_YML)
        manager = DatabricksYAMLManager(tmp_path)

        manager.update_target_variables(["dev"], {"catalog": "my_catalog"})

        result = manager.get_target_variables("dev")
        assert result["catalog"] == "my_catalog"

    def test_update_target_variables_file_missing_raises_lhp_file_error(self, tmp_path):
        """update_target_variables should raise LHPFileError when file is missing."""
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(LHPFileError):
            manager.update_target_variables(["dev"], {"key": "val"})

    def test_update_target_variables_missing_target_raises_error(self, tmp_path):
        """update_target_variables should raise MissingDatabricksTargetError for non-existent target."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(MissingDatabricksTargetError):
            manager.update_target_variables(["nonexistent"], {"key": "val"})

    def test_update_target_variables_os_error_raises_bundle_resource_error(self, tmp_path):
        """OSError during file write should raise BundleResourceError."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        with patch("builtins.open", side_effect=OSError("disk full")):
            with pytest.raises(BundleResourceError, match="Failed to update"):
                manager.update_target_variables(["dev"], {"key": "val"})

    def test_update_target_variables_overwrites_existing(self, tmp_path):
        """Updating an existing variable should overwrite its value."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        manager.update_target_variables(["dev"], {"catalog": "updated_catalog"})

        result = manager.get_target_variables("dev")
        assert result["catalog"] == "updated_catalog"

    # --- get_target_variables ---

    def test_get_target_variables_happy_path(self, tmp_path):
        """get_target_variables should return variables dict."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        result = manager.get_target_variables("dev")
        assert result == {"catalog": "dev_catalog"}

    def test_get_target_variables_target_not_found(self, tmp_path):
        """get_target_variables for a missing target should raise MissingDatabricksTargetError."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        with pytest.raises(MissingDatabricksTargetError, match="Target 'staging' not found"):
            manager.get_target_variables("staging")

    def test_get_target_variables_file_missing(self, tmp_path):
        """get_target_variables with missing file should raise LHPFileError."""
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(LHPFileError):
            manager.get_target_variables("dev")

    def test_get_target_variables_no_variables_section_returns_empty(self, tmp_path):
        """Target without variables section should return empty dict."""
        self._write_databricks_yml(tmp_path, TARGETS_NO_VARIABLES_YML)
        manager = DatabricksYAMLManager(tmp_path)

        result = manager.get_target_variables("dev")
        assert result == {}

    def test_get_target_variables_missing_targets_section(self, tmp_path):
        """get_target_variables with no 'targets' key should raise MissingDatabricksTargetError."""
        self._write_databricks_yml(tmp_path, NO_TARGETS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        with pytest.raises(MissingDatabricksTargetError, match="missing 'targets' section"):
            manager.get_target_variables("dev")

    # --- bulk_update_all_targets ---

    def test_bulk_update_all_targets_multi_env(self, tmp_path):
        """bulk_update_all_targets should update each target with its own variables."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        env_vars = {
            "dev": {"pipeline_name": "dev_pipeline"},
            "prod": {"pipeline_name": "prod_pipeline"},
        }
        manager.bulk_update_all_targets(["dev", "prod"], env_vars)

        dev_vars = manager.get_target_variables("dev")
        prod_vars = manager.get_target_variables("prod")
        assert dev_vars["pipeline_name"] == "dev_pipeline"
        assert dev_vars["catalog"] == "dev_catalog"  # original preserved
        assert prod_vars["pipeline_name"] == "prod_pipeline"
        assert prod_vars["catalog"] == "prod_catalog"  # original preserved

    def test_bulk_update_all_targets_creates_variables_section(self, tmp_path):
        """bulk_update should create variables section in targets that lack one."""
        self._write_databricks_yml(tmp_path, TARGETS_NO_VARIABLES_YML)
        manager = DatabricksYAMLManager(tmp_path)

        env_vars = {
            "dev": {"catalog": "dev_cat"},
            "prod": {"catalog": "prod_cat"},
        }
        manager.bulk_update_all_targets(["dev", "prod"], env_vars)

        assert manager.get_target_variables("dev") == {"catalog": "dev_cat"}
        assert manager.get_target_variables("prod") == {"catalog": "prod_cat"}

    def test_bulk_update_all_targets_file_missing_raises_lhp_file_error(self, tmp_path):
        """bulk_update_all_targets with missing file should raise LHPFileError."""
        manager = DatabricksYAMLManager(tmp_path)
        with pytest.raises(LHPFileError):
            manager.bulk_update_all_targets(["dev"], {"dev": {"key": "val"}})

    def test_bulk_update_all_targets_os_error_raises_bundle_resource_error(self, tmp_path):
        """OSError during bulk update should raise BundleResourceError."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        with patch("builtins.open", side_effect=OSError("permission denied")):
            with pytest.raises(BundleResourceError, match="Failed to bulk update"):
                manager.bulk_update_all_targets(
                    ["dev"], {"dev": {"key": "val"}}
                )

    def test_bulk_update_all_targets_missing_target_raises_error(self, tmp_path):
        """bulk_update_all_targets requesting non-existent target should raise."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        with pytest.raises(MissingDatabricksTargetError):
            manager.bulk_update_all_targets(
                ["dev", "staging"],
                {"dev": {"k": "v"}, "staging": {"k": "v"}},
            )

    def test_bulk_update_skips_envs_not_in_environment_variables(self, tmp_path):
        """Targets listed in environments but not in environment_variables dict should get variables section but no updates."""
        self._write_databricks_yml(tmp_path, BASIC_DATABRICKS_YML)
        manager = DatabricksYAMLManager(tmp_path)

        # Only provide variables for dev, not prod
        env_vars = {"dev": {"new_key": "new_val"}}
        manager.bulk_update_all_targets(["dev", "prod"], env_vars)

        dev_vars = manager.get_target_variables("dev")
        prod_vars = manager.get_target_variables("prod")
        assert dev_vars["new_key"] == "new_val"
        # prod should keep its original variables unchanged
        assert prod_vars["catalog"] == "prod_catalog"
