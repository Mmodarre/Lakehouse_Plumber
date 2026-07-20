"""Tests for unknown field validation."""

import pytest

from lhp.core.validators import ConfigFieldValidator
from lhp.errors import LHPError


class TestUnknownFieldValidation:
    """Test unknown field validation functionality."""

    def setup_method(self):
        """Set up test fixtures."""
        self.validator = ConfigFieldValidator()

    def test_valid_cloudfiles_source(self):
        """Test valid cloudfiles source configuration passes validation."""
        source_config = {
            "type": "cloudfiles",
            "path": "/mnt/data/files/*.csv",
            "format": "csv",
            "readMode": "stream",
            "options": {"cloudFiles.header": True},
        }

        self.validator.validate_load_source(source_config, "test_action")

    def test_unknown_field_in_cloudfiles_source(self):
        """Test unknown field in cloudfiles source raises error."""
        source_config = {
            "type": "cloudfiles",
            "path": "/mnt/data/files/*.csv",
            "format": "csv",
            "mode": "stream",  # Should be readMode
            "unknown_field": "value",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(source_config, "test_action")

        error = exc_info.value
        assert "Unknown fields: mode, unknown_field in load source (cloudfiles)" in str(
            error
        )
        assert "'mode' → 'readMode'" in str(error)

    def test_valid_delta_source(self):
        """Test valid delta source configuration passes validation."""
        source_config = {
            "type": "delta",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "readMode": "batch",
        }

        self.validator.validate_load_source(source_config, "test_action")

    def test_unknown_field_in_delta_source(self):
        """Test unknown field in delta source raises error."""
        source_config = {
            "type": "delta",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "invalid_option": "value",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(source_config, "test_action")

        error = exc_info.value
        assert "Unknown field 'invalid_option' in load source (delta)" in str(error)

    def test_valid_streaming_table_write_target(self):
        """Test valid streaming table write target passes validation."""
        write_target = {
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "create_table": True,
            "table_properties": {"delta.enableChangeDataFeed": "true"},
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_unknown_field_in_write_target(self):
        """Test unknown field in write target raises error."""
        write_target = {
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "invalid_field": "value",
            "another_unknown": 123,
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_write_target(write_target, "test_action")

        error = exc_info.value
        assert (
            "Unknown fields: another_unknown, invalid_field in write target (streaming_table)"
            in str(error)
        )

    def test_valid_materialized_view_write_target(self):
        """Test valid materialized view write target passes validation."""
        write_target = {
            "type": "materialized_view",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_view",
            "refresh_schedule": "@daily",
            "sql": "SELECT * FROM source_table",
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_materialized_view_with_cluster_by_auto_and_refresh_policy(self):
        """MV write target with cluster_by_auto and refresh_policy passes validation."""
        write_target = {
            "type": "materialized_view",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_view",
            "cluster_by_auto": True,
            "refresh_policy": "@daily",
            "sql": "SELECT * FROM source_table",
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_streaming_table_with_cluster_by_auto(self):
        """Streaming table write target with cluster_by_auto passes validation."""
        write_target = {
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "cluster_by_auto": True,
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_streaming_table_rejects_refresh_policy(self):
        """refresh_policy is unknown for streaming tables and must be rejected."""
        write_target = {
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "refresh_policy": "@daily",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_write_target(write_target, "test_action")

        assert (
            "Unknown field 'refresh_policy' in write target (streaming_table)"
            in str(exc_info.value)
        )

    def test_streaming_table_tags_file_valid(self):
        """tags_file is a known field on streaming table write targets."""
        write_target = {
            "type": "streaming_table",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_table",
            "tags_file": "schemas/orders_tags.yaml",
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_materialized_view_tags_file_valid(self):
        """tags_file is a known field on materialized view write targets."""
        write_target = {
            "type": "materialized_view",
            "catalog": "catalog",
            "schema": "schema",
            "table": "my_view",
            "sql": "SELECT * FROM source_table",
            "tags_file": "schemas/mv_tags.yaml",
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_sink_rejects_tags_file(self):
        """tags_file is unknown for sink write targets and must be rejected."""
        write_target = {
            "type": "sink",
            "sink_type": "delta",
            "sink_name": "external_delta",
            "tags_file": "schemas/sink_tags.yaml",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_write_target(write_target, "test_action")

        error = exc_info.value
        assert error.code == "LHP-CFG-001"
        assert "Unknown field 'tags_file' in write target (sink)" in str(error)

    def test_valid_action_fields(self):
        """Test valid action fields pass validation."""
        action_dict = {
            "name": "test_action",
            "type": "load",
            "source": {"type": "cloudfiles"},
            "target": "v_data",
            "description": "Test action",
            "readMode": "stream",
        }

        self.validator.validate_action_fields(action_dict, "test_action")

    def test_unknown_action_field(self):
        """Test unknown action field raises error."""
        action_dict = {
            "name": "test_action",
            "type": "load",
            "source": {"type": "cloudfiles"},
            "target": "v_data",
            "read_mode": "stream",  # Should be readMode (camelCase)
            "invalid_field": "value",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_action_fields(action_dict, "test_action")

        error = exc_info.value
        assert "invalid_field" in str(error)
        assert "'read_mode' → 'readMode'" in str(error)

    def test_sql_source_validation(self):
        """Test SQL source validation."""
        valid_sql = {"type": "sql", "sql": "SELECT * FROM table"}
        self.validator.validate_load_source(valid_sql, "test_action")

        invalid_sql = {
            "type": "sql",
            "sql": "SELECT * FROM table",
            "unknown_param": "value",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(invalid_sql, "test_action")

        assert "Unknown field 'unknown_param' in load source (sql)" in str(
            exc_info.value
        )

    def test_jdbc_source_validation(self):
        """Test JDBC source validation."""
        valid_jdbc = {
            "type": "jdbc",
            "url": "jdbc:postgresql://localhost:5432/db",
            "user": "user",
            "password": "pass",
            "table": "my_table",
        }
        self.validator.validate_load_source(valid_jdbc, "test_action")

        invalid_jdbc = {
            "type": "jdbc",
            "url": "jdbc:postgresql://localhost:5432/db",
            "user": "user",
            "password": "pass",
            "table": "my_table",
            "connection_pool_size": 10,  # Not a valid JDBC field
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(invalid_jdbc, "test_action")

        assert "Unknown field 'connection_pool_size' in load source (jdbc)" in str(
            exc_info.value
        )

    def test_python_source_validation(self):
        """Test Python source validation."""
        valid_python = {
            "type": "python",
            "module_path": "my_module",
            "function_name": "get_data",
            "parameters": {"param1": "value1"},
        }
        self.validator.validate_load_source(valid_python, "test_action")

        invalid_python = {
            "type": "python",
            "module_path": "my_module",
            "function_name": "get_data",
            "timeout": 30,  # Not a valid Python source field
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(invalid_python, "test_action")

        assert "Unknown field 'timeout' in load source (python)" in str(exc_info.value)

    def test_skip_validation_for_non_dict_source(self):
        """Test validation is skipped for non-dict sources."""
        self.validator.validate_load_source("SELECT * FROM table", "test_action")
        self.validator.validate_load_source(["view1", "view2"], "test_action")

    def test_skip_validation_for_unknown_source_type(self):
        """Test validation is skipped for unknown source types."""
        unknown_source = {"type": "unknown_type", "any_field": "any_value"}

        # Unknown source types are silently skipped — caught by a different validator
        self.validator.validate_load_source(unknown_source, "test_action")

    def test_error_message_quality(self):
        """Test that error messages are helpful and well-formatted."""
        source_config = {
            "type": "cloudfiles",
            "path": "/data/*.csv",
            "mode": "stream",  # Common mistake
            "format": "csv",
            "uknown_typo": "value",  # Typo
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(source_config, "load_data")

        error = exc_info.value
        error_str = str(error)

        assert "load_data" in error_str  # Action name
        assert "cloudfiles" in error_str  # Source type
        assert "'mode' → 'readMode'" in error_str  # Suggestion
        assert "Fix:" in error_str  # Example section

    def test_mv_with_sql_path_passes(self):
        """Test materialized view with sql_path passes field validation."""
        write_target = {
            "type": "materialized_view",
            "catalog": "gold_cat",
            "schema": "gold_sch",
            "table": "ecomm_summary",
            "sql_path": "sql/gold/ecomm_summary.sql",
        }

        self.validator.validate_write_target(write_target, "test_action")

    def test_custom_datasource_valid(self):
        """Test valid custom_datasource source passes validation."""
        source_config = {
            "type": "custom_datasource",
            "module_path": "sources/custom.py",
            "custom_datasource_class": "MyDataSource",
            "options": {"key": "value"},
        }

        self.validator.validate_load_source(source_config, "test_action")

    def test_custom_datasource_typo_detected(self):
        """Test that typo in custom_datasource field is detected."""
        source_config = {
            "type": "custom_datasource",
            "module_path": "sources/custom.py",
            "custom_datasource_class": "MyDataSource",
            "optionss": {"key": "value"},  # Typo: 'optionss'
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(source_config, "test_action")

        assert "optionss" in str(exc_info.value)

    def test_delta_path_rejected(self):
        """Test that 'path' field in delta source is rejected."""
        source_config = {
            "type": "delta",
            "path": "/mnt/data/table",  # Not a valid delta field
            "table": "my_table",
        }

        with pytest.raises(LHPError) as exc_info:
            self.validator.validate_load_source(source_config, "test_action")

        assert "path" in str(exc_info.value)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
