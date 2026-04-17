"""Extended tests for ProjectConfigLoader covering edge cases and error paths."""

import pytest
from pathlib import Path

from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.utils.error_formatter import LHPError


class TestLoadProjectConfig:
    """Tests for load_project_config method."""

    def test_empty_yaml_raises_error(self, tmp_path):
        """Empty YAML file raises LHPError (wrapped as CFG-002).

        An empty file yields 0 documents from yaml.safe_load_all, triggering
        MultiDocumentError (LHP-IO-003) inside yaml_loader, which is then caught
        by load_project_config's except Exception handler and wrapped as CFG-002.
        """
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("")
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        # Original error details are preserved in the message
        assert "IO-003" in str(exc_info.value)

    def test_missing_config_file_returns_none(self, tmp_path):
        """Non-existent lhp.yaml returns None."""
        loader = ProjectConfigLoader(tmp_path)
        result = loader.load_project_config()
        assert result is None

    def test_invalid_yaml_syntax(self, tmp_path):
        """Invalid YAML syntax is handled gracefully.

        The yaml_loader converts YAML errors to LHPConfigError (a ValueError subclass).
        load_project_config catches ValueError, but the 'Invalid YAML' substring check
        does not match the yaml_loader's error format, so the function returns None.
        """
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\n  invalid: indentation error")
        loader = ProjectConfigLoader(tmp_path)
        result = loader.load_project_config()
        assert result is None

    def test_null_document_yaml_returns_none(self, tmp_path):
        """YAML file with only '---' (null document) returns None.

        With allow_empty=False, a null document returns None from yaml_loader,
        and load_project_config treats falsy config_data as empty.
        """
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("---\n")
        loader = ProjectConfigLoader(tmp_path)
        result = loader.load_project_config()
        assert result is None

    def test_minimal_valid_config(self, tmp_path):
        """Minimal valid config with just a name loads successfully."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: my_project\n")
        loader = ProjectConfigLoader(tmp_path)
        result = loader.load_project_config()
        assert result is not None
        assert result.name == "my_project"
        assert result.version == "1.0"

    def test_general_exception_raises_cfg_002(self, tmp_path):
        """General non-ValueError exception raises LHPError with code CFG-002."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\n")
        loader = ProjectConfigLoader(tmp_path)

        # Force a non-ValueError exception in _parse_project_config
        def bad_parse(config_data):
            raise RuntimeError("unexpected failure")

        loader._parse_project_config = bad_parse

        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code


class TestParseIncludePatterns:
    """Tests for _parse_include_patterns method.

    Note: LHPError raised inside _parse_include_patterns propagates through
    _parse_project_config and is caught by load_project_config's except Exception
    handler, which wraps it as CFG-002. The original error code appears in the
    wrapped error's message string.
    """

    def test_non_list_include_raises_error(self, tmp_path):
        """Non-list include field raises LHPError (originally CFG-003, wrapped as CFG-002)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\ninclude: not_a_list\n")
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-003" in str(exc_info.value)

    def test_non_string_element_raises_error(self, tmp_path):
        """Non-string element in include list raises LHPError (originally CFG-004)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\ninclude:\n  - 123\n")
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-004" in str(exc_info.value)

    def test_valid_include_patterns(self, tmp_path):
        """Valid glob patterns in include list are accepted."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            'name: test\ninclude:\n  - "*.yaml"\n  - "bronze_*.yaml"\n'
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.include is not None
        assert len(config.include) == 2


class TestParseEventLogConfig:
    """Tests for _parse_event_log_config method."""

    def test_non_dict_event_log_raises_error(self, tmp_path):
        """Non-dict event_log value raises LHPError (originally CFG-006, wrapped as CFG-002)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\nevent_log: not_a_dict\n")
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-006" in str(exc_info.value)

    def test_valid_event_log_config(self, tmp_path):
        """Valid event_log config is parsed correctly."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\nevent_log:\n  catalog: my_catalog\n  schema: _meta\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.event_log is not None
        assert config.event_log.catalog == "my_catalog"

    def test_event_log_enabled_without_required_fields_raises_error(self, tmp_path):
        """Enabled event_log without catalog/schema raises LHPError (originally CFG-007)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\nevent_log:\n  enabled: true\n")
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-007" in str(exc_info.value)


class TestValidatePresetReferences:
    """Tests for _validate_preset_references method."""

    def test_preset_references_undefined_column_raises_error(self, tmp_path):
        """Preset referencing an undefined column raises LHPError (originally CFG-005)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    ingestion_time:\n"
            '      expression: "current_timestamp()"\n'
            "  presets:\n"
            "    basic:\n"
            "      columns:\n"
            "        - nonexistent_column\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-005" in str(exc_info.value)

    def test_preset_references_valid_column(self, tmp_path):
        """Preset referencing a defined column passes validation."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    ingestion_time:\n"
            '      expression: "current_timestamp()"\n'
            "  presets:\n"
            "    basic:\n"
            "      columns:\n"
            "        - ingestion_time\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.operational_metadata is not None
        assert "basic" in config.operational_metadata.presets


class TestParseOperationalMetadata:
    """Tests for _parse_operational_metadata_config method."""

    def test_string_col_config_converted_to_dict(self, tmp_path):
        """String column config is converted to dict with 'expression' key."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            '    ingestion_time: "current_timestamp()"\n'
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.operational_metadata is not None
        assert "ingestion_time" in config.operational_metadata.columns
        col = config.operational_metadata.columns["ingestion_time"]
        assert col.expression == "current_timestamp()"

    def test_non_dict_non_string_col_config_raises_error(self, tmp_path):
        """Non-dict non-string column config raises LHPError (originally CFG-003)."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    bad_column: 42\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError) as exc_info:
            loader.load_project_config()
        assert "LHP-CFG-002" in exc_info.value.code
        assert "CFG-003" in str(exc_info.value)

    def test_dict_col_config_with_all_fields(self, tmp_path):
        """Full dict column config with all optional fields is parsed correctly."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    ingestion_time:\n"
            '      expression: "current_timestamp()"\n'
            '      description: "When the data was ingested"\n'
            "      applies_to:\n"
            "        - streaming_table\n"
            "      enabled: true\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        col = config.operational_metadata.columns["ingestion_time"]
        assert col.expression == "current_timestamp()"
        assert col.description == "When the data was ingested"
        assert col.applies_to == ["streaming_table"]
        assert col.enabled is True

    def test_preset_as_list_shorthand(self, tmp_path):
        """Preset defined as a list (shorthand) is converted to dict with columns key."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    col_a:\n"
            '      expression: "expr_a"\n'
            "    col_b:\n"
            '      expression: "expr_b"\n'
            "  presets:\n"
            "    basic:\n"
            "      - col_a\n"
            "      - col_b\n"
        )
        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert "basic" in config.operational_metadata.presets
        preset = config.operational_metadata.presets["basic"]
        assert preset.columns == ["col_a", "col_b"]


class TestGetOperationalMetadataConfig:
    """Tests for get_operational_metadata_config method."""

    def test_config_without_operational_metadata_returns_none(self, tmp_path):
        """Config without operational_metadata section returns None."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text("name: test\n")
        loader = ProjectConfigLoader(tmp_path)
        result = loader.get_operational_metadata_config()
        assert result is None

    def test_config_with_operational_metadata_returns_config(self, tmp_path):
        """Config with operational_metadata returns the parsed config."""
        config_file = tmp_path / "lhp.yaml"
        config_file.write_text(
            "name: test\n"
            "operational_metadata:\n"
            "  columns:\n"
            "    ingestion_time:\n"
            '      expression: "current_timestamp()"\n'
        )
        loader = ProjectConfigLoader(tmp_path)
        result = loader.get_operational_metadata_config()
        assert result is not None
        assert "ingestion_time" in result.columns

    def test_missing_config_file_returns_none(self, tmp_path):
        """No lhp.yaml means get_operational_metadata_config returns None."""
        loader = ProjectConfigLoader(tmp_path)
        result = loader.get_operational_metadata_config()
        assert result is None
