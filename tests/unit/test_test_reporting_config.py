"""Unit tests for test reporting configuration models and config loader."""

import pytest
from pydantic import ValidationError

from lhp.models.config import (
    Action,
    ActionType,
    ProjectConfig,
    TestReportingConfig,
)


@pytest.mark.unit
class TestTestReportingConfig:
    """Tests for TestReportingConfig model."""

    __test__ = True  # Explicitly mark as a test class (overrides __test__ = False on some models)

    def test_required_fields(self):
        config = TestReportingConfig(
            module_path="src/publisher.py",
            function_name="publish_results",
        )
        assert config.module_path == "src/publisher.py"
        assert config.function_name == "publish_results"
        assert config.config_file is None

    def test_with_config_file(self):
        config = TestReportingConfig(
            module_path="src/publisher.py",
            function_name="publish_results",
            config_file="config/ado_config.yaml",
        )
        assert config.config_file == "config/ado_config.yaml"

    def test_missing_module_path(self):
        with pytest.raises(ValidationError):
            TestReportingConfig(function_name="publish_results")

    def test_missing_function_name(self):
        with pytest.raises(ValidationError):
            TestReportingConfig(module_path="src/publisher.py")

    def test_missing_all_required(self):
        with pytest.raises(ValidationError):
            TestReportingConfig()


@pytest.mark.unit
class TestActionTestId:
    """Tests for test_id field on Action model."""

    def test_test_id_defaults_to_none(self):
        action = Action(name="test_1", type=ActionType.TEST)
        assert action.test_id is None

    def test_test_id_set(self):
        action = Action(
            name="test_1",
            type=ActionType.TEST,
            test_id="SIT-G01",
        )
        assert action.test_id == "SIT-G01"

    def test_test_id_on_non_test_action(self):
        """test_id is accepted on any action type (model doesn't restrict)."""
        action = Action(
            name="load_1",
            type=ActionType.LOAD,
            test_id="X-99",
        )
        assert action.test_id == "X-99"


@pytest.mark.unit
class TestProjectConfigTestReporting:
    """Tests for test_reporting field on ProjectConfig."""

    def test_defaults_to_none(self):
        config = ProjectConfig(name="test_project")
        assert config.test_reporting is None

    def test_with_test_reporting(self):
        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="publish",
        )
        config = ProjectConfig(name="test_project", test_reporting=tr)
        assert config.test_reporting is not None
        assert config.test_reporting.module_path == "src/pub.py"


@pytest.mark.unit
class TestProjectConfigLoaderTestReporting:
    """Tests for test_reporting parsing in ProjectConfigLoader."""

    def test_parses_test_reporting_section(self, tmp_path):
        """Config loader parses test_reporting from lhp.yaml."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\n"
            "test_reporting:\n"
            "  module_path: src/publisher.py\n"
            "  function_name: publish_results\n"
            "  config_file: config/ado_config.yaml\n"
        )
        from lhp.core.project_config_loader import ProjectConfigLoader

        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.test_reporting is not None
        assert config.test_reporting.module_path == "src/publisher.py"
        assert config.test_reporting.function_name == "publish_results"
        assert config.test_reporting.config_file == "config/ado_config.yaml"

    def test_parses_without_test_reporting(self, tmp_path):
        """Config loader returns None for test_reporting when absent."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: test_project\n")
        from lhp.core.project_config_loader import ProjectConfigLoader

        loader = ProjectConfigLoader(tmp_path)
        config = loader.load_project_config()
        assert config is not None
        assert config.test_reporting is None

    def test_rejects_missing_required_fields(self, tmp_path):
        """Config loader raises error for missing required fields."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text(
            "name: test_project\n"
            "test_reporting:\n"
            "  config_file: config/ado_config.yaml\n"
        )
        from lhp.core.project_config_loader import ProjectConfigLoader
        from lhp.utils.error_formatter import LHPError

        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError, match="missing required fields"):
            loader.load_project_config()

    def test_rejects_non_dict_test_reporting(self, tmp_path):
        """Config loader raises error for non-dict test_reporting."""
        lhp_yaml = tmp_path / "lhp.yaml"
        lhp_yaml.write_text("name: test_project\n" "test_reporting: just_a_string\n")
        from lhp.core.project_config_loader import ProjectConfigLoader
        from lhp.utils.error_formatter import LHPError

        loader = ProjectConfigLoader(tmp_path)
        with pytest.raises(LHPError, match="must be a mapping"):
            loader.load_project_config()
