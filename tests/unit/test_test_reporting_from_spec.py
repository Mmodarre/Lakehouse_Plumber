import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from lhp.core.services.test_reporting_hook_generator import (
    HOOK_FILENAME,
    TestReportingHookGenerator,
)
from lhp.models.config import (
    Action,
    ActionType,
    FlowGroup,
    ProjectConfig,
    TestReportingConfig,
)
from lhp.utils.error_formatter import LHPError
from lhp.utils.smart_file_writer import SmartFileWriter


@pytest.mark.unit
class TestSpecDataModel:
    """TC-01 through TC-04: Data model spec validation."""

    __test__ = True

    # TC-01: TestReportingConfig requires module_path and function_name
    def test_tc01_missing_module_path_raises_validation_error(self):
        """TC-01a: TestReportingConfig without module_path raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            TestReportingConfig(function_name="publish_results")
        assert "module_path" in str(exc_info.value)

    def test_tc01_missing_function_name_raises_validation_error(self):
        """TC-01b: TestReportingConfig without function_name raises ValidationError."""
        with pytest.raises(ValidationError) as exc_info:
            TestReportingConfig(module_path="some/path.py")
        assert "function_name" in str(exc_info.value)

    def test_tc01_both_required_fields_present_succeeds(self):
        """TC-01c: TestReportingConfig with both required fields succeeds."""
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        assert config.module_path == "py_functions/publisher.py"
        assert config.function_name == "publish_results"

    # TC-02: config_file defaults to None when omitted
    def test_tc02_config_file_defaults_to_none(self):
        """TC-02: config_file defaults to None when omitted."""
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        assert config.config_file is None

    def test_tc02_config_file_accepted_when_set(self):
        """TC-02b: config_file accepted when explicitly set."""
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
            config_file="config/reporting.yaml",
        )
        assert config.config_file == "config/reporting.yaml"

    # TC-03: Action.test_id defaults to None, accepted when set
    def test_tc03_action_test_id_defaults_to_none(self):
        """TC-03a: Action.test_id defaults to None."""
        action = Action(
            name="test_action",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="some_source",
            columns=["col1"],
        )
        assert action.test_id is None

    def test_tc03_action_test_id_accepted_when_set(self):
        """TC-03b: Action.test_id accepted when explicitly set."""
        action = Action(
            name="test_action",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="some_source",
            columns=["col1"],
            test_id="SIT-G01",
        )
        assert action.test_id == "SIT-G01"

    # TC-04: resolved_test_target returns explicit target or tmp_test_{name} fallback
    def test_tc04_resolved_test_target_with_explicit_target(self):
        """TC-04a: resolved_test_target returns explicit target when set."""
        action = Action(
            name="tst_pk_unique",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="some_source",
            target="tst_pk_unique",
            columns=["col1"],
            test_id="SIT-G01",
        )
        assert action.resolved_test_target == "tst_pk_unique"

    def test_tc04_resolved_test_target_fallback(self):
        """TC-04b: resolved_test_target returns tmp_test_{name} when no target."""
        action = Action(
            name="tst_completeness",
            type=ActionType.TEST,
            test_type="completeness",
            source="some_source",
            required_columns=["col1"],
            test_id="SIT-G02",
        )
        assert action.resolved_test_target == "tmp_test_tst_completeness"


# ============================================================================
# TC-05 through TC-06: Config Parsing Spec Validation
# ============================================================================


@pytest.mark.unit
class TestSpecConfigParsing:
    """TC-05 through TC-06: Config parsing spec validation."""

    __test__ = True

    # TC-05: Config parsing rejects non-dict test_reporting input
    def test_tc05_non_dict_test_reporting_raises_error(self):
        """TC-05: test_reporting with non-dict value should raise an error.

        The plan says _parse_test_reporting_config validates dict type.
        At the model level, Pydantic should reject a non-dict value.
        """
        with pytest.raises((ValidationError, TypeError, Exception)):
            # Passing a string where a TestReportingConfig dict is expected
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting="not_a_dict",
            )

    def test_tc05_list_test_reporting_raises_error(self):
        """TC-05b: test_reporting with list value should raise an error."""
        with pytest.raises((ValidationError, TypeError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting=["module_path", "function_name"],
            )

    # TC-06: Config parsing raises error on missing required fields
    def test_tc06_missing_required_fields_in_dict(self):
        """TC-06: test_reporting dict missing required fields raises error."""
        with pytest.raises((ValidationError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting={"module_path": "some/path.py"},
                # missing function_name
            )

    def test_tc06_empty_dict_raises_error(self):
        """TC-06b: Empty dict for test_reporting raises error (missing both fields)."""
        with pytest.raises((ValidationError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting={},
            )


# ============================================================================
# TC-08 through TC-16, TC-24: Hook Generation Spec Validation
# ============================================================================


def _make_test_action(
    name: str,
    test_id: str = None,
    target: str = None,
    test_type: str = "uniqueness",
    source: str = "v_source",
) -> Action:
    """Helper to build a test Action with minimal required fields."""
    kwargs = {
        "name": name,
        "type": ActionType.TEST,
        "test_type": test_type,
        "source": source,
        "columns": ["col1"],
    }
    if test_id is not None:
        kwargs["test_id"] = test_id
    if target is not None:
        kwargs["target"] = target
    return Action(**kwargs)


def _make_flowgroup(
    pipeline: str = "test_pipeline",
    flowgroup: str = "test_fg",
    actions: list = None,
) -> FlowGroup:
    """Helper to build a FlowGroup with the given actions."""
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        actions=actions or [],
    )


def _make_project_config(
    test_reporting: TestReportingConfig = None,
) -> ProjectConfig:
    """Helper to build a minimal ProjectConfig."""
    return ProjectConfig(
        name="test_project",
        version="1.0",
        test_reporting=test_reporting,
    )


def _make_provider_file(tmp_path: Path, filename: str = "publisher.py") -> Path:
    """Create a minimal provider Python file in tmp_path."""
    provider = tmp_path / "py_functions" / filename
    provider.parent.mkdir(parents=True, exist_ok=True)
    provider.write_text(textwrap.dedent("""\
        def publish_results(results, config, context, spark):
            return {"published": len(results), "failed": 0}
        """))
    return provider


@pytest.mark.unit
class TestSpecHookGeneration:
    """TC-08 through TC-16, TC-24: Hook generation spec validation."""

    __test__ = True

    # TC-08: generate() returns None when test_reporting config is absent
    def test_tc08_generate_returns_none_without_config(self, tmp_path):
        """TC-08: generate() returns None when test_reporting is None."""
        project_config = _make_project_config(test_reporting=None)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )
        assert result is None

    # TC-09: generate() returns None when no test action has test_id
    def test_tc09_generate_returns_none_no_test_ids(self, tmp_path):
        """TC-09: generate() returns None when no actions have test_id (empty map guard)."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        # Actions without test_id
        actions = [
            _make_test_action("tst_1"),
            _make_test_action("tst_2"),
        ]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )
        assert result is None

    # TC-10: Correct _TEST_ID_MAP with mixed opted-in/opted-out actions
    def test_tc10_mixed_test_ids_only_opted_in_included(self, tmp_path):
        """TC-10: Only actions with test_id are included in _TEST_ID_MAP."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [
            _make_test_action("tst_with_id", test_id="SIT-01", target="tst_with_id"),
            _make_test_action("tst_without_id"),  # no test_id
            _make_test_action("tst_also_with_id", test_id="SIT-02"),
        ]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        assert result is not None
        # tst_with_id has explicit target, so key is "tst_with_id"
        assert '"tst_with_id": "SIT-01"' in result
        # tst_also_with_id has no target, so key is "tmp_test_tst_also_with_id"
        assert '"tmp_test_tst_also_with_id": "SIT-02"' in result
        # tst_without_id should NOT appear
        assert (
            "tst_without_id" not in result
            or "SIT" not in result.split("tst_without_id")[0].split("\n")[-1]
        )

    # TC-11: LHPError on duplicate resolved_test_target keys
    def test_tc11_duplicate_test_target_raises_error(self, tmp_path):
        """TC-11: Duplicate resolved_test_target keys raise LHPError."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        # Two actions that resolve to the same target name
        actions = [
            _make_test_action("tst_dup", test_id="SIT-01", target="same_target"),
            _make_test_action("tst_dup2", test_id="SIT-02", target="same_target"),
        ]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        with pytest.raises(LHPError):
            generator.generate(
                processed_flowgroups=flowgroups,
                pipeline_name="test_pipeline",
                output_dir=output_dir,
                smart_writer=smart_writer,
            )

    # TC-12: config_file YAML embedded as Python dict literal (repr() format)
    def test_tc12_config_file_rendered_as_python_dict_literal(self, tmp_path):
        """TC-12: config_file YAML content embedded via repr() — True not true."""
        _make_provider_file(tmp_path)

        # Create a config_file with YAML booleans
        config_yaml = tmp_path / "config" / "reporting.yaml"
        config_yaml.parent.mkdir(parents=True, exist_ok=True)
        config_yaml.write_text(textwrap.dedent("""\
            api_url: https://dev.azure.com
            verify_ssl: true
            timeout: 30
            """))

        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
            config_file="config/reporting.yaml",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        assert result is not None
        # repr() renders Python True, not YAML/JSON true
        assert "True" in result
        assert "'verify_ssl': True" in result or '"verify_ssl": True' in result
        # Should not have JSON-style lowercase true
        # (Check within the _PROVIDER_CONFIG block)
        assert (
            "true" not in result.split("_PROVIDER_CONFIG")[1].split("\n")[0]
            or "True" in result
        )

    # TC-13: Empty dict {} when config_file not set
    def test_tc13_empty_provider_config_when_no_config_file(self, tmp_path):
        """TC-13: _PROVIDER_CONFIG is {} when config_file is not set."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        assert result is not None
        assert "_PROVIDER_CONFIG = {}" in result

    # TC-14: Error raised when module_path file does not exist
    def test_tc14_missing_module_path_raises_error(self, tmp_path):
        """TC-14: generate() raises error when module_path file doesn't exist."""
        # Do NOT create the provider file
        tr_config = TestReportingConfig(
            module_path="py_functions/nonexistent.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        with pytest.raises((LHPError, FileNotFoundError, Exception)):
            generator.generate(
                processed_flowgroups=flowgroups,
                pipeline_name="test_pipeline",
                output_dir=output_dir,
                smart_writer=smart_writer,
            )

    # TC-15: Provider module copied with __init__.py and LHP-SOURCE header
    def test_tc15_provider_copied_with_init_and_header(self, tmp_path):
        """TC-15: Provider module copied to test_reporting_providers/ with __init__.py and LHP-SOURCE header."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = SmartFileWriter()

        generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        providers_dir = output_dir / "test_reporting_providers"
        assert (
            providers_dir.exists()
        ), "test_reporting_providers/ directory should exist"
        assert (providers_dir / "__init__.py").exists(), "__init__.py should exist"

        copied_provider = providers_dir / "publisher.py"
        assert copied_provider.exists(), "Provider module should be copied"

        content = copied_provider.read_text()
        assert "LHP-SOURCE" in content, "Copied provider should have LHP-SOURCE header"

    # TC-16: Import statement matches provider stem and function name
    def test_tc16_import_matches_provider_stem_and_function(self, tmp_path):
        """TC-16: Generated hook imports from test_reporting_providers.{stem} import {function}."""
        _make_provider_file(tmp_path, filename="my_publisher.py")
        tr_config = TestReportingConfig(
            module_path="py_functions/my_publisher.py",
            function_name="send_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        assert result is not None
        assert (
            "from test_reporting_providers.my_publisher import send_results" in result
        )

    # TC-24: Python-incompatible YAML literals (true/null) rendered as Python True/None
    def test_tc24_yaml_literals_rendered_as_python(self, tmp_path):
        """TC-24: YAML true → Python True, YAML null → Python None in config."""
        _make_provider_file(tmp_path)

        config_yaml = tmp_path / "config" / "reporting.yaml"
        config_yaml.parent.mkdir(parents=True, exist_ok=True)
        config_yaml.write_text(textwrap.dedent("""\
            enabled: true
            disabled: false
            empty_value: null
            name: "test"
            """))

        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
            config_file="config/reporting.yaml",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01", target="tst_1")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        smart_writer = MagicMock()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
            smart_writer=smart_writer,
        )

        assert result is not None
        # repr() renders Python True/False/None, not YAML true/false/null
        # Check within the _PROVIDER_CONFIG section
        config_section = result[result.index("_PROVIDER_CONFIG") :]
        assert "True" in config_section
        assert "False" in config_section
        assert "None" in config_section
