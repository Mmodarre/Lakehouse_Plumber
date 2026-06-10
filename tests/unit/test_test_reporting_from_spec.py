import textwrap
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import yaml
from pydantic import ValidationError

from lhp.core.codegen.tst_reporting_hook_generator import (
    HOOK_FILENAME,
    TestReportingHookGenerator,
)
from lhp.errors import LHPError
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    ProjectConfig,
    TestReportingConfig,
)


@pytest.mark.unit
class TestSpecDataModel:
    __test__ = True

    def test_tc01_missing_module_path_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            TestReportingConfig(function_name="publish_results")
        assert "module_path" in str(exc_info.value)

    def test_tc01_missing_function_name_raises_validation_error(self):
        with pytest.raises(ValidationError) as exc_info:
            TestReportingConfig(module_path="some/path.py")
        assert "function_name" in str(exc_info.value)

    def test_tc01_both_required_fields_present_succeeds(self):
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        assert config.module_path == "py_functions/publisher.py"
        assert config.function_name == "publish_results"

    def test_tc02_config_file_defaults_to_none(self):
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        assert config.config_file is None

    def test_tc02_config_file_accepted_when_set(self):
        config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
            config_file="config/reporting.yaml",
        )
        assert config.config_file == "config/reporting.yaml"

    def test_tc03_action_test_id_defaults_to_none(self):
        action = Action(
            name="test_action",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="some_source",
            columns=["col1"],
        )
        assert action.test_id is None

    def test_tc03_action_test_id_accepted_when_set(self):
        action = Action(
            name="test_action",
            type=ActionType.TEST,
            test_type="uniqueness",
            source="some_source",
            columns=["col1"],
            test_id="SIT-G01",
        )
        assert action.test_id == "SIT-G01"

    def test_tc04_resolved_test_target_with_explicit_target(self):
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
        action = Action(
            name="tst_completeness",
            type=ActionType.TEST,
            test_type="completeness",
            source="some_source",
            required_columns=["col1"],
            test_id="SIT-G02",
        )
        assert action.resolved_test_target == "tmp_test_tst_completeness"


@pytest.mark.unit
class TestSpecConfigParsing:
    __test__ = True

    def test_tc05_non_dict_test_reporting_raises_error(self):
        with pytest.raises((ValidationError, TypeError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting="not_a_dict",
            )

    def test_tc05_list_test_reporting_raises_error(self):
        with pytest.raises((ValidationError, TypeError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting=["module_path", "function_name"],
            )

    def test_tc06_missing_required_fields_in_dict(self):
        with pytest.raises((ValidationError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting={"module_path": "some/path.py"},
                # missing function_name
            )

    def test_tc06_empty_dict_raises_error(self):
        with pytest.raises((ValidationError, Exception)):
            ProjectConfig(
                name="test_project",
                version="1.0",
                test_reporting={},
            )


def _make_test_action(
    name: str,
    test_id: str | None = None,
    target: str | None = None,
    test_type: str = "uniqueness",
    source: str = "v_source",
) -> Action:
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
    actions: list | None = None,
) -> FlowGroup:
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=flowgroup,
        actions=actions or [],
    )


def _make_project_config(
    test_reporting: TestReportingConfig = None,
) -> ProjectConfig:
    return ProjectConfig(
        name="test_project",
        version="1.0",
        test_reporting=test_reporting,
    )


def _make_provider_file(tmp_path: Path, filename: str = "publisher.py") -> Path:
    provider = tmp_path / "py_functions" / filename
    provider.parent.mkdir(parents=True, exist_ok=True)
    provider.write_text(
        textwrap.dedent("""\
        def publish_results(results, config, context, spark):
            return {"published": len(results), "failed": 0}
        """)
    )
    return provider


@pytest.mark.unit
class TestSpecHookGeneration:
    __test__ = True

    def test_tc08_generate_returns_none_without_config(self, tmp_path):
        project_config = _make_project_config(test_reporting=None)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [_make_test_action("tst_1", test_id="SIT-01")]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )
        assert result is None

    def test_tc09_generate_returns_none_no_test_ids(self, tmp_path):
        """Returns None when no actions have test_id (empty map guard)."""
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [
            _make_test_action("tst_1"),
            _make_test_action("tst_2"),
        ]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )
        assert result is None

    def test_tc10_mixed_test_ids_only_opted_in_included(self, tmp_path):
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

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        assert result is not None
        # repr() quote style (single vs double) is left to ruff format and NOT asserted here.
        assert "tst_with_id" in result and "SIT-01" in result
        assert "tmp_test_tst_also_with_id" in result and "SIT-02" in result
        assert (
            "tst_without_id" not in result
            or "SIT" not in result.split("tst_without_id")[0].split("\n")[-1]
        )

    def test_tc11_duplicate_test_target_raises_error(self, tmp_path):
        _make_provider_file(tmp_path)
        tr_config = TestReportingConfig(
            module_path="py_functions/publisher.py",
            function_name="publish_results",
        )
        project_config = _make_project_config(test_reporting=tr_config)
        generator = TestReportingHookGenerator(project_config, tmp_path)

        actions = [
            _make_test_action("tst_dup", test_id="SIT-01", target="same_target"),
            _make_test_action("tst_dup2", test_id="SIT-02", target="same_target"),
        ]
        flowgroups = [_make_flowgroup(actions=actions)]

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(LHPError):
            generator.generate(
                processed_flowgroups=flowgroups,
                pipeline_name="test_pipeline",
                output_dir=output_dir,
            )

    def test_tc12_config_file_rendered_as_python_dict_literal(self, tmp_path):
        """config_file YAML content embedded via repr() — True not true."""
        _make_provider_file(tmp_path)

        config_yaml = tmp_path / "config" / "reporting.yaml"
        config_yaml.parent.mkdir(parents=True, exist_ok=True)
        config_yaml.write_text(
            textwrap.dedent("""\
            api_url: https://dev.azure.com
            verify_ssl: true
            timeout: 30
            """)
        )

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

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        assert result is not None
        assert "True" in result
        assert "'verify_ssl': True" in result or '"verify_ssl": True' in result
        assert (
            "true" not in result.split("_PROVIDER_CONFIG")[1].split("\n")[0]
            or "True" in result
        )

    def test_tc13_empty_provider_config_when_no_config_file(self, tmp_path):
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

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        assert result is not None
        assert "_PROVIDER_CONFIG = {}" in result

    def test_tc14_missing_module_path_raises_error(self, tmp_path):
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

        with pytest.raises((LHPError, FileNotFoundError, Exception)):
            generator.generate(
                processed_flowgroups=flowgroups,
                pipeline_name="test_pipeline",
                output_dir=output_dir,
            )

    def test_tc15_provider_copied_with_init_and_header(self, tmp_path):
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

        generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        providers_dir = output_dir / "test_reporting_providers"
        assert providers_dir.exists(), (
            "test_reporting_providers/ directory should exist"
        )
        assert (providers_dir / "__init__.py").exists(), "__init__.py should exist"

        copied_provider = providers_dir / "publisher.py"
        assert copied_provider.exists(), "Provider module should be copied"

        content = copied_provider.read_text()
        assert "LHP-SOURCE" in content, "Copied provider should have LHP-SOURCE header"

    def test_tc16_import_matches_provider_stem_and_function(self, tmp_path):
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

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        assert result is not None
        assert (
            "from test_reporting_providers.my_publisher import send_results" in result
        )

    def test_tc24_yaml_literals_rendered_as_python(self, tmp_path):
        """YAML true → Python True, YAML null → Python None in config."""
        _make_provider_file(tmp_path)

        config_yaml = tmp_path / "config" / "reporting.yaml"
        config_yaml.parent.mkdir(parents=True, exist_ok=True)
        config_yaml.write_text(
            textwrap.dedent("""\
            enabled: true
            disabled: false
            empty_value: null
            name: "test"
            """)
        )

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

        result = generator.generate(
            processed_flowgroups=flowgroups,
            pipeline_name="test_pipeline",
            output_dir=output_dir,
        )

        assert result is not None
        config_section = result[result.index("_PROVIDER_CONFIG") :]
        assert "True" in config_section
        assert "False" in config_section
        assert "None" in config_section
