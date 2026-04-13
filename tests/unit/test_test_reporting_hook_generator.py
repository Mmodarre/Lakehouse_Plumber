"""Unit tests for TestReportingHookGenerator."""

import pytest

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
from lhp.utils.smart_file_writer import SmartFileWriter


def _make_project_config(
    test_reporting: TestReportingConfig = None,
) -> ProjectConfig:
    return ProjectConfig(name="test_project", test_reporting=test_reporting)


def _make_flowgroup(actions=None, pipeline="test_pipeline", name="fg1"):
    return FlowGroup(
        pipeline=pipeline,
        flowgroup=name,
        actions=actions or [],
    )


def _make_test_action(name, test_id=None, target=None):
    return Action(
        name=name,
        type=ActionType.TEST,
        test_type="custom_sql",
        test_id=test_id,
        target=target,
    )


@pytest.mark.unit
class TestTestReportingHookGenerator:
    """Tests for TestReportingHookGenerator."""

    __test__ = True

    def test_returns_none_without_config(self, tmp_path):
        """No test_reporting config → returns None."""
        config = _make_project_config(test_reporting=None)
        gen = TestReportingHookGenerator(config, tmp_path)
        writer = SmartFileWriter()
        result = gen.generate(
            processed_flowgroups=[],
            pipeline_name="p1",
            output_dir=tmp_path / "output",
            smart_writer=writer,
        )
        assert result is None

    def test_returns_none_with_no_test_ids(self, tmp_path):
        """Config exists but no test actions have test_id → returns None."""
        # Create provider module
        provider = tmp_path / "src" / "publisher.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def publish(results, config, context, spark): pass\n")

        tr = TestReportingConfig(
            module_path="src/publisher.py",
            function_name="publish",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_one"),  # no test_id
                _make_test_action("tst_two"),  # no test_id
            ]
        )

        writer = SmartFileWriter()
        output_dir = tmp_path / "output"
        output_dir.mkdir()
        result = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )
        assert result is None

    def test_generates_hook_with_test_ids(self, tmp_path):
        """Config + test actions with test_id → generates hook."""
        provider = tmp_path / "src" / "publisher.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def publish(results, config, context, spark): pass\n")

        tr = TestReportingConfig(
            module_path="src/publisher.py",
            function_name="publish",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action(
                    "tst_pk_null", test_id="SIT-G01", target="tst_pk_null"
                ),
                _make_test_action("tst_completeness", test_id="SIT-G02"),
                _make_test_action("tst_no_id"),  # excluded: no test_id
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="my_pipeline",
            output_dir=output_dir,
            smart_writer=writer,
        )

        assert content is not None
        # Check test_id map contains the opted-in actions
        assert "tst_pk_null" in content
        assert "SIT-G01" in content
        assert "SIT-G02" in content
        # Default target: tmp_test_tst_completeness
        assert "tmp_test_tst_completeness" in content
        # Excluded action not in map
        assert "tst_no_id" not in content
        # Hook file written
        assert (output_dir / HOOK_FILENAME).exists()

    def test_test_id_map_uses_default_target(self, tmp_path):
        """When action.target is None, key is tmp_test_{action.name}."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def publish(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="publish")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("my_test", test_id="T-1"),  # target=None
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )
        assert "tmp_test_my_test" in content
        assert "T-1" in content

    def test_test_id_map_uses_explicit_target(self, tmp_path):
        """When action.target is set, that value is the key."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def publish(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="publish")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("my_test", test_id="T-1", target="custom_target"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )
        assert "custom_target" in content
        assert "tmp_test_my_test" not in content

    def test_copies_provider_module(self, tmp_path):
        """Provider module is copied to test_reporting_providers/ with __init__.py."""
        provider = tmp_path / "src" / "ado_publisher.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(
            module_path="src/ado_publisher.py", function_name="pub"
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )

        providers_dir = output_dir / "test_reporting_providers"
        assert providers_dir.exists()
        assert (providers_dir / "ado_publisher.py").exists()
        assert (providers_dir / "__init__.py").exists()
        # Check header
        copied_content = (providers_dir / "ado_publisher.py").read_text()
        assert "LHP-SOURCE" in copied_content

    def test_loads_provider_config_file(self, tmp_path):
        """Provider config from YAML is embedded in hook."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def publish(r, c, ctx, s): pass\n")

        config_file = tmp_path / "config" / "ado_config.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.write_text(
            "plan_id: 12345\n" "connection:\n" "  organization: myorg\n"
        )

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="publish",
            config_file="config/ado_config.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )

        assert "plan_id" in content
        assert "12345" in content
        assert "myorg" in content

    def test_raises_on_missing_module_path(self, tmp_path):
        """Missing provider module file raises error."""
        tr = TestReportingConfig(
            module_path="src/nonexistent.py", function_name="publish"
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()

        from lhp.utils.error_formatter import LHPError

        with pytest.raises(LHPError, match="not found"):
            gen.generate(
                processed_flowgroups=[fg],
                pipeline_name="p1",
                output_dir=output_dir,
                smart_writer=writer,
            )

    def test_hook_contains_pipeline_name(self, tmp_path):
        """Pipeline name appears in hook header and context."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="bronze_pipeline",
            output_dir=output_dir,
            smart_writer=writer,
        )
        assert "bronze_pipeline" in content

    def test_hook_import_uses_correct_stem(self, tmp_path):
        """Import statement uses the stem of module_path, not full path."""
        provider = tmp_path / "custom_functions" / "my_ado_publisher.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def go(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(
            module_path="custom_functions/my_ado_publisher.py",
            function_name="go",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )
        assert "from test_reporting_providers.my_ado_publisher import go" in content


@pytest.mark.unit
class TestTestReportingValidation:
    """Tests for TestReportingHookGenerator.validate()."""

    __test__ = True

    def test_validate_no_config(self, tmp_path):
        """No test_reporting config → no errors."""
        config = _make_project_config()
        gen = TestReportingHookGenerator(config, tmp_path)
        errors = gen.validate()
        assert errors == []

    def test_validate_missing_module_file(self, tmp_path):
        """Module file doesn't exist → error."""
        tr = TestReportingConfig(module_path="src/nonexistent.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)
        errors = gen.validate()
        assert len(errors) == 1
        assert "module_path" in errors[0]

    def test_validate_missing_config_file(self, tmp_path):
        """Config file doesn't exist → error."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("pass\n")

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            config_file="config/missing.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)
        errors = gen.validate()
        assert len(errors) == 1
        assert "config_file" in errors[0]

    def test_validate_all_files_exist(self, tmp_path):
        """All files exist → no errors."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("pass\n")
        cfg = tmp_path / "config" / "ado.yaml"
        cfg.parent.mkdir(parents=True)
        cfg.write_text("plan_id: 1\n")

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            config_file="config/ado.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)
        errors = gen.validate()
        assert errors == []

    def test_validate_include_tests_no_test_ids(self, tmp_path):
        """--include-tests with no test_ids → warning error."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1"),  # no test_id
            ]
        )

        errors = gen.validate(processed_flowgroups=[fg], include_tests=True)
        assert len(errors) == 1
        assert "no test actions have test_id" in errors[0]

    def test_validate_include_tests_with_test_ids(self, tmp_path):
        """--include-tests with test_ids → no errors."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_1", test_id="T-1"),
            ]
        )

        errors = gen.validate(processed_flowgroups=[fg], include_tests=True)
        assert errors == []
