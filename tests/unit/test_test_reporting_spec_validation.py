import pytest
from pydantic import ValidationError

from lhp.core.codegen.tst_reporting_hook_generator import (
    HOOK_FILENAME,
    TestReportingHookGenerator,
)
from lhp.core.loaders import ProjectConfigLoader
from lhp.core.validators import ConfigFieldValidator
from lhp.errors import LHPError
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    ProjectConfig,
    TestReportingConfig,
)


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
class TestTC01RequiredFields:
    pass


@pytest.mark.unit
class TestTC02ConfigFileDefault:
    pass


@pytest.mark.unit
class TestTC03ActionTestId:
    pass


@pytest.mark.unit
class TestTC04ResolvedTestTarget:
    def test_returns_explicit_target_when_set(self):
        action = Action(
            name="tst_pk",
            type=ActionType.TEST,
            test_type="uniqueness",
            target="my_explicit_target",
        )
        assert action.resolved_test_target == "my_explicit_target"

    def test_falls_back_to_tmp_test_name(self):
        action = Action(
            name="tst_pk",
            type=ActionType.TEST,
            test_type="uniqueness",
        )
        assert action.resolved_test_target == "tmp_test_tst_pk"

    def test_explicit_target_takes_precedence_over_default(self):
        action = Action(
            name="tst_pk",
            type=ActionType.TEST,
            test_type="uniqueness",
            target="custom",
        )
        assert action.resolved_test_target == "custom"
        assert "tmp_test_" not in action.resolved_test_target


@pytest.mark.unit
class TestTC05RejectNonDict:
    pass


@pytest.mark.unit
class TestTC06MissingRequiredFields:
    pass


@pytest.mark.unit
class TestTC07TestIdFieldValidator:
    def test_test_id_in_action_fields(self):
        validator = ConfigFieldValidator()
        assert "test_id" in validator.action_fields

    def test_validate_action_with_test_id_no_error(self):
        validator = ConfigFieldValidator()
        action_dict = {
            "name": "tst_pk",
            "type": "test",
            "test_type": "uniqueness",
            "test_id": "SIT-001",
            "columns": ["id"],
            "on_violation": "warn",
        }
        validator.validate_action_fields(action_dict, "tst_pk")


@pytest.mark.unit
class TestTC10MixedTestIdMap:
    def test_map_contains_only_opted_in_actions(self, tmp_path):
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_opted_in", test_id="T-1", target="target_a"),
                _make_test_action("tst_no_id"),  # no test_id
                _make_test_action("tst_also_in", test_id="T-2"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
        )

        assert content is not None
        assert "target_a" in content
        assert "T-1" in content
        assert "tmp_test_tst_also_in" in content
        assert "T-2" in content
        assert "tst_no_id" not in content


@pytest.mark.unit
class TestTC11DuplicateTestTarget:
    def test_raises_on_duplicate_target(self, tmp_path):
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_a", test_id="T-1", target="shared_target"),
                _make_test_action("tst_b", test_id="T-2", target="shared_target"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(LHPError, match="Duplicate"):
            gen.generate(
                processed_flowgroups=[fg],
                pipeline_name="p1",
                output_dir=output_dir,
            )

    def test_raises_on_duplicate_default_target(self, tmp_path):
        """Two actions with same name (both using default target) also collide."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg1 = _make_flowgroup(
            actions=[_make_test_action("tst_dup", test_id="T-1")],
            name="fg1",
        )
        fg2 = _make_flowgroup(
            actions=[_make_test_action("tst_dup", test_id="T-2")],
            name="fg2",
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()

        with pytest.raises(LHPError, match="Duplicate"):
            gen.generate(
                processed_flowgroups=[fg1, fg2],
                pipeline_name="p1",
                output_dir=output_dir,
            )


@pytest.mark.unit
class TestTC12ConfigFileEmbedding:
    def test_config_embedded_as_repr(self, tmp_path):
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        config_file = tmp_path / "config" / "reporting.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.write_text("plan_id: 42\norg: acme\n")

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            config_file="config/reporting.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(actions=[_make_test_action("tst_1", test_id="T-1")])

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
        )

        assert content is not None
        # repr() quote style (single vs double) is left to ruff format and NOT asserted here.
        assert "plan_id" in content
        assert "42" in content
        assert "acme" in content


@pytest.mark.unit
class TestTC13EmptyConfigDict:
    def test_empty_dict_in_hook(self, tmp_path):
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            # No config_file
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(actions=[_make_test_action("tst_1", test_id="T-1")])

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
        )

        assert content is not None
        assert "{}" in content


@pytest.mark.unit
class TestTC24PythonIncompatibleLiterals:
    def test_yaml_booleans_and_nulls_in_config(self, tmp_path):
        """YAML true → Python True, YAML null → Python None in config."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        config_file = tmp_path / "config" / "special.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.write_text(
            "enabled: true\ndisabled: false\nempty_value: null\ncount: 42\n"
        )

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            config_file="config/special.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(actions=[_make_test_action("tst_1", test_id="T-1")])

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
        )

        assert content is not None
        assert "True" in content
        assert "False" in content
        assert "None" in content
        assert "42" in content
        assert ": true" not in content
        assert ": false" not in content
        assert ": null" not in content
