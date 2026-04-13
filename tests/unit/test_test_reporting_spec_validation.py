"""Spec-derived validation tests for test result reporting feature.

Tests TC-01 through TC-16 and TC-24 from the test reporting specification.
Only covers cases NOT already exercised by test_test_reporting_config.py
and test_test_reporting_hook_generator.py.
"""

import pytest
from pydantic import ValidationError

from lhp.core.config_field_validator import ConfigFieldValidator
from lhp.core.project_config_loader import ProjectConfigLoader
from lhp.core.services.tst_reporting_hook_generator import (
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


# ============================================================================
# Helpers (same pattern as existing test_test_reporting_hook_generator.py)
# ============================================================================


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


# ============================================================================
# TC-01: TestReportingConfig requires module_path and function_name
# ALREADY COVERED by test_test_reporting_config.py:
#   test_missing_module_path, test_missing_function_name, test_missing_all_required
# Included here for completeness with parametrized form.
# ============================================================================


@pytest.mark.unit
class TestTC01RequiredFields:
    """TC-01: TestReportingConfig rejects construction without required fields."""

    # NOTE: Already covered by TestTestReportingConfig in test_test_reporting_config.py.
    # Skipping to avoid duplication.
    pass


# ============================================================================
# TC-02: config_file defaults to None
# ALREADY COVERED by test_test_reporting_config.py: test_required_fields (line 27)
# ============================================================================


@pytest.mark.unit
class TestTC02ConfigFileDefault:
    """TC-02: config_file defaults to None when omitted."""

    # NOTE: Already covered by TestTestReportingConfig.test_required_fields.
    # Skipping to avoid duplication.
    pass


# ============================================================================
# TC-03: Action.test_id defaults to None and is accepted when set
# ALREADY COVERED by test_test_reporting_config.py:
#   test_test_id_defaults_to_none, test_test_id_set
# ============================================================================


@pytest.mark.unit
class TestTC03ActionTestId:
    """TC-03: Action.test_id defaults to None and is accepted when set."""

    # NOTE: Already covered by TestActionTestId in test_test_reporting_config.py.
    # Skipping to avoid duplication.
    pass


# ============================================================================
# TC-04: Action.resolved_test_target returns explicit target when set,
#         falls back to tmp_test_{name}
# NOT covered by existing tests (property tested indirectly via hook content,
# but never tested in isolation on the model).
# ============================================================================


@pytest.mark.unit
class TestTC04ResolvedTestTarget:
    """TC-04: Action.resolved_test_target property."""

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
        # Explicit target must win, not tmp_test_tst_pk
        assert action.resolved_test_target == "custom"
        assert "tmp_test_" not in action.resolved_test_target


# ============================================================================
# TC-05: _parse_test_reporting_config rejects non-dict input
# ALREADY COVERED by test_test_reporting_config.py:
#   test_rejects_non_dict_test_reporting
# ============================================================================


@pytest.mark.unit
class TestTC05RejectNonDict:
    """TC-05: _parse_test_reporting_config rejects non-dict input."""

    # NOTE: Already covered by TestProjectConfigLoaderTestReporting.
    #   test_rejects_non_dict_test_reporting
    # Skipping to avoid duplication.
    pass


# ============================================================================
# TC-06: _parse_test_reporting_config raises LHPError on missing required fields
# ALREADY COVERED by test_test_reporting_config.py:
#   test_rejects_missing_required_fields
# ============================================================================


@pytest.mark.unit
class TestTC06MissingRequiredFields:
    """TC-06: _parse_test_reporting_config raises LHPConfigError on missing fields."""

    # NOTE: Already covered. Skipping to avoid duplication.
    pass


# ============================================================================
# TC-07: test_id is recognized by ConfigFieldValidator (no "Unknown field" error)
# NOT covered by existing tests.
# ============================================================================


@pytest.mark.unit
class TestTC07TestIdFieldValidator:
    """TC-07: test_id is a recognized action field in ConfigFieldValidator."""

    def test_test_id_in_action_fields(self):
        """test_id must be in ConfigFieldValidator.action_fields."""
        validator = ConfigFieldValidator()
        assert "test_id" in validator.action_fields

    def test_validate_action_with_test_id_no_error(self):
        """Validating an action dict containing test_id must not raise."""
        validator = ConfigFieldValidator()
        action_dict = {
            "name": "tst_pk",
            "type": "test",
            "test_type": "uniqueness",
            "test_id": "SIT-001",
            "columns": ["id"],
            "on_violation": "warn",
        }
        # Should not raise LHPError
        validator.validate_action_fields(action_dict, "tst_pk")


# ============================================================================
# TC-08: generate() returns None when test_reporting config is absent
# ALREADY COVERED by test_test_reporting_hook_generator.py:
#   test_returns_none_without_config
# ============================================================================

# TC-09: generate() returns None when no test action has test_id
# ALREADY COVERED by test_test_reporting_hook_generator.py:
#   test_returns_none_with_no_test_ids


# ============================================================================
# TC-10: generate() builds correct _TEST_ID_MAP with mixed opted-in/opted-out
# ALREADY partially covered by test_generates_hook_with_test_ids,
# but that test checks string membership, not the actual map structure.
# Adding a dedicated check for the map dictionary content.
# ============================================================================


@pytest.mark.unit
class TestTC10MixedTestIdMap:
    """TC-10: _TEST_ID_MAP contains only opted-in actions."""

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
        writer = SmartFileWriter()
        content = gen.generate(
            processed_flowgroups=[fg],
            pipeline_name="p1",
            output_dir=output_dir,
            smart_writer=writer,
        )

        assert content is not None
        # Opted-in entries present
        assert "target_a" in content
        assert "T-1" in content
        assert "tmp_test_tst_also_in" in content
        assert "T-2" in content
        # Opted-out entry absent
        assert "tst_no_id" not in content


# ============================================================================
# TC-11: generate() raises LHPError on duplicate resolved_test_target keys
# NOT covered by existing tests.
# ============================================================================


@pytest.mark.unit
class TestTC11DuplicateTestTarget:
    """TC-11: Duplicate resolved_test_target raises LHPError."""

    def test_raises_on_duplicate_target(self, tmp_path):
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        # Two actions with the same explicit target
        fg = _make_flowgroup(
            actions=[
                _make_test_action("tst_a", test_id="T-1", target="shared_target"),
                _make_test_action("tst_b", test_id="T-2", target="shared_target"),
            ]
        )

        output_dir = tmp_path / "output"
        output_dir.mkdir()
        writer = SmartFileWriter()

        with pytest.raises(LHPError, match="Duplicate"):
            gen.generate(
                processed_flowgroups=[fg],
                pipeline_name="p1",
                output_dir=output_dir,
                smart_writer=writer,
            )

    def test_raises_on_duplicate_default_target(self, tmp_path):
        """Two actions with same name (both using default target) also collide."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        tr = TestReportingConfig(module_path="src/pub.py", function_name="pub")
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        # Two actions with same name across different flowgroups
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
        writer = SmartFileWriter()

        with pytest.raises(LHPError, match="Duplicate"):
            gen.generate(
                processed_flowgroups=[fg1, fg2],
                pipeline_name="p1",
                output_dir=output_dir,
                smart_writer=writer,
            )


# ============================================================================
# TC-12: generate() loads config_file YAML and embeds it as repr() dict literal
# ALREADY partially covered by test_loads_provider_config_file, but does not
# verify the repr() embedding format. Adding a focused check.
# ============================================================================


@pytest.mark.unit
class TestTC12ConfigFileEmbedding:
    """TC-12: config_file YAML is embedded as Python dict literal via repr()."""

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

        fg = _make_flowgroup(
            actions=[_make_test_action("tst_1", test_id="T-1")]
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

        assert content is not None
        # repr() embeds the dict, then Black normalizes to double quotes
        # e.g. {"plan_id": 42, "org": "acme"}
        assert '"plan_id"' in content
        assert "42" in content
        assert '"acme"' in content


# ============================================================================
# TC-13: generate() uses empty dict when config_file is not set
# ALREADY covered indirectly by test_generates_hook_with_test_ids (no config_file).
# Adding an explicit check for the empty dict representation.
# ============================================================================


@pytest.mark.unit
class TestTC13EmptyConfigDict:
    """TC-13: Empty dict used when config_file is absent."""

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

        fg = _make_flowgroup(
            actions=[_make_test_action("tst_1", test_id="T-1")]
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

        assert content is not None
        # The repr({}) literal should appear in the generated content
        assert "{}" in content


# ============================================================================
# TC-14: generate() raises error when module_path file does not exist
# ALREADY COVERED by test_test_reporting_hook_generator.py:
#   test_raises_on_missing_module_path
# ============================================================================

# TC-15: Provider module copied with __init__.py and LHP-SOURCE header
# ALREADY COVERED by test_copies_provider_module

# TC-16: Generated import statement matches provider stem and function name
# ALREADY COVERED by test_hook_import_uses_correct_stem


# ============================================================================
# TC-24: config_file YAML with Python-incompatible literals (booleans, nulls)
# NOT covered by existing tests.
# ============================================================================


@pytest.mark.unit
class TestTC24PythonIncompatibleLiterals:
    """TC-24: YAML booleans/nulls rendered correctly via repr()."""

    def test_yaml_booleans_and_nulls_in_config(self, tmp_path):
        """YAML true/false/null are rendered as Python True/False/None."""
        provider = tmp_path / "src" / "pub.py"
        provider.parent.mkdir(parents=True)
        provider.write_text("def pub(r, c, ctx, s): pass\n")

        config_file = tmp_path / "config" / "special.yaml"
        config_file.parent.mkdir(parents=True)
        config_file.write_text(
            "enabled: true\n"
            "disabled: false\n"
            "empty_value: null\n"
            "count: 42\n"
        )

        tr = TestReportingConfig(
            module_path="src/pub.py",
            function_name="pub",
            config_file="config/special.yaml",
        )
        config = _make_project_config(test_reporting=tr)
        gen = TestReportingHookGenerator(config, tmp_path)

        fg = _make_flowgroup(
            actions=[_make_test_action("tst_1", test_id="T-1")]
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

        assert content is not None
        # Python repr() renders booleans as True/False, not true/false
        assert "True" in content
        assert "False" in content
        assert "None" in content
        assert "42" in content
        # YAML-style literals should NOT appear in the Python code
        assert ": true" not in content
        assert ": false" not in content
        assert ": null" not in content
