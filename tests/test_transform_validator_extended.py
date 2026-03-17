"""Tests for transform action validator — extended coverage."""

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from lhp.core.validators.transform_validator import TransformActionValidator
from lhp.models.config import Action, ActionType, TransformType


def _make_validator(project_root=None, project_config=None):
    """Create a TransformActionValidator with mocked dependencies."""
    registry = MagicMock()
    registry.is_generator_available.return_value = True
    field_validator = MagicMock()
    return TransformActionValidator(
        registry, field_validator, project_root, project_config
    )


def _make_action(**kwargs):
    """Create an Action with sensible transform defaults."""
    defaults = dict(
        name="test_action",
        type=ActionType.TRANSFORM,
        target="v_output",
        transform_type="sql",
        source="v_input",
    )
    defaults.update(kwargs)
    return Action(**defaults)


class TestValidateSqlTransform:
    """Tests for SQL transform validation."""

    def test_sql_missing_source_produces_error(self):
        """SQL transform without source should report an error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="sql",
            source=None,
            sql="SELECT 1",
        )
        errors = validator._validate_sql_transform(action, "test")
        assert any("source" in e and "view(s)" in e for e in errors)

    def test_sql_missing_sql_and_sql_path_produces_error(self):
        """SQL transform without sql or sql_path should report an error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="sql",
            sql=None,
            sql_path=None,
        )
        errors = validator._validate_sql_transform(action, "test")
        assert any("sql" in e.lower() and "sql_path" in e for e in errors)

    def test_sql_valid_with_inline_sql(self):
        """SQL transform with source and inline sql should pass."""
        validator = _make_validator()
        action = _make_action(
            transform_type="sql",
            sql="SELECT * FROM v_input",
        )
        errors = validator._validate_sql_transform(action, "test")
        assert errors == []

    def test_sql_valid_with_sql_path(self):
        """SQL transform with source and sql_path should pass."""
        validator = _make_validator()
        action = _make_action(
            transform_type="sql",
            sql_path="sql/my_query.sql",
        )
        errors = validator._validate_sql_transform(action, "test")
        assert errors == []


class TestValidateDataQualityTransform:
    """Tests for data quality transform validation."""

    def test_dq_missing_source_produces_error(self):
        """Data quality transform without source should report an error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="data_quality",
            source=None,
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert any("source" in e for e in errors)

    def test_dq_with_source_passes(self):
        """Data quality transform with source should pass."""
        validator = _make_validator()
        action = _make_action(
            transform_type="data_quality",
        )
        errors = validator._validate_data_quality_transform(action, "test")
        assert errors == []


class TestValidatePythonTransform:
    """Tests for Python transform validation."""

    def _make_python_action(self, **overrides):
        """Helper for Python transform actions."""
        defaults = dict(
            name="test_py",
            type=ActionType.TRANSFORM,
            target="v_output",
            transform_type="python",
            source="v_input",
            module_path="transforms/my_transform.py",
            function_name="my_func",
        )
        defaults.update(overrides)
        return Action(**defaults)

    def test_python_valid_action_passes(self):
        """A fully specified Python transform should pass validation."""
        validator = _make_validator()
        action = self._make_python_action()
        errors = validator._validate_python_transform(action, "test")
        assert errors == []

    def test_python_missing_source_produces_error(self):
        """Python transform without source should report an error."""
        validator = _make_validator()
        action = self._make_python_action(source=None)
        errors = validator._validate_python_transform(action, "test")
        assert any("source" in e for e in errors)

    def test_python_non_str_non_list_source_produces_error(self):
        """Python transform with non-string/non-list source should error."""
        validator = _make_validator()
        # Pydantic rejects invalid types at construction, so use a mock to
        # bypass model validation and test the validator logic directly.
        action = MagicMock()
        action.name = "test_py"
        action.source = 123  # invalid type
        action.module_path = "transforms/my_transform.py"
        action.function_name = "my_func"
        action.parameters = None
        errors = validator._validate_python_transform(action, "test")
        assert any("string or list of strings" in e for e in errors)

    def test_python_source_list_with_non_string_element_produces_error(self):
        """Python transform source list with non-string item should error."""
        validator = _make_validator()
        action = MagicMock()
        action.name = "test_py"
        action.source = ["v_input", 42]
        action.module_path = "transforms/my_transform.py"
        action.function_name = "my_func"
        action.parameters = None
        errors = validator._validate_python_transform(action, "test")
        assert any("source list item" in e and "must be a string" in e for e in errors)

    def test_python_source_list_of_strings_passes(self):
        """Python transform with a list-of-strings source should pass."""
        validator = _make_validator()
        action = self._make_python_action(source=["v_input", "v_lookup"])
        errors = validator._validate_python_transform(action, "test")
        # No source-related errors
        assert not any("source" in e.lower() for e in errors)

    def test_python_missing_module_path_produces_error(self):
        """Python transform without module_path should error."""
        validator = _make_validator()
        action = self._make_python_action(module_path=None)
        errors = validator._validate_python_transform(action, "test")
        assert any("module_path" in e for e in errors)

    def test_python_missing_function_name_produces_error(self):
        """Python transform without function_name should error."""
        validator = _make_validator()
        action = self._make_python_action(function_name=None)
        errors = validator._validate_python_transform(action, "test")
        assert any("function_name" in e for e in errors)

    def test_python_non_dict_parameters_produces_error(self):
        """Python transform with non-dict parameters should error."""
        validator = _make_validator()
        # Pydantic rejects non-dict parameters at construction, so use a mock.
        action = MagicMock()
        action.name = "test_py"
        action.source = "v_input"
        action.module_path = "transforms/my_transform.py"
        action.function_name = "my_func"
        action.parameters = "not_a_dict"
        errors = validator._validate_python_transform(action, "test")
        assert any("parameters must be a dictionary" in e for e in errors)

    def test_python_dict_parameters_passes(self):
        """Python transform with dict parameters should pass."""
        validator = _make_validator()
        action = self._make_python_action(parameters={"key": "value"})
        errors = validator._validate_python_transform(action, "test")
        assert not any("parameters" in e for e in errors)

    def test_python_module_file_not_found(self, tmp_path):
        """Python transform with non-existent module file should error when project_root set."""
        validator = _make_validator(project_root=tmp_path)
        action = self._make_python_action(module_path="nonexistent/module.py")
        errors = validator._validate_python_transform(action, "test")
        assert any("not found" in e for e in errors)

    def test_python_module_file_exists(self, tmp_path):
        """Python transform with existing module file should pass."""
        module_dir = tmp_path / "transforms"
        module_dir.mkdir()
        (module_dir / "my_transform.py").write_text("def my_func(): pass")

        validator = _make_validator(project_root=tmp_path)
        action = self._make_python_action(module_path="transforms/my_transform.py")
        errors = validator._validate_python_transform(action, "test")
        assert not any("not found" in e for e in errors)


class TestValidateTempTableTransform:
    """Tests for temp table transform validation."""

    def test_temp_table_missing_source_produces_error(self):
        """Temp table transform without source should report an error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="temp_table",
            source=None,
        )
        errors = validator._validate_temp_table_transform(action, "test")
        assert any("source" in e for e in errors)

    def test_temp_table_with_source_passes(self):
        """Temp table transform with source should pass."""
        validator = _make_validator()
        action = _make_action(
            transform_type="temp_table",
            source="v_raw",
        )
        errors = validator._validate_temp_table_transform(action, "test")
        assert errors == []


class TestValidateSchemaTransform:
    """Tests for schema transform validation."""

    def test_schema_missing_both_inline_and_file_produces_error(self):
        """Schema transform without schema_inline or schema_file should error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline=None,
            schema_file=None,
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("must specify either" in e for e in errors)

    def test_schema_both_inline_and_file_produces_error(self):
        """Schema transform with both schema_inline and schema_file should error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline="col1: string",
            schema_file="schema.yaml",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("cannot specify both" in e for e in errors)

    def test_schema_invalid_enforcement_produces_error(self):
        """Invalid enforcement value should produce an error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline="col1: string",
            enforcement="invalid_value",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("strict" in e and "permissive" in e for e in errors)

    def test_schema_strict_enforcement_passes(self):
        """enforcement='strict' should pass validation."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline="col1: string",
            enforcement="strict",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert not any("enforcement" in e for e in errors)

    def test_schema_permissive_enforcement_passes(self):
        """enforcement='permissive' should pass validation."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline="col1: string",
            enforcement="permissive",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert not any("enforcement" in e for e in errors)

    def test_schema_dict_source_deprecated_format_error(self):
        """Dict source (old nested format) should produce a deprecation error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            source={"view": "v_input", "schema": "col1: string"},
            schema_inline="col1: string",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("deprecated" in e.lower() for e in errors)

    def test_schema_non_string_source_produces_error(self):
        """Non-string, non-dict source should produce a type error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            source=["v_input", "v_other"],
            schema_inline="col1: string",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("must be a string" in e for e in errors)

    def test_schema_missing_source_produces_error(self):
        """Schema transform without source should error."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            source=None,
            schema_inline="col1: string",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("source" in e for e in errors)

    def test_schema_valid_with_inline_passes(self):
        """Valid schema transform with inline schema should pass."""
        validator = _make_validator()
        action = _make_action(
            transform_type="schema",
            schema_inline="col1: string\ncol2: int",
        )
        errors = validator._validate_schema_transform(action, "test")
        # No errors about schema definition
        assert not any("must specify either" in e for e in errors)
        assert not any("cannot specify both" in e for e in errors)

    def test_schema_file_not_found(self, tmp_path):
        """Schema file that does not exist should produce an error."""
        validator = _make_validator(project_root=tmp_path)
        action = _make_action(
            transform_type="schema",
            schema_file="nonexistent_schema.yaml",
        )
        errors = validator._validate_schema_transform(action, "test")
        assert any("not found" in e for e in errors)


class TestValidateQuarantineExpectations:
    """Tests for _validate_quarantine_expectations edge cases."""

    def test_missing_expectations_file_returns_early(self):
        """When expectations_file is None, no error is appended."""
        validator = _make_validator(project_root=Path("/some/root"))
        action = _make_action(
            transform_type="data_quality",
            expectations_file=None,
        )
        errors = []
        validator._validate_quarantine_expectations(action, "test", errors)
        assert errors == []

    def test_missing_project_root_returns_early(self):
        """When project_root is None, no error is appended."""
        validator = _make_validator(project_root=None)
        action = _make_action(
            transform_type="data_quality",
            expectations_file="expectations.yaml",
        )
        errors = []
        validator._validate_quarantine_expectations(action, "test", errors)
        assert errors == []

    def test_both_missing_returns_early(self):
        """When both expectations_file and project_root are None, no error."""
        validator = _make_validator(project_root=None)
        action = _make_action(
            transform_type="data_quality",
            expectations_file=None,
        )
        errors = []
        validator._validate_quarantine_expectations(action, "test", errors)
        assert errors == []


class TestValidateTopLevel:
    """Tests for the top-level validate() method dispatch."""

    def test_missing_target_produces_error(self):
        """Transform action without target should produce an error."""
        validator = _make_validator()
        action = _make_action(target=None)
        errors = validator.validate(action, "test")
        assert any("target" in e for e in errors)

    def test_missing_transform_type_produces_error(self):
        """Transform action without transform_type should produce an error."""
        validator = _make_validator()
        action = _make_action(transform_type=None)
        errors = validator.validate(action, "test")
        assert any("transform_type" in e for e in errors)

    def test_unknown_transform_type_produces_error(self):
        """Transform action with unsupported transform type should error."""
        validator = _make_validator()
        # Make the registry say the type is not available
        validator.action_registry.is_generator_available.return_value = False
        action = _make_action(transform_type="sql")
        errors = validator.validate(action, "test")
        assert any("Unknown transform type" in e for e in errors)

    def test_valid_sql_transform_dispatches_correctly(self):
        """Valid SQL transform should dispatch to _validate_sql_transform."""
        validator = _make_validator()
        action = _make_action(
            transform_type="sql",
            sql="SELECT 1",
        )
        errors = validator.validate(action, "test")
        assert errors == []
