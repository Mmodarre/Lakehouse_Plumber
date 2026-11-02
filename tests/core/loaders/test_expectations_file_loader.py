"""``load_expectations_file``: resolve, parse and unwrap a data-quality expectations file.

The data_quality generator and the expectations-file validator both read an
action's ``expectations_file`` through this loader. Real files on disk, no mocks.
"""

import json
from pathlib import Path

import pytest
import yaml

from lhp.core.loaders import load_expectations_file
from lhp.errors import (
    LHPConfigError,
    LHPFileError,
    LHPValidationError,
    MultiDocumentError,
)

pytestmark = pytest.mark.unit

_RULES = [
    {"name": "id_check", "constraint": "id IS NOT NULL", "failureAction": "drop"},
    {"name": "age_check", "expression": "age > 0", "failureAction": "fail"},
]
_CONSTRAINT_MAP = {
    "id IS NOT NULL": {"action": "drop", "name": "id_check"},
    "age > 0": {"action": "warn", "name": "age_check"},
}


def _write(tmp_path: Path, name: str, text: str) -> str:
    folder = tmp_path / "expectations"
    folder.mkdir(exist_ok=True)
    (folder / name).write_text(text)
    return f"expectations/{name}"


class TestFormats:
    def test_rule_list_is_returned_as_written(self, tmp_path):
        path = _write(tmp_path, "rules.yaml", yaml.dump(_RULES))

        assert load_expectations_file(path, tmp_path) == _RULES

    def test_constraint_mapping_is_returned_as_written(self, tmp_path):
        path = _write(
            tmp_path, "rules.yaml", yaml.dump(_CONSTRAINT_MAP, sort_keys=False)
        )

        result = load_expectations_file(path, tmp_path)

        assert result == _CONSTRAINT_MAP
        assert list(result) == ["id IS NOT NULL", "age > 0"]

    def test_wrapped_rule_list_is_unwrapped(self, tmp_path):
        document = {"version": "1.0", "table": "customer", "expectations": _RULES}
        path = _write(tmp_path, "rules.json", json.dumps(document))

        assert load_expectations_file(path, tmp_path) == _RULES

    def test_wrapped_constraint_mapping_is_unwrapped(self, tmp_path):
        path = _write(
            tmp_path, "rules.yaml", yaml.dump({"expectations": _CONSTRAINT_MAP})
        )

        assert load_expectations_file(path, tmp_path) == _CONSTRAINT_MAP

    def test_null_document_loads_as_empty_mapping(self, tmp_path):
        path = _write(tmp_path, "rules.yaml", "null\n")

        assert load_expectations_file(path, tmp_path) == {}


class TestPathResolution:
    def test_relative_path_resolves_against_project_root(self, tmp_path):
        _write(tmp_path, "rules.yaml", yaml.dump(_RULES))

        assert load_expectations_file(Path("expectations/rules.yaml"), tmp_path) == (
            _RULES
        )

    def test_absolute_path_ignores_project_root(self, tmp_path):
        path = _write(tmp_path, "rules.yaml", yaml.dump(_RULES))
        absolute = str(tmp_path / path)

        assert load_expectations_file(absolute, tmp_path / "elsewhere") == _RULES


class TestErrors:
    def test_missing_file_names_the_expectations_file(self, tmp_path):
        with pytest.raises(LHPFileError) as exc_info:
            load_expectations_file("expectations/missing.yaml", tmp_path)

        assert exc_info.value.code == "LHP-IO-001"
        assert "expectations file" in str(exc_info.value)
        assert "expectations/missing.yaml" in str(exc_info.value)

    @pytest.mark.parametrize(
        ("text", "actual_type"),
        [('"just a string"\n', "str"), ("42\n", "int"), ("true\n", "bool")],
    )
    def test_scalar_document_is_rejected(self, tmp_path, text, actual_type):
        path = _write(tmp_path, "rules.yaml", text)

        with pytest.raises(LHPValidationError) as exc_info:
            load_expectations_file(path, tmp_path)

        error = exc_info.value
        assert error.code == "LHP-VAL-008"
        assert error.context["Actual Type"] == actual_type
        assert error.context["File"] == str(tmp_path / path)
        assert path in error.details
        assert f"holds a {actual_type}" in error.details

    def test_invalid_yaml_names_the_error_context(self, tmp_path):
        path = _write(tmp_path, "rules.yaml", "key: [unclosed\n")

        with pytest.raises(LHPConfigError) as exc_info:
            load_expectations_file(
                path, tmp_path, error_context="quarantine expectations file"
            )

        assert exc_info.value.code == "LHP-CFG-009"
        assert "Context: quarantine expectations file" in exc_info.value.details

    def test_multiple_documents_name_the_error_context(self, tmp_path):
        path = _write(tmp_path, "rules.yaml", "a: 1\n---\nb: 2\n")

        with pytest.raises(MultiDocumentError) as exc_info:
            load_expectations_file(
                path, tmp_path, error_context="data quality expectations file"
            )

        assert exc_info.value.code == "LHP-IO-003"
        assert "data quality expectations file contains 2 documents" in (
            exc_info.value.details
        )
