"""``validate_expectations_file``: check a data-quality ``expectations_file``.

The file is read through the loader the data_quality generator uses. In
quarantine mode it must hold at least one rule, and a ``fail``/``warn`` rule is
logged as coerced to ``drop``. Real files on disk, no mocks.
"""

import logging

import pytest
import yaml

from lhp.core.validators.field import validate_expectations_file

pytestmark = pytest.mark.unit

_PREFIX = "FlowGroup 'fg' action 'dq'"
_LOGGER = "lhp.core.validators.field.expectations_file"


def _write(tmp_path, document, name="rules.yaml") -> str:
    (tmp_path / name).write_text(yaml.dump(document, sort_keys=False))
    return name


def _warnings(caplog) -> list:
    return [r.getMessage() for r in caplog.records if r.levelno == logging.WARNING]


class TestNothingToCheck:
    @pytest.mark.parametrize("expectations_file", [None, ""])
    def test_unset_file_passes(self, tmp_path, expectations_file):
        assert (
            validate_expectations_file(
                expectations_file, tmp_path, _PREFIX, quarantine=True
            )
            == []
        )

    def test_unknown_project_root_skips_the_file(self):
        assert (
            validate_expectations_file("missing.yaml", None, _PREFIX, quarantine=True)
            == []
        )


class TestLoadErrors:
    def test_missing_file_in_quarantine_mode(self, tmp_path):
        errors = validate_expectations_file(
            "missing.yaml", tmp_path, _PREFIX, quarantine=True
        )

        assert len(errors) == 1
        assert errors[0].startswith(
            f"{_PREFIX}: Failed to load expectations file for quarantine mode: "
        )
        assert "LHP-IO-001" in errors[0]
        assert "missing.yaml" in errors[0]

    def test_missing_file_outside_quarantine_mode(self, tmp_path):
        errors = validate_expectations_file("missing.yaml", tmp_path, _PREFIX)

        assert len(errors) == 1
        assert errors[0].startswith(f"{_PREFIX}: Failed to load expectations file: ")
        assert "LHP-IO-001" in errors[0]

    def test_invalid_yaml_in_quarantine_mode_names_quarantine(self, tmp_path):
        (tmp_path / "rules.yaml").write_text("key: [unclosed\n")

        errors = validate_expectations_file(
            "rules.yaml", tmp_path, _PREFIX, quarantine=True
        )

        assert len(errors) == 1
        assert "LHP-CFG-009" in errors[0]
        assert "quarantine expectations file" in errors[0]

    def test_scalar_document_in_quarantine_mode(self, tmp_path):
        path = _write(tmp_path, "hello")

        assert validate_expectations_file(path, tmp_path, _PREFIX, quarantine=True) == [
            f"{_PREFIX}: Expectations file has unexpected format for quarantine mode"
        ]

    def test_scalar_document_outside_quarantine_mode(self, tmp_path):
        path = _write(tmp_path, 42)

        assert validate_expectations_file(path, tmp_path, _PREFIX) == [
            f"{_PREFIX}: Expectations file has unexpected format"
        ]


class TestQuarantineRuleCount:
    @pytest.mark.parametrize(
        "document",
        [[], {}, {"expectations": []}, [{"name": "no_constraint"}]],
        ids=["empty-list", "empty-mapping", "empty-wrapped", "no-constraint"],
    )
    def test_file_without_rules_is_an_error(self, tmp_path, document):
        path = _write(tmp_path, document)

        assert validate_expectations_file(path, tmp_path, _PREFIX, quarantine=True) == [
            f"{_PREFIX}: Quarantine mode requires at least one expectation, "
            f"but the expectations file '{path}' contains none. "
            "An empty expectations set would produce an invalid inverse filter."
        ]

    def test_file_without_rules_is_accepted_outside_quarantine_mode(self, tmp_path):
        path = _write(tmp_path, [])

        assert validate_expectations_file(path, tmp_path, _PREFIX) == []

    def test_wrapped_rules_count(self, tmp_path):
        path = _write(
            tmp_path,
            {
                "version": "1.0",
                "expectations": [
                    {"name": "id_check", "expression": "id IS NOT NULL"},
                ],
            },
        )

        assert (
            validate_expectations_file(path, tmp_path, _PREFIX, quarantine=True) == []
        )


class TestQuarantineCoercionWarnings:
    def test_list_rules_with_fail_or_warn_are_logged(self, tmp_path, caplog):
        path = _write(
            tmp_path,
            [
                {
                    "name": "id_check",
                    "constraint": "id IS NOT NULL",
                    "failureAction": "FAIL",
                },
                {
                    "message": "age_msg",
                    "constraint": "age > 0",
                    "failureAction": "warn",
                },
                {"constraint": "x > 0", "failureAction": "fail"},
                {"name": "kept", "constraint": "y > 0", "failureAction": "drop"},
            ],
        )

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            errors = validate_expectations_file(
                path, tmp_path, _PREFIX, quarantine=True
            )

        assert errors == []
        assert _warnings(caplog) == [
            f"{_PREFIX}: Expectation 'id_check' has failureAction='fail' but "
            "quarantine mode coerces all expectations to 'drop'. "
            "The original action will be ignored.",
            f"{_PREFIX}: Expectation 'age_msg' has failureAction='warn' but "
            "quarantine mode coerces all expectations to 'drop'. "
            "The original action will be ignored.",
            f"{_PREFIX}: Expectation 'unknown' has failureAction='fail' but "
            "quarantine mode coerces all expectations to 'drop'. "
            "The original action will be ignored.",
        ]

    def test_mapping_rules_with_fail_or_warn_are_logged(self, tmp_path, caplog):
        path = _write(
            tmp_path,
            {
                "id IS NOT NULL": {"action": "fail", "name": "id_check"},
                "age > 0": {"action": "Warn"},
                "y > 0": {"action": "drop", "name": "kept"},
            },
        )

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            errors = validate_expectations_file(
                path, tmp_path, _PREFIX, quarantine=True
            )

        assert errors == []
        assert _warnings(caplog) == [
            f"{_PREFIX}: Expectation 'id_check' has action='fail' but "
            "quarantine mode coerces all expectations to 'drop'. "
            "The original action will be ignored.",
            f"{_PREFIX}: Expectation 'age > 0' has action='warn' but "
            "quarantine mode coerces all expectations to 'drop'. "
            "The original action will be ignored.",
        ]

    def test_drop_only_rules_log_nothing(self, tmp_path, caplog):
        path = _write(
            tmp_path,
            {
                "id IS NOT NULL": {"action": "drop", "name": "id_check"},
                "age > 0": {"action": "drop", "name": "age_check"},
            },
        )

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            errors = validate_expectations_file(
                path, tmp_path, _PREFIX, quarantine=True
            )

        assert errors == []
        assert _warnings(caplog) == []

    def test_coercion_is_not_logged_outside_quarantine_mode(self, tmp_path, caplog):
        path = _write(tmp_path, {"id IS NOT NULL": {"action": "fail"}})

        with caplog.at_level(logging.WARNING, logger=_LOGGER):
            errors = validate_expectations_file(path, tmp_path, _PREFIX)

        assert errors == []
        assert _warnings(caplog) == []
