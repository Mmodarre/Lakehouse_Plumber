"""Streaming-table ``write_target.mode`` validation (#345).

An unknown mode used to fall through to an append flow. ``WriteActionValidator``
now rejects it with LHP-VAL-006, and ``STREAMING_TABLE_MODES`` is pinned to
the mode enum in the packaged authoring schemas.
"""

import json
from importlib.resources import files

import pytest

from lhp.core.registry import ActionRegistry
from lhp.core.validators import (
    STREAMING_TABLE_MODES,
    ConfigFieldValidator,
    WriteActionValidator,
)
from lhp.errors import LHPError
from lhp.models import Action

pytestmark = pytest.mark.unit

_PREFIX = "Action[0] 'write_customer'"


def _errors_for(**write_target) -> list:
    action = Action(
        name="write_customer",
        type="write",
        source="v_customer",
        write_target={
            "type": "streaming_table",
            "catalog": "main",
            "schema": "silver",
            "table": "customer",
            **write_target,
        },
    )
    validator = WriteActionValidator(ActionRegistry(), ConfigFieldValidator())
    return validator.validate(action, _PREFIX)


def _mode_errors(errors: list) -> list:
    return [
        e
        for e in errors
        if isinstance(e, LHPError) and e.context.get("Field") == "write_target.mode"
    ]


class TestUnknownMode:
    def test_typo_is_rejected_with_a_did_you_mean(self):
        errors = _errors_for(mode="cdcc")

        assert len(errors) == 1
        [error] = _mode_errors(errors)
        assert error.code == "LHP-VAL-006"
        assert error.title == "Invalid value for 'write_target.mode'"
        assert error.details == (
            "Action 'write_customer' has invalid value 'cdcc' for field "
            "'write_target.mode'. Valid values: cdc, replace, snapshot_cdc, standard"
        )
        assert error.suggestions == [
            "Did you mean 'cdc'?",
            "Use one of: cdc, replace, snapshot_cdc, standard (omit 'mode' for "
            "'standard')",
        ]
        assert error.context == {
            "Action": "write_customer",
            "Field": "write_target.mode",
            "Provided": "cdcc",
        }

    def test_wrong_case_suggests_the_lower_case_mode(self):
        [error] = _mode_errors(_errors_for(mode="SNAPSHOT_CDC"))

        assert error.suggestions[0] == "Did you mean 'snapshot_cdc'?"

    def test_unrelated_value_has_no_did_you_mean(self):
        [error] = _mode_errors(_errors_for(mode="append"))

        assert error.suggestions == [
            "Use one of: cdc, replace, snapshot_cdc, standard (omit 'mode' for "
            "'standard')"
        ]

    @pytest.mark.parametrize("mode", [None, ["cdc"]], ids=["null", "list"])
    def test_non_string_mode_is_rejected(self, mode):
        [error] = _mode_errors(_errors_for(mode=mode))

        assert error.code == "LHP-VAL-006"
        assert error.context["Provided"] == str(mode)


class TestKnownModes:
    def test_absent_mode_is_standard(self):
        assert _errors_for() == []

    def test_standard_is_accepted(self):
        assert _errors_for(mode="standard") == []

    @pytest.mark.parametrize("mode", sorted(STREAMING_TABLE_MODES))
    def test_no_known_mode_is_reported_as_unknown(self, mode):
        # cdc / snapshot_cdc / replace still report their own missing config.
        assert _mode_errors(_errors_for(mode=mode)) == []


class TestSchemaParity:
    """The validator's mode set is the authoring schemas' mode enum."""

    @staticmethod
    def _schema(name: str) -> dict:
        return json.loads((files("lhp.schemas") / name).read_text(encoding="utf-8"))

    def test_flowgroup_schema_enum(self):
        mode = self._schema("flowgroup.schema.json")["definitions"]["WriteTarget"][
            "properties"
        ]["mode"]

        assert STREAMING_TABLE_MODES == set(mode["enum"])

    def test_template_schema_literal_enum(self):
        # Template values may also be a {{ }} expression; compare the literals.
        mode = self._schema("template.schema.json")["definitions"]["WriteTarget"][
            "properties"
        ]["mode"]
        literals = {v for branch in mode["anyOf"] for v in branch.get("enum", [])}

        assert STREAMING_TABLE_MODES == literals
