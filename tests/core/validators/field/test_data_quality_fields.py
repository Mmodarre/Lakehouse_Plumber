"""LHP-CFG-073: a data-quality field on an action that is not a data_quality transform.

Only a ``transform_type: data_quality`` transform reads the action-level
``expectations_file``, ``quarantine`` and ``mode``; any other action used to
drop them silently. ``ConfigFieldValidator.validate_action_fields`` now raises
LHP-CFG-073, the same way it raises LHP-CFG-001 for an unknown field.
"""

import pytest

from lhp.core.validators import ConfigFieldValidator, ConfigValidator
from lhp.errors import LHPConfigError
from lhp.errors.codes import ALL_CODES, CFG_073
from lhp.models import Action

pytestmark = pytest.mark.unit

_QUARANTINE = {"dlq_table": "c.s.dlq", "source_table": "c.s.src"}


def _write(**fields) -> Action:
    return Action(
        name="write_gate",
        type="write",
        source="v_candidates",
        write_target={
            "type": "materialized_view",
            "catalog": "c",
            "schema": "s",
            "table": "gate",
        },
        **fields,
    )


def _load(**fields) -> Action:
    return Action(
        name="load_src",
        type="load",
        source={"type": "delta", "catalog": "c", "schema": "s", "table": "t"},
        target="v_src",
        **fields,
    )


def _sql_transform(**fields) -> Action:
    return Action(
        name="clean",
        type="transform",
        transform_type="sql",
        source="v_src",
        target="v_clean",
        sql="SELECT * FROM v_src",
        **fields,
    )


def _check(action: Action) -> None:
    ConfigFieldValidator().validate_action_fields(action.model_dump(), action.name)


def _raised(action: Action) -> LHPConfigError:
    with pytest.raises(LHPConfigError) as exc_info:
        _check(action)
    return exc_info.value


def test_code_is_registered():
    assert CFG_073.code == "LHP-CFG-073"
    assert CFG_073 in ALL_CODES


class TestRejected:
    def test_expectations_file_on_a_write_points_to_write_target(self):
        error = _raised(_write(expectations_file="expectations/gate.yaml"))

        assert error.code == "LHP-CFG-073"
        assert (
            error.title == "Data-quality field on a write action: 'expectations_file'"
        )
        assert error.details == (
            "Action 'write_gate' is a write action but sets 'expectations_file'. "
            "Only a transform with transform_type 'data_quality' reads this "
            "field, so this action would silently ignore it."
        )
        assert error.suggestions == [
            "Move 'expectations_file' to write_target.expectations_file "
            "(materialized_view writes)"
        ]
        assert error.context == {
            "Action": "write_gate",
            "Action Type": "write",
            "Fields": ["expectations_file"],
        }

    def test_expectations_file_on_a_load(self):
        error = _raised(_load(expectations_file="expectations/gate.yaml"))

        assert error.suggestions == [
            "Put 'expectations_file' on a data_quality transform"
        ]

    def test_quarantine_on_a_sql_transform_names_its_transform_type(self):
        error = _raised(_sql_transform(quarantine=_QUARANTINE))

        assert error.title == (
            "Data-quality field on a transform (transform_type 'sql') action: "
            "'quarantine'"
        )
        assert error.suggestions == [
            "Put 'quarantine' on a data_quality transform with mode: quarantine"
        ]

    def test_every_misplaced_field_is_reported(self):
        error = _raised(
            _sql_transform(
                expectations_file="e.yaml", quarantine=_QUARANTINE, mode="quarantine"
            )
        )

        assert error.title == (
            "Data-quality fields on a transform (transform_type 'sql') action: "
            "'expectations_file', 'quarantine', 'mode'"
        )
        assert "reads these fields, so this action would silently ignore them" in (
            error.details
        )
        assert error.context["Fields"] == ["expectations_file", "quarantine", "mode"]
        assert len(error.suggestions) == 3

    @pytest.mark.parametrize(
        ("action", "hint"),
        [
            (_load(mode="stream"), "Use 'readMode' to choose a stream or batch read"),
            (_write(mode="Batch"), "Use 'readMode' to choose a stream or batch read"),
            (_write(mode="cdc"), "Set the streaming-table mode as write_target.mode"),
            (
                _load(mode="quarantine"),
                "'mode' (dqe or quarantine) only applies to a data_quality transform",
            ),
            (
                _write(mode="quarantine"),
                "'mode' (dqe or quarantine) only applies to a data_quality transform",
            ),
            (
                _write(mode="DQE"),
                "'mode' (dqe or quarantine) only applies to a data_quality transform",
            ),
        ],
        ids=[
            "read-mode-on-load",
            "read-mode-on-write",
            "st-mode-on-write",
            "dq-mode",
            "dq-mode-on-write",
            "dqe-mode-on-write",
        ],
    )
    def test_mode_hint_follows_the_value_and_action(self, action, hint):
        assert _raised(action).suggestions == [hint]

    def test_test_action_is_checked_too(self):
        action = Action(
            name="pk_unique",
            type="test",
            test_type="uniqueness",
            source="v_src",
            columns=["id"],
            mode="dqe",
        )

        assert _raised(action).context["Action Type"] == "test"

    def test_config_validator_raises_it_for_the_action(self):
        with pytest.raises(LHPConfigError) as exc_info:
            ConfigValidator().validate_action(_write(expectations_file="e.yaml"), 0)

        assert exc_info.value.code == "LHP-CFG-073"


class TestAccepted:
    def test_data_quality_transform_may_set_all_three(self):
        _check(
            Action(
                name="dq",
                type="transform",
                transform_type="data_quality",
                source="v_src",
                target="v_dq",
                expectations_file="e.yaml",
                mode="quarantine",
                quarantine=_QUARANTINE,
            )
        )

    @pytest.mark.parametrize("factory", [_write, _load, _sql_transform])
    def test_unset_fields_are_not_reported(self, factory):
        _check(factory())

    def test_check_is_by_value_not_by_key(self):
        action_dict = _write().model_dump()
        assert action_dict["expectations_file"] is None  # present, but unset

        ConfigFieldValidator().validate_action_fields(action_dict, "write_gate")
