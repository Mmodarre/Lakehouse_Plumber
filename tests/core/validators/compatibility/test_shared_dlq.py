"""Tests for SharedDlqSourceTableValidator (LHP-VAL-069).

Quarantine actions may share one DLQ table, but each one that does must use its
own ``source_table``: the generated MERGEs and the recycle and outbox reads all
filter on ``_dlq_source_table``, so two actions with the same pair would read
and write the same DLQ slice.
"""

from typing import Any, Dict, Optional, Union

import pytest

from lhp.core.validators.compatibility import SharedDlqSourceTableValidator
from lhp.models import Action, ActionType, FlowGroup, QuarantineConfig, TransformType

pytestmark = pytest.mark.unit

DLQ = "cat.sch.orders_dlq"
SOURCE = "cat.sch.orders"


def _dq_action(
    name: str,
    *,
    dlq_table: str = DLQ,
    source_table: str = SOURCE,
    mode: Optional[str] = "quarantine",
) -> Action:
    """Build a data_quality transform; quarantine block only for quarantine mode."""
    return Action(
        name=name,
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_raw",
        target=f"v_{name}",
        expectations_file="expectations/rules.yaml",
        mode=mode,
        quarantine=(
            QuarantineConfig(dlq_table=dlq_table, source_table=source_table)
            if mode == "quarantine"
            else None
        ),
    )


def _fg(name: str, *actions: Action) -> FlowGroup:
    return FlowGroup(pipeline="p", flowgroup=name, actions=list(actions))


def _validate(*flowgroups: FlowGroup) -> list[str]:
    return SharedDlqSourceTableValidator().validate(list(flowgroups))


def test_same_dlq_and_source_across_flowgroups_reports_one_conflict():
    errors = _validate(
        _fg("fg_orders", _dq_action("dq_orders")),
        _fg("fg_orders_eu", _dq_action("dq_orders_eu")),
    )

    assert len(errors) == 1
    assert "fg_orders.dq_orders" in errors[0]
    assert "fg_orders_eu.dq_orders_eu" in errors[0]
    assert DLQ in errors[0]
    assert SOURCE in errors[0]
    assert "distinct source_table" in errors[0]


def test_same_dlq_and_source_within_one_flowgroup_reports_one_conflict():
    errors = _validate(_fg("fg_orders", _dq_action("dq_a"), _dq_action("dq_b")))

    assert len(errors) == 1
    assert "fg_orders.dq_a" in errors[0]
    assert "fg_orders.dq_b" in errors[0]


def test_three_actions_on_one_pair_report_a_single_message_naming_all():
    errors = _validate(
        _fg("fg_a", _dq_action("dq_a")),
        _fg("fg_b", _dq_action("dq_b")),
        _fg("fg_c", _dq_action("dq_c")),
    )

    assert len(errors) == 1
    for offender in ("fg_a.dq_a", "fg_b.dq_b", "fg_c.dq_c"):
        assert offender in errors[0]


def test_each_conflicting_pair_gets_its_own_message():
    errors = _validate(
        _fg("fg_a", _dq_action("dq_a1"), _dq_action("dq_a2")),
        _fg(
            "fg_b",
            _dq_action("dq_b1", source_table="cat.sch.customers"),
            _dq_action("dq_b2", source_table="cat.sch.customers"),
        ),
    )

    assert len(errors) == 2
    assert "fg_a.dq_a1" in errors[0] and "fg_a.dq_a2" in errors[0]
    assert "fg_b.dq_b1" in errors[1] and "fg_b.dq_b2" in errors[1]


def test_same_dlq_with_distinct_sources_is_allowed():
    errors = _validate(
        _fg("fg_orders", _dq_action("dq_orders", source_table="cat.sch.orders")),
        _fg("fg_returns", _dq_action("dq_returns", source_table="cat.sch.returns")),
    )

    assert errors == []


def test_same_source_on_distinct_dlqs_is_allowed():
    errors = _validate(
        _fg("fg_a", _dq_action("dq_a", dlq_table="cat.sch.dlq_a")),
        _fg("fg_b", _dq_action("dq_b", dlq_table="cat.sch.dlq_b")),
    )

    assert errors == []


def test_dlq_table_differing_only_by_case_is_the_same_dlq():
    errors = _validate(
        _fg("fg_a", _dq_action("dq_a", dlq_table="cat.sch.orders_dlq")),
        _fg("fg_b", _dq_action("dq_b", dlq_table="CAT.SCH.Orders_DLQ")),
    )

    assert len(errors) == 1
    assert "fg_a.dq_a" in errors[0]
    assert "fg_b.dq_b" in errors[0]


def test_source_table_differing_only_by_case_is_not_a_conflict():
    """The generated filters compare source_table as an exact string."""
    errors = _validate(
        _fg("fg_a", _dq_action("dq_a", source_table="cat.sch.orders")),
        _fg("fg_b", _dq_action("dq_b", source_table="CAT.SCH.ORDERS")),
    )

    assert errors == []


def test_dqe_mode_actions_are_ignored():
    dqe = _dq_action("dq_dqe", mode="dqe")
    # A stray quarantine block on a dqe action is the per-flowgroup validator's
    # job to reject; this validator must not count it as a DLQ user.
    dqe.quarantine = QuarantineConfig(dlq_table=DLQ, source_table=SOURCE)

    errors = _validate(
        _fg("fg_quarantine", _dq_action("dq_quarantine")),
        _fg("fg_dqe", dqe),
    )

    assert errors == []


def test_quarantine_config_given_as_dict_is_handled():
    raw: Dict[str, Any] = {"dlq_table": DLQ, "source_table": SOURCE}
    dict_action = _dq_action("dq_dict")
    dict_action.quarantine = raw  # type: ignore[assignment]

    errors = _validate(
        _fg("fg_model", _dq_action("dq_model")),
        _fg("fg_dict", dict_action),
    )

    assert len(errors) == 1
    assert "fg_model.dq_model" in errors[0]
    assert "fg_dict.dq_dict" in errors[0]


@pytest.mark.parametrize(
    "quarantine",
    [
        None,
        {"dlq_table": DLQ},
        {"source_table": SOURCE},
        {"dlq_table": "", "source_table": SOURCE},
    ],
)
def test_incomplete_quarantine_config_is_skipped(
    quarantine: Union[None, Dict[str, Any]],
):
    """Missing or empty fields are reported by the per-flowgroup validator."""
    incomplete = _dq_action("dq_incomplete")
    incomplete.quarantine = quarantine  # type: ignore[assignment]

    errors = _validate(
        _fg("fg_ok", _dq_action("dq_ok")),
        _fg("fg_incomplete", incomplete),
    )

    assert errors == []


def test_no_quarantine_actions_returns_no_errors():
    assert _validate(_fg("fg_empty")) == []
