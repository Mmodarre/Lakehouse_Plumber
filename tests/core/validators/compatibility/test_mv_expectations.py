"""``MvExpectationsValidator``: LHP-VAL-071 for expectation names on a fused view.

A materialized view receives its own ``expectations_file`` rules and, when a
data_quality action fuses onto it, that action's rules. The decorators key
rules by name, so a name two of them share cannot be generated. Real files on
disk, no mocks.
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, List, Optional

import pytest
import yaml

from lhp.core.registry import ActionRegistry
from lhp.core.validators import ConfigValidator, MvExpectationsValidator
from lhp.errors import LHPError
from lhp.generators.registration import register_all
from lhp.models import Action, ActionType, FlowGroup, TransformType

pytestmark = pytest.mark.unit


def _rules(*names: str, action: str = "warn") -> List[dict]:
    return [
        {"name": n, "constraint": f"{n} > 0", "failureAction": action} for n in names
    ]


def _write(root: Path, name: str, rules: Any) -> str:
    (root / name).write_text(yaml.safe_dump(rules))
    return name


def _flowgroup(
    dq_rules: Any = None,
    dq_file: Optional[str] = None,
    mv_file: Optional[str] = None,
    dq_read_mode: Optional[str] = None,
) -> FlowGroup:
    dq_fields: dict = {"expectations_file": dq_file}
    if dq_rules is not None:
        dq_fields = {"expectations": dq_rules}
    mv_target = {
        "type": "materialized_view",
        "catalog": "c",
        "schema": "s",
        "table": "orders_mv",
    }
    if mv_file:
        mv_target["expectations_file"] = mv_file
    return FlowGroup(
        pipeline="p",
        flowgroup="fg",
        actions=[
            Action(
                name="load_orders",
                type=ActionType.LOAD,
                source={"type": "delta", "catalog": "c", "schema": "s", "table": "o"},
                target="v_orders",
            ),
            Action(
                name="dq_orders",
                type=ActionType.TRANSFORM,
                transform_type=TransformType.DATA_QUALITY,
                source="v_orders",
                target="v_orders_dq",
                readMode=dq_read_mode,
                **dq_fields,
            ),
            Action(
                name="write_mv",
                type=ActionType.WRITE,
                source="v_orders_dq",
                write_target=mv_target,
            ),
        ],
    )


@pytest.fixture
def validator(tmp_path: Path) -> MvExpectationsValidator:
    register_all()
    return MvExpectationsValidator(ActionRegistry(), tmp_path)


def test_distinct_names_pass(tmp_path, validator):
    dq = _write(tmp_path, "dq.yaml", _rules("a", "b"))
    mv = _write(tmp_path, "mv.yaml", _rules("c"))

    assert validator.validate(_flowgroup(dq_file=dq, mv_file=mv)) == []


def test_a_name_in_both_sources_is_val_071(tmp_path, validator):
    dq = _write(tmp_path, "dq.yaml", _rules("a", "shared"))
    mv = _write(tmp_path, "mv.yaml", _rules("shared"))

    [error] = validator.validate(_flowgroup(dq_file=dq, mv_file=mv))

    assert isinstance(error, LHPError)
    assert error.code == "LHP-VAL-071"
    assert error.title == "Duplicate expectation name"
    assert error.context["Expectation"] == "shared"
    assert error.context["Action"] == "dq_orders"
    assert error.context["Materialized View"] == "write_mv"
    assert "view's expectations file" in error.details


def test_a_name_used_twice_in_the_fused_actions_own_rules_is_val_071(
    tmp_path, validator
):
    dq = _write(tmp_path, "dq.yaml", _rules("dup") + _rules("dup", action="drop"))

    [error] = validator.validate(_flowgroup(dq_file=dq))

    assert error.code == "LHP-VAL-071"
    assert error.context["Expectation"] == "dup"


def test_a_name_in_two_categories_of_the_fused_rules_is_val_071(tmp_path, validator):
    dq = _write(
        tmp_path, "dq.yaml", _rules("dup", action="warn") + _rules("dup", action="fail")
    )

    [error] = validator.validate(_flowgroup(dq_file=dq))

    assert error.code == "LHP-VAL-071"


def test_inline_rules_of_the_fused_action_are_checked(tmp_path, validator):
    mv = _write(tmp_path, "mv.yaml", _rules("shared"))

    [error] = validator.validate(_flowgroup(dq_rules=_rules("shared"), mv_file=mv))

    assert error.code == "LHP-VAL-071"


def test_each_shared_name_is_one_error(tmp_path, validator):
    dq = _write(tmp_path, "dq.yaml", _rules("x", "y"))
    mv = _write(tmp_path, "mv.yaml", _rules("x", "y"))

    errors = validator.validate(_flowgroup(dq_file=dq, mv_file=mv))

    assert sorted(e.context["Expectation"] for e in errors) == ["x", "y"]


def test_a_duplicate_inside_the_views_own_file_is_left_to_its_own_validation(
    tmp_path, validator
):
    dq = _write(tmp_path, "dq.yaml", _rules("a"))
    mv = _write(tmp_path, "mv.yaml", _rules("twice") + _rules("twice", action="drop"))

    assert validator.validate(_flowgroup(dq_file=dq, mv_file=mv)) == []


def test_nothing_is_checked_when_the_action_is_not_fused(tmp_path, validator):
    dq = _write(tmp_path, "dq.yaml", _rules("shared"))
    mv = _write(tmp_path, "mv.yaml", _rules("shared"))

    flowgroup = _flowgroup(dq_file=dq, mv_file=mv, dq_read_mode="stream")

    assert validator.validate(flowgroup) == []


def test_unreadable_rules_are_left_to_the_actions_own_validation(tmp_path, validator):
    mv = _write(tmp_path, "mv.yaml", _rules("a"))

    assert validator.validate(_flowgroup(dq_file="missing.yaml", mv_file=mv)) == []
    bad = _write(tmp_path, "bad.yaml", [{"name": "n", "constraint": "x > 0"}, 5])
    assert validator.validate(_flowgroup(dq_file=bad, mv_file=mv)) == []


def test_flowgroup_validation_reports_val_071(tmp_path):
    register_all()
    dq = _write(tmp_path, "dq.yaml", _rules("shared"))
    mv = _write(tmp_path, "mv.yaml", _rules("shared"))

    errors = ConfigValidator(project_root=tmp_path).validate_flowgroup(
        _flowgroup(dq_file=dq, mv_file=mv)
    )

    assert [e.code for e in errors if isinstance(e, LHPError)] == ["LHP-VAL-071"]
