"""``fusion_target``: when a data_quality action fuses.

A data_quality action fuses into the materialized view that is the only reader
of its view when it is in ``dqe`` mode, reads in batch, and the view is a
pass-through (a single ``source``, no SQL and no ``expectations_file`` of its
own, the only write to its table). A test action reading the view does not
count as a reader.
"""

from __future__ import annotations

from typing import Any, List, Optional

import pytest

from lhp.core.processing import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators import dq_source_view, fusion_target
from lhp.generators.registration import register_all
from lhp.models import Action, ActionType, TransformType

pytestmark = pytest.mark.unit


@pytest.fixture(autouse=True)
def _registered():
    register_all()


def _target(dq: Action, actions: List[Action], **kwargs: Any) -> Optional[Action]:
    return fusion_target(dq, actions, registry=ActionRegistry(), **kwargs)


def _load(read_mode: Optional[str] = None) -> Action:
    return Action(
        name="load_orders",
        type=ActionType.LOAD,
        source={"type": "delta", "catalog": "c", "schema": "s", "table": "orders"},
        target="v_orders",
        readMode=read_mode,
    )


def _dq(**fields: Any) -> Action:
    values: dict = {
        "name": "dq_orders",
        "type": ActionType.TRANSFORM,
        "transform_type": TransformType.DATA_QUALITY,
        "source": "v_orders",
        "target": "v_orders_dq",
        "expectations_file": "rules.yaml",
    }
    values.update(fields)
    return Action(**values)


def _mv(name: str = "write_mv", source: Any = "v_orders_dq", **target: Any) -> Action:
    write_target = {
        "type": "materialized_view",
        "catalog": "c",
        "schema": "s",
        "table": "orders_mv",
    }
    write_target.update(target)
    return Action(
        name=name, type=ActionType.WRITE, source=source, write_target=write_target
    )


def _fusable(extra: Optional[List[Action]] = None) -> List[Action]:
    return [_load(), _dq(), _mv(), *(extra or [])]


def test_a_dqe_action_read_by_one_pass_through_view_fuses():
    actions = _fusable()

    assert _target(actions[1], actions) is actions[2]


def test_an_explicit_batch_read_fuses_over_an_unknown_producer():
    # No in-flowgroup producer: the DQ would default to a stream; readMode wins.
    actions = [_dq(source="c.s.orders", readMode="batch"), _mv()]

    assert _target(actions[0], actions) is not None


def test_view_names_compare_case_insensitively():
    actions = [_load(), _dq(), _mv(source="V_ORDERS_DQ")]

    assert _target(actions[1], actions) is actions[2]


def test_a_mapping_source_names_its_view():
    assert dq_source_view(_dq(source={"view": "v_x"})) == "v_x"
    assert dq_source_view(_dq(source={"source": "v_y"})) == "v_y"
    assert dq_source_view(_dq(source={"table": "t"})) == ""


@pytest.mark.parametrize(
    "actions",
    [
        pytest.param([_load(), _dq(readMode="stream"), _mv()], id="stream-read"),
        pytest.param([_load("stream"), _dq(), _mv()], id="stream-producer"),
        pytest.param([_dq(), _mv()], id="external-source-defaults-to-stream"),
        pytest.param(
            [
                _load(),
                _dq(
                    mode="quarantine",
                    quarantine={"dlq_table": "c.s.d", "source_table": "c.s.t"},
                ),
                _mv(),
            ],
            id="quarantine",
        ),
        pytest.param([_load(), _dq()], id="no-reader"),
        pytest.param(
            [_load(), _dq(), _mv(), _mv(name="write_other", table="other_mv")],
            id="two-readers",
        ),
        pytest.param(
            [
                _load(),
                _dq(),
                _mv(),
                Action(
                    name="next_dq",
                    type=ActionType.TRANSFORM,
                    transform_type=TransformType.DATA_QUALITY,
                    source="v_orders_dq",
                    target="v_next",
                    expectations_file="r.yaml",
                ),
            ],
            id="a-transform-also-reads-it",
        ),
        pytest.param([_load(), _dq(), _mv(sql="SELECT 1")], id="view-with-its-own-sql"),
        pytest.param(
            [_load(), _dq(), _mv(sql_path="q.sql")], id="view-with-its-own-sql-path"
        ),
        pytest.param(
            [_load(), _dq(), _mv(source=["v_orders_dq", "v_other"])],
            id="two-item-list-source",
        ),
        pytest.param(
            [_load(), _dq(), _mv(expectations_file="mv_rules.yaml")],
            id="view-with-its-own-expectations-file",
        ),
        pytest.param(
            [_load(), _dq(), _mv(), _mv(name="write_again", source="v_orders")],
            id="table-shared-with-another-write",
        ),
        pytest.param(
            [
                _load(),
                _dq(),
                Action(
                    name="write_st",
                    type=ActionType.WRITE,
                    source="v_orders_dq",
                    write_target={
                        "type": "streaming_table",
                        "catalog": "c",
                        "schema": "s",
                        "table": "t",
                    },
                ),
            ],
            id="streaming-table-reader",
        ),
    ],
)
def test_it_does_not_fuse(actions):
    dq = next(a for a in actions if a.transform_type == TransformType.DATA_QUALITY)

    assert _target(dq, actions) is None


def test_a_non_data_quality_action_never_fuses():
    actions = _fusable()

    assert _target(actions[0], actions) is None
    assert _target(actions[2], actions) is None


def test_a_streaming_dq_does_not_fuse_even_over_a_pass_through_view():
    actions = [_load(), _dq(readMode="stream"), _mv()]

    assert _target(actions[1], actions) is None


def test_a_one_element_list_source_is_the_view_alone():
    # The materialized-view generator reads source[0], so ["v"] means "v".
    actions = [_load(), _dq(), _mv(source=["v_orders_dq"])]

    assert _target(actions[1], actions) is actions[2]


@pytest.mark.parametrize("own_file", [None, ""])
def test_an_unset_expectations_file_does_not_stop_fusion(own_file):
    actions = [_load(), _dq(), _mv(expectations_file=own_file)]

    assert _target(actions[1], actions) is actions[2]


def _test_reader() -> Action:
    return Action(
        name="check_dq_view",
        type=ActionType.TEST,
        test_type="row_count",
        source=["v_orders_dq", "v_orders"],
    )


def test_a_test_action_reading_the_view_is_not_a_reader():
    # Fusion must not depend on --include-tests: the data_quality view is still
    # generated for the test, so the same flowgroup fuses with and without it.
    with_test = _fusable([_test_reader()])
    without_test = _fusable()

    assert _target(with_test[1], with_test) is with_test[2]
    assert _target(without_test[1], without_test) is without_test[2]


def test_a_sql_file_reader_is_found_through_the_substituted_name(tmp_path):
    # `${stage}_dq` names the data_quality view only once substituted, so the
    # fusion decision must read the file the way code generation does.
    (tmp_path / "next.sql").write_text("SELECT * FROM ${stage}_dq")
    reader = Action(
        name="read_sql",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        sql_path="next.sql",
        target="v_next",
    )
    actions = _fusable([reader])
    manager = EnhancedSubstitutionManager()
    manager.mappings["stage"] = "v_orders"

    target = _target(
        actions[1], actions, project_root=tmp_path, substitution_mgr=manager
    )

    assert target is None


def test_a_sql_transform_reading_the_view_is_a_second_reader(tmp_path):
    reader = Action(
        name="read_sql",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        sql="SELECT * FROM v_orders_dq",
        target="v_next",
    )
    actions = _fusable([reader])

    assert _target(actions[1], actions, project_root=tmp_path) is None
