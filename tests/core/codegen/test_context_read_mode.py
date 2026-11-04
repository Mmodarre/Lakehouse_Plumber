"""``GenerationContextBuilder`` carries an action's effective read mode.

``build`` derives ``effective_read_mode`` from the action it builds the context
for (a data_quality transform) and the registry it dispatches by.
"""

from pathlib import Path

import pytest

from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.models import Action, ActionType, FlowGroup, TransformType

pytestmark = pytest.mark.unit


def _build(builder: GenerationContextBuilder, **kwargs):
    return builder.build(
        flowgroup=FlowGroup(pipeline="p", flowgroup="fg"),
        substitution_mgr=EnhancedSubstitutionManager(),
        preset_config={},
        output_dir=None,
        source_yaml=None,
        env="dev",
        **kwargs,
    )


def _dq(**fields) -> Action:
    return Action(
        name="dq",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_orders",
        target="v_orders_dq",
        expectations_file="rules.yaml",
        **fields,
    )


def test_context_carries_the_effective_read_mode_of_a_data_quality_action():
    dq = _dq(readMode="batch")
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=[dq])

    context = GenerationContextBuilder().build(
        flowgroup,
        EnhancedSubstitutionManager(),
        {},
        None,
        None,
        "dev",
        action=dq,
        action_registry=ActionRegistry(),
    )

    assert context["effective_read_mode"] == "batch"


def test_context_has_no_effective_read_mode_by_default():
    assert _build(GenerationContextBuilder())["effective_read_mode"] is None


def test_context_has_no_effective_read_mode_for_another_action():
    load = Action(
        name="load", type=ActionType.LOAD, source="SELECT 1", target="v_orders"
    )

    context = _build(GenerationContextBuilder(), action=load)

    assert context["effective_read_mode"] is None


def test_a_data_quality_action_needs_the_registry_it_is_dispatched_by():
    with pytest.raises(TypeError, match="action_registry"):
        _build(GenerationContextBuilder(), action=_dq())


def test_effective_read_mode_reads_the_flowgroup(tmp_path: Path):
    # The producer's SQL file is found under the builder's project root.
    (tmp_path / "orders.sql").write_text("SELECT * FROM c.s.orders")
    producer = Action(
        name="orders",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.SQL,
        sql_path="orders.sql",
        target="v_orders",
    )
    dq = Action(
        name="dq",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_orders",
        target="v_orders_dq",
        expectations_file="rules.yaml",
    )
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=[producer, dq])
    manager = EnhancedSubstitutionManager()

    with_root = GenerationContextBuilder(project_root=tmp_path)
    without_file = GenerationContextBuilder(project_root=tmp_path / "elsewhere")

    registry = ActionRegistry()

    assert with_root.effective_read_mode(dq, flowgroup, manager, registry) == "batch"
    assert (
        without_file.effective_read_mode(dq, flowgroup, manager, registry) == "stream"
    )
