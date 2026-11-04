"""``plan_dq_fusion`` and the context builder's fusion plan.

The plan names which data_quality actions fuse into which materialized view,
and carries the operational-metadata columns the data_quality generator itself
renders, so the view renders exactly what validation checked under the
data_quality action. Real generators, no mocks.
"""

from __future__ import annotations

from typing import Any, Optional

import pytest

from lhp.core.codegen import DqFusionPlan
from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.codegen.dq_fusion import plan_dq_fusion
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.generators.registration import register_all
from lhp.generators.transform import DataQualityTransformGenerator
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    TransformType,
)

pytestmark = pytest.mark.unit

_VIEWS = ["view"]


@pytest.fixture(autouse=True)
def _registered():
    register_all()


def _actions(
    dq_read_mode: Optional[str] = None, metadata: Optional[list] = None
) -> list:
    return [
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
            expectations_file="rules.yaml",
            readMode=dq_read_mode,
            operational_metadata=metadata,
        ),
        Action(
            name="write_mv",
            type=ActionType.WRITE,
            source="v_orders_dq",
            write_target={
                "type": "materialized_view",
                "catalog": "c",
                "schema": "s",
                "table": "orders_mv",
            },
        ),
    ]


def _project_config() -> ProjectConfig:
    return ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_loaded_at": MetadataColumnConfig(
                    expression="F.current_timestamp()",
                    description="load time",
                    applies_to=_VIEWS,
                ),
                "_row_hash": MetadataColumnConfig(
                    expression="F.xxhash64(*[F.col(c) for c in df.columns])",
                    description="hash",
                    applies_to=_VIEWS,
                    additional_imports=["from pyspark.sql.functions import hash"],
                ),
            }
        ),
    )


def test_a_fusable_action_is_planned_with_its_view_and_source():
    actions = _actions()

    plan = plan_dq_fusion(actions, registry=ActionRegistry())

    [fused] = plan.fusions
    assert fused.dq_action is actions[1]
    assert fused.write_action is actions[2]
    assert fused.source_view == "v_orders"
    assert fused.metadata_columns == {}
    assert fused.metadata_imports == ()


def test_for_write_finds_the_fusion_by_the_writes_name():
    actions = _actions()
    plan = plan_dq_fusion(actions, registry=ActionRegistry())

    copy = actions[2].model_copy(deep=True)

    assert plan.for_write(copy) is plan.fusions[0]
    assert plan.for_write(actions[0]) is None


@pytest.mark.parametrize("read_mode", ["stream"])
def test_nothing_is_planned_for_an_action_that_does_not_fuse(read_mode):
    assert (
        plan_dq_fusion(_actions(dq_read_mode=read_mode), registry=ActionRegistry())
        == DqFusionPlan()
    )


def test_the_plan_is_frozen():
    plan = plan_dq_fusion(_actions(), registry=ActionRegistry())

    with pytest.raises(AttributeError):
        plan.fusions = ()  # type: ignore[misc]  # intentional: asserts the plan is frozen


def _builder_plan(tmp_path, actions, project_config=None) -> DqFusionPlan:
    builder = GenerationContextBuilder(
        project_config=project_config, project_root=tmp_path
    )
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    return builder.dq_fusion_plan(
        flowgroup, EnhancedSubstitutionManager(), {}, ActionRegistry()
    )


def test_the_plan_carries_exactly_the_columns_the_dq_generator_renders(tmp_path):
    actions = _actions(metadata=["_loaded_at", "_row_hash"])
    config = _project_config()
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    builder = GenerationContextBuilder(project_config=config, project_root=tmp_path)

    plan = builder.dq_fusion_plan(
        flowgroup, EnhancedSubstitutionManager(), {}, ActionRegistry()
    )

    # What the data_quality generator renders for that action.
    generator = DataQualityTransformGenerator()
    context = builder.build(
        flowgroup, EnhancedSubstitutionManager(), {}, None, None, None
    )
    _, expected = generator._get_operational_metadata(actions[1], context)
    [fused] = plan.fusions
    assert dict(fused.metadata_columns) == dict(expected)
    assert set(fused.metadata_columns) == {"_loaded_at", "_row_hash"}
    assert set(fused.metadata_imports) == {
        "from pyspark.sql import functions as F",
        "from pyspark.sql.functions import hash",
    }


def test_a_dq_action_without_metadata_plans_no_columns(tmp_path):
    [fused] = _builder_plan(tmp_path, _actions(), _project_config()).fusions

    assert fused.metadata_columns == {}
    assert fused.metadata_imports == ()


def test_the_builder_plans_nothing_when_no_action_fuses(tmp_path):
    plan = _builder_plan(tmp_path, _actions(dq_read_mode="stream"))

    assert plan.fusions == ()


def test_build_carries_the_fused_action_for_the_write_in_the_context(tmp_path):
    builder = GenerationContextBuilder(project_root=tmp_path)
    actions = _actions()
    plan = plan_dq_fusion(actions, registry=ActionRegistry())
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    manager = EnhancedSubstitutionManager()
    write = actions[2].model_copy(deep=True)

    fused = builder.build(
        flowgroup, manager, {}, None, None, None, action=write, fusion_plan=plan
    )
    other_action = builder.build(
        flowgroup, manager, {}, None, None, None, action=actions[0], fusion_plan=plan
    )
    no_action = builder.build(flowgroup, manager, {}, None, None, None)

    assert fused["fused_dq"] is plan.fusions[0]
    assert other_action["fused_dq"] is None
    assert no_action["fused_dq"] is None


class _AllStream(ActionRegistry):
    """A registry whose every action reads a stream, unlike the real ones."""

    def consumer_read_mode(self, action: Any) -> str:
        return "stream"


def test_effective_read_mode_uses_the_registry_it_is_given(tmp_path):
    # A spawned worker's default registry holds no generators, so the context
    # builder must read each action's generator through the registry the
    # dispatcher passes it.
    actions = _actions()
    flowgroup = FlowGroup(pipeline="p", flowgroup="fg", actions=actions)
    builder = GenerationContextBuilder(project_root=tmp_path)
    manager = EnhancedSubstitutionManager()

    default = builder.effective_read_mode(
        actions[1], flowgroup, manager, ActionRegistry()
    )
    given = builder.effective_read_mode(
        actions[1], flowgroup, manager, action_registry=_AllStream()
    )

    assert default == "batch"
    assert given == "stream"


def test_a_view_with_an_expectations_file_of_its_own_is_planned_nothing():
    actions = _actions()
    actions[2] = actions[2].model_copy(deep=True)
    actions[2].write_target["expectations_file"] = "mv_rules.yaml"

    assert plan_dq_fusion(actions, registry=ActionRegistry()) == DqFusionPlan()


def _view_code(tmp_path, include_tests: bool) -> str:
    """The generated code of the materialized view that reads the DQ view, in
    a flowgroup that also has a test action reading that DQ view."""
    from lhp.core.codegen.action_dispatch import ActionDispatcher
    from lhp.core.codegen.grouping import WriteActionGrouper

    (tmp_path / "rules.yaml").write_text('{"id IS NOT NULL": {"action": "drop"}}')
    actions = _actions()
    actions.append(
        Action(
            name="check_dq_view",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_orders_dq", "v_orders"],
            target="tmp_test_row_count",
        )
    )
    dispatcher = ActionDispatcher(
        action_registry=ActionRegistry(),
        grouping=WriteActionGrouper(),
        context_builder=GenerationContextBuilder(project_root=tmp_path),
    )
    sections, _, _ = dispatcher.generate_action_sections(
        FlowGroup(pipeline="p", flowgroup="fg", actions=actions),
        actions,
        EnhancedSubstitutionManager(),
        {},
        None,
        None,
        "dev",
        include_tests=include_tests,
    )
    [view] = [s for s in sections if "@dp.materialized_view" in s]
    return view


def test_fusion_is_the_same_with_and_without_the_test_actions(tmp_path):
    without_tests = _view_code(tmp_path, include_tests=False)
    with_tests = _view_code(tmp_path, include_tests=True)

    assert with_tests == without_tests
    assert 'spark.read.table("v_orders")' in with_tests
    assert "@dp.expect_all_or_drop" in with_tests
