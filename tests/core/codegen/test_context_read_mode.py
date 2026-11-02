"""``GenerationContextBuilder`` carries an action's effective read mode.

Code generation computes ``effective_read_mode`` once per data_quality action
and passes it to the generator in the context.
"""

from pathlib import Path

import pytest

from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.processing.substitution import EnhancedSubstitutionManager
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


def test_context_carries_the_effective_read_mode():
    context = _build(GenerationContextBuilder(), effective_read_mode="batch")

    assert context["effective_read_mode"] == "batch"


def test_context_has_no_effective_read_mode_by_default():
    assert _build(GenerationContextBuilder())["effective_read_mode"] is None


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

    assert with_root.effective_read_mode(dq, flowgroup, manager) == "batch"
    assert without_file.effective_read_mode(dq, flowgroup, manager) == "stream"
