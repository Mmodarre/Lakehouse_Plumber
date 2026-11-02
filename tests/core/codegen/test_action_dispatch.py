"""``ActionDispatcher`` gives each data_quality action its effective read mode.

#344: a data_quality action with no ``readMode`` over a batch delta load used
to read the batch view with ``spark.readStream``, which Databricks rejects at
analysis time. Code generation now computes the action's read from its
flowgroup (``lhp.core.dependencies.effective_read_mode``) and passes it to the
generator.
"""

from pathlib import Path
from typing import Any, List, Optional

import pytest

from lhp.core.codegen.action_dispatch import ActionDispatcher
from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.codegen.grouping import WriteActionGrouper
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.models import Action, ActionType, FlowGroup, TransformType

pytestmark = pytest.mark.unit


def _dq(read_mode: Optional[str] = None, **fields: Any) -> Action:
    return Action(
        name="check_orders",
        type=ActionType.TRANSFORM,
        transform_type=TransformType.DATA_QUALITY,
        source="v_orders",
        target="v_orders_checked",
        readMode=read_mode,
        expectations_file="rules.yaml",
        **fields,
    )


def _load(source: Any) -> Action:
    return Action(
        name="load_orders", type=ActionType.LOAD, source=source, target="v_orders"
    )


_DELTA = {"type": "delta", "catalog": "c", "schema": "s", "table": "orders"}
_CLOUDFILES = {"type": "cloudfiles", "path": "/landing/orders", "format": "json"}


def _dq_section(tmp_path: Path, actions: List[Action]) -> str:
    (tmp_path / "rules.yaml").write_text('{"id IS NOT NULL": {"action": "drop"}}')
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
        include_tests=False,
    )
    [section] = [s for s in sections if "def v_orders_checked():" in s]
    return section


@pytest.mark.parametrize(
    ("actions", "expected"),
    [
        ([_load(_DELTA), _dq()], 'spark.read.table("v_orders")'),
        ([_load(_CLOUDFILES), _dq()], 'spark.readStream.table("v_orders")'),
        ([_load("SELECT * FROM c.s.orders"), _dq()], 'spark.read.table("v_orders")'),
        (
            [_load("SELECT * FROM STREAM(c.s.orders)"), _dq()],
            'spark.readStream.table("v_orders")',
        ),
        ([_dq()], 'spark.readStream.table("v_orders")'),
        ([_load(_DELTA), _dq("stream")], 'spark.readStream.table("v_orders")'),
        ([_load(_CLOUDFILES), _dq("batch")], 'spark.read.table("v_orders")'),
    ],
    ids=[
        "inherits-delta-batch",
        "inherits-cloudfiles-stream",
        "inherits-sql-batch",
        "inherits-sql-stream",
        "external-source-stream",
        "explicit-stream-wins",
        "explicit-batch-wins",
    ],
)
def test_data_quality_reads_with_its_effective_read_mode(
    tmp_path: Path, actions: List[Action], expected: str
):
    section = _dq_section(tmp_path, actions)

    assert f"df = {expected}" in section


def test_quarantine_reads_a_stream_over_a_batch_view(tmp_path: Path):
    dq = _dq(
        mode="quarantine",
        quarantine={"dlq_table": "c.s.dlq", "source_table": "c.s.orders"},
    )

    section = _dq_section(tmp_path, [_load(_DELTA), dq])

    assert "spark.readStream.table" in section
    assert "spark.read.table" not in section
