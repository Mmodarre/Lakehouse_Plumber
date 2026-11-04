"""A materialized view with a data_quality action fused onto it.

The view reads the data_quality action's source view, carries its expectations
(merged with any ``write_target.expectations_file``) and renders the
operational-metadata columns the data_quality generator selected for it. Real
rule files and generators, no mocks.
"""

from __future__ import annotations

from pathlib import Path
from typing import List, Optional

import pytest
import yaml

from lhp.core.codegen.context import GenerationContextBuilder
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.generators.registration import register_all
from lhp.generators.transform import DataQualityTransformGenerator
from lhp.generators.write import MaterializedViewWriteGenerator
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

_DQ_RULES = [
    {"name": "has_id", "constraint": "id IS NOT NULL", "failureAction": "fail"},
    {"name": "pos_amt", "constraint": "amount > 0", "failureAction": "drop"},
    {"name": "small_amt", "constraint": "amount < 1000", "failureAction": "warn"},
]


@pytest.fixture(autouse=True)
def _registered():
    register_all()


def _project_config() -> ProjectConfig:
    return ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_loaded_at": MetadataColumnConfig(
                    expression="F.current_timestamp()",
                    description="load time",
                    applies_to=["view"],
                ),
                "_source_name": MetadataColumnConfig(
                    expression="F.lit('orders')",
                    description="source",
                    applies_to=["view"],
                ),
            }
        ),
    )


def _flowgroup(
    mv_rules_file: Optional[str] = None, metadata: Optional[List[str]] = None
) -> FlowGroup:
    target = {
        "type": "materialized_view",
        "catalog": "gold",
        "schema": "sch",
        "table": "orders_mv",
    }
    if mv_rules_file:
        target["expectations_file"] = mv_rules_file
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
                expectations_file="dq.yaml",
                operational_metadata=metadata,
            ),
            Action(
                name="write_mv",
                type=ActionType.WRITE,
                source="v_orders_dq",
                write_target=target,
            ),
        ],
    )


def _generate(root: Path, flowgroup: FlowGroup):
    """The view's code and generator, planned and generated as code generation does."""
    builder = GenerationContextBuilder(
        project_config=_project_config(), project_root=root
    )
    manager = EnhancedSubstitutionManager()
    plan = builder.dq_fusion_plan(flowgroup, manager, {}, ActionRegistry())
    write = flowgroup.actions[2]
    context = builder.build(
        flowgroup, manager, {}, None, None, None, action=write, fusion_plan=plan
    )
    generator = MaterializedViewWriteGenerator()
    return generator.generate(write, context), generator, builder, manager


@pytest.fixture
def root(tmp_path: Path) -> Path:
    (tmp_path / "dq.yaml").write_text(yaml.safe_dump(_DQ_RULES))
    return tmp_path


def test_the_view_reads_the_dq_actions_source_with_its_three_decorators(root):
    code, *_ = _generate(root, _flowgroup())

    assert 'df = spark.read.table("v_orders")' in code
    assert "v_orders_dq" not in code
    assert '@dp.expect_all({"small_amt": "amount < 1000"})' in code
    assert '@dp.expect_all_or_drop({"pos_amt": "amount > 0"})' in code
    assert '@dp.expect_all_or_fail({"has_id": "id IS NOT NULL"})' in code
    # Below @dp.materialized_view(...), above the function: the order the
    # workspace spike proved keeps the view incremental.
    assert code.index("@dp.materialized_view") < code.index("@dp.expect_all")
    assert code.index("@dp.expect_all_or_fail") < code.index("def orders_mv")


def test_a_view_with_its_own_file_is_not_fused(root):
    # Unfused, the view's own rules see only the rows the data_quality view
    # lets through, so such a view keeps reading the data_quality view.
    (root / "mv.yaml").write_text(
        yaml.safe_dump(
            [{"name": "mv_only", "constraint": "x > 0", "failureAction": "warn"}]
        )
    )

    code, *_ = _generate(root, _flowgroup(mv_rules_file="mv.yaml"))

    assert 'df = spark.read.table("v_orders_dq")' in code
    assert code.count("@dp.expect_all(") == 1
    assert '"mv_only": "x > 0"' in code
    assert "small_amt" not in code


def test_the_view_renders_exactly_the_dq_views_metadata_columns(root):
    flowgroup = _flowgroup(metadata=["_loaded_at", "_source_name"])

    code, view_generator, builder, manager = _generate(root, flowgroup)

    # What the data_quality generator renders for the same action.
    dq_generator = DataQualityTransformGenerator()
    dq_context = builder.build(flowgroup, manager, {}, None, None, None)
    dq_code = dq_generator.generate(
        flowgroup.actions[1], {**dq_context, "spec_dir": root}
    )
    _, expected = dq_generator._get_operational_metadata(
        flowgroup.actions[1], dq_context
    )
    assert set(expected) == {"_loaded_at", "_source_name"}
    # One withColumns call, as the data_quality view renders them, so a
    # column expression sees the same dataframe in both.
    assert code.count("df = df.withColumns({") == 1
    assert "withColumn(" not in code
    for name, expression in expected.items():
        assert f"'{name}': {expression}" in code
        assert f"'{name}': {expression}" in dq_code
    assert "# Add operational metadata columns" in code
    # The imports the columns need are registered on the view's generator.
    assert "from pyspark.sql import functions as F" in view_generator.imports
    # The view itself still declares no metadata of its own.
    assert MaterializedViewWriteGenerator.renders_operational_metadata is False


def test_a_dq_action_with_no_metadata_adds_no_columns(root):
    code, *_ = _generate(root, _flowgroup())

    assert "withColumn" not in code
    assert "operational metadata" not in code


def test_a_view_that_is_not_fused_is_unchanged(root):
    flowgroup = _flowgroup()
    generator = MaterializedViewWriteGenerator()

    code = generator.generate(flowgroup.actions[2], {"project_root": root})

    assert 'df = spark.read.table("v_orders_dq")' in code
    assert "@dp.expect_all" not in code


def test_the_fused_view_matches_the_golden_baseline(root, golden):
    code, *_ = _generate(root, _flowgroup(metadata=["_loaded_at"]))

    golden(code, "write_materialized_view_fused")
