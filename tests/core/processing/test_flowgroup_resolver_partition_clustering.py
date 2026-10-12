"""Partitioning vs liquid clustering checked after preset merge (#297).

Validation runs on the resolved flowgroup, so a preset that supplies liquid
clustering and a flowgroup that supplies ``partition_columns`` still collide.
The error surfaces through the ``LHP-VAL-007`` "FlowGroup validation failed"
wrapper. Real validators and a real ``PresetManager`` — no mocks.
"""

from pathlib import Path

import pytest

from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.validators import ConfigValidator, SecretValidator
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext
from lhp.presets.preset_manager import PresetManager


def _resolver(tmp_path: Path, write_type: str, clustering_yaml: str):
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir()
    (presets_dir / "clustered.yaml").write_text(
        'name: clustered\nversion: "1.0"\ndefaults:\n  write_actions:\n'
        f"    {write_type}:\n{clustering_yaml}"
    )
    return FlowgroupResolutionService(
        preset_manager=PresetManager(presets_dir),
        config_validator=ConfigValidator(),
        secret_validator=SecretValidator(),
    )


def _ctx(write_type: str, **table_options) -> FlowGroupContext:
    flowgroup = FlowGroup(
        pipeline="p",
        flowgroup="fg_orders",
        presets=["clustered"],
        actions=[
            Action(
                name="load_orders",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": "SELECT 1 AS order_id"},
                target="v_orders",
            ),
            Action(
                name="write_orders",
                type=ActionType.WRITE,
                source="v_orders",
                write_target={
                    "type": write_type,
                    "catalog": "cat",
                    "schema": "sch",
                    "table": "orders",
                    **table_options,
                },
            ),
        ],
    )
    return FlowGroupContext(flowgroup=flowgroup, source_yaml=Path("pipelines/p.yaml"))


def _mgr() -> EnhancedSubstitutionManager:
    return EnhancedSubstitutionManager(substitution_file=None, env="dev")


WRITE_TYPES = ["streaming_table", "materialized_view"]
PRESET_CLUSTERING = [
    pytest.param(
        "      cluster_columns: [order_id]\n",
        ("cluster_columns", ["order_id"]),
        id="cluster_columns",
    ),
    pytest.param(
        "      cluster_by_auto: true\n",
        ("cluster_by_auto", True),
        id="cluster_by_auto",
    ),
]


@pytest.mark.unit
@pytest.mark.parametrize("write_type", WRITE_TYPES)
@pytest.mark.parametrize(("clustering_yaml", "merged"), PRESET_CLUSTERING)
class TestPresetClusteringWithFlowgroupPartitioning:
    def test_flowgroup_partition_columns_raise_val_007(
        self, tmp_path, write_type, clustering_yaml, merged
    ):
        resolver = _resolver(tmp_path, write_type, clustering_yaml)

        with pytest.raises(LHPError) as excinfo:
            resolver.process_flowgroup(
                _ctx(write_type, partition_columns=["order_date"]), _mgr()
            )

        assert excinfo.value.code == "LHP-VAL-007"
        details = excinfo.value.details
        assert (
            "cannot specify both 'partition_columns' and liquid clustering" in details
        )

    def test_preset_clustering_alone_resolves(
        self, tmp_path, write_type, clustering_yaml, merged
    ):
        resolver = _resolver(tmp_path, write_type, clustering_yaml)

        ctx_out = resolver.process_flowgroup(_ctx(write_type), _mgr())

        field, value = merged
        write_target = ctx_out.flowgroup.actions[1].write_target
        assert write_target[field] == value  # the preset was merged in
        assert "partition_columns" not in write_target
