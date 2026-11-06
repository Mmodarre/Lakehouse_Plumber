"""The LHP-VAL-073 warning recorded by flowgroup resolution (#346).

:class:`~lhp.core.validators.ReplaceWhereDeterminismValidator` is pure: it
returns its warning message. ``FlowgroupResolutionService.process_flowgroup``,
the step both ``lhp validate`` and ``lhp generate`` run per flowgroup inside the
worker's warning scope, records it as ONE
:class:`~lhp.models.processing.ValidationWarningRecord` per flowgroup. Real
validators, no mocks.
"""

from pathlib import Path
from typing import Any, Dict, List, Tuple

import pytest

from lhp.core.codegen import OperationalMetadataService
from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators import (
    ConfigValidator,
    ReplaceWhereDeterminismValidator,
    SecretValidator,
)
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    FlowGroupContext,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
)
from lhp.models.processing import ValidationWarningRecord
from lhp.models.run_warnings import collect_deprecations, drain_deprecations

_SOURCE_YAML = Path("pipelines/repro/repro_fg.yaml")


def _resolver(tmp_path: Path) -> FlowgroupResolutionService:
    project_config = ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_processing_timestamp": MetadataColumnConfig(
                    expression="F.current_timestamp()", applies_to=["view"]
                )
            }
        ),
    )
    return FlowgroupResolutionService(
        config_validator=ConfigValidator(tmp_path, project_config),
        secret_validator=SecretValidator(),
        replace_where_determinism_validator=ReplaceWhereDeterminismValidator(
            project_config,
            ActionRegistry(),
            OperationalMetadataService().resolve_selected_columns,
        ),
    )


def _mgr(tmp_path: Path) -> EnhancedSubstitutionManager:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: c\n")
    return EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")


def _ctx(suffixes: Tuple[str, ...], metadata: List[str]) -> FlowGroupContext:
    actions: List[Action] = []
    for suffix in suffixes:
        write_target: Dict[str, Any] = {
            "type": "streaming_table",
            "mode": "replace_where",
            "catalog": "c",
            "schema": "silver",
            "table": f"orders_{suffix}",
            "replace_where_config": {
                "replace_where": "order_date >= date_add(current_date(), -7)"
            },
        }
        actions += [
            Action(
                name=f"load_{suffix}",
                type=ActionType.LOAD,
                readMode="batch",
                source={
                    "type": "delta",
                    "catalog": "c",
                    "schema": "bronze",
                    "table": f"orders_{suffix}",
                },
                target=f"v_{suffix}",
                operational_metadata=metadata,
            ),
            Action(
                name=f"write_{suffix}",
                type=ActionType.WRITE,
                source=f"v_{suffix}",
                write_target=write_target,
            ),
        ]
    flowgroup = FlowGroup(pipeline="repro", flowgroup="repro_fg", actions=actions)
    return FlowGroupContext(flowgroup=flowgroup, source_yaml=_SOURCE_YAML)


def _resolve_in_worker_scope(
    tmp_path: Path, ctx: FlowGroupContext
) -> Tuple[object, ...]:
    with collect_deprecations(file=_SOURCE_YAML, flowgroup="repro_fg") as collector:
        _resolver(tmp_path).process_flowgroup(ctx, _mgr(tmp_path))
    return drain_deprecations(collector)


@pytest.mark.unit
class TestResolverRecordsReplaceWhereDeterminismWarning:
    def test_two_offending_writes_record_one_warning_naming_both(self, tmp_path):
        records = _resolve_in_worker_scope(
            tmp_path, _ctx(("a", "b"), ["_processing_timestamp"])
        )

        assert len(records) == 1, records
        (record,) = records
        assert isinstance(record, ValidationWarningRecord)
        assert record.code == "LHP-VAL-073"
        assert record.file == _SOURCE_YAML
        assert record.flowgroup == "repro_fg"
        assert "write 'write_a' <- action 'load_a'" in record.message
        assert "write 'write_b' <- action 'load_b'" in record.message

    def test_deterministic_flowgroup_records_nothing(self, tmp_path):
        assert _resolve_in_worker_scope(tmp_path, _ctx(("a",), [])) == ()
