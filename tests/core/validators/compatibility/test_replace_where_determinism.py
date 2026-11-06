"""``ReplaceWhereDeterminismValidator``: the LHP-VAL-073 warning (#346).

A non-deterministic operational-metadata column (``current_timestamp()``,
``now()``, ``uuid()``, ...) computed anywhere upstream of a ``replace_where``
write inside the flowgroup makes Databricks run a ``COMPLETE_REPLACE_WHERE``
(a full rewrite of the window) on every refresh. The validator never fails the
flowgroup and records nothing: it returns ONE warning message per flowgroup,
listing every offending (write, upstream action, column) triple, because the
warning channel its caller records it on dedups by ``(code, file)``. The
recording is pinned in ``tests/core/processing/``.
"""

import pickle
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Union

import pytest

from lhp.core.codegen import OperationalMetadataService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators.compatibility import ReplaceWhereDeterminismValidator
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    TransformType,
)

_PROCESSING_TIMESTAMP = {"_processing_timestamp": "F.current_timestamp()"}


def _project(columns: Dict[str, Union[str, MetadataColumnConfig]]) -> ProjectConfig:
    resolved = {
        name: (
            spec
            if isinstance(spec, MetadataColumnConfig)
            else MetadataColumnConfig(expression=spec, applies_to=["view"])
        )
        for name, spec in columns.items()
    }
    return ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(columns=resolved),
    )


class _RecordingResolver:
    """Delegates to the render path's resolver and records each action name."""

    def __init__(self) -> None:
        self.calls: List[str] = []
        self._real = OperationalMetadataService().resolve_selected_columns

    def __call__(
        self,
        action: Action,
        flowgroup: FlowGroup,
        preset_config: Dict[str, Any],
        project_config: Optional[ProjectConfig],
        substitution_mgr: EnhancedSubstitutionManager,
    ) -> Mapping[str, str]:
        self.calls.append(action.name)
        return self._real(
            action, flowgroup, preset_config, project_config, substitution_mgr
        )


def _validator(
    project_config: Optional[ProjectConfig], resolver=None
) -> ReplaceWhereDeterminismValidator:
    """Wired the way the composition root (``layers.py``) wires it."""
    return ReplaceWhereDeterminismValidator(
        project_config,
        ActionRegistry(),
        resolver or OperationalMetadataService().resolve_selected_columns,
    )


def _mgr(tmp_path: Path) -> EnhancedSubstitutionManager:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  catalog: c\n")
    return EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")


def _load(
    operational_metadata: Union[bool, List[str], None] = None,
    name: str = "load_raw",
) -> Action:
    return Action(
        name=name,
        type=ActionType.LOAD,
        source={"type": "delta", "table": "c.bronze.orders"},
        readMode="batch",
        target=f"v_{name}",
        operational_metadata=operational_metadata,
    )


def _write(
    source: str = "v_load_raw",
    *,
    name: str = "write_rw",
    table: str = "orders_recent",
    mode: str = "replace_where",
) -> Action:
    write_target: Dict[str, Any] = {
        "type": "streaming_table",
        "catalog": "c",
        "schema": "silver",
        "table": table,
        "mode": mode,
    }
    if mode == "replace_where":
        write_target["replace_where_config"] = {
            "replace_where": "order_date >= date_add(current_date(), -7)"
        }
    return Action(
        name=name, type=ActionType.WRITE, source=source, write_target=write_target
    )


def _flowgroup(*actions: Action, operational_metadata=None) -> FlowGroup:
    return FlowGroup(
        pipeline="repro",
        flowgroup="repro_fg",
        actions=list(actions),
        operational_metadata=operational_metadata,
    )


def _finding(
    validator: ReplaceWhereDeterminismValidator,
    flowgroup: FlowGroup,
    tmp_path: Path,
) -> Optional[str]:
    """The warning message the validator returns for ``flowgroup``."""
    return validator.validate(flowgroup, _mgr(tmp_path), {})


def _single_message(finding: Optional[str]) -> str:
    """The one message of the flowgroup; it never repeats its header."""
    assert isinstance(finding, str), finding
    assert finding.count("COMPLETE_REPLACE_WHERE") == 1, finding
    return finding


@pytest.mark.unit
class TestReplaceWhereDeterminismValidator:
    def test_processing_timestamp_on_load_warns(self, tmp_path):
        validator = _validator(_project(_PROCESSING_TIMESTAMP))
        flowgroup = _flowgroup(
            _load(operational_metadata=["_processing_timestamp"]), _write()
        )

        message = _single_message(_finding(validator, flowgroup, tmp_path))

        assert (
            "write 'write_rw' <- action 'load_raw': column '_processing_timestamp'"
            in message
        )
        assert "F.current_timestamp()" in message
        assert "COMPLETE_REPLACE_WHERE" in message
        assert "streaming-table-replace-where" in message

    def test_metadata_on_intermediate_transform_warns(self, tmp_path):
        validator = _validator(_project(_PROCESSING_TIMESTAMP))
        transform = Action(
            name="shape_orders",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SQL,
            source="v_load_raw",
            target="v_shaped",
            sql="SELECT * FROM v_load_raw",
            operational_metadata=["_processing_timestamp"],
        )
        flowgroup = _flowgroup(_load(), transform, _write(source="v_shaped"))

        message = _single_message(_finding(validator, flowgroup, tmp_path))

        assert (
            "write 'write_rw' <- action 'shape_orders': column "
            "'_processing_timestamp'" in message
        )
        assert "action 'load_raw'" not in message

    def test_expr_now_warns(self, tmp_path):
        validator = _validator(_project({"_loaded_at": "F.expr('now()')"}))
        flowgroup = _flowgroup(_load(operational_metadata=["_loaded_at"]), _write())

        message = _single_message(_finding(validator, flowgroup, tmp_path))

        assert "column '_loaded_at'" in message

    def test_record_hash_only_no_warning(self, tmp_path):
        validator = _validator(
            _project({"_record_hash": "F.xxhash64(*[F.col(c) for c in df.columns])"})
        )
        flowgroup = _flowgroup(_load(operational_metadata=["_record_hash"]), _write())

        assert _finding(validator, flowgroup, tmp_path) is None

    def test_action_level_metadata_false_no_warning(self, tmp_path):
        validator = _validator(_project(_PROCESSING_TIMESTAMP))
        flowgroup = _flowgroup(
            _load(operational_metadata=False),
            _write(),
            operational_metadata=["_processing_timestamp"],
        )

        assert _finding(validator, flowgroup, tmp_path) is None

    def test_external_source_no_warning(self, tmp_path):
        """The write reads a view no action in this flowgroup produces; the
        traversal stops there, so an unrelated timestamped load is not
        upstream of it."""
        validator = _validator(_project(_PROCESSING_TIMESTAMP))
        flowgroup = _flowgroup(
            _load(operational_metadata=["_processing_timestamp"]),
            _write(source="v_from_another_flowgroup"),
        )

        assert _finding(validator, flowgroup, tmp_path) is None

    def test_no_replace_where_skips_resolution(self, tmp_path):
        resolver = _RecordingResolver()
        validator = _validator(_project(_PROCESSING_TIMESTAMP), resolver)
        flowgroup = _flowgroup(
            _load(operational_metadata=["_processing_timestamp"]),
            _write(mode="standard"),
        )

        assert _finding(validator, flowgroup, tmp_path) is None
        assert resolver.calls == []

    def test_two_offending_writes_single_record_listing_both(self, tmp_path):
        validator = _validator(_project(_PROCESSING_TIMESTAMP))
        flowgroup = _flowgroup(
            _load(operational_metadata=["_processing_timestamp"], name="load_a"),
            _load(operational_metadata=["_processing_timestamp"], name="load_b"),
            _write(source="v_load_a", name="write_a", table="t_a"),
            _write(source="v_load_b", name="write_b", table="t_b"),
        )

        message = _single_message(_finding(validator, flowgroup, tmp_path))

        assert "write 'write_a' <- action 'load_a'" in message
        assert "write 'write_b' <- action 'load_b'" in message


@pytest.mark.unit
class TestNonDeterministicExpressionForms:
    @pytest.mark.parametrize(
        "expression",
        [
            "F.current_timestamp()",
            "F.expr('current_timestamp')",
            "F.expr('CURRENT_DATE')",
            "F.expr('localtimestamp()')",
            "F.rand()",
            "F.expr('uuid()')",
            "F.monotonically_increasing_id()",
            "F.input_file_name()",
            "F.expr('input_file_block_start()')",
            "F.spark_partition_id()",
            "F.unix_timestamp()",
        ],
    )
    def test_matches(self, tmp_path, expression):
        validator = _validator(_project({"_col": expression}))
        flowgroup = _flowgroup(_load(operational_metadata=["_col"]), _write())

        assert _finding(validator, flowgroup, tmp_path) is not None

    @pytest.mark.parametrize(
        "expression",
        [
            "F.col('_metadata.file_path')",
            "F.unix_timestamp(F.col('event_time'))",
            "F.expr('operand(1)')",
        ],
    )
    def test_deterministic_does_not_match(self, tmp_path, expression):
        validator = _validator(_project({"_col": expression}))
        flowgroup = _flowgroup(_load(operational_metadata=["_col"]), _write())

        assert _finding(validator, flowgroup, tmp_path) is None


@pytest.mark.unit
def test_pickles_for_the_spawn_worker_boundary():
    """``FlowgroupResolutionService`` (and so this validator) is pickled into
    ``spawn`` workers."""
    validator = _validator(_project(_PROCESSING_TIMESTAMP))
    assert isinstance(
        pickle.loads(pickle.dumps(validator)), ReplaceWhereDeterminismValidator
    )
