"""``OperationalMetadataExpressionValidator`` — the validate-path half of #289.

The validator resolves exactly the expressions the render path would resolve
for the flowgroup: only actions whose generator renders operational metadata
(the generator's ``renders_operational_metadata`` declaration, read through
the ``ActionRegistry``), and for those the same selection (preset + flowgroup
+ action levels), ``view`` target type, ``applies_to`` filter and per-action
context tokens, through the shared ``resolve_metadata_expression``.
"""

import pickle
from pathlib import Path
from typing import Dict, List, Optional, Union

import pytest

from lhp.core.codegen import OperationalMetadataService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators import OperationalMetadataExpressionValidator
from lhp.errors import LHPError
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
    TransformType,
)


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


def _validator(
    project_config: Optional[ProjectConfig],
) -> OperationalMetadataExpressionValidator:
    """Wired the way the composition root (``layers.py``) wires it."""
    return OperationalMetadataExpressionValidator(
        project_config,
        ActionRegistry(),
        OperationalMetadataService().resolve_selected_columns,
    )


def _mgr(tmp_path: Path, body: str = "dev:\n  fd_source: STXX\n"):
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text(body)
    return EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")


def _load(
    source: Optional[dict] = None,
    operational_metadata: Union[bool, List[str], None] = None,
    name: str = "load_raw",
) -> Action:
    return Action(
        name=name,
        type=ActionType.LOAD,
        source=source or {"type": "sql", "sql": "SELECT 1 AS id"},
        target=f"v_{name}",
        operational_metadata=operational_metadata,
    )


def _write(
    write_target: dict, operational_metadata: Union[List[str], None] = None
) -> Action:
    return Action(
        name="write_raw",
        type=ActionType.WRITE,
        source="v_load_raw",
        write_target=write_target,
        operational_metadata=operational_metadata,
    )


_STREAMING_TABLE = {
    "type": "streaming_table",
    "catalog": "c",
    "schema": "s",
    "table": "t",
}
_MATERIALIZED_VIEW = {
    "type": "materialized_view",
    "catalog": "c",
    "schema": "s",
    "table": "t",
}
_DELTA_SINK = {
    "type": "sink",
    "sink_type": "delta",
    "sink_name": "s",
    "options": {"tableName": "c.s.t"},
}


def _flowgroup(*actions: Action, operational_metadata=None) -> FlowGroup:
    return FlowGroup(
        pipeline="repro",
        flowgroup="repro_fg",
        actions=list(actions),
        operational_metadata=operational_metadata,
    )


@pytest.mark.unit
class TestOperationalMetadataExpressionValidator:
    def test_selected_column_with_env_token_passes(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_source}')"}))
        validator.validate(
            _flowgroup(_load(operational_metadata=["_source"])),
            _mgr(tmp_path),
            {},
        )

    def test_selected_column_with_undefined_token_is_cfg_010(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(_load(operational_metadata=["_source"])),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.code == "LHP-CFG-010"
        assert "_source" in str(excinfo.value)

    def test_selected_column_with_secret_is_cfg_070(self, tmp_path):
        validator = _validator(_project({"_key": "F.lit('${secret:scope/key}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(_load(), operational_metadata=["_key"]),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.code == "LHP-CFG-070"

    def test_error_names_the_pipeline_and_flowgroup(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(_load(operational_metadata=["_source"])),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.context["Pipeline"] == "repro"
        assert excinfo.value.context["FlowGroup"] == "repro_fg"

    def test_unselected_column_is_not_checked(self, tmp_path):
        validator = _validator(
            _project(
                {
                    "_source": "F.lit('${fd_source}')",
                    "_unused": "F.lit('${fd_missing}')",
                }
            )
        )
        validator.validate(
            _flowgroup(_load(operational_metadata=["_source"])),
            _mgr(tmp_path),
            {},
        )

    def test_action_level_false_disables_the_check(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        validator.validate(
            _flowgroup(
                _load(operational_metadata=False), operational_metadata=["_source"]
            ),
            _mgr(tmp_path),
            {},
        )

    def test_preset_level_selection_is_checked(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(_load()),
                _mgr(tmp_path),
                {"operational_metadata": ["_source"]},
            )
        assert excinfo.value.code == "LHP-CFG-010"

    def test_column_not_applying_to_view_is_not_checked(self, tmp_path):
        """The render path only resolves columns whose ``applies_to`` includes
        ``view``; the validator matches it."""
        validator = _validator(
            _project(
                {
                    "_st_only": MetadataColumnConfig(
                        expression="F.lit('${fd_missing}')",
                        applies_to=["streaming_table"],
                    )
                }
            )
        )
        validator.validate(
            _flowgroup(_load(operational_metadata=["_st_only"])),
            _mgr(tmp_path),
            {},
        )

    def test_source_table_is_a_context_token_for_delta_loads(self, tmp_path):
        validator = _validator(_project({"_source_table": "F.lit('${source_table}')"}))
        validator.validate(
            _flowgroup(
                _load(
                    source={"type": "delta", "table": "src.customers"},
                    operational_metadata=["_source_table"],
                )
            ),
            _mgr(tmp_path),
            {},
        )

    def test_source_table_is_unresolved_for_other_loads(self, tmp_path):
        validator = _validator(_project({"_source_table": "F.lit('${source_table}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(_load(operational_metadata=["_source_table"])),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.code == "LHP-CFG-010"

    def test_project_without_operational_metadata_passes(self, tmp_path):
        validator = _validator(None)
        validator.validate(
            _flowgroup(_load(operational_metadata=["_pipeline_name"])),
            _mgr(tmp_path),
            {},
        )

    def test_pickles_for_the_spawn_worker_boundary(self):
        """``FlowgroupResolutionService`` (and so this validator) is pickled
        into ``spawn`` workers."""
        validator = _validator(_project({"_source": "F.lit('${fd_source}')"}))
        assert isinstance(
            pickle.loads(pickle.dumps(validator)),
            OperationalMetadataExpressionValidator,
        )


@pytest.mark.unit
class TestOnlyRenderedSelectionsAreChecked:
    """Code generation renders metadata columns only for actions whose
    generator declares it; a column selected anywhere else never reaches the
    generated code, so it must not fail validation (or generation)."""

    @pytest.mark.parametrize(
        "write_target",
        [_STREAMING_TABLE, _MATERIALIZED_VIEW],
        ids=["streaming_table", "materialized_view"],
    )
    def test_table_write_selection_is_not_checked(self, tmp_path, write_target):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        validator.validate(
            _flowgroup(_load(), _write(write_target, operational_metadata=["_source"])),
            _mgr(tmp_path),
            {},
        )

    def test_test_action_selection_is_not_checked(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        test_action = Action(
            name="check_rows",
            type=ActionType.TEST,
            test_type="row_count",
            source=["v_a", "v_b"],
        )
        validator.validate(
            _flowgroup(test_action, operational_metadata=["_source"]),
            _mgr(tmp_path),
            {},
        )

    def test_schema_transform_selection_is_not_checked(self, tmp_path):
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        schema_transform = Action(
            name="cast_columns",
            type=ActionType.TRANSFORM,
            transform_type=TransformType.SCHEMA,
            source="v_load_raw",
            target="v_cast",
            schema_inline="id: BIGINT",
            operational_metadata=["_source"],
        )
        validator.validate(_flowgroup(_load(), schema_transform), _mgr(tmp_path), {})

    def test_sink_write_selection_is_checked(self, tmp_path):
        """Sink writes do render metadata columns, so their selection is checked."""
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(
                    _load(), _write(_DELTA_SINK, operational_metadata=["_source"])
                ),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.code == "LHP-CFG-010"

    def test_flowgroup_selection_is_checked_on_the_rendering_action(self, tmp_path):
        """A flowgroup-level selection reaches every action; the load renders it."""
        validator = _validator(_project({"_source": "F.lit('${fd_missing}')"}))
        with pytest.raises(LHPError) as excinfo:
            validator.validate(
                _flowgroup(
                    _load(), _write(_STREAMING_TABLE), operational_metadata=["_source"]
                ),
                _mgr(tmp_path),
                {},
            )
        assert excinfo.value.code == "LHP-CFG-010"
