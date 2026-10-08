"""Operational-metadata expression check wired into flowgroup resolution (#289).

``FlowgroupResolutionService.process_flowgroup`` is the step BOTH ``lhp
validate`` and ``lhp generate`` run per flowgroup, so wiring the
``OperationalMetadataExpressionValidator`` there makes the two commands report
the same LHP-CFG-010 / LHP-CFG-070 errors. Real validators and a real
``PresetManager`` — no mocks.
"""

from pathlib import Path

import pytest

from lhp.core.codegen import OperationalMetadataService
from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.registry import ActionRegistry
from lhp.core.validators import (
    ConfigValidator,
    OperationalMetadataExpressionValidator,
    SecretValidator,
)
from lhp.errors import LHPError
from lhp.models import (
    Action,
    ActionType,
    FlowGroup,
    FlowGroupContext,
    MetadataColumnConfig,
    ProjectConfig,
    ProjectOperationalMetadataConfig,
)
from lhp.presets.preset_manager import PresetManager


def _project_config(expression: str) -> ProjectConfig:
    return ProjectConfig(
        name="p",
        version="1.0",
        operational_metadata=ProjectOperationalMetadataConfig(
            columns={
                "_source": MetadataColumnConfig(
                    expression=expression, applies_to=["view"]
                )
            }
        ),
    )


def _resolver(tmp_path: Path, project_config: ProjectConfig):
    presets_dir = tmp_path / "presets"
    presets_dir.mkdir(exist_ok=True)
    (presets_dir / "bronze.yaml").write_text(
        'name: bronze\nversion: "1.0"\ndefaults:\n  operational_metadata:\n'
        "    - _source\n"
    )
    return FlowgroupResolutionService(
        preset_manager=PresetManager(presets_dir),
        config_validator=ConfigValidator(tmp_path, project_config),
        secret_validator=SecretValidator(),
        operational_metadata_validator=OperationalMetadataExpressionValidator(
            project_config,
            ActionRegistry(),
            OperationalMetadataService().resolve_selected_columns,
        ),
    )


def _mgr(tmp_path: Path) -> EnhancedSubstitutionManager:
    sub_file = tmp_path / "dev.yaml"
    sub_file.write_text("dev:\n  fd_source: STXX\n")
    return EnhancedSubstitutionManager(substitution_file=sub_file, env="dev")


def _ctx(*, presets=None, operational_metadata=None) -> FlowGroupContext:
    flowgroup = FlowGroup(
        pipeline="repro",
        flowgroup="repro_fg",
        presets=presets or [],
        operational_metadata=operational_metadata,
        actions=[
            Action(
                name="load_raw",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": "SELECT 1 AS id"},
                target="v_raw",
            ),
            Action(
                name="write_raw",
                type=ActionType.WRITE,
                source="v_raw",
                write_target={
                    "type": "streaming_table",
                    "catalog": "c",
                    "schema": "s",
                    "table": "t",
                },
            ),
        ],
    )
    return FlowGroupContext(flowgroup=flowgroup, source_yaml=Path("pipelines/p.yaml"))


@pytest.mark.unit
class TestResolverOperationalMetadataCheck:
    def test_undefined_token_in_selected_column_raises_cfg_010(self, tmp_path):
        resolver = _resolver(tmp_path, _project_config("F.lit('${fd_missing}')"))
        with pytest.raises(LHPError) as excinfo:
            resolver.process_flowgroup(
                _ctx(operational_metadata=["_source"]), _mgr(tmp_path)
            )
        assert excinfo.value.code == "LHP-CFG-010"
        assert "_source" in str(excinfo.value)

    def test_preset_selection_reaches_the_check(self, tmp_path):
        resolver = _resolver(tmp_path, _project_config("F.lit('${fd_missing}')"))
        with pytest.raises(LHPError) as excinfo:
            resolver.process_flowgroup(_ctx(presets=["bronze"]), _mgr(tmp_path))
        assert excinfo.value.code == "LHP-CFG-010"

    def test_resolvable_token_passes(self, tmp_path):
        resolver = _resolver(tmp_path, _project_config("F.lit('${fd_source}')"))
        ctx_out = resolver.process_flowgroup(
            _ctx(operational_metadata=["_source"]), _mgr(tmp_path)
        )
        assert ctx_out.flowgroup.flowgroup == "repro_fg"

    def test_validate_config_false_skips_the_check(self, tmp_path):
        """The ``lhp dag`` path (no codegen) does not resolve expressions."""
        resolver = _resolver(tmp_path, _project_config("F.lit('${fd_missing}')"))
        ctx_out = resolver.process_flowgroup(
            _ctx(operational_metadata=["_source"]),
            _mgr(tmp_path),
            validate_config=False,
        )
        assert ctx_out.flowgroup.flowgroup == "repro_fg"
