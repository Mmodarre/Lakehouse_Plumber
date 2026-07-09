"""``validate_config`` gating on the flowgroup resolver.

``validate_config=False`` (the ``lhp dag``/``deps`` path) skips ONLY the
per-flowgroup config validation pass (``LHP-VAL-007``); secret validation
(``LHP-VAL-008``) always runs. The default (True) keeps generate/validate
behavior unchanged. Real validators throughout — no mocks.
"""

from pathlib import Path

import pytest

from lhp.core.processing.flowgroup_resolver import FlowgroupResolutionService
from lhp.core.processing.substitution import EnhancedSubstitutionManager
from lhp.core.validators import ConfigValidator, SecretValidator
from lhp.errors import LHPError
from lhp.models import Action, ActionType, FlowGroup, FlowGroupContext


def _resolver() -> FlowgroupResolutionService:
    return FlowgroupResolutionService(
        config_validator=ConfigValidator(),
        secret_validator=SecretValidator(),
    )


def _mgr() -> EnhancedSubstitutionManager:
    return EnhancedSubstitutionManager(substitution_file=None, env="dev")


def _config_invalid_flowgroup(sql: str = "SELECT 1") -> FlowGroup:
    """Pydantic-valid but config-invalid: duplicate action names."""
    return FlowGroup(
        pipeline="p_dup",
        flowgroup="fg_dup",
        actions=[
            Action(
                name="dup",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": sql},
                target="v_a",
            ),
            Action(
                name="dup",
                type=ActionType.LOAD,
                source={"type": "sql", "sql": "SELECT 2"},
                target="v_b",
            ),
        ],
    )


def _ctx(flowgroup: FlowGroup) -> FlowGroupContext:
    return FlowGroupContext(
        flowgroup=flowgroup, source_yaml=Path("pipelines/fg_dup.yaml")
    )


@pytest.mark.unit
class TestValidateConfigGate:
    def test_validate_config_false_skips_val_007(self):
        ctx_out = _resolver().process_flowgroup(
            _ctx(_config_invalid_flowgroup()), _mgr(), validate_config=False
        )
        assert isinstance(ctx_out.flowgroup, FlowGroup)
        assert [a.name for a in ctx_out.flowgroup.actions] == ["dup", "dup"]

    def test_default_still_raises_val_007(self):
        with pytest.raises(LHPError) as excinfo:
            _resolver().process_flowgroup(_ctx(_config_invalid_flowgroup()), _mgr())
        assert excinfo.value.code == "LHP-VAL-007"

    def test_resolve_forwards_validate_config(self):
        ctx_out = _resolver().resolve(
            _ctx(_config_invalid_flowgroup()), _mgr(), validate_config=False
        )
        assert [a.name for a in ctx_out.flowgroup.actions] == ["dup", "dup"]

    def test_secret_validation_still_runs_with_validate_config_false(self):
        flowgroup = _config_invalid_flowgroup(sql="SELECT '${secret:bad.scope/my_key}'")
        with pytest.raises(LHPError) as excinfo:
            _resolver().process_flowgroup(
                _ctx(flowgroup), _mgr(), validate_config=False
            )
        assert excinfo.value.code == "LHP-VAL-008"
