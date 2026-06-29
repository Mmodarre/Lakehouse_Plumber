"""Tests for the developer-sandbox models.

Covers the team policy (``SandboxConfig``), the personal profile
(``SandboxProfile``), the ``ProjectConfig.sandbox`` wiring, and the internal
run DTOs (``SandboxRunConfig`` / ``SandboxWarningRecord``) including their
pickle round-trips and acceptance on ``FlowgroupOutcome.warnings``.
"""

import pickle
from dataclasses import FrozenInstanceError
from pathlib import Path

import pytest
from pydantic import ValidationError

from lhp.models import ProjectConfig, SandboxConfig, SandboxProfile
from lhp.models.processing import (
    DeprecationWarningRecord,
    FlowgroupOutcome,
    SandboxRunConfig,
    SandboxWarningRecord,
)


class TestSandboxConfig:
    def test_defaults(self):
        cfg = SandboxConfig()
        assert cfg.strategy == "table"
        assert cfg.table_pattern == "{namespace}_{table}"
        assert cfg.allowed_envs is None

    def test_strategy_table_accepted(self):
        assert SandboxConfig(strategy="table").strategy == "table"

    @pytest.mark.parametrize("strategy", ["schema", "catalog", "TABLE", ""])
    def test_strategy_literal_rejects_unknown(self, strategy):
        with pytest.raises(ValidationError):
            SandboxConfig(strategy=strategy)

    def test_allowed_envs_empty_list_permitted_by_model(self):
        # The model deliberately permits [] — rejecting it (CFG_062) is the
        # parser's job, not the model's.
        assert SandboxConfig(allowed_envs=[]).allowed_envs == []

    def test_allowed_envs_list(self):
        cfg = SandboxConfig(allowed_envs=["dev", "tst"])
        assert cfg.allowed_envs == ["dev", "tst"]


class TestSandboxConfigTablePattern:
    @pytest.mark.parametrize(
        "pattern",
        [
            "{namespace}_{table}",  # default prefix style
            "{table}_{namespace}",  # suffix style
            "{namespace}{table}",  # no literal text at all
            "sbx_{namespace}_{table}_v2",  # extra identifier literals
        ],
    )
    def test_accepted(self, pattern):
        assert SandboxConfig(table_pattern=pattern).table_pattern == pattern

    @pytest.mark.parametrize(
        "pattern",
        [
            "{namespace}",  # missing {table}
            "{table}",  # missing {namespace}
            "namespace_table",  # no placeholders at all
            "{namespace}_{table}_{foo}",  # unknown placeholder
            "{foo}_{table}",  # unknown placeholder
            "{namespace}_{table:>10}",  # format spec
            "{namespace}_{table!r}",  # conversion
            "{namespace}-{table}",  # '-' literal
            "{namespace}.{table}",  # '.' literal
            "{namespace} {table}",  # space literal
            "{namespace}_{table}{",  # unbalanced brace
            "{{namespace}}_{table}",  # escaped braces become literal '{'/'}'
            "{}_{table}",  # empty (auto-numbered) field
            "{0}_{table}",  # positional field
        ],
    )
    def test_rejected(self, pattern):
        with pytest.raises(ValidationError):
            SandboxConfig(table_pattern=pattern)


class TestSandboxProfile:
    @pytest.mark.parametrize(
        "namespace",
        [
            "a",  # minimal
            "mehdi",
            "dev_mehdi_2",
            "a" + "b" * 63,  # 64 chars — the maximum
        ],
    )
    def test_namespace_accepted(self, namespace):
        profile = SandboxProfile(namespace=namespace, pipelines=["p1"])
        assert profile.namespace == namespace

    @pytest.mark.parametrize(
        "namespace",
        [
            "",  # empty
            "Mehdi",  # uppercase
            "1dev",  # leading digit
            "_dev",  # leading underscore
            "dev-mehdi",  # hyphen
            "dev mehdi",  # space
            "a" + "b" * 64,  # 65 chars — one over the maximum
        ],
    )
    def test_namespace_rejected(self, namespace):
        with pytest.raises(ValidationError):
            SandboxProfile(namespace=namespace, pipelines=["p1"])

    def test_pipelines_empty_rejected(self):
        with pytest.raises(ValidationError):
            SandboxProfile(namespace="dev", pipelines=[])

    def test_pipelines_missing_rejected(self):
        with pytest.raises(ValidationError):
            SandboxProfile(namespace="dev")

    def test_pipelines_names_and_globs(self):
        profile = SandboxProfile(namespace="dev", pipelines=["bronze_*", "silver_load"])
        assert profile.pipelines == ["bronze_*", "silver_load"]


class TestProjectConfigSandboxWiring:
    def test_absent_defaults_to_none(self):
        assert ProjectConfig(name="proj").sandbox is None

    def test_present_block_parsed(self):
        cfg = ProjectConfig(
            name="proj",
            sandbox={"table_pattern": "{table}_{namespace}", "allowed_envs": ["dev"]},
        )
        assert isinstance(cfg.sandbox, SandboxConfig)
        assert cfg.sandbox.table_pattern == "{table}_{namespace}"
        assert cfg.sandbox.allowed_envs == ["dev"]

    def test_invalid_block_rejected(self):
        with pytest.raises(ValidationError):
            ProjectConfig(name="proj", sandbox={"table_pattern": "{table}"})


def _make_run_config() -> SandboxRunConfig:
    return SandboxRunConfig(
        namespace="dev_mehdi",
        table_pattern="{namespace}_{table}",
        strategy="table",
        pipelines=("bronze_*", "silver_load"),
    )


def _make_warning_record() -> SandboxWarningRecord:
    return SandboxWarningRecord(
        code="LHP-SBX-001",
        message="table also produced by an out-of-scope pipeline",
        file=Path("pipelines/bronze/customers.yaml"),
        flowgroup="customers_bronze",
    )


class TestSandboxRunDTOs:
    def test_sandbox_run_config_pickle_round_trip(self):
        original = _make_run_config()
        restored = pickle.loads(pickle.dumps(original))
        assert restored == original

    def test_sandbox_run_config_frozen(self):
        with pytest.raises(FrozenInstanceError):
            _make_run_config().namespace = "other"

    def test_sandbox_warning_record_pickle_round_trip(self):
        original = _make_warning_record()
        restored = pickle.loads(pickle.dumps(original))
        assert restored == original

    def test_sandbox_warning_record_none_fields_round_trip(self):
        original = SandboxWarningRecord(
            code="LHP-SBX-001", message="msg", file=None, flowgroup=None
        )
        restored = pickle.loads(pickle.dumps(original))
        assert restored == original

    def test_sandbox_warning_record_frozen(self):
        with pytest.raises(FrozenInstanceError):
            _make_warning_record().message = "other"


class TestFlowgroupOutcomeWidening:
    def test_ok_accepts_sandbox_warning_record(self):
        sandbox_rec = _make_warning_record()
        depr_rec = DeprecationWarningRecord(
            code="LHP-DEPR-002", message="deprecated", file=None, flowgroup=None
        )
        outcome = FlowgroupOutcome.ok("pipe", "fg", warnings=[sandbox_rec, depr_rec])
        assert outcome.warnings == (sandbox_rec, depr_rec)

    def test_failure_accepts_sandbox_warning_record(self):
        sandbox_rec = _make_warning_record()
        outcome = FlowgroupOutcome.failure(
            "pipe", "fg", errors=["boom"], warnings=[sandbox_rec]
        )
        assert outcome.warnings == (sandbox_rec,)

    def test_outcome_with_mixed_warnings_pickle_round_trip(self):
        outcome = FlowgroupOutcome.ok(
            "pipe",
            "fg",
            warnings=[
                _make_warning_record(),
                DeprecationWarningRecord(
                    code="LHP-DEPR-002", message="deprecated", file=None, flowgroup=None
                ),
            ],
        )
        restored = pickle.loads(pickle.dumps(outcome))
        assert restored == outcome
