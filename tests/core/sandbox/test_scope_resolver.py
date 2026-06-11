"""Tests for ``resolve_sandbox_run`` (sandbox scope resolution).

Profile ``pipelines`` entries — exact names or case-sensitive ``fnmatchcase``
globs — expand against the discovered pipelines with the monitoring pipeline
silently excluded. Any zero-match entry raises ``LHP-VAL-064`` (one error
listing every offender); an exact entry naming the monitoring pipeline raises
``LHP-VAL-064`` with the cannot-be-sandboxed message; an environment absent
from ``allowed_envs`` raises ``LHP-CFG-065``. A ``None`` team config means
defaults apply (strategy ``table``, pattern ``{namespace}_{table}``, any env).
"""

from __future__ import annotations

from typing import List, Optional

import pytest

from lhp.core.sandbox import resolve_sandbox_run
from lhp.errors import LHPError
from lhp.models import SandboxConfig, SandboxProfile
from lhp.models.processing import SandboxRunConfig

DISCOVERED = ["raw_ingest", "silver_core", "silver_extras", "gold_marts"]


def _profile(pipelines: List[str], namespace: str = "alice") -> SandboxProfile:
    return SandboxProfile(namespace=namespace, pipelines=pipelines)


def _resolve(
    pipelines: List[str],
    *,
    sandbox_config: Optional[SandboxConfig] = None,
    env: str = "dev",
    discovered: Optional[List[str]] = None,
    monitoring: Optional[str] = None,
) -> SandboxRunConfig:
    return resolve_sandbox_run(
        sandbox_config,
        _profile(pipelines),
        env,
        DISCOVERED if discovered is None else discovered,
        monitoring,
    )


@pytest.mark.unit
class TestScopeMatching:
    """Exact names and ``fnmatchcase`` globs expand to discovered pipelines."""

    def test_exact_name_match(self):
        """An exact entry selects exactly that pipeline."""
        run = _resolve(["silver_core"])

        assert run.pipelines == ("silver_core",)

    def test_prefix_glob_match(self):
        """A ``prefix*`` glob selects every pipeline sharing the prefix."""
        run = _resolve(["silver_*"])

        assert run.pipelines == ("silver_core", "silver_extras")

    def test_question_mark_glob_match(self):
        """``?`` matches exactly one character."""
        run = _resolve(["gold_mart?"], discovered=["gold_marts", "gold_martinis"])

        assert run.pipelines == ("gold_marts",)

    def test_glob_matching_is_case_sensitive(self):
        """``fnmatchcase``: 'ACMI*' does not match 'acmi_x'."""
        run = _resolve(["ACMI*"], discovered=["acmi_x", "ACMI_y"])

        assert run.pipelines == ("ACMI_y",)

    def test_overlapping_entries_dedup_and_sort(self):
        """Overlapping entries dedupe; the result tuple is sorted."""
        run = _resolve(["silver_*", "silver_core", "*_core", "raw_ingest"])

        assert run.pipelines == ("raw_ingest", "silver_core", "silver_extras")


@pytest.mark.unit
class TestZeroMatchEntries:
    """Any entry matching zero pipelines raises a single ``LHP-VAL-064``."""

    def test_single_offender_names_entry_and_available(self):
        """The error names the offending entry and the available pipelines."""
        with pytest.raises(LHPError) as exc_info:
            _resolve(["bronze_*"])

        error = exc_info.value
        assert error.code == "LHP-VAL-064"
        assert "'bronze_*'" in error.details
        assert "gold_marts, raw_ingest, silver_core, silver_extras" in error.details

    def test_multiple_offenders_all_named_in_one_error(self):
        """Every zero-match entry appears in ONE error; matches don't mask it."""
        with pytest.raises(LHPError) as exc_info:
            _resolve(["bronze_*", "silver_core", "no_such_pipeline"])

        error = exc_info.value
        assert error.code == "LHP-VAL-064"
        assert "'bronze_*'" in error.details
        assert "'no_such_pipeline'" in error.details

    def test_empty_discovered_pipelines(self):
        """No discovered pipelines: every entry offends, 'none' stated."""
        with pytest.raises(LHPError) as exc_info:
            _resolve(["silver_core"], discovered=[])

        error = exc_info.value
        assert error.code == "LHP-VAL-064"
        assert "'silver_core'" in error.details
        assert "none" in error.details


@pytest.mark.unit
class TestAllowedEnvsGate:
    """``allowed_envs`` gates the run before any scope expansion."""

    def test_env_in_allowed_list_passes(self):
        """An env present in allowed_envs resolves normally."""
        config = SandboxConfig(allowed_envs=["dev", "tst"])

        run = _resolve(["silver_core"], sandbox_config=config, env="tst")

        assert run.pipelines == ("silver_core",)

    def test_env_not_in_allowed_list_raises_cfg_065(self):
        """An env absent from allowed_envs raises LHP-CFG-065 naming both."""
        config = SandboxConfig(allowed_envs=["dev", "tst"])

        with pytest.raises(LHPError) as exc_info:
            _resolve(["silver_core"], sandbox_config=config, env="prod")

        error = exc_info.value
        assert error.code == "LHP-CFG-065"
        assert "prod" in error.details
        assert "dev, tst" in error.details

    def test_allowed_envs_none_is_unrestricted(self):
        """allowed_envs None: any environment may run sandboxed."""
        config = SandboxConfig(allowed_envs=None)

        run = _resolve(["silver_core"], sandbox_config=config, env="prod")

        assert run.pipelines == ("silver_core",)


@pytest.mark.unit
class TestMonitoringExclusion:
    """The monitoring pipeline never enters sandbox scope."""

    def test_glob_match_silently_excludes_monitoring(self):
        """A glob covering the monitoring pipeline omits it without error."""
        run = _resolve(
            ["silver_*"],
            discovered=["silver_core", "silver_extras", "silver_monitoring"],
            monitoring="silver_monitoring",
        )

        assert run.pipelines == ("silver_core", "silver_extras")

    def test_exact_entry_naming_monitoring_raises_val_064(self):
        """An exact (non-glob) entry naming it: cannot-be-sandboxed VAL_064."""
        with pytest.raises(LHPError) as exc_info:
            _resolve(
                ["silver_monitoring"],
                discovered=["silver_core", "silver_monitoring"],
                monitoring="silver_monitoring",
            )

        error = exc_info.value
        assert error.code == "LHP-VAL-064"
        assert "monitoring pipeline cannot be sandboxed" in error.details

    def test_monitoring_none_means_no_exclusion(self):
        """No monitoring pipeline configured: nothing is excluded."""
        run = _resolve(
            ["silver_*"],
            discovered=["silver_core", "silver_monitoring"],
            monitoring=None,
        )

        assert run.pipelines == ("silver_core", "silver_monitoring")


@pytest.mark.unit
class TestRunConfigConstruction:
    """The returned ``SandboxRunConfig`` merges profile and team policy."""

    def test_sandbox_config_none_applies_team_defaults(self):
        """No sandbox: block in lhp.yaml -> defaults, any env allowed."""
        run = _resolve(["silver_core"], sandbox_config=None, env="prod")

        assert run.strategy == "table"
        assert run.table_pattern == "{namespace}_{table}"
        assert run.pipelines == ("silver_core",)

    def test_fields_populated_from_profile_and_config(self):
        """namespace from profile; pattern/strategy from the team config."""
        config = SandboxConfig(
            strategy="table",
            table_pattern="{namespace}__{table}",
            allowed_envs=["dev"],
        )

        run = resolve_sandbox_run(
            config, _profile(["gold_*"], namespace="bob"), "dev", DISCOVERED, None
        )

        assert isinstance(run, SandboxRunConfig)
        assert run.namespace == "bob"
        assert run.table_pattern == "{namespace}__{table}"
        assert run.strategy == "table"
        assert run.pipelines == ("gold_marts",)
