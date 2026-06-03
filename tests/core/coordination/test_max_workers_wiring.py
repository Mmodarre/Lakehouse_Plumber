"""Engine-level tests for ``max_workers`` resolution and auto-detection.

These exercise the orchestrator wiring DIRECTLY (no CLI): the CLI surface only
parses ``--max-workers`` into a kwarg, and that parse is covered by the thin
CLI test in ``tests/cli/test_max_workers_flag.py``. Everything load-bearing —
explicit value vs. ``LHP_MAX_WORKERS`` env var vs. auto-detect, the zero/garbage
clamps, and the 20%-headroom math in ``_auto_max_workers`` — is engine behavior
and lives here (constitution §8.4 / §9.11).

Relocated from the former ``tests/test_max_workers_wiring.py``; the orchestrator
assertions never needed a ``CliRunner``.
"""

import os
from pathlib import Path
from unittest.mock import patch

import pytest

from lhp.core.coordination.layers import build_facade_orchestrator
from lhp.core.coordination.orchestrator import _auto_max_workers


@pytest.fixture
def _bare_project(tmp_path):
    """Minimal lhp.yaml so build_facade_orchestrator can construct."""
    (tmp_path / "lhp.yaml").write_text(
        "name: test_project\nversion: 1.0\nauthor: test\n"
    )
    return tmp_path


@pytest.fixture(autouse=True)
def _clean_env(monkeypatch):
    """Ensure LHP_MAX_WORKERS does not leak between tests."""
    monkeypatch.delenv("LHP_MAX_WORKERS", raising=False)


class TestOrchestratorMaxWorkers:
    """build_facade_orchestrator must honour max_workers; resolved value is
    accessible via ``orch.max_workers`` on the returned orchestrator.
    """

    def test_explicit_max_workers_propagates_to_orchestrator(self, _bare_project):
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, max_workers=3
        )
        assert orch.max_workers == 3

    def test_default_max_workers_resolves_via_auto_detect(self, _bare_project):
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch.max_workers == _auto_max_workers()

    def test_max_workers_one_means_sequential_capable(self, _bare_project):
        """--max-workers 1 must be accepted; the orchestrator stores it as 1."""
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, max_workers=1
        )
        assert orch.max_workers == 1

    def test_explicit_arg_wins_over_env_var(self, _bare_project, monkeypatch):
        """--max-workers on the CLI must beat LHP_MAX_WORKERS."""
        monkeypatch.setenv("LHP_MAX_WORKERS", "16")
        orch = build_facade_orchestrator(
            _bare_project, enforce_version=False, max_workers=2
        )
        assert orch.max_workers == 2

    def test_env_var_used_when_no_explicit_arg(self, _bare_project, monkeypatch):
        monkeypatch.setenv("LHP_MAX_WORKERS", "5")
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch.max_workers == 5

    def test_env_var_zero_clamps_to_one(self, _bare_project, monkeypatch):
        """LHP_MAX_WORKERS=0 must clamp to 1 (sequential), not disable the pool."""
        monkeypatch.setenv("LHP_MAX_WORKERS", "0")
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch.max_workers == 1

    def test_invalid_env_var_falls_back_to_auto(self, _bare_project, monkeypatch):
        """Garbage in LHP_MAX_WORKERS logs a warning and falls back to auto-detect."""
        monkeypatch.setenv("LHP_MAX_WORKERS", "not-a-number")
        orch = build_facade_orchestrator(_bare_project, enforce_version=False)
        assert orch.max_workers == _auto_max_workers()


def _patch_detected_cpu_count(value: int):
    """Patch whichever CPU detector _auto_max_workers would pick first.

    The helper has a 3-branch fallback chain (process_cpu_count → sched_getaffinity
    → cpu_count); tests want to control the *detected* value regardless of which
    branch the running interpreter takes.
    """
    if hasattr(os, "process_cpu_count"):
        return patch.object(os, "process_cpu_count", return_value=value)
    if hasattr(os, "sched_getaffinity"):
        return patch.object(os, "sched_getaffinity", return_value=set(range(value)))
    return patch("lhp.core.coordination.orchestrator.os.cpu_count", return_value=value)


@pytest.mark.unit
class TestAutoMaxWorkers:
    """_auto_max_workers applies a 20% headroom and clamps to >= 1."""

    def test_floor_is_one_on_single_core(self):
        """1 CPU * 0.8 = 0.8; floor would be 0, but max(1, ...) guards it."""
        with _patch_detected_cpu_count(1):
            assert _auto_max_workers() == 1

    def test_two_cores_floors_to_one(self):
        """2 CPUs * 0.8 = 1.6 -> floor 1 (typical 2-core hosted CI runner)."""
        with _patch_detected_cpu_count(2):
            assert _auto_max_workers() == 1

    def test_eight_cores_yields_six(self):
        """8 CPUs * 0.8 = 6.4 -> floor 6."""
        with _patch_detected_cpu_count(8):
            assert _auto_max_workers() == 6

    def test_sixteen_cores_yields_twelve(self):
        """16 CPUs * 0.8 = 12.8 -> floor 12."""
        with _patch_detected_cpu_count(16):
            assert _auto_max_workers() == 12

    def test_sixty_four_cores_scales_proportionally(self):
        """64 CPUs * 0.8 = 51.2 -> floor 51 (no more 'cap at 8' regression)."""
        with _patch_detected_cpu_count(64):
            assert _auto_max_workers() == 51

    def test_unpatched_call_returns_at_least_one(self):
        """Sanity: on any real platform the helper returns a positive integer."""
        result = _auto_max_workers()
        assert isinstance(result, int)
        assert result >= 1
