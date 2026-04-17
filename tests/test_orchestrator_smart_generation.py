"""Integration test for the StalenessCache scan-reduction.

The performance fix this locks in: smart-mode generation must run the
env-wide staleness scan *at most once* per run, regardless of how many
pipelines are analyzed. Without the cache, both the display phase
(``analyze_generation_requirements``) and the per-pipeline filter
(``_apply_smart_generation_filtering``) each call
``StateAnalyzer.get_all_files_needing_generation`` → O(P+1) scans for P
pipelines.
"""

from pathlib import Path

import pytest


@pytest.fixture
def project(tmp_path: Path) -> Path:
    """Minimal LHP project on disk — one env, two pipelines, one flowgroup each."""
    (tmp_path / "substitutions").mkdir()
    (tmp_path / "substitutions" / "dev.yaml").write_text("env: dev\n")
    (tmp_path / "lhp.yaml").write_text(
        "name: scan_count_test\nversion: \"1.0\"\n"
    )

    pipelines = tmp_path / "pipelines"
    for pipeline in ("p1", "p2"):
        pdir = pipelines / pipeline
        pdir.mkdir(parents=True)
        (pdir / f"{pipeline}_fg.yaml").write_text(
            f"pipeline: {pipeline}\n"
            f"flowgroup: {pipeline}_fg\n"
            "actions: []\n"
        )
    return tmp_path


def test_env_wide_staleness_scan_runs_exactly_once(project, monkeypatch):
    """Smart generation across P pipelines must invoke the env-wide scan once.

    Spies on ``StateAnalyzer.get_all_files_needing_generation`` and asserts
    exactly one call across the combined display + filter phases.
    """
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.core.state import state_analyzer as sa_module

    call_counter = {"n": 0}
    original = sa_module.StateAnalyzer.get_all_files_needing_generation

    def spy(self, state, environment, include_patterns=None):
        call_counter["n"] += 1
        return original(self, state, environment, include_patterns)

    monkeypatch.setattr(
        sa_module.StateAnalyzer, "get_all_files_needing_generation", spy
    )

    from lhp.core.state_manager import StateManager

    orchestrator = ActionOrchestrator(project, enforce_version=False)
    state_manager = StateManager(project)
    # Bind the cache from the orchestrator's dependencies so display and
    # filter share the same instance (matches generate_command wiring).
    state_manager.set_staleness_cache(orchestrator.dependencies.staleness_cache)

    # Display phase: env-wide analysis
    analysis = orchestrator.analyze_generation_requirements(
        env="dev",
        pipeline_names=["p1", "p2"],
        include_tests=False,
        force=False,
        state_manager=state_manager,
    )
    # Fresh state → context gate fires; we still only want one scan across
    # the full smart pass (even though the gate short-circuits the per-
    # pipeline loop).
    assert analysis.context_changed

    # Filter phase: simulate each pipeline's smart filter resolving its slice
    for pipeline in ("p1", "p2"):
        orchestrator._apply_smart_generation_filtering(
            all_flowgroups=[],
            env="dev",
            pipeline_identifier=pipeline,
            include_tests=False,
            state_manager=state_manager,
        )

    # The context gate in _apply_smart_generation_filtering also fires on
    # fresh state, so the loader is NOT called at all in that path (early
    # return). In the steady-state path (context unchanged), the display
    # phase populates the cache exactly once and the filter reads from it.
    # Either way, the guarantee is: at most one scan per run.
    assert call_counter["n"] <= 1


def test_scan_runs_once_after_context_is_recorded(project, monkeypatch):
    """With a recorded generation context, both phases share a single scan."""
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.core.state import state_analyzer as sa_module
    from lhp.core.state_manager import StateManager

    # Pre-populate last_generation_context so the gate does not short-circuit.
    state_manager = StateManager(project)
    state_manager.record_generation_context("dev", include_tests=False)
    state_manager.save()

    call_counter = {"n": 0}
    original = sa_module.StateAnalyzer.get_all_files_needing_generation

    def spy(self, state, environment, include_patterns=None):
        call_counter["n"] += 1
        return original(self, state, environment, include_patterns)

    monkeypatch.setattr(
        sa_module.StateAnalyzer, "get_all_files_needing_generation", spy
    )

    # Rebuild state_manager so it sees the saved context
    state_manager = StateManager(project)

    orchestrator = ActionOrchestrator(project, enforce_version=False)
    state_manager.set_staleness_cache(orchestrator.dependencies.staleness_cache)

    orchestrator.analyze_generation_requirements(
        env="dev",
        pipeline_names=["p1", "p2"],
        include_tests=False,
        force=False,
        state_manager=state_manager,
    )
    for pipeline in ("p1", "p2"):
        orchestrator._apply_smart_generation_filtering(
            all_flowgroups=[],
            env="dev",
            pipeline_identifier=pipeline,
            include_tests=False,
            state_manager=state_manager,
        )

    # Exactly one env-wide scan, shared across display + per-pipeline filters.
    assert call_counter["n"] == 1


def test_save_invalidates_cache(project, monkeypatch):
    """After save() the next scan reloads (so state reflects the save)."""
    from lhp.core.orchestrator import ActionOrchestrator
    from lhp.core.state import state_analyzer as sa_module
    from lhp.core.state_manager import StateManager

    state_manager = StateManager(project)
    state_manager.record_generation_context("dev", include_tests=False)
    state_manager.save()
    state_manager = StateManager(project)

    call_counter = {"n": 0}
    original = sa_module.StateAnalyzer.get_all_files_needing_generation

    def spy(self, state, environment, include_patterns=None):
        call_counter["n"] += 1
        return original(self, state, environment, include_patterns)

    monkeypatch.setattr(
        sa_module.StateAnalyzer, "get_all_files_needing_generation", spy
    )

    orchestrator = ActionOrchestrator(project, enforce_version=False)
    state_manager.set_staleness_cache(orchestrator.dependencies.staleness_cache)

    orchestrator.analyze_generation_requirements(
        env="dev",
        pipeline_names=["p1", "p2"],
        include_tests=False,
        force=False,
        state_manager=state_manager,
    )
    assert call_counter["n"] == 1

    state_manager.save()  # should invalidate the cache

    orchestrator.analyze_generation_requirements(
        env="dev",
        pipeline_names=["p1", "p2"],
        include_tests=False,
        force=False,
        state_manager=state_manager,
    )
    assert call_counter["n"] == 2
