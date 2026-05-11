"""Release-time performance gate.

Runs the LHP generator against each heavy fixture (5 iterations, ~12 min
wall-clock each) and compares the medians to the latest committed baseline.
Skipped in CI by the ``performance`` marker; intended for local runs before
cutting a release.

Workflow:

1. Capture baselines once per release::

       python -m tests.performance.benchmark capture --fixture performance_testing
       python -m tests.performance.benchmark capture --fixture performance_testing_blueprint

2. Run the gate (~25 min total)::

       pytest tests/performance/ -m performance -v

Failures print a release-manager-friendly diff with concrete next steps.
"""

from __future__ import annotations

import os
import warnings
from pathlib import Path

import pytest

from tests.performance.benchmark import (
    BASELINES_DIR,
    REPO_ROOT,
    _format_failure_message,
    compare,
    load_latest_baseline,
    machine_fingerprint,
    run_benchmark,
    summarize,
)


FIXTURES = ["performance_testing", "performance_testing_blueprint"]


@pytest.fixture
def cwd_guard():
    """Restore ``cwd`` even if a benchmark iteration leaves it changed."""
    saved = os.getcwd()
    try:
        yield
    finally:
        os.chdir(saved)


@pytest.mark.performance
@pytest.mark.parametrize("fixture_name", FIXTURES)
def test_release_benchmark_no_regressions(fixture_name: str, cwd_guard) -> None:
    fixture_path = REPO_ROOT / "Example_Projects" / fixture_name
    if not fixture_path.exists():
        pytest.skip(f"Fixture not present at {fixture_path}")

    baseline_dir = BASELINES_DIR / fixture_name
    if not baseline_dir.exists() or not any(baseline_dir.glob("v*.json")):
        pytest.skip(
            f"No baseline for {fixture_name}; capture one with "
            f"`python -m tests.performance.benchmark capture "
            f"--fixture {fixture_name}`"
        )

    baseline = load_latest_baseline(baseline_dir)

    current_fp = machine_fingerprint()
    if baseline["machine_fingerprint"] != current_fp:
        warnings.warn(
            f"Machine fingerprint mismatch for {fixture_name}: "
            f"baseline={baseline['machine_fingerprint']}, "
            f"current={current_fp}. Wall-time thresholds may be unreliable "
            "across architectures; count invariants still apply.",
            stacklevel=2,
        )

    runs = run_benchmark(fixture_path, runs=5)
    candidate = summarize(runs)
    candidate_shape = dict(runs[-1].counts)
    result = compare(baseline, candidate, candidate_shape=candidate_shape)

    for entry in result.improvements:
        print(
            f"[IMPROVEMENT] {entry.metric}: "
            f"{entry.baseline_median:.3f} -> {entry.candidate_median:.3f} "
            f"({entry.delta_pct:+.1%})"
        )
    for entry in result.shape_mismatches:
        warnings.warn(
            f"Project shape drift for {fixture_name}: {entry.metric}: "
            f"{entry.reason}. Recapture baseline if intentional: "
            f"`python -m tests.performance.benchmark capture --fixture {fixture_name}`.",
            stacklevel=2,
        )

    if result.regressions:
        pytest.fail(
            _format_failure_message(
                result, fixture_name, baseline["lhp_version"]
            )
        )
