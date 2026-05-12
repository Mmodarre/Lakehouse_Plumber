# LHP performance benchmark suite

Release-time performance gate for Lakehouse Plumber. **Not a CI gate** — the
full suite takes ~25 minutes wall-clock and is intended to run locally before
cutting a major release.

## What this is

The harness drives `lhp --perf generate --env dev --force` against each of the
heavy fixture projects, reads structured metrics from the in-process perf
summary (`get_perf_summary()` — see `src/lhp/utils/performance_timer.py`),
summarizes 5 runs via medians and the p25/p75 interquartile range, and
compares the result against a committed baseline JSON.

Three signals are reported:

- **regression** — fails the test. For seconds metrics, both
  `candidate.p25 > baseline.p75` *and* a ≥25% increase in median are required
  (dual-signal AND-gate). For count metrics, any drift is a regression — these
  are algorithmic invariants (e.g. `category.bundle_extract_keys.cnt` jumping
  from 100 to 10100 would catch an O(N²) regression like the v0.8.7 fix).
- **shape_mismatch** — warning, not a failure. The fixture itself changed
  (flowgroup count, synthetic flowgroups, etc.). Recapture the baseline if
  the change is intentional.
- **improvement** — informational. Printed in the test output for the
  release notes.

Stable and `skipped` metrics (seconds with baseline median below 1.0s) are
counted but not printed.

## The two fixtures

| Fixture | What it stresses | Wall-clock per run |
|---|---|---|
| `Example_Projects/performance_testing` | 4001 generated files; bundle sync hot-path; `find_source_yaml`/`generate_code` per-flowgroup loops | ~150s |
| `Example_Projects/performance_testing_blueprint` | Blueprint discovery + expansion to 600 synthetic flowgroups; smaller bundle sync footprint | ~25s |

Both are required: regressions can be specific to one shape (e.g. the v0.8.7
bundle-sync O(P²) regressed Bundle sync on `performance_testing` only).

## Quick start

```bash
# Run the gate against the latest committed baseline (5 iterations per fixture, ~25 min)
pytest tests/performance/ -m performance -v

# Or run just the fast unit tests (~100ms, no fixtures required)
pytest tests/performance/test_benchmark_unit.py -v
```

The gate test (`test_release_benchmark.py`) is silently **skipped** on any
invocation that does not put the word `performance` in the `-m` expression —
including plain `pytest`, `pytest tests/`, `pytest tests/performance/`,
`pytest -m "not slow"`, and CI. The opt-in is enforced by
`tests/performance/conftest.py`, which is more reliable than the marker
registration alone (the registration is documentation; the conftest is the
runtime guard). The test also `pytest.skip()`s with a recapture hint if the
fixture or baseline is missing.

## Capturing a new baseline

After a release branch is cut and you want to gate against this build:

```bash
python -m tests.performance.benchmark capture --fixture performance_testing --runs 5 --version v0.8.7
python -m tests.performance.benchmark capture --fixture performance_testing_blueprint --runs 5 --version v0.8.7
```

Outputs:

- `tests/performance/baselines/<fixture>/vX.Y.Z.json` — the gating baseline
  read by `load_latest_baseline()` (sorted by semver, highest wins).
- `.perf_runs/<version>/<fixture>/run{1..5}.perf.log` — raw per-iteration
  archives, so any reader can re-derive or sanity-check the JSON without
  rerunning. These are gitignored by default; selectively commit only the
  release-specific archives if you want them in history.

`--version` defaults to `importlib.metadata.version("lakehouse-plumber")` with
a leading `v` prepended, so on a tagged release you can omit it.

## Seeding a historical reference (one-off)

For *historical* reference baselines (not gating-quality), convert one
existing `perf.log` into a single-run baseline:

```bash
python -m tests.performance.benchmark seed \
    --from .perf_runs/v080-performance_testing.perf.log \
    --fixture performance_testing \
    --version v0.8.0
```

Single-run seeds set `median = p25 = p75`, so the seconds gate effectively
reduces to "any change > 25%". They are not used for gating as long as a
fresh `capture` baseline supersedes them (the loader picks the highest
semver). Seed baselines exist purely for trend visibility.

## Interpreting failures

Regression output looks like::

    PERFORMANCE REGRESSION — fixture: performance_testing
    baseline: v0.8.7  candidate (this run)

      phase.Bundle sync
        baseline median   0.249s   p25=0.241s  p75=0.258s
        candidate median  3.150s   p25=3.080s  p75=3.220s
        delta             +1165.5% (threshold: +25% AND candidate.p25 > baseline.p75)

      category.bundle_extract_keys.cnt
        baseline median   100      candidate median  10100
        delta             +10000   (counts must match exactly; algorithmic regression)

The IQR-disjoint AND-gate means a single noisy run will not fail the suite.
**Always retry once before declaring a real regression**:

```bash
pytest tests/performance/ -m performance -v -k performance_testing
```

If the second run also fails on the same metric, it is a real regression. If
the second run is stable, you have a flaky environment (other processes
contending for CPU, thermal throttling) — re-run on a quieter machine.

## Counts vs seconds

The two signals carry different weight:

- **count metrics** (`category.<name>.cnt`, `count.<name>`): must match the
  baseline *exactly*. Any drift is treated as a regression because counts
  reflect algorithmic invariants — the number of calls to a hot function, the
  number of items in a loop. The v0.8.7 fix was caught by exactly this kind of
  invariant: the bundle-sync key-extraction was being called O(P²) instead of
  O(P) times.
- **seconds metrics** (`phase.<name>`, `category.<name>.total/.avg`,
  `wall_clock`): allowed ±25% wiggle, plus the IQR-disjoint requirement.
  Below 1.0s baseline median, seconds metrics are *skipped* — sub-second
  measurements are too noisy across systems to gate on.

## Cross-machine baselines

Each baseline records a `machine_fingerprint` of the form
`{system}-{machine}-py{major}.{minor}` (e.g. `Darwin-arm64-py3.12`). When the
current fingerprint differs from the baseline, the test emits a warning
(*not* a failure) because wall-time thresholds are only meaningful on the
same hardware. Count invariants still apply across machines.

If you regularly run on different hardware than the committed baseline, plan
to recapture before each release on the canonical machine, or accept the
warning and rely on count drift as the gating signal.

## Files in this directory

| Path | Purpose |
|---|---|
| `benchmark.py` | Harness — dataclasses, helpers, `run_benchmark`, `compare`, `__main__` CLI |
| `test_release_benchmark.py` | The `@pytest.mark.performance` gate test (one per fixture, parametrized) |
| `test_benchmark_unit.py` | Fast unit tests for the comparator decision table and JSON round-trip (default pytest selector) |
| `baselines/<fixture>/vX.Y.Z.json` | Committed gating baselines, one per release |
| `benchmark_generation.sh` / `benchmark_comparison.sh` | Pre-existing scripts; out of scope for this harness |

`.perf_runs/` at the repo root holds raw archives — see "Capturing a new
baseline" above.
