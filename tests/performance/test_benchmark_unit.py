"""Fast, fixture-free unit tests for ``tests/performance/benchmark.py``.

Runs under the default pytest selector — no ``-m performance`` needed.
Covers the comparator decision table, the percentiles helper, the semver
sort key, the perf.log seed parser, and the JSON round-trip.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from tests.performance.benchmark import (
    BaselineDoc,
    BenchmarkRun,
    ComparisonEntry,
    ComparisonResult,
    MetricSummary,
    _build_baseline_doc,
    _classify,
    _flatten_run,
    _parse_perf_log_text,
    _percentiles,
    _semver_key,
    compare,
    load_latest_baseline,
    machine_fingerprint,
    summarize,
)

# ---------------------------------------------------------------------------
# _semver_key
# ---------------------------------------------------------------------------


class TestSemverKey:
    def test_basic_parse(self):
        assert _semver_key("v0.8.7.json") == (0, 8, 7)
        assert _semver_key("v1.2.3.json") == (1, 2, 3)

    def test_sorts_correctly(self):
        names = ["v0.10.0.json", "v0.9.0.json", "v0.8.7.json", "v0.10.3.json"]
        ordered = sorted(names, key=_semver_key)
        assert ordered == [
            "v0.8.7.json",
            "v0.9.0.json",
            "v0.10.0.json",
            "v0.10.3.json",
        ]

    def test_lexsort_failure_mode_is_avoided(self):
        assert _semver_key("v0.10.0.json") > _semver_key("v0.9.0.json")

    @pytest.mark.parametrize(
        "bad",
        [
            "v0.8.json",
            "0.8.7.json",
            "v0.8.7",
            "v0.8.7.txt",
            "weird.json",
            "v0.8.7.json.bak",
        ],
    )
    def test_rejects_non_semver(self, bad):
        with pytest.raises(ValueError):
            _semver_key(bad)


# ---------------------------------------------------------------------------
# _percentiles
# ---------------------------------------------------------------------------


class TestPercentiles:
    def test_empty(self):
        assert _percentiles([]) == (0.0, 0.0, 0.0)

    def test_single_value(self):
        assert _percentiles([3.5]) == (3.5, 3.5, 3.5)

    def test_five_values(self):
        p25, median, p75 = _percentiles([1.0, 2.0, 3.0, 4.0, 5.0])
        assert median == pytest.approx(3.0)
        assert p25 < median < p75

    def test_monotonic(self):
        p25, median, p75 = _percentiles([10.0, 11.0, 12.0, 13.0, 14.0])
        assert p25 <= median <= p75


# ---------------------------------------------------------------------------
# _classify decision table
# ---------------------------------------------------------------------------


class TestClassify:
    @staticmethod
    def _m(median, p25=None, p75=None, unit="seconds"):
        return MetricSummary(
            median=median,
            p25=p25 if p25 is not None else median,
            p75=p75 if p75 is not None else median,
            unit=unit,  # type: ignore[arg-type]
        )

    def test_count_equal_is_stable(self):
        b = self._m(100, unit="count")
        c = self._m(100, unit="count")
        assert _classify(b, c) == "stable"

    def test_count_different_is_regression(self):
        b = self._m(100, unit="count")
        c = self._m(101, unit="count")
        assert _classify(b, c) == "regression"

    def test_count_decreased_is_also_regression(self):
        """Any count drift breaks the algorithmic invariant."""
        b = self._m(100, unit="count")
        c = self._m(99, unit="count")
        assert _classify(b, c) == "regression"

    def test_seconds_sub_one_baseline_skipped(self):
        b = self._m(0.5, p25=0.4, p75=0.6)
        c = self._m(5.0, p25=4.5, p75=5.5)
        assert _classify(b, c) == "skipped"

    def test_seconds_overlapping_iqr_is_stable(self):
        b = self._m(10.0, p25=9.5, p75=10.5)
        c = self._m(11.0, p25=10.4, p75=11.6)
        assert _classify(b, c) == "stable"

    def test_seconds_iqr_disjoint_but_delta_under_25pct_is_stable(self):
        b = self._m(10.0, p25=9.5, p75=10.0)
        c = self._m(11.2, p25=10.5, p75=11.5)
        assert _classify(b, c) == "stable"

    def test_seconds_iqr_disjoint_and_delta_over_25pct_is_regression(self):
        b = self._m(10.0, p25=9.5, p75=10.5)
        c = self._m(20.0, p25=18.0, p75=22.0)
        assert _classify(b, c) == "regression"

    def test_seconds_iqr_disjoint_low_and_delta_over_25pct_is_improvement(self):
        b = self._m(10.0, p25=9.5, p75=10.5)
        c = self._m(5.0, p25=4.0, p75=6.0)
        assert _classify(b, c) == "improvement"

    def test_seconds_minor_improvement_is_stable(self):
        """Improvement also needs the dual-signal AND-gate."""
        b = self._m(10.0, p25=9.5, p75=10.5)
        c = self._m(9.5, p25=9.0, p75=9.9)
        assert _classify(b, c) == "stable"

    def test_unit_mismatch_is_stable_safe_default(self):
        b = self._m(5.0, unit="count")
        c = self._m(5.0, unit="seconds")
        assert _classify(b, c) == "stable"


# ---------------------------------------------------------------------------
# _flatten_run + summarize
# ---------------------------------------------------------------------------


def _make_run(**overrides) -> BenchmarkRun:
    defaults = dict(
        phases={"Pipeline discovery": 1.5, "Bundle sync": 0.25},
        sub_phases={"Pipeline discovery": (("Blueprint expansion", 0.3),)},
        categories={
            "format_code": {
                "cnt": 100,
                "total": 5.0,
                "min": 0.01,
                "max": 0.2,
                "avg": 0.05,
            },
        },
        counts={"blueprints": 1, "instances": 18},
        wall_clock_seconds=2.0,
        exit_code=0,
    )
    defaults.update(overrides)
    return BenchmarkRun(**defaults)


class TestFlattenRun:
    def test_keys_follow_documented_paths(self):
        out = _flatten_run(_make_run())
        assert out["phase.Pipeline discovery"] == (1.5, "seconds")
        assert out["phase.Bundle sync"] == (0.25, "seconds")
        assert out["category.format_code.total"] == (5.0, "seconds")
        assert out["category.format_code.avg"] == (0.05, "seconds")
        assert out["category.format_code.cnt"] == (100.0, "count")
        assert out["count.blueprints"] == (1.0, "count")
        assert out["count.instances"] == (18.0, "count")
        assert out["wall_clock"] == (2.0, "seconds")


class TestSummarize:
    def test_empty_runs(self):
        assert summarize([]) == {}

    def test_single_run_yields_degenerate_summary(self):
        out = summarize([_make_run()])
        bundle = out["phase.Bundle sync"]
        assert bundle.median == bundle.p25 == bundle.p75 == 0.25

    def test_multi_run_computes_percentiles(self):
        runs = [
            _make_run(phases={"Bundle sync": v}, sub_phases={})
            for v in (0.20, 0.25, 0.30, 0.35, 0.40)
        ]
        out = summarize(runs)
        bundle = out["phase.Bundle sync"]
        assert bundle.median == pytest.approx(0.30)
        assert bundle.p25 < bundle.median < bundle.p75
        assert bundle.unit == "seconds"


# ---------------------------------------------------------------------------
# compare()
# ---------------------------------------------------------------------------


def _baseline_doc(
    metrics: dict[str, dict],
    shape: dict[str, int] | None = None,
    fingerprint: str | None = None,
) -> BaselineDoc:
    return BaselineDoc(
        lhp_version="v0.8.7",
        captured_at="2026-05-11T00:00:00+00:00",
        machine_fingerprint=fingerprint or machine_fingerprint(),
        fixture="performance_testing",
        runs=5,
        project_shape=shape or {"blueprints": 1, "instances": 18},
        metrics=metrics,
    )


def _summary(median, p25=None, p75=None, unit="seconds") -> MetricSummary:
    return MetricSummary(
        median=median,
        p25=p25 if p25 is not None else median,
        p75=p75 if p75 is not None else median,
        unit=unit,  # type: ignore[arg-type]
    )


class TestCompare:
    def test_stable_baseline_yields_zero_regressions(self):
        b = _baseline_doc(
            {
                "phase.Bundle sync": {
                    "median": 10.0,
                    "p25": 9.5,
                    "p75": 10.5,
                    "unit": "seconds",
                },
            }
        )
        c = {"phase.Bundle sync": _summary(10.1, 9.6, 10.6)}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert result.regressions == ()
        assert len(result.stable) == 1

    def test_seconds_regression_caught(self):
        b = _baseline_doc(
            {
                "phase.Bundle sync": {
                    "median": 10.0,
                    "p25": 9.5,
                    "p75": 10.5,
                    "unit": "seconds",
                },
            }
        )
        c = {"phase.Bundle sync": _summary(20.0, 18.0, 22.0)}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert len(result.regressions) == 1
        entry = result.regressions[0]
        assert entry.metric == "phase.Bundle sync"
        assert entry.delta_pct == pytest.approx(1.0)
        assert entry.unit == "seconds"

    def test_count_regression_caught(self):
        b = _baseline_doc(
            {
                "category.bundle_extract_keys.cnt": {
                    "median": 100.0,
                    "p25": 100.0,
                    "p75": 100.0,
                    "unit": "count",
                },
            }
        )
        c = {"category.bundle_extract_keys.cnt": _summary(10100.0, unit="count")}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert len(result.regressions) == 1
        assert result.regressions[0].unit == "count"

    def test_improvement_classified_separately(self):
        b = _baseline_doc(
            {
                "phase.Bundle sync": {
                    "median": 10.0,
                    "p25": 9.5,
                    "p75": 10.5,
                    "unit": "seconds",
                },
            }
        )
        c = {"phase.Bundle sync": _summary(5.0, 4.0, 6.0)}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert result.regressions == ()
        assert len(result.improvements) == 1

    def test_sub_second_baseline_skipped(self):
        b = _baseline_doc(
            {
                "phase.tiny": {
                    "median": 0.5,
                    "p25": 0.4,
                    "p75": 0.6,
                    "unit": "seconds",
                },
            }
        )
        c = {"phase.tiny": _summary(5.0, 4.0, 6.0)}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert result.regressions == ()
        assert len(result.skipped) == 1

    def test_shape_mismatch_warns_and_suppresses_count_regression(self):
        b = _baseline_doc(
            {
                "count.blueprints": {
                    "median": 1.0,
                    "p25": 1.0,
                    "p75": 1.0,
                    "unit": "count",
                },
            },
            shape={"blueprints": 1},
        )
        c = {"count.blueprints": _summary(2.0, unit="count")}
        result = compare(b, c, candidate_shape={"blueprints": 2})
        assert (
            result.regressions == ()
        ), "shape drift must not double-fire as a count regression"
        assert len(result.shape_mismatches) == 1
        assert result.shape_mismatches[0].metric == "project_shape.blueprints"

    def test_shape_new_key_in_candidate_emits_mismatch(self):
        b = _baseline_doc(
            {},
            shape={"blueprints": 1},
        )
        result = compare(b, {}, candidate_shape={"blueprints": 1, "instances": 18})
        assert len(result.shape_mismatches) == 1
        assert result.shape_mismatches[0].metric == "project_shape.instances"

    def test_unknown_baseline_metric_is_ignored(self):
        """A new metric in candidate (not in baseline) is silently dropped."""
        b = _baseline_doc(
            {
                "phase.A": {"median": 10.0, "p25": 9.5, "p75": 10.5, "unit": "seconds"},
            }
        )
        c = {
            "phase.A": _summary(10.0, 9.5, 10.5),
            "phase.B_new": _summary(99.0, 95.0, 103.0),
        }
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert result.regressions == ()
        assert all(e.metric != "phase.B_new" for e in result.stable)

    def test_fingerprint_mismatch_reported(self):
        b = _baseline_doc(
            {"phase.A": {"median": 10.0, "p25": 9.5, "p75": 10.5, "unit": "seconds"}},
            fingerprint="Linux-x86_64-py3.11",
        )
        c = {"phase.A": _summary(10.0, 9.5, 10.5)}
        result = compare(b, c, candidate_shape={"blueprints": 1, "instances": 18})
        assert result.fingerprint_mismatch is True


# ---------------------------------------------------------------------------
# JSON round-trip + load_latest_baseline
# ---------------------------------------------------------------------------


class TestJsonRoundTrip:
    def test_round_trip_via_build_and_parse(self, tmp_path: Path):
        runs = [
            _make_run(phases={"Bundle sync": v}, sub_phases={})
            for v in (0.20, 0.25, 0.30, 0.35, 0.40)
        ]
        summary = summarize(runs)
        doc = _build_baseline_doc(
            fixture_name="performance_testing",
            lhp_version="v0.8.7",
            runs_count=5,
            summary=summary,
            project_shape={"blueprints": 1, "instances": 18},
        )
        baseline_path = tmp_path / "performance_testing"
        baseline_path.mkdir()
        target = baseline_path / "v0.8.7.json"
        target.write_text(json.dumps(doc, indent=2), encoding="utf-8")

        loaded = load_latest_baseline(baseline_path)
        assert loaded["lhp_version"] == "v0.8.7"
        assert loaded["fixture"] == "performance_testing"
        assert loaded["runs"] == 5
        assert loaded["project_shape"]["blueprints"] == 1
        assert loaded["metrics"]["phase.Bundle sync"]["unit"] == "seconds"
        assert loaded["metrics"]["phase.Bundle sync"]["median"] == pytest.approx(0.30)

    def test_load_latest_picks_highest_semver(self, tmp_path: Path):
        for ver in ("v0.8.7", "v0.10.0", "v0.9.5", "v0.8.0"):
            doc = _baseline_doc({})
            doc["lhp_version"] = ver
            (tmp_path / f"{ver}.json").write_text(json.dumps(doc), encoding="utf-8")
        loaded = load_latest_baseline(tmp_path)
        assert loaded["lhp_version"] == "v0.10.0"

    def test_load_latest_skips_unparsable_files(self, tmp_path: Path):
        good = _baseline_doc({})
        good["lhp_version"] = "v0.8.7"
        (tmp_path / "v0.8.7.json").write_text(json.dumps(good))
        (tmp_path / "vNOT_SEMVER.json").write_text("{}")
        loaded = load_latest_baseline(tmp_path)
        assert loaded["lhp_version"] == "v0.8.7"

    def test_load_latest_raises_when_empty(self, tmp_path: Path):
        with pytest.raises(FileNotFoundError):
            load_latest_baseline(tmp_path)


# ---------------------------------------------------------------------------
# _parse_perf_log_text (seed parser)
# ---------------------------------------------------------------------------


PERF_LOG_FIXTURE = """\
2026-05-11 14:31:21,001 - [PERF] Orchestrator init: 0.0490s
2026-05-11 14:31:45,440 - [PERF] ============ PERFORMANCE SUMMARY ============
2026-05-11 14:31:45,440 - [PERF] Started: 2026-05-11 14:31:21
2026-05-11 14:31:45,440 - [PERF] Ended:   2026-05-11 14:31:45
2026-05-11 14:31:45,440 - [PERF]
2026-05-11 14:31:45,440 - [PERF] Project shape:
2026-05-11 14:31:45,440 - [PERF]   blueprints                                 1
2026-05-11 14:31:45,440 - [PERF]   instances                                  3
2026-05-11 14:31:45,440 - [PERF]   synthetic_flowgroups                     600
2026-05-11 14:31:45,440 - [PERF]
2026-05-11 14:31:45,440 - [PERF] Phase breakdown:
2026-05-11 14:31:45,440 - [PERF]   Orchestrator init                      0.049s  (  0.2%)
2026-05-11 14:31:45,440 - [PERF]   Pipeline discovery                     0.355s  (  1.5%)
2026-05-11 14:31:45,440 - [PERF]     ↳ Blueprint expansion                0.268s
2026-05-11 14:31:45,440 - [PERF]   Pipeline generation [site_a_bronze]    1.468s  (  6.3%)
2026-05-11 14:31:45,440 - [PERF]   Bundle sync                            0.443s  (  1.9%)
2026-05-11 14:31:45,440 - [PERF]   Total                                 23.176s
2026-05-11 14:31:45,440 - [PERF]
2026-05-11 14:31:45,440 - [PERF] Per-category aggregate stats:
2026-05-11 14:31:45,440 - [PERF]   blueprint_discovery    cnt=1    avg=0.228s  min=0.228s  max=0.228s  total=   0.23s
2026-05-11 14:31:45,440 - [PERF]   format_code            cnt=601  avg=0.015s  min=0.003s  max=0.211s  total=   8.95s
2026-05-11 14:31:45,440 - [PERF] =============================================
"""

PERF_LOG_OLD_LABEL_FIXTURE = """\
2026-05-11 14:25:52,488 - [PERF] ============ PERFORMANCE SUMMARY ============
2026-05-11 14:25:52,488 - [PERF] Started: 2026-05-11 14:23:18
2026-05-11 14:25:52,488 - [PERF] Ended:   2026-05-11 14:25:52
2026-05-11 14:25:52,488 - [PERF]
2026-05-11 14:25:52,488 - [PERF] Phase breakdown:
2026-05-11 14:25:52,488 - [PERF]   Orchestrator init                      0.020s  (  0.0%)
2026-05-11 14:25:52,488 - [PERF]   Pipeline discovery                     5.779s  (  3.8%)
2026-05-11 14:25:52,489 - [PERF]   Bundle sync                            6.609s  (  4.3%)
2026-05-11 14:25:52,489 - [PERF]   Total                                153.819s
2026-05-11 14:25:52,489 - [PERF]
2026-05-11 14:25:52,489 - [PERF] Per-flowgroup aggregate stats:
2026-05-11 14:25:52,489 - [PERF]   find_source_yaml       cnt=4001 avg=0.017s  min=0.000s  max=5.468s  total=  69.45s
2026-05-11 14:25:52,489 - [PERF] =============================================
"""


class TestParsePerfLogText:
    def test_parses_phase_breakdown(self):
        run = _parse_perf_log_text(PERF_LOG_FIXTURE)
        assert run.phases["Orchestrator init"] == pytest.approx(0.049)
        assert run.phases["Pipeline discovery"] == pytest.approx(0.355)
        assert run.phases["Pipeline generation [site_a_bronze]"] == pytest.approx(1.468)
        assert run.phases["Bundle sync"] == pytest.approx(0.443)

    def test_parses_sub_phases(self):
        run = _parse_perf_log_text(PERF_LOG_FIXTURE)
        assert "Pipeline discovery" in run.sub_phases
        subs = run.sub_phases["Pipeline discovery"]
        assert len(subs) == 1
        name, dur = subs[0]
        assert name == "Blueprint expansion"
        assert dur == pytest.approx(0.268)

    def test_parses_project_shape(self):
        run = _parse_perf_log_text(PERF_LOG_FIXTURE)
        assert run.counts == {
            "blueprints": 1,
            "instances": 3,
            "synthetic_flowgroups": 600,
        }

    def test_parses_categories(self):
        run = _parse_perf_log_text(PERF_LOG_FIXTURE)
        assert "blueprint_discovery" in run.categories
        bd = run.categories["blueprint_discovery"]
        assert bd["cnt"] == 1
        assert bd["total"] == pytest.approx(0.23)
        fc = run.categories["format_code"]
        assert fc["cnt"] == 601
        assert fc["avg"] == pytest.approx(0.015)

    def test_wall_clock_from_total(self):
        run = _parse_perf_log_text(PERF_LOG_FIXTURE)
        assert run.wall_clock_seconds == pytest.approx(23.176)

    def test_old_per_flowgroup_label_accepted(self):
        run = _parse_perf_log_text(PERF_LOG_OLD_LABEL_FIXTURE)
        assert "find_source_yaml" in run.categories
        assert run.categories["find_source_yaml"]["cnt"] == 4001
        assert run.counts == {}, "v0.8.0 logs have no Project shape section"
        assert run.wall_clock_seconds == pytest.approx(153.819)
