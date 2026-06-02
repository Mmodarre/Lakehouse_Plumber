"""LHP release-time performance benchmark harness.

Pure-function helpers plus a CliRunner-backed driver that runs ``lhp --perf
generate`` N times against a fixture, summarizes the metrics via medians and
the p25/p75 interquartile range, and writes / compares a baseline JSON.

CLI usage::

    python -m tests.performance.benchmark capture \\
        --fixture performance_testing --runs 5
    python -m tests.performance.benchmark seed \\
        --from .perf_runs/v080-performance_testing.perf.log \\
        --fixture performance_testing --version v0.8.0

See ``tests/performance/README.md`` for the full workflow.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import platform
import re
import shutil
import statistics
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Literal, Mapping, TypedDict

REPO_ROOT = Path(__file__).resolve().parents[2]
BASELINES_DIR = Path(__file__).resolve().parent / "baselines"


# ---------------------------------------------------------------------------
# Types
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class BenchmarkRun:
    phases: Mapping[str, float]
    sub_phases: Mapping[str, tuple[tuple[str, float], ...]]
    categories: Mapping[str, Mapping[str, float]]
    counts: Mapping[str, int]
    wall_clock_seconds: float
    exit_code: int


@dataclass(frozen=True, slots=True)
class MetricSummary:
    median: float
    p25: float
    p75: float
    unit: Literal["seconds", "count"]


@dataclass(frozen=True, slots=True)
class ComparisonEntry:
    metric: str
    baseline_median: float
    candidate_median: float
    delta_pct: float
    reason: str
    baseline_p25: float = 0.0
    baseline_p75: float = 0.0
    candidate_p25: float = 0.0
    candidate_p75: float = 0.0
    unit: str = "seconds"


@dataclass(frozen=True, slots=True)
class ComparisonResult:
    regressions: tuple[ComparisonEntry, ...]
    improvements: tuple[ComparisonEntry, ...]
    stable: tuple[ComparisonEntry, ...]
    skipped: tuple[ComparisonEntry, ...]
    shape_mismatches: tuple[ComparisonEntry, ...]
    fingerprint_mismatch: bool


class BaselineDoc(TypedDict):
    lhp_version: str
    captured_at: str
    machine_fingerprint: str
    fixture: str
    runs: int
    project_shape: dict[str, int]
    metrics: dict[str, dict[str, float | str]]


# ---------------------------------------------------------------------------
# Internal helpers (testable in isolation)
# ---------------------------------------------------------------------------


_SEMVER_RE = re.compile(r"^v(\d+)\.(\d+)\.(\d+)\.json$")


def _semver_key(filename: str) -> tuple[int, int, int]:
    """Sort key for ``vMAJOR.MINOR.PATCH.json`` baseline filenames."""
    m = _SEMVER_RE.match(filename)
    if not m:
        raise ValueError(f"Not a vX.Y.Z.json filename: {filename!r}")
    return (int(m.group(1)), int(m.group(2)), int(m.group(3)))


def _percentiles(values: list[float]) -> tuple[float, float, float]:
    """Return ``(p25, median, p75)`` for the input.

    Falls back to ``(v, v, v)`` for a single-value input (the seed case).
    """
    if not values:
        return (0.0, 0.0, 0.0)
    if len(values) == 1:
        v = values[0]
        return (v, v, v)
    qs = statistics.quantiles(values, n=4)
    return (qs[0], qs[1], qs[2])


def _flatten_run(run: BenchmarkRun) -> dict[str, tuple[float, str]]:
    """Convert a run into a flat ``{metric_key: (value, unit)}`` mapping."""
    out: dict[str, tuple[float, str]] = {}
    for name, dur in run.phases.items():
        out[f"phase.{name}"] = (float(dur), "seconds")
    for cat, stats in run.categories.items():
        out[f"category.{cat}.total"] = (float(stats["total"]), "seconds")
        out[f"category.{cat}.avg"] = (float(stats["avg"]), "seconds")
        out[f"category.{cat}.cnt"] = (float(stats["cnt"]), "count")
    for name, value in run.counts.items():
        out[f"count.{name}"] = (float(value), "count")
    out["wall_clock"] = (float(run.wall_clock_seconds), "seconds")
    return out


def _classify(baseline: MetricSummary, candidate: MetricSummary) -> str:
    """Decide ``regression`` / ``improvement`` / ``stable`` / ``skipped``."""
    if baseline.unit != candidate.unit:
        return "stable"
    if baseline.unit == "count":
        return "regression" if candidate.median != baseline.median else "stable"
    if baseline.median < 1.0:
        return "skipped"
    if baseline.median == 0:
        return "stable"
    delta = (candidate.median - baseline.median) / baseline.median
    if candidate.p25 > baseline.p75 and delta > 0.25:
        return "regression"
    if candidate.p75 < baseline.p25 and -delta > 0.25:
        return "improvement"
    return "stable"


def _wipe_state(fixture_path: Path) -> None:
    """Reset a fixture's generated artifacts before a benchmark iteration."""
    for sub in ("generated", "resources/lhp"):
        path = fixture_path / sub
        if path.exists():
            shutil.rmtree(path, ignore_errors=True)
    perf_log = fixture_path / ".lhp/logs/perf.log"
    perf_log.unlink(missing_ok=True)


# Pre-compiled regexes for _parse_perf_log_text.
_PERF_LINE_RE = re.compile(r"\[PERF\]\s*(.*)$")
_PHASE_RE = re.compile(r"^(\S.*?)\s+(\d+\.\d+)s\s+\(\s*\d+\.\d+%\)$")
_SUBPHASE_RE = re.compile(r"^↳\s+(.+?)\s+(\d+\.\d+)s$")
_TOTAL_RE = re.compile(r"^Total\s+(\d+\.\d+)s$")
_COUNT_RE = re.compile(r"^(\S.*?)\s+(\d+)$")
_CAT_RE = re.compile(
    r"^(\S+)\s+cnt=(\d+)\s+avg=(\d+\.\d+)s\s+"
    r"min=(\d+\.\d+)s\s+max=(\d+\.\d+)s\s+total=\s*(\d+\.\d+)s$"
)


def _parse_perf_log_text(text: str) -> BenchmarkRun:
    """Parse the SUMMARY block of a ``perf.log`` into a ``BenchmarkRun``.

    Tolerant of the historical "Per-flowgroup aggregate stats:" label as well
    as the current "Per-category aggregate stats:" label. A missing
    "Project shape:" section (e.g. in v0.8.0 logs predating ``record_count``)
    yields an empty ``counts`` dict — not an error.
    """
    phases: dict[str, float] = {}
    sub_phases: dict[str, list[tuple[str, float]]] = {}
    categories: dict[str, dict[str, float]] = {}
    counts: dict[str, int] = {}
    last_phase: str | None = None
    section: str | None = None
    total_seconds = 0.0

    for raw in text.splitlines():
        match = _PERF_LINE_RE.search(raw)
        if not match:
            continue
        content = match.group(1)
        stripped = content.strip()
        if not stripped:
            continue
        if "PERFORMANCE SUMMARY" in stripped:
            section = None
            continue
        if stripped.startswith(("Started:", "Ended:", "===")):
            continue
        if stripped == "Project shape:":
            section = "shape"
            continue
        if stripped == "Phase breakdown:":
            section = "phases"
            last_phase = None
            continue
        if stripped.startswith(
            ("Per-category aggregate stats:", "Per-flowgroup aggregate stats:")
        ):
            section = "categories"
            continue

        if section == "shape":
            cm = _COUNT_RE.match(stripped)
            if cm:
                counts[cm.group(1).strip()] = int(cm.group(2))
        elif section == "phases":
            tm = _TOTAL_RE.match(stripped)
            if tm:
                total_seconds = float(tm.group(1))
                continue
            sm = _SUBPHASE_RE.match(stripped)
            if sm:
                if last_phase is not None:
                    sub_phases.setdefault(last_phase, []).append(
                        (sm.group(1).strip(), float(sm.group(2)))
                    )
                continue
            pm = _PHASE_RE.match(stripped)
            if pm:
                name = pm.group(1).strip()
                phases[name] = float(pm.group(2))
                last_phase = name
        elif section == "categories":
            cm = _CAT_RE.match(stripped)
            if cm:
                categories[cm.group(1)] = {
                    "cnt": int(cm.group(2)),
                    "avg": float(cm.group(3)),
                    "min": float(cm.group(4)),
                    "max": float(cm.group(5)),
                    "total": float(cm.group(6)),
                }

    wall_clock = total_seconds if total_seconds > 0 else sum(phases.values())
    return BenchmarkRun(
        phases=phases,
        sub_phases={k: tuple(v) for k, v in sub_phases.items()},
        categories=categories,
        counts=counts,
        wall_clock_seconds=wall_clock,
        exit_code=0,
    )


def _format_count(value: float) -> str:
    """Render a count value: integer if whole, else 3-decimal float."""
    return f"{int(value):d}" if float(value).is_integer() else f"{value:.3f}"


def _format_failure_message(
    result: ComparisonResult, fixture: str, baseline_version: str
) -> str:
    """Render a release-manager-friendly regression report."""
    lines = [
        f"PERFORMANCE REGRESSION — fixture: {fixture}",
        f"baseline: {baseline_version}  candidate (this run)",
        "",
    ]
    for entry in result.regressions:
        lines.append(f"  {entry.metric}")
        if entry.unit == "count":
            b = _format_count(entry.baseline_median)
            c = _format_count(entry.candidate_median)
            delta = entry.candidate_median - entry.baseline_median
            sign = "+" if delta >= 0 else ""
            lines.append(f"    baseline median   {b:<8s} candidate median  {c}")
            lines.append(
                f"    delta             {sign}{_format_count(delta)}   "
                "(counts must match exactly; algorithmic regression)"
            )
        else:
            lines.append(
                f"    baseline median   {entry.baseline_median:.3f}s"
                f"   p25={entry.baseline_p25:.3f}s  p75={entry.baseline_p75:.3f}s"
            )
            lines.append(
                f"    candidate median  {entry.candidate_median:.3f}s"
                f"   p25={entry.candidate_p25:.3f}s  p75={entry.candidate_p75:.3f}s"
            )
            lines.append(
                f"    delta             {entry.delta_pct:+.1%} "
                "(threshold: +25% AND candidate.p25 > baseline.p75)"
            )
        lines.append("")
    lines.append(f"Stable metrics: {len(result.stable)} (suppressed)")
    lines.append(f"Improvements: {len(result.improvements)}")
    lines.append(f"Skipped (sub-second baseline): {len(result.skipped)}")
    lines.append(f"Shape mismatches: {len(result.shape_mismatches)}")
    fp = machine_fingerprint()
    note = (
        "(mismatch — see warning)"
        if result.fingerprint_mismatch
        else "(matches baseline)"
    )
    lines.append(f"Machine fingerprint: {fp} {note}")
    lines.extend(
        [
            "",
            "To investigate:",
            f"  cd Example_Projects/{fixture}",
            "  rm -rf generated/ resources/lhp/",
            "  lhp --perf generate --env dev",
            "  cat .lhp/logs/perf.log",
            "",
            "To accept and reset (only if drift is intentional):",
            f"  python -m tests.performance.benchmark capture --fixture {fixture}",
        ]
    )
    return "\n".join(lines)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def machine_fingerprint() -> str:
    """Return ``{system}-{machine}-py{major}.{minor}`` for baseline portability."""
    return (
        f"{platform.system()}-{platform.machine()}"
        f"-py{sys.version_info.major}.{sys.version_info.minor}"
    )


def run_benchmark(fixture_path: Path, runs: int = 5) -> list[BenchmarkRun]:
    """Run ``lhp --perf generate --env dev`` ``runs`` times.

    Wipes fixture state between iterations to ensure each run measures a full
    cold-start generation. Raises ``RuntimeError`` if any iteration exits
    non-zero or if ``--perf`` instrumentation collected no data.
    """
    from click.testing import CliRunner

    from lhp.cli.main import cli
    from lhp.utils.performance_timer import (
        _summary,
        reset_perf_summary,
    )

    results: list[BenchmarkRun] = []
    for _ in range(runs):
        _wipe_state(fixture_path)
        reset_perf_summary()
        runner = CliRunner()
        saved_cwd = os.getcwd()
        try:
            os.chdir(fixture_path)
            t0 = time.perf_counter()
            result = runner.invoke(cli, ["--perf", "generate", "--env", "dev"])
            wall = time.perf_counter() - t0
        finally:
            os.chdir(saved_cwd)

        if result.exit_code != 0:
            raise RuntimeError(
                f"lhp generate failed on {fixture_path.name}: "
                f"exit={result.exit_code}\n"
                f"{result.output}\nException: {result.exception!r}"
            )
        snap = _summary.snapshot()
        if not snap["phases"]:
            raise RuntimeError(
                "--perf produced no phase data; harness wiring is broken "
                "(_summary.snapshot() returned empty phases)"
            )
        results.append(
            BenchmarkRun(
                phases=dict(snap["phases"]),
                sub_phases={
                    k: tuple(tuple(t) for t in v) for k, v in snap["sub_phases"].items()
                },
                categories={k: dict(v) for k, v in snap["categories"].items()},
                counts=dict(snap["counts"]),
                wall_clock_seconds=wall,
                exit_code=result.exit_code,
            )
        )
    return results


def summarize(runs: list[BenchmarkRun]) -> dict[str, MetricSummary]:
    """Compute ``MetricSummary`` (median, p25, p75) per metric across runs."""
    if not runs:
        return {}
    accum: dict[str, tuple[list[float], str]] = {}
    for run in runs:
        for key, (value, unit) in _flatten_run(run).items():
            if key in accum:
                accum[key][0].append(value)
            else:
                accum[key] = ([value], unit)
    out: dict[str, MetricSummary] = {}
    for key, (values, unit) in accum.items():
        p25, median, p75 = _percentiles(values)
        out[key] = MetricSummary(median=median, p25=p25, p75=p75, unit=unit)  # type: ignore[arg-type]
    return out


def compare(
    baseline: BaselineDoc,
    candidate: Mapping[str, MetricSummary],
    candidate_shape: Mapping[str, int],
) -> ComparisonResult:
    """Compare candidate metrics against baseline; classify each entry."""
    regressions: list[ComparisonEntry] = []
    improvements: list[ComparisonEntry] = []
    stable: list[ComparisonEntry] = []
    skipped: list[ComparisonEntry] = []
    shape_mismatches: list[ComparisonEntry] = []

    fingerprint_mismatch = baseline["machine_fingerprint"] != machine_fingerprint()

    shape_changed: set[str] = set()
    baseline_shape = baseline.get("project_shape", {})
    for name, baseline_count in baseline_shape.items():
        candidate_count = candidate_shape.get(name)
        if candidate_count != baseline_count:
            shape_changed.add(name)
            shape_mismatches.append(
                ComparisonEntry(
                    metric=f"project_shape.{name}",
                    baseline_median=float(baseline_count),
                    candidate_median=(
                        float(candidate_count)
                        if candidate_count is not None
                        else math.nan
                    ),
                    delta_pct=0.0,
                    reason=(
                        f"baseline={baseline_count}, " f"candidate={candidate_count}"
                    ),
                    unit="count",
                )
            )
    for name, candidate_count in candidate_shape.items():
        if name not in baseline_shape:
            shape_changed.add(name)
            shape_mismatches.append(
                ComparisonEntry(
                    metric=f"project_shape.{name}",
                    baseline_median=math.nan,
                    candidate_median=float(candidate_count),
                    delta_pct=0.0,
                    reason=f"new in candidate: {name}={candidate_count}",
                    unit="count",
                )
            )

    for metric, c in candidate.items():
        # Suppress count.<name> regressions if the shape already drifted.
        if metric.startswith("count."):
            shape_key = metric.split(".", 1)[1]
            if shape_key in shape_changed:
                continue
        if metric not in baseline["metrics"]:
            continue
        b_data = baseline["metrics"][metric]
        try:
            b = MetricSummary(
                median=float(b_data["median"]),
                p25=float(b_data["p25"]),
                p75=float(b_data["p75"]),
                unit=str(b_data["unit"]),  # type: ignore[arg-type]
            )
        except (KeyError, ValueError, TypeError):
            continue

        verdict = _classify(b, c)
        if b.median != 0:
            delta_pct = (c.median - b.median) / b.median
        else:
            delta_pct = 0.0 if c.median == 0 else math.inf
        entry = ComparisonEntry(
            metric=metric,
            baseline_median=b.median,
            candidate_median=c.median,
            delta_pct=delta_pct,
            reason=verdict,
            baseline_p25=b.p25,
            baseline_p75=b.p75,
            candidate_p25=c.p25,
            candidate_p75=c.p75,
            unit=b.unit,
        )
        bucket = {
            "regression": regressions,
            "improvement": improvements,
            "skipped": skipped,
        }.get(verdict, stable)
        bucket.append(entry)

    return ComparisonResult(
        regressions=tuple(regressions),
        improvements=tuple(improvements),
        stable=tuple(stable),
        skipped=tuple(skipped),
        shape_mismatches=tuple(shape_mismatches),
        fingerprint_mismatch=fingerprint_mismatch,
    )


def _round_metric(value: float) -> float:
    """Round to 6 decimals for JSON serialization."""
    if not math.isfinite(value):
        return value
    return round(value, 6)


def _build_baseline_doc(
    fixture_name: str,
    lhp_version: str,
    runs_count: int,
    summary: Mapping[str, MetricSummary],
    project_shape: Mapping[str, int],
) -> BaselineDoc:
    return BaselineDoc(
        lhp_version=lhp_version,
        captured_at=datetime.now(timezone.utc).isoformat(timespec="seconds"),
        machine_fingerprint=machine_fingerprint(),
        fixture=fixture_name,
        runs=runs_count,
        project_shape={k: int(v) for k, v in project_shape.items()},
        metrics={
            metric: {
                "median": _round_metric(s.median),
                "p25": _round_metric(s.p25),
                "p75": _round_metric(s.p75),
                "unit": s.unit,
            }
            for metric, s in sorted(summary.items())
        },
    )


def _default_lhp_version() -> str:
    try:
        from importlib.metadata import version as _v

        v = _v("lakehouse-plumber")
    except Exception:
        v = "0.0.0"
    return v if v.startswith("v") else f"v{v}"


def capture_baseline(
    fixture_path: Path,
    output_dir: Path,
    runs: int = 5,
    lhp_version: str | None = None,
    archive_dir: Path | None = None,
    max_workers: int | None = None,
) -> Path:
    """Run the benchmark and write a baseline JSON; archive raw perf.log files.

    If ``max_workers`` is given, it is forwarded to ``lhp generate`` as
    ``--max-workers N``; otherwise the CLI default (``min(cpu_count, 8)``)
    applies.

    Returns the path of the written baseline JSON.
    """
    version = lhp_version or _default_lhp_version()
    output_dir.mkdir(parents=True, exist_ok=True)
    target = output_dir / f"{version}.json"

    benchmark_runs: list[BenchmarkRun] = []
    perf_log = fixture_path / ".lhp" / "logs" / "perf.log"
    if archive_dir is not None:
        archive_dir.mkdir(parents=True, exist_ok=True)

    from click.testing import CliRunner

    from lhp.cli.main import cli
    from lhp.utils.performance_timer import (
        _summary,
        reset_perf_summary,
    )

    cli_args = ["--perf", "generate", "--env", "dev"]
    if max_workers is not None:
        cli_args.extend(["--max-workers", str(max_workers)])

    for i in range(runs):
        _wipe_state(fixture_path)
        reset_perf_summary()
        runner = CliRunner()
        saved_cwd = os.getcwd()
        try:
            os.chdir(fixture_path)
            t0 = time.perf_counter()
            result = runner.invoke(cli, cli_args)
            wall = time.perf_counter() - t0
        finally:
            os.chdir(saved_cwd)

        if result.exit_code != 0:
            raise RuntimeError(
                f"lhp generate failed (run {i+1}/{runs}, fixture "
                f"{fixture_path.name}): exit={result.exit_code}\n"
                f"{result.output}\nException: {result.exception!r}"
            )
        snap = _summary.snapshot()
        if not snap["phases"]:
            raise RuntimeError(
                "--perf produced no phase data; harness wiring is broken"
            )
        benchmark_runs.append(
            BenchmarkRun(
                phases=dict(snap["phases"]),
                sub_phases={
                    k: tuple(tuple(t) for t in v) for k, v in snap["sub_phases"].items()
                },
                categories={k: dict(v) for k, v in snap["categories"].items()},
                counts=dict(snap["counts"]),
                wall_clock_seconds=wall,
                exit_code=result.exit_code,
            )
        )

        if archive_dir is not None and perf_log.exists():
            shutil.copy2(perf_log, archive_dir / f"run{i+1}.perf.log")

    summary = summarize(benchmark_runs)
    doc = _build_baseline_doc(
        fixture_name=fixture_path.name,
        lhp_version=version,
        runs_count=runs,
        summary=summary,
        project_shape=dict(benchmark_runs[-1].counts),
    )
    target.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return target


def load_latest_baseline(baselines_dir: Path) -> BaselineDoc:
    """Return the baseline with the highest semver in ``baselines_dir``."""
    candidates: list[tuple[tuple[int, int, int], Path]] = []
    for p in baselines_dir.glob("v*.json"):
        try:
            key = _semver_key(p.name)
        except ValueError:
            continue
        candidates.append((key, p))
    if not candidates:
        raise FileNotFoundError(f"No vX.Y.Z.json baseline found under {baselines_dir}")
    candidates.sort(reverse=True)
    chosen = candidates[0][1]
    data = json.loads(chosen.read_text(encoding="utf-8"))
    return data  # type: ignore[return-value]


def _seed_from_log(
    perf_log_path: Path,
    fixture_name: str,
    version: str,
    output_dir: Path,
) -> Path:
    """Build a single-run baseline JSON from one existing ``perf.log``."""
    text = perf_log_path.read_text(encoding="utf-8")
    run = _parse_perf_log_text(text)
    if not run.phases:
        raise RuntimeError(
            f"No phases parsed from {perf_log_path}; not a valid perf.log"
        )
    summary = summarize([run])
    output_dir.mkdir(parents=True, exist_ok=True)
    doc = _build_baseline_doc(
        fixture_name=fixture_name,
        lhp_version=version,
        runs_count=1,
        summary=summary,
        project_shape=dict(run.counts),
    )
    target = output_dir / f"{version}.json"
    target.write_text(json.dumps(doc, indent=2) + "\n", encoding="utf-8")
    return target


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _resolve_fixture_path(name: str) -> Path:
    return REPO_ROOT / "Example_Projects" / name


def _cmd_capture(args: argparse.Namespace) -> int:
    fixture_path = _resolve_fixture_path(args.fixture)
    if not fixture_path.exists():
        print(f"Fixture not found: {fixture_path}", file=sys.stderr)
        return 2
    version = args.version or _default_lhp_version()
    output_dir = BASELINES_DIR / args.fixture
    archive_dir = REPO_ROOT / ".perf_runs" / version / args.fixture
    workers_note = f" max_workers={args.max_workers}" if args.max_workers else ""
    print(
        f"Capturing baseline: fixture={args.fixture} version={version} "
        f"runs={args.runs}{workers_note}"
    )
    target = capture_baseline(
        fixture_path=fixture_path,
        output_dir=output_dir,
        runs=args.runs,
        lhp_version=version,
        archive_dir=archive_dir,
        max_workers=args.max_workers,
    )
    print(f"Wrote: {target}")
    print(f"Archived raw perf.log -> {archive_dir}")
    return 0


def _cmd_seed(args: argparse.Namespace) -> int:
    log_path = Path(args.from_path).resolve()
    if not log_path.exists():
        print(f"Source log not found: {log_path}", file=sys.stderr)
        return 2
    version = args.version
    if not version.startswith("v"):
        version = f"v{version}"
    output_dir = BASELINES_DIR / args.fixture
    print(
        f"Seeding baseline: fixture={args.fixture} version={version} "
        f"from={log_path}"
    )
    target = _seed_from_log(
        perf_log_path=log_path,
        fixture_name=args.fixture,
        version=version,
        output_dir=output_dir,
    )
    print(f"Wrote: {target}")
    print(
        "Note: single-run seed has median=p25=p75; not gating-quality. "
        "Use `capture` for the active gating baseline."
    )
    return 0


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m tests.performance.benchmark",
        description=(
            "LHP release-time performance benchmark harness. "
            "Use `capture` for gating baselines (median-of-N from this build), "
            "or `seed` to convert one existing perf.log into a historical "
            "reference baseline."
        ),
    )
    sub = parser.add_subparsers(dest="command", required=True)

    capture = sub.add_parser(
        "capture", help="Run N iterations and write a baseline JSON"
    )
    capture.add_argument("--fixture", required=True)
    capture.add_argument("--runs", type=int, default=5)
    capture.add_argument(
        "--version",
        default=None,
        help="Baseline version label, e.g. v0.8.7. "
        "Defaults to importlib.metadata.version('lakehouse-plumber').",
    )
    capture.add_argument(
        "--max-workers",
        type=int,
        default=None,
        help="Forward --max-workers N to `lhp generate`. "
        "Default: CLI default (min(cpu_count, 8)).",
    )
    capture.set_defaults(func=_cmd_capture)

    seed = sub.add_parser(
        "seed",
        help="Convert one existing perf.log into a single-run baseline JSON",
    )
    seed.add_argument("--from", dest="from_path", required=True)
    seed.add_argument("--fixture", required=True)
    seed.add_argument("--version", required=True)
    seed.set_defaults(func=_cmd_seed)

    return parser


def main(argv: Iterable[str] | None = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(list(argv) if argv is not None else None)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
