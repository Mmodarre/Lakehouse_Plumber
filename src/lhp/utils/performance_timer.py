"""Lightweight performance timing instrumentation for LakehousePlumber.

Provides a zero-overhead (when disabled) context manager for timing code sections
and aggregating results into a summary report. All output goes to a dedicated
perf.log file, never to the main lhp.log or console.

Usage:
    lhp --perf generate --env dev --force
    # Output: .lhp/logs/perf.log
"""

import logging
import threading
import time
from contextlib import contextmanager
from datetime import datetime
from pathlib import Path
from typing import Optional

# ---------------------------------------------------------------------------
# Module-level state
# ---------------------------------------------------------------------------

_enabled: bool = False
_start_wall_clock: Optional[str] = None
_perf_logger: logging.Logger = logging.getLogger("lhp.perf")
_perf_logger.propagate = False  # Never leak to main lhp.log or console


class PerfSummary:
    """Thread-safe aggregator for performance timings."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._timings: dict[str, list[float]] = {}
        self._phase_timings: dict[str, float] = {}

    def record(self, category: str, duration: float) -> None:
        """Record a duration for a named category (thread-safe)."""
        with self._lock:
            self._timings.setdefault(category, []).append(duration)

    def record_phase(self, phase: str, duration: float) -> None:
        """Record a one-off phase duration (thread-safe)."""
        with self._lock:
            self._phase_timings[phase] = duration

    def reset(self) -> None:
        """Clear all collected timings."""
        with self._lock:
            self._timings.clear()
            self._phase_timings.clear()

    def log_summary(self) -> None:
        """Log the aggregated summary at INFO level to perf logger."""
        end_wall_clock = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        lines = [
            "[PERF] ============ PERFORMANCE SUMMARY ============",
            f"[PERF] Started: {_start_wall_clock}",
            f"[PERF] Ended:   {end_wall_clock}",
            "[PERF]",
        ]

        # Phase breakdown
        with self._lock:
            phase_timings = dict(self._phase_timings)
            timings = {k: list(v) for k, v in self._timings.items()}

        if phase_timings:
            total_phase = sum(phase_timings.values())
            lines.append("[PERF] Phase breakdown:")
            for phase, elapsed in phase_timings.items():
                pct = (elapsed / total_phase * 100) if total_phase > 0 else 0
                lines.append(
                    f"[PERF]   {phase:<35s} {elapsed:>8.3f}s  ({pct:>5.1f}%)"
                )
            lines.append(
                f"[PERF]   {'Total':<35s} {total_phase:>8.3f}s"
            )
            lines.append("[PERF]")

        # Per-category aggregate stats
        if timings:
            lines.append("[PERF] Per-flowgroup aggregate stats:")
            for category, durations in sorted(timings.items()):
                cnt = len(durations)
                total = sum(durations)
                avg = total / cnt if cnt else 0
                mn = min(durations) if durations else 0
                mx = max(durations) if durations else 0
                lines.append(
                    f"[PERF]   {category:<22s} cnt={cnt:<4d} "
                    f"avg={avg:.3f}s  min={mn:.3f}s  max={mx:.3f}s  total={total:>7.2f}s"
                )

        lines.append("[PERF] =============================================")

        for line in lines:
            _perf_logger.info(line)


# Module-level singleton
_summary = PerfSummary()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def enable_perf_timing(project_root: Optional[Path] = None) -> None:
    """Enable performance timing and configure perf.log output.

    Args:
        project_root: Project root for placing .lhp/logs/perf.log.
            If None, timing is still enabled but no file handler is set up.
    """
    global _enabled, _start_wall_clock

    _enabled = True
    _start_wall_clock = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    _summary.reset()

    # Remove any stale handlers from previous runs
    for handler in _perf_logger.handlers[:]:
        handler.close()
        _perf_logger.removeHandler(handler)

    _perf_logger.setLevel(logging.DEBUG)
    _perf_logger.propagate = False

    if project_root is not None:
        log_dir = project_root / ".lhp" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(
            log_dir / "perf.log", mode="w", encoding="utf-8"
        )
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter("%(asctime)s - %(message)s"))
        _perf_logger.addHandler(handler)


def is_perf_enabled() -> bool:
    """Return whether performance timing is currently enabled."""
    return _enabled


def log_perf_summary() -> None:
    """Log the aggregated performance summary (INFO level to perf.log)."""
    if _enabled:
        _summary.log_summary()


def reset_perf_summary() -> None:
    """Clear all collected timings."""
    _summary.reset()


@contextmanager
def perf_timer(
    label: str,
    category: Optional[str] = None,
    phase: bool = False,
):
    """Time a code block and log / aggregate the result.

    When ``_enabled`` is False this yields immediately with zero overhead
    beyond a single boolean check.

    Args:
        label: Human-readable label for the timing line.
        category: If provided, duration is recorded into the aggregate
            summary under this category name.
        phase: If True, duration is recorded as a top-level phase in the
            summary's phase breakdown table.
    """
    if not _enabled:
        yield
        return

    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed = time.perf_counter() - start
        _perf_logger.debug(f"[PERF] {label}: {elapsed:.4f}s")
        if category:
            _summary.record(category, elapsed)
        if phase:
            _summary.record_phase(label, elapsed)
