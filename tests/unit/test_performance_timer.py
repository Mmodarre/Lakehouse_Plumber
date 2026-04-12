"""Unit tests for the performance_timer module."""

import logging
import threading
import time

import pytest

from lhp.utils.performance_timer import (
    PerfSummary,
    _perf_logger,
    enable_perf_timing,
    is_perf_enabled,
    log_perf_summary,
    perf_timer,
    reset_perf_summary,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_perf_state():
    """Reset module-level perf state before and after each test."""
    import lhp.utils.performance_timer as pt

    pt._enabled = False
    pt._start_wall_clock = None
    pt._summary.reset()
    # Remove any file handlers added during tests
    for handler in _perf_logger.handlers[:]:
        handler.close()
        _perf_logger.removeHandler(handler)
    yield
    pt._enabled = False
    pt._start_wall_clock = None
    pt._summary.reset()
    for handler in _perf_logger.handlers[:]:
        handler.close()
        _perf_logger.removeHandler(handler)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestPerfTimerDisabled:
    """Tests for when perf timing is disabled (the default)."""

    def test_perf_timer_disabled_is_noop(self, caplog):
        """Zero logging when _enabled is False."""
        assert not is_perf_enabled()
        with perf_timer("should_not_log"):
            pass
        # No perf log records at all
        assert "[PERF]" not in caplog.text

    def test_perf_timer_disabled_still_executes_body(self):
        """The wrapped code block still runs even when disabled."""
        executed = False
        with perf_timer("test"):
            executed = True
        assert executed


class TestPerfTimerEnabled:
    """Tests for when perf timing is enabled."""

    def test_perf_timer_enabled_logs_at_debug(self, tmp_path):
        """Individual timings are logged at DEBUG with [PERF] prefix."""
        enable_perf_timing(tmp_path)

        with perf_timer("my_operation"):
            time.sleep(0.01)

        perf_log = tmp_path / ".lhp" / "logs" / "perf.log"
        assert perf_log.exists()
        content = perf_log.read_text()
        assert "[PERF] my_operation:" in content

    def test_perf_timer_with_category_records_aggregate(self, tmp_path):
        """Category timings are collected in PerfSummary."""
        import lhp.utils.performance_timer as pt

        enable_perf_timing(tmp_path)

        for _ in range(3):
            with perf_timer("op", category="my_cat"):
                pass

        assert "my_cat" in pt._summary._timings
        assert len(pt._summary._timings["my_cat"]) == 3

    def test_perf_timer_with_phase_records_phase(self, tmp_path):
        """Phase timings are recorded in phase_timings dict."""
        import lhp.utils.performance_timer as pt

        enable_perf_timing(tmp_path)

        with perf_timer("Discovery phase", phase=True):
            time.sleep(0.01)

        assert "Discovery phase" in pt._summary._phase_timings
        assert pt._summary._phase_timings["Discovery phase"] >= 0.005

    def test_perf_timer_measures_elapsed_time(self, tmp_path):
        """Measured elapsed time is in a reasonable range."""
        import lhp.utils.performance_timer as pt

        enable_perf_timing(tmp_path)

        with perf_timer("sleep_test", category="timing"):
            time.sleep(0.05)

        durations = pt._summary._timings["timing"]
        assert len(durations) == 1
        assert 0.04 <= durations[0] <= 0.3  # generous upper bound for CI


class TestPerfSummary:
    """Tests for the PerfSummary aggregator."""

    def test_perf_summary_thread_safety(self):
        """Concurrent recording from multiple threads doesn't corrupt data."""
        summary = PerfSummary()
        num_threads = 10
        records_per_thread = 100
        barrier = threading.Barrier(num_threads)

        def worker():
            barrier.wait()
            for _ in range(records_per_thread):
                summary.record("cat", 0.001)

        threads = [threading.Thread(target=worker) for _ in range(num_threads)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert len(summary._timings["cat"]) == num_threads * records_per_thread

    def test_perf_summary_log_output(self, tmp_path):
        """Summary output contains expected sections at INFO level."""
        enable_perf_timing(tmp_path)

        import lhp.utils.performance_timer as pt

        pt._summary.record_phase("Phase A", 1.234)
        pt._summary.record_phase("Phase B", 2.345)
        pt._summary.record("process_fg", 0.1)
        pt._summary.record("process_fg", 0.2)

        log_perf_summary()

        perf_log = tmp_path / ".lhp" / "logs" / "perf.log"
        content = perf_log.read_text()
        assert "PERFORMANCE SUMMARY" in content
        assert "Phase breakdown:" in content
        assert "Phase A" in content
        assert "Phase B" in content
        assert "Per-flowgroup aggregate stats:" in content
        assert "process_fg" in content
        assert "cnt=2" in content

    def test_wall_clock_timestamps_in_summary(self, tmp_path):
        """Summary contains Started and Ended timestamps."""
        enable_perf_timing(tmp_path)
        log_perf_summary()

        perf_log = tmp_path / ".lhp" / "logs" / "perf.log"
        content = perf_log.read_text()
        assert "Started:" in content
        assert "Ended:" in content


class TestEnableReset:
    """Tests for enable and reset behavior."""

    def test_enable_resets_summary(self, tmp_path):
        """enable_perf_timing() clears previous data."""
        import lhp.utils.performance_timer as pt

        enable_perf_timing(tmp_path)
        pt._summary.record("old_cat", 1.0)
        assert "old_cat" in pt._summary._timings

        # Re-enable should clear
        enable_perf_timing(tmp_path)
        assert "old_cat" not in pt._summary._timings

    def test_enable_without_project_root(self):
        """enable_perf_timing(None) enables timing without file handler."""
        enable_perf_timing(None)
        assert is_perf_enabled()
        # No file handlers added
        assert len(_perf_logger.handlers) == 0

    def test_enable_sets_flag(self, tmp_path):
        """Flag is set to True after enabling."""
        assert not is_perf_enabled()
        enable_perf_timing(tmp_path)
        assert is_perf_enabled()

    def test_reset_clears_all(self):
        """reset_perf_summary clears timings and phases."""
        import lhp.utils.performance_timer as pt

        pt._summary.record("cat", 1.0)
        pt._summary.record_phase("phase", 2.0)
        reset_perf_summary()
        assert len(pt._summary._timings) == 0
        assert len(pt._summary._phase_timings) == 0

    def test_perf_log_not_propagated(self, tmp_path):
        """Perf logger does not propagate to root logger."""
        assert _perf_logger.propagate is False
        enable_perf_timing(tmp_path)
        assert _perf_logger.propagate is False
