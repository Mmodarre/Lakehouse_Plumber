"""Tests for OpenCodeProcess race condition fixes.

Verifies that the health monitor does not interfere with
``ensure_workspace()`` stop-and-restart transitions, and that
``_wait_for_healthy()`` detects early process exit (e.g. port conflict).

Adapted from original OpenCodeManager tests for the new OpenCodeProcess class.
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lhp.api.services.opencode_manager import OpenCodeProcess


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_process(**kwargs) -> OpenCodeProcess:
    """Create a process with sensible test defaults."""
    defaults = {
        "user_id": "test-user",
        "workspace_path": Path("/tmp"),
        "port": 4096,
    }
    defaults.update(kwargs)
    return OpenCodeProcess(**defaults)


def _mock_subprocess(*, returncode=None, stderr_data=b""):
    """Create a mock asyncio.subprocess.Process.

    Args:
        returncode: None means "still running", int means "already exited".
        stderr_data: bytes returned by stderr.read().
    """
    proc = MagicMock()
    proc.returncode = returncode
    proc.pid = 12345

    stderr_stream = AsyncMock()
    stderr_stream.read = AsyncMock(return_value=stderr_data)
    proc.stderr = stderr_stream

    proc.wait = AsyncMock()
    proc.send_signal = MagicMock()
    proc.kill = MagicMock()
    return proc


# ---------------------------------------------------------------------------
# _restarting flag: suppress _try_restart during ensure_workspace
# ---------------------------------------------------------------------------


class TestRestartingSuppression:
    """Verify _restarting flag prevents health monitor interference."""

    @pytest.mark.asyncio
    async def test_try_restart_skipped_when_restarting(self):
        """_try_restart() should be a no-op while _restarting is True."""
        proc = _make_process()
        proc._restarting = True
        proc._process = _mock_subprocess(returncode=1)

        initial_restart_count = proc._restart_count
        await proc._try_restart()

        # Restart count should NOT have incremented
        assert proc._restart_count == initial_restart_count

    @pytest.mark.asyncio
    async def test_try_restart_works_when_not_restarting(self):
        """_try_restart() should proceed normally when _restarting is False."""
        proc = _make_process()
        proc._restarting = False
        proc._process = _mock_subprocess(returncode=1)

        with patch.object(proc, "_spawn_process", new_callable=AsyncMock) as mock_spawn:
            await proc._try_restart()
            mock_spawn.assert_called_once()
            assert proc._restart_count == 1

    @pytest.mark.asyncio
    async def test_try_restart_skipped_when_stopping(self):
        """_try_restart() should be a no-op while _stopping is True."""
        proc = _make_process()
        proc._stopping = True
        proc._process = _mock_subprocess(returncode=1)

        await proc._try_restart()
        assert proc._restart_count == 0


# ---------------------------------------------------------------------------
# ensure_workspace: _restarting lifecycle
# ---------------------------------------------------------------------------


class TestEnsureWorkspaceRestarting:
    """Verify ensure_workspace sets/clears _restarting correctly."""

    @pytest.mark.asyncio
    async def test_restarting_flag_set_during_transition(self):
        """_restarting should be True while stop+spawn are in progress."""
        proc = _make_process(workspace_path=Path("/tmp"))
        proc._available = True
        proc._process = _mock_subprocess()

        observed_during_stop = None
        observed_during_spawn = None

        async def spy_stop():
            nonlocal observed_during_stop
            observed_during_stop = proc._restarting
            proc._process = None
            proc._available = False

        async def spy_spawn():
            nonlocal observed_during_spawn
            observed_during_spawn = proc._restarting
            proc._available = True

        with (
            patch.object(proc, "_stop_process", side_effect=spy_stop),
            patch.object(proc, "_spawn_process", side_effect=spy_spawn),
        ):
            await proc.ensure_workspace(Path("/workspace/project"))

        assert observed_during_stop is True, "_restarting should be True during _stop_process"
        assert observed_during_spawn is True, "_restarting should be True during _spawn_process"
        assert proc._restarting is False, "_restarting should be cleared after transition"

    @pytest.mark.asyncio
    async def test_restarting_flag_cleared_on_error(self):
        """_restarting should be reset even if _spawn_process raises."""
        proc = _make_process(workspace_path=Path("/tmp"))
        proc._available = True
        proc._process = _mock_subprocess()

        async def failing_spawn():
            raise RuntimeError("spawn failed")

        with (
            patch.object(proc, "_stop_process", new_callable=AsyncMock),
            patch.object(proc, "_spawn_process", side_effect=failing_spawn),
        ):
            with pytest.raises(RuntimeError, match="spawn failed"):
                await proc.ensure_workspace(Path("/workspace/project"))

        assert proc._restarting is False, "_restarting must be cleared even on error"

    @pytest.mark.asyncio
    async def test_ensure_workspace_noop_when_already_correct(self):
        """No restart if already running in the correct directory."""
        proc = _make_process(workspace_path=Path("/workspace/project"))
        proc._available = True

        with patch.object(proc, "_stop_process", new_callable=AsyncMock) as mock_stop:
            await proc.ensure_workspace(Path("/workspace/project"))
            mock_stop.assert_not_called()

        assert proc._restarting is False


# ---------------------------------------------------------------------------
# _wait_for_healthy: early exit on process death
# ---------------------------------------------------------------------------


class TestWaitForHealthyLiveness:
    """Verify _wait_for_healthy detects early process exit."""

    @pytest.mark.asyncio
    async def test_returns_false_when_process_exits_immediately(self):
        """If the process already exited, return False without polling."""
        proc = _make_process()
        proc._process = _mock_subprocess(
            returncode=1,
            stderr_data=b"Error: address already in use :4096\n",
        )

        result = await proc._wait_for_healthy()
        assert result is False

    @pytest.mark.asyncio
    async def test_stderr_captured_on_early_exit(self, caplog):
        """Stderr content should appear in the error log."""
        proc = _make_process()
        proc._process = _mock_subprocess(
            returncode=1,
            stderr_data=b"bind: address already in use",
        )

        with caplog.at_level("ERROR", logger="lhp.api.services.opencode_manager"):
            result = await proc._wait_for_healthy()

        assert result is False
        assert "OpenCode exited immediately (rc=1)" in caplog.text
        assert "bind: address already in use" in caplog.text

    @pytest.mark.asyncio
    async def test_stderr_read_failure_handled(self, caplog):
        """If stderr.read() raises, we still get the return code logged."""
        proc = _make_process()
        mock_proc = _mock_subprocess(returncode=137)
        mock_proc.stderr.read = AsyncMock(side_effect=OSError("broken pipe"))
        proc._process = mock_proc

        with caplog.at_level("ERROR", logger="lhp.api.services.opencode_manager"):
            result = await proc._wait_for_healthy()

        assert result is False
        assert "rc=137" in caplog.text

    @pytest.mark.asyncio
    async def test_returns_true_when_healthy(self):
        """Normal case: process stays alive and health check passes."""
        proc = _make_process()
        proc._process = _mock_subprocess(returncode=None)

        with patch.object(proc, "health_check", new_callable=AsyncMock, return_value=True):
            result = await proc._wait_for_healthy()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_timeout(self):
        """If health check never passes and process stays alive, return False."""
        proc = _make_process()
        proc._process = _mock_subprocess(returncode=None)

        with (
            patch.object(
                proc, "health_check", new_callable=AsyncMock, return_value=False
            ),
            patch(
                "lhp.api.services.opencode_manager._STARTUP_WAIT_S", 0.2
            ),
            patch(
                "lhp.api.services.opencode_manager._STARTUP_POLL_S", 0.05
            ),
        ):
            result = await proc._wait_for_healthy()

        assert result is False

    @pytest.mark.asyncio
    async def test_no_process_still_polls_health(self):
        """If _process is None (edge case), just poll health endpoint."""
        proc = _make_process()
        proc._process = None

        with patch.object(proc, "health_check", new_callable=AsyncMock, return_value=True):
            result = await proc._wait_for_healthy()

        assert result is True


# ---------------------------------------------------------------------------
# Race condition scenario: simulated interleaving
# ---------------------------------------------------------------------------


class TestRaceConditionPrevention:
    """Simulate the exact race described in the bug report."""

    @pytest.mark.asyncio
    async def test_health_monitor_cannot_restart_during_workspace_transition(self):
        """Simulate: health monitor wakes up mid-transition and tries to restart.

        Before the fix, the health monitor could call _try_restart() during
        ensure_workspace()'s stop-and-restart window, spawning a competing
        process with the OLD workspace. The _restarting flag prevents this.
        """
        proc = _make_process(workspace_path=Path("/tmp"))
        proc._available = True
        proc._process = _mock_subprocess()

        restart_calls = []

        async def tracking_try_restart():
            restart_calls.append(proc._restarting)

        async def slow_stop():
            proc._process = _mock_subprocess(returncode=0)
            proc._available = False
            await asyncio.sleep(0)
            await tracking_try_restart()
            proc._process = None

        async def mock_spawn():
            proc._process = _mock_subprocess(returncode=None)
            proc._available = True

        with (
            patch.object(proc, "_stop_process", side_effect=slow_stop),
            patch.object(proc, "_spawn_process", side_effect=mock_spawn),
        ):
            await proc.ensure_workspace(Path("/workspace/project"))

        assert len(restart_calls) == 1
        assert restart_calls[0] is True, (
            "Health monitor should see _restarting=True during transition"
        )
        assert proc.workspace_path == Path("/workspace/project")
        assert proc._restarting is False
