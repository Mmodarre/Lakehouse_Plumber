"""Tests for OpenCodeManager race condition fixes.

Verifies that the health monitor does not interfere with
``ensure_workspace()`` stop-and-restart transitions, and that
``_wait_for_healthy()`` detects early process exit (e.g. port conflict).
"""

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from lhp.api.services.opencode_manager import OpenCodeManager


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_manager(**kwargs) -> OpenCodeManager:
    """Create a manager in subprocess mode with sensible test defaults."""
    return OpenCodeManager(port=4096, **kwargs)


def _mock_process(*, returncode=None, stderr_data=b""):
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
        mgr = _make_manager()
        mgr._restarting = True
        mgr._process = _mock_process(returncode=1)
        mgr._current_cwd = "/tmp"

        initial_restart_count = mgr._restart_count
        await mgr._try_restart()

        # Restart count should NOT have incremented
        assert mgr._restart_count == initial_restart_count

    @pytest.mark.asyncio
    async def test_try_restart_works_when_not_restarting(self):
        """_try_restart() should proceed normally when _restarting is False."""
        mgr = _make_manager()
        mgr._restarting = False
        mgr._process = _mock_process(returncode=1)
        mgr._current_cwd = "/tmp"

        with patch.object(mgr, "_spawn_process", new_callable=AsyncMock) as mock_spawn:
            await mgr._try_restart()
            mock_spawn.assert_called_once_with("/tmp")
            assert mgr._restart_count == 1

    @pytest.mark.asyncio
    async def test_try_restart_skipped_when_stopping(self):
        """_try_restart() should be a no-op while _stopping is True."""
        mgr = _make_manager()
        mgr._stopping = True
        mgr._process = _mock_process(returncode=1)

        await mgr._try_restart()
        assert mgr._restart_count == 0

    @pytest.mark.asyncio
    async def test_try_restart_skipped_for_external(self):
        """_try_restart() should be a no-op in external mode."""
        mgr = _make_manager(external_url="http://external:4096")
        mgr._process = _mock_process(returncode=1)

        await mgr._try_restart()
        assert mgr._restart_count == 0


# ---------------------------------------------------------------------------
# ensure_workspace: _restarting lifecycle
# ---------------------------------------------------------------------------


class TestEnsureWorkspaceRestarting:
    """Verify ensure_workspace sets/clears _restarting correctly."""

    @pytest.mark.asyncio
    async def test_restarting_flag_set_during_transition(self):
        """_restarting should be True while stop+spawn are in progress."""
        mgr = _make_manager()
        mgr._current_cwd = "/tmp"
        mgr._available = True
        mgr._process = _mock_process()

        observed_during_stop = None
        observed_during_spawn = None

        original_stop = mgr._stop_process
        original_spawn = mgr._spawn_process

        async def spy_stop():
            nonlocal observed_during_stop
            observed_during_stop = mgr._restarting
            mgr._process = None
            mgr._available = False

        async def spy_spawn(cwd):
            nonlocal observed_during_spawn
            observed_during_spawn = mgr._restarting
            mgr._current_cwd = cwd
            mgr._available = True

        with (
            patch.object(mgr, "_stop_process", side_effect=spy_stop),
            patch.object(mgr, "_spawn_process", side_effect=spy_spawn),
            patch.object(mgr, "_ensure_opencode_config", new_callable=AsyncMock),
        ):
            await mgr.ensure_workspace(Path("/workspace/project"))

        assert observed_during_stop is True, "_restarting should be True during _stop_process"
        assert observed_during_spawn is True, "_restarting should be True during _spawn_process"
        assert mgr._restarting is False, "_restarting should be cleared after transition"

    @pytest.mark.asyncio
    async def test_restarting_flag_cleared_on_error(self):
        """_restarting should be reset even if _spawn_process raises."""
        mgr = _make_manager()
        mgr._current_cwd = "/tmp"
        mgr._available = True
        mgr._process = _mock_process()

        async def failing_spawn(cwd):
            raise RuntimeError("spawn failed")

        with (
            patch.object(
                mgr, "_stop_process", new_callable=AsyncMock
            ),
            patch.object(mgr, "_spawn_process", side_effect=failing_spawn),
            patch.object(mgr, "_ensure_opencode_config", new_callable=AsyncMock),
        ):
            with pytest.raises(RuntimeError, match="spawn failed"):
                await mgr.ensure_workspace(Path("/workspace/project"))

        assert mgr._restarting is False, "_restarting must be cleared even on error"

    @pytest.mark.asyncio
    async def test_ensure_workspace_noop_when_already_correct(self):
        """No restart if already running in the correct directory."""
        mgr = _make_manager()
        mgr._current_cwd = "/workspace/project"
        mgr._available = True

        with patch.object(mgr, "_stop_process", new_callable=AsyncMock) as mock_stop:
            await mgr.ensure_workspace(Path("/workspace/project"))
            mock_stop.assert_not_called()

        assert mgr._restarting is False

    @pytest.mark.asyncio
    async def test_ensure_workspace_noop_for_external_mode(self):
        """External mode should deploy config but not restart."""
        mgr = _make_manager(external_url="http://external:4096")

        with (
            patch.object(mgr, "_stop_process", new_callable=AsyncMock) as mock_stop,
            patch.object(mgr, "_ensure_opencode_config", new_callable=AsyncMock),
        ):
            await mgr.ensure_workspace(Path("/workspace/project"))
            mock_stop.assert_not_called()


# ---------------------------------------------------------------------------
# _wait_for_healthy: early exit on process death
# ---------------------------------------------------------------------------


class TestWaitForHealthyLiveness:
    """Verify _wait_for_healthy detects early process exit."""

    @pytest.mark.asyncio
    async def test_returns_false_when_process_exits_immediately(self):
        """If the process already exited, return False without polling."""
        mgr = _make_manager()
        mgr._process = _mock_process(
            returncode=1,
            stderr_data=b"Error: address already in use :4096\n",
        )

        result = await mgr._wait_for_healthy()

        assert result is False

    @pytest.mark.asyncio
    async def test_stderr_captured_on_early_exit(self, caplog):
        """Stderr content should appear in the error log."""
        mgr = _make_manager()
        mgr._process = _mock_process(
            returncode=1,
            stderr_data=b"bind: address already in use",
        )

        with caplog.at_level("ERROR", logger="lhp.api.services.opencode_manager"):
            result = await mgr._wait_for_healthy()

        assert result is False
        assert "OpenCode exited immediately (rc=1)" in caplog.text
        assert "bind: address already in use" in caplog.text

    @pytest.mark.asyncio
    async def test_stderr_read_failure_handled(self, caplog):
        """If stderr.read() raises, we still get the return code logged."""
        mgr = _make_manager()
        proc = _mock_process(returncode=137)
        proc.stderr.read = AsyncMock(side_effect=OSError("broken pipe"))
        mgr._process = proc

        with caplog.at_level("ERROR", logger="lhp.api.services.opencode_manager"):
            result = await mgr._wait_for_healthy()

        assert result is False
        assert "rc=137" in caplog.text

    @pytest.mark.asyncio
    async def test_returns_true_when_healthy(self):
        """Normal case: process stays alive and health check passes."""
        mgr = _make_manager()
        mgr._process = _mock_process(returncode=None)

        with patch.object(mgr, "_health_check", new_callable=AsyncMock, return_value=True):
            result = await mgr._wait_for_healthy()

        assert result is True

    @pytest.mark.asyncio
    async def test_returns_false_on_timeout(self):
        """If health check never passes and process stays alive, return False."""
        mgr = _make_manager()
        mgr._process = _mock_process(returncode=None)

        with (
            patch.object(
                mgr, "_health_check", new_callable=AsyncMock, return_value=False
            ),
            patch(
                "lhp.api.services.opencode_manager._STARTUP_WAIT_S", 0.2
            ),
            patch(
                "lhp.api.services.opencode_manager._STARTUP_POLL_S", 0.05
            ),
        ):
            result = await mgr._wait_for_healthy()

        assert result is False

    @pytest.mark.asyncio
    async def test_no_process_still_polls_health(self):
        """If _process is None (e.g. external), just poll health endpoint."""
        mgr = _make_manager()
        mgr._process = None

        with patch.object(mgr, "_health_check", new_callable=AsyncMock, return_value=True):
            result = await mgr._wait_for_healthy()

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
        process with the OLD cwd. The _restarting flag prevents this.
        """
        mgr = _make_manager()
        mgr._current_cwd = "/tmp"
        mgr._available = True
        mgr._process = _mock_process()

        restart_calls = []

        original_try_restart = mgr._try_restart

        async def tracking_try_restart():
            restart_calls.append(mgr._restarting)
            # Don't actually restart — just record whether the flag was set

        async def slow_stop():
            """Simulate _stop_process that yields to event loop."""
            mgr._process = _mock_process(returncode=0)
            mgr._available = False
            # Yield to event loop — this is where the health monitor would wake up
            await asyncio.sleep(0)

            # Simulate what the health monitor would do if it woke up here:
            # It sees the process is dead and tries to restart
            await tracking_try_restart()

            mgr._process = None

        async def mock_spawn(cwd):
            mgr._current_cwd = cwd
            mgr._process = _mock_process(returncode=None)
            mgr._available = True

        with (
            patch.object(mgr, "_stop_process", side_effect=slow_stop),
            patch.object(mgr, "_spawn_process", side_effect=mock_spawn),
            patch.object(mgr, "_ensure_opencode_config", new_callable=AsyncMock),
        ):
            await mgr.ensure_workspace(Path("/workspace/project"))

        # The simulated health monitor call saw _restarting=True
        assert len(restart_calls) == 1
        assert restart_calls[0] is True, (
            "Health monitor should see _restarting=True during transition"
        )

        # After transition, cwd should be the new workspace
        assert mgr._current_cwd == "/workspace/project"
        assert mgr._restarting is False
