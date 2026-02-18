"""TDD tests for AutoCommitService.

These tests define the expected behavior of the auto-commit service BEFORE
the production code exists. They should all fail with ImportError until
lhp.api.services.auto_commit_service.AutoCommitService is implemented.
"""

import asyncio
import logging
from unittest.mock import AsyncMock, MagicMock, call, patch

import pytest

from lhp.api.services.auto_commit_service import AutoCommitService

pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_git_service(
    commit_sha: str = "abc123",
) -> MagicMock:
    """Create a mock GitService with commit_files and push methods."""
    git_svc = MagicMock()
    git_svc.commit_files = MagicMock(return_value=commit_sha)
    git_svc.push = MagicMock()
    return git_svc


def _resolver_for(git_svc: MagicMock):
    """Return a resolver callback that always returns *git_svc*."""
    return lambda user_hash: git_svc


def _none_resolver():
    """Return a resolver callback that always returns None (stale workspace)."""
    return lambda user_hash: None


# ---------------------------------------------------------------------------
# TestNotifyChange
# ---------------------------------------------------------------------------


class TestNotifyChange:
    """Verifies that notify_change correctly tracks changed files per user."""

    @pytest.mark.asyncio
    async def test_tracks_changed_file(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "pipelines/foo.yaml")

        assert "pipelines/foo.yaml" in svc._changed_files["user1"]

    @pytest.mark.asyncio
    async def test_accumulates_multiple_files(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "pipelines/foo.yaml")
        await svc.notify_change("user1", "pipelines/bar.yaml")

        tracked = svc._changed_files["user1"]
        assert "pipelines/foo.yaml" in tracked
        assert "pipelines/bar.yaml" in tracked
        assert len(tracked) == 2


# ---------------------------------------------------------------------------
# TestDebounce
# ---------------------------------------------------------------------------


class TestDebounce:
    """Verifies debounce timer behavior -- commit fires only after inactivity."""

    @pytest.mark.asyncio
    async def test_fires_commit_after_debounce_period(self):
        """A single change should trigger a commit after the debounce elapses."""
        svc = AutoCommitService(debounce_seconds=0.1)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file_a.yaml")

        # Wait for debounce + a small margin
        await asyncio.sleep(0.2)

        git_svc.commit_files.assert_called_once()

    @pytest.mark.asyncio
    async def test_timer_resets_on_new_change(self):
        """A second change arriving mid-debounce should reset the timer.

        Scenario:
        - notify at t=0 (starts 0.15s timer)
        - wait 0.08s (just over half)
        - notify at t~0.08 (resets timer to 0.15s from NOW)
        - at t=0.15 the original timer WOULD have fired -- commit should NOT
          have happened yet
        - wait until t~0.30 (well past the reset timer)
        - now the commit should have fired exactly once with both files
        """
        svc = AutoCommitService(debounce_seconds=0.15)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file_a.yaml")
        await asyncio.sleep(0.08)

        # This should reset the debounce timer
        await svc.notify_change("user1", "file_b.yaml")

        # At t~0.15 the original timer would have fired
        await asyncio.sleep(0.08)
        git_svc.commit_files.assert_not_called()

        # Wait for the reset timer to expire
        await asyncio.sleep(0.15)
        git_svc.commit_files.assert_called_once()

        # Both files should be included
        committed_files = git_svc.commit_files.call_args[0][0]
        assert set(committed_files) == {"file_a.yaml", "file_b.yaml"}

    @pytest.mark.asyncio
    async def test_rapid_changes_result_in_single_commit(self):
        """10 rapid changes should batch into a single commit."""
        svc = AutoCommitService(debounce_seconds=0.1)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        files = [f"file_{i}.yaml" for i in range(10)]
        for f in files:
            await svc.notify_change("user1", f)

        # Wait for debounce to fire
        await asyncio.sleep(0.2)

        git_svc.commit_files.assert_called_once()
        committed_files = git_svc.commit_files.call_args[0][0]
        assert set(committed_files) == set(files)


# ---------------------------------------------------------------------------
# TestFlush
# ---------------------------------------------------------------------------


class TestFlush:
    """Verifies flush() commits immediately without waiting for the timer."""

    @pytest.mark.asyncio
    async def test_flush_immediately_commits(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file_a.yaml")
        result = await svc.flush("user1")

        git_svc.commit_files.assert_called_once()
        assert result is not None

    @pytest.mark.asyncio
    async def test_flush_cancels_pending_timer(self):
        """After flush, the debounce timer should not fire a second commit."""
        svc = AutoCommitService(debounce_seconds=0.1)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file_a.yaml")
        await svc.flush("user1")

        # Wait past the original debounce period
        await asyncio.sleep(0.2)

        # Should have been called exactly once (from flush), not twice
        git_svc.commit_files.assert_called_once()

    @pytest.mark.asyncio
    async def test_flush_returns_none_when_no_pending(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        result = await svc.flush("user1")

        assert result is None
        git_svc.commit_files.assert_not_called()


# ---------------------------------------------------------------------------
# TestCommitBehavior
# ---------------------------------------------------------------------------


class TestCommitBehavior:
    """Verifies the git operations performed during a commit."""

    @pytest.mark.asyncio
    async def test_commits_only_tracked_files(self):
        """commit_files should receive exactly the files tracked for the user."""
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "alpha.yaml")
        await svc.notify_change("user1", "beta.yaml")
        await svc.flush("user1")

        committed_files = git_svc.commit_files.call_args[0][0]
        assert set(committed_files) == {"alpha.yaml", "beta.yaml"}

    @pytest.mark.asyncio
    async def test_uses_auto_commit_message(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file.yaml")
        await svc.flush("user1")

        commit_message = git_svc.commit_files.call_args[0][1]
        assert commit_message == "Auto-commit: workspace changes"

    @pytest.mark.asyncio
    async def test_attempts_push_after_commit(self):
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "file.yaml")
        await svc.flush("user1")

        git_svc.push.assert_called_once()


# ---------------------------------------------------------------------------
# TestStaleGuard
# ---------------------------------------------------------------------------


class TestStaleGuard:
    """Verifies graceful handling when workspace is deleted or no resolver set."""

    @pytest.mark.asyncio
    async def test_skips_commit_when_resolver_returns_none(self):
        """If the workspace was deleted, resolver returns None -- no crash."""
        svc = AutoCommitService(debounce_seconds=10.0)
        svc.set_git_service_resolver(_none_resolver())

        await svc.notify_change("user1", "file.yaml")
        result = await svc.flush("user1")

        assert result is None

    @pytest.mark.asyncio
    async def test_logs_warning_when_no_resolver_set(self, caplog):
        """With no resolver configured, a warning should be logged."""
        svc = AutoCommitService(debounce_seconds=10.0)
        # Deliberately NOT calling set_git_service_resolver

        await svc.notify_change("user1", "file.yaml")

        with caplog.at_level(logging.WARNING):
            result = await svc.flush("user1")

        assert result is None
        assert any("resolver" in record.message.lower() for record in caplog.records)


# ---------------------------------------------------------------------------
# TestShutdown
# ---------------------------------------------------------------------------


class TestShutdown:
    """Verifies shutdown() flushes all pending users and cancels timers."""

    @pytest.mark.asyncio
    async def test_flushes_all_pending_on_shutdown(self):
        """Two users with pending changes should both get committed."""
        svc = AutoCommitService(debounce_seconds=10.0)

        git_svc_1 = _make_git_service(commit_sha="sha_user1")
        git_svc_2 = _make_git_service(commit_sha="sha_user2")

        def resolver(user_hash):
            if user_hash == "user1":
                return git_svc_1
            if user_hash == "user2":
                return git_svc_2
            return None

        svc.set_git_service_resolver(resolver)

        await svc.notify_change("user1", "a.yaml")
        await svc.notify_change("user2", "b.yaml")
        await svc.shutdown()

        git_svc_1.commit_files.assert_called_once()
        git_svc_2.commit_files.assert_called_once()

    @pytest.mark.asyncio
    async def test_cancels_all_timers(self):
        """After shutdown, no lingering asyncio tasks should remain."""
        svc = AutoCommitService(debounce_seconds=10.0)
        git_svc = _make_git_service()
        svc.set_git_service_resolver(_resolver_for(git_svc))

        await svc.notify_change("user1", "a.yaml")
        await svc.notify_change("user2", "b.yaml")
        await svc.shutdown()

        # All internal timers / tasks should be cleaned up
        # Check that _timers dict is empty or all tasks are done
        for user_hash, task in getattr(svc, "_timers", {}).items():
            assert task is None or task.done(), (
                f"Timer for {user_hash} still active after shutdown"
            )

        # Pending files should be cleared
        for user_hash, files in svc._changed_files.items():
            assert len(files) == 0, (
                f"Changed files for {user_hash} not cleared after shutdown"
            )
