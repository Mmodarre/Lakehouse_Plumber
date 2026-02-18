"""TDD tests for WorkspaceManager — written before the production code.

These tests exercise the full workspace lifecycle: creation, resume, heartbeat,
stop, delete, project root resolution, and TTL-based cleanup.  They use REAL
git repositories (via ``mock_git_remote``) and the filesystem (via
``mock_workspace_root``) so there is no mocking.
"""

import time

import pytest

from lhp.api.auth import UserContext
from lhp.api.services.workspace_manager import (
    WorkspaceInfo,
    WorkspaceManager,
    WorkspaceState,
)


pytestmark = pytest.mark.api


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _second_user() -> UserContext:
    """Return a different user for multi-user tests."""
    return UserContext(
        email="other@test.com",
        username="otheruser",
        user_id="other-user-456",
    )


def _make_manager(
    workspace_root,
    source_repo,
    *,
    max_workspaces=50,
    idle_ttl_hours=24.0,
    stopped_ttl_hours=168.0,
) -> WorkspaceManager:
    return WorkspaceManager(
        workspace_root=workspace_root,
        source_repo=source_repo,
        max_workspaces=max_workspaces,
        idle_ttl_hours=idle_ttl_hours,
        stopped_ttl_hours=stopped_ttl_hours,
    )


# ---------------------------------------------------------------------------
# TestWorkspaceCreation
# ---------------------------------------------------------------------------


class TestWorkspaceCreation:
    """Creating a workspace for a new user."""

    def test_creates_workspace_directory(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        assert info.workspace_path.exists()

    def test_creates_git_clone(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        assert (info.workspace_path / ".git").exists()

    def test_returns_active_state(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        assert info.state == WorkspaceState.ACTIVE

    def test_creates_branch(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        assert info.branch == f"{test_user.username}/workspace"

    def test_persists_to_store(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        store_file = mock_workspace_root / ".lhp_workspaces.json"
        assert store_file.exists()


# ---------------------------------------------------------------------------
# TestWorkspaceResume
# ---------------------------------------------------------------------------


class TestWorkspaceResume:
    """Resuming an existing workspace (second call to create_or_resume)."""

    def test_resumes_existing_workspace(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        first = mgr.create_or_resume(test_user)
        second = mgr.create_or_resume(test_user)
        assert first.workspace_path == second.workspace_path

    def test_resume_updates_state_to_active(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        info = mgr.create_or_resume(test_user)
        assert info.state == WorkspaceState.ACTIVE

    def test_resume_updates_last_activity(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        first = mgr.create_or_resume(test_user)
        time.sleep(0.05)
        second = mgr.create_or_resume(test_user)
        assert second.last_activity > first.last_activity


# ---------------------------------------------------------------------------
# TestWorkspaceGet
# ---------------------------------------------------------------------------


class TestWorkspaceGet:
    """Querying workspace information."""

    def test_returns_info_for_existing_workspace(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        info = mgr.get_workspace(test_user)
        assert isinstance(info, WorkspaceInfo)

    def test_returns_none_for_nonexistent_user(
        self, mock_workspace_root, mock_git_remote
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        unknown = UserContext(
            email="nobody@test.com", username="nobody", user_id="no-such-id"
        )
        assert mgr.get_workspace(unknown) is None

    def test_reports_uncommitted_changes(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        # Create an uncommitted file inside the workspace
        (info.workspace_path / "dirty.txt").write_text("uncommitted")
        refreshed = mgr.get_workspace(test_user)
        assert refreshed.has_uncommitted_changes is True


# ---------------------------------------------------------------------------
# TestWorkspaceHeartbeat
# ---------------------------------------------------------------------------


class TestWorkspaceHeartbeat:
    """Heartbeat updates for active workspaces."""

    def test_heartbeat_updates_last_activity(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        original_ts = info.last_activity
        time.sleep(0.05)
        mgr.heartbeat(test_user)
        updated = mgr.get_workspace(test_user)
        assert updated.last_activity > original_ts

    def test_heartbeat_no_op_for_nonexistent(
        self, mock_workspace_root, mock_git_remote
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        unknown = UserContext(
            email="ghost@test.com", username="ghost", user_id="ghost-id"
        )
        # Should not raise
        mgr.heartbeat(unknown)


# ---------------------------------------------------------------------------
# TestWorkspaceStop
# ---------------------------------------------------------------------------


class TestWorkspaceStop:
    """Soft-stopping a workspace (state change, files kept)."""

    def test_stop_changes_state_to_stopped(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        info = mgr.get_workspace(test_user)
        assert info.state == WorkspaceState.STOPPED

    def test_stop_removes_git_service_handle(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        assert mgr.get_git_service(test_user) is None

    def test_stop_keeps_files_on_disk(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        workspace_path = info.workspace_path
        mgr.stop_workspace(test_user)
        assert workspace_path.exists()


# ---------------------------------------------------------------------------
# TestWorkspaceDelete
# ---------------------------------------------------------------------------


class TestWorkspaceDelete:
    """Hard-deleting a workspace (files removed)."""

    def test_delete_removes_directory(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        workspace_path = info.workspace_path
        mgr.delete_workspace(test_user)
        assert not workspace_path.exists()

    def test_delete_removes_from_store(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.delete_workspace(test_user)
        assert mgr.get_workspace(test_user) is None

    def test_delete_removes_git_service_handle(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.delete_workspace(test_user)
        assert mgr.get_git_service(test_user) is None


# ---------------------------------------------------------------------------
# TestWorkspaceProjectRoot
# ---------------------------------------------------------------------------


class TestWorkspaceProjectRoot:
    """Resolving the project root path for a user."""

    def test_returns_path_for_active_workspace(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        root = mgr.get_workspace_project_root(test_user)
        assert root == info.workspace_path

    def test_returns_path_for_idle_workspace(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        info = mgr.create_or_resume(test_user)
        # Manually set the workspace to IDLE via the store to simulate timeout
        mgr._store.update_state(test_user.user_id_hash, WorkspaceState.IDLE)
        root = mgr.get_workspace_project_root(test_user)
        assert root == info.workspace_path

    def test_returns_none_for_stopped_workspace(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        assert mgr.get_workspace_project_root(test_user) is None

    def test_returns_none_for_nonexistent(
        self, mock_workspace_root, mock_git_remote
    ):
        mgr = _make_manager(mock_workspace_root, mock_git_remote)
        unknown = UserContext(
            email="anon@test.com", username="anon", user_id="anon-id"
        )
        assert mgr.get_workspace_project_root(unknown) is None


# ---------------------------------------------------------------------------
# TestWorkspaceCleanup
# ---------------------------------------------------------------------------


class TestWorkspaceCleanup:
    """TTL-based expiration of idle and stopped workspaces."""

    def test_removes_idle_past_ttl(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(
            mock_workspace_root, mock_git_remote, idle_ttl_hours=0.001
        )
        info = mgr.create_or_resume(test_user)
        # Force the workspace into IDLE state with an old timestamp
        mgr._store.update_state(test_user.user_id_hash, WorkspaceState.IDLE)
        mgr._store.update_last_activity(
            test_user.user_id_hash, time.time() - 100_000
        )
        removed = mgr.cleanup_expired()
        assert test_user.user_id_hash in removed

    def test_removes_stopped_past_ttl(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(
            mock_workspace_root, mock_git_remote, stopped_ttl_hours=0.001
        )
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        mgr._store.update_last_activity(
            test_user.user_id_hash, time.time() - 100_000
        )
        removed = mgr.cleanup_expired()
        assert test_user.user_id_hash in removed

    def test_keeps_active_workspaces(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(
            mock_workspace_root, mock_git_remote, idle_ttl_hours=0.001
        )
        mgr.create_or_resume(test_user)
        # Even with a very old last_activity, ACTIVE workspaces must survive
        mgr._store.update_last_activity(
            test_user.user_id_hash, time.time() - 100_000
        )
        removed = mgr.cleanup_expired()
        assert test_user.user_id_hash not in removed

    def test_returns_removed_hashes(
        self, mock_workspace_root, mock_git_remote, test_user
    ):
        mgr = _make_manager(
            mock_workspace_root, mock_git_remote, stopped_ttl_hours=0.001
        )
        mgr.create_or_resume(test_user)
        mgr.stop_workspace(test_user)
        mgr._store.update_last_activity(
            test_user.user_id_hash, time.time() - 100_000
        )

        second = _second_user()
        mgr.create_or_resume(second)
        mgr.stop_workspace(second)
        mgr._store.update_last_activity(
            second.user_id_hash, time.time() - 100_000
        )

        removed = mgr.cleanup_expired()
        assert isinstance(removed, list)
        assert len(removed) == 2
