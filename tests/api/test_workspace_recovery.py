"""TDD tests for WorkspaceManager state recovery after simulated server restart.

These tests exercise workspace persistence and recovery using real git repos
via the ``mock_git_remote`` fixture (bare repo on disk, no network). The
production module ``lhp.api.services.workspace_manager`` does not exist yet;
these tests are written first to drive the implementation.

Recovery spec:
- On instantiation, WorkspaceManager calls ``_recover_workspaces()``
- For each persisted workspace in ``.lhp_workspaces.json``:
  - If workspace directory exists and has ``.git``: mark as IDLE, log recovery
  - If directory missing: remove stale metadata, log warning
- Recovered workspaces have state = IDLE (not ACTIVE)
- ``create_or_resume()`` on a recovered IDLE workspace changes state to ACTIVE
"""

from __future__ import annotations

import shutil
from pathlib import Path

import pytest

from lhp.api.auth import UserContext
from lhp.api.services.workspace_manager import WorkspaceManager, WorkspaceState

pytestmark = pytest.mark.api


@pytest.mark.integration
class TestWorkspaceRecovery:
    """Integration tests for workspace state recovery after simulated restart."""

    def test_workspaces_recovered_on_startup(
        self,
        mock_git_remote: Path,
        mock_workspace_root: Path,
        test_user: UserContext,
    ):
        """Persisted workspaces are recovered as IDLE on new manager startup."""
        # Arrange: create a workspace with mgr1 (simulates first server run)
        mgr1 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )
        mgr1.create_or_resume(test_user)
        ws1 = mgr1.get_workspace(test_user)
        assert ws1 is not None
        assert ws1.state == WorkspaceState.ACTIVE

        # Act: create a NEW manager with the same root (simulates restart)
        mgr2 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )

        # Assert: recovered workspace exists but is IDLE, not ACTIVE
        ws2 = mgr2.get_workspace(test_user)
        assert ws2 is not None
        assert ws2.state == WorkspaceState.IDLE

    def test_resumed_workspace_becomes_active(
        self,
        mock_git_remote: Path,
        mock_workspace_root: Path,
        test_user: UserContext,
    ):
        """Calling create_or_resume on a recovered IDLE workspace makes it ACTIVE."""
        # Arrange: create workspace, then simulate restart
        mgr1 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )
        mgr1.create_or_resume(test_user)

        mgr2 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )
        # Verify it starts as IDLE after recovery
        ws_before = mgr2.get_workspace(test_user)
        assert ws_before is not None
        assert ws_before.state == WorkspaceState.IDLE

        # Act: resume the recovered workspace
        mgr2.create_or_resume(test_user)

        # Assert: state is now ACTIVE
        ws_after = mgr2.get_workspace(test_user)
        assert ws_after is not None
        assert ws_after.state == WorkspaceState.ACTIVE

    def test_stale_metadata_cleaned_on_startup(
        self,
        mock_git_remote: Path,
        mock_workspace_root: Path,
        test_user: UserContext,
    ):
        """Stale metadata is removed when the workspace directory no longer exists."""
        # Arrange: create workspace and note its path
        mgr1 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )
        mgr1.create_or_resume(test_user)
        ws1 = mgr1.get_workspace(test_user)
        assert ws1 is not None
        workspace_path = Path(ws1.path)
        assert workspace_path.exists()

        # Delete the workspace directory (simulates external cleanup or disk failure)
        shutil.rmtree(workspace_path)
        assert not workspace_path.exists()

        # Act: create a NEW manager (simulates restart)
        mgr2 = WorkspaceManager(
            workspace_root=mock_workspace_root,
            source_repo=str(mock_git_remote),
        )

        # Assert: stale entry was cleaned up — get_workspace returns None
        ws2 = mgr2.get_workspace(test_user)
        assert ws2 is None
