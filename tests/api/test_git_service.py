"""TDD tests for GitService — wraps GitPython for workspace git operations.

These tests exercise real git repos via the ``mock_git_remote`` fixture
(bare repo on disk, no network). The production module
``lhp.api.services.git_service`` does not exist yet; these tests are
written first to drive the implementation.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from lhp.api.services.git_service import GitLogEntry, GitService, GitStatus

pytestmark = pytest.mark.api

# Git environment variables used for deterministic commits in tests.
_GIT_ENV = {
    **os.environ,
    "GIT_AUTHOR_NAME": "Test",
    "GIT_AUTHOR_EMAIL": "test@test.com",
    "GIT_COMMITTER_NAME": "Test",
    "GIT_COMMITTER_EMAIL": "test@test.com",
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _clone_work_dir(mock_git_remote: Path, tmp_path: Path, name: str = "work") -> Path:
    """Clone the bare remote into a working directory using raw git."""
    work_dir = tmp_path / name
    subprocess.run(
        ["git", "clone", str(mock_git_remote), str(work_dir)],
        check=True,
        env=_GIT_ENV,
    )
    return work_dir


def _make_change(work_dir: Path, filename: str = "change.txt", content: str = "x") -> Path:
    """Write a file inside *work_dir* (does NOT stage or commit)."""
    p = work_dir / filename
    p.write_text(content)
    return p


# ---------------------------------------------------------------------------
# TestGitServiceClone
# ---------------------------------------------------------------------------


class TestGitServiceClone:
    """Tests for ``GitService.clone`` classmethod."""

    def test_clone_creates_local_repo(self, mock_git_remote: Path, tmp_path: Path):
        target = tmp_path / "cloned"
        svc = GitService.clone(str(mock_git_remote), target)
        assert (target / ".git").exists()

    def test_clone_with_branch_creates_branch(self, mock_git_remote: Path, tmp_path: Path):
        target = tmp_path / "cloned_branch"
        svc = GitService.clone(str(mock_git_remote), target, branch="feature-x")
        status = svc.get_status()
        assert status.branch == "feature-x"

    def test_clone_to_existing_dir_raises(self, mock_git_remote: Path, tmp_path: Path):
        target = tmp_path / "already_exists"
        target.mkdir()
        (target / "blocker.txt").write_text("occupied")
        with pytest.raises(Exception):
            GitService.clone(str(mock_git_remote), target)


# ---------------------------------------------------------------------------
# TestGitServiceBranch
# ---------------------------------------------------------------------------


class TestGitServiceBranch:
    """Tests for ``create_branch``."""

    def test_create_new_branch(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        result = svc.create_branch("new-branch")
        assert result is True

    def test_create_existing_branch_raises_value_error(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        svc.create_branch("dup-branch")
        with pytest.raises(ValueError):
            svc.create_branch("dup-branch")

    def test_checkout_existing_branch(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        svc.create_branch("shared-branch")
        # Switch away first so checkout_existing actually switches back
        subprocess.run(["git", "checkout", "main"], cwd=str(work), check=True, env=_GIT_ENV)
        result = svc.create_branch("shared-branch", checkout_existing=True)
        assert result is False
        assert svc.get_status().branch == "shared-branch"


# ---------------------------------------------------------------------------
# TestGitServiceStatus
# ---------------------------------------------------------------------------


class TestGitServiceStatus:
    """Tests for ``get_status``."""

    def test_clean_repo_has_no_changes(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        status = svc.get_status()
        assert status.modified == []
        assert status.staged == []
        assert status.untracked == []

    def test_modified_file_shows_in_status(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        # Modify a tracked file
        (work / "lhp.yaml").write_text("project_name: changed\n")
        svc = GitService(work)
        status = svc.get_status()
        assert "lhp.yaml" in status.modified

    def test_untracked_file_shows_in_status(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        _make_change(work, "new_file.txt", "hello")
        svc = GitService(work)
        status = svc.get_status()
        assert "new_file.txt" in status.untracked

    def test_branch_name_in_status(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        status = svc.get_status()
        assert status.branch == "main"


# ---------------------------------------------------------------------------
# TestGitServiceLog
# ---------------------------------------------------------------------------


class TestGitServiceLog:
    """Tests for ``get_log``."""

    def test_returns_commit_entries(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        log = svc.get_log()
        assert len(log) >= 1

    def test_respects_max_count(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        # Create extra commits so we have more than 1
        for i in range(3):
            _make_change(work, f"file_{i}.txt", str(i))
            subprocess.run(["git", "add", "-A"], cwd=str(work), check=True, env=_GIT_ENV)
            subprocess.run(
                ["git", "commit", "-m", f"Commit {i}"],
                cwd=str(work),
                check=True,
                env=_GIT_ENV,
            )
        svc = GitService(work)
        log = svc.get_log(max_count=2)
        assert len(log) == 2

    def test_entry_has_required_fields(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        entry = svc.get_log()[0]
        assert isinstance(entry, GitLogEntry)
        assert entry.sha
        assert entry.message
        assert entry.author
        assert entry.date is not None


# ---------------------------------------------------------------------------
# TestGitServiceCommit
# ---------------------------------------------------------------------------


class TestGitServiceCommit:
    """Tests for ``commit_all`` and ``commit_files``."""

    def test_commit_all_with_changes_returns_sha(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        _make_change(work, "added.txt", "data")
        svc = GitService(work)
        sha = svc.commit_all("Add file")
        assert sha is not None
        assert isinstance(sha, str)
        assert len(sha) >= 7

    def test_commit_all_without_changes_returns_none(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        sha = svc.commit_all("Nothing here")
        assert sha is None

    def test_commit_files_stages_specific_files(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        _make_change(work, "included.txt", "yes")
        _make_change(work, "excluded.txt", "no")
        svc = GitService(work)
        sha = svc.commit_files(["included.txt"], "Selective commit")
        assert sha is not None
        # excluded.txt should still be untracked
        status = svc.get_status()
        assert "excluded.txt" in status.untracked


# ---------------------------------------------------------------------------
# TestGitServicePushPull
# ---------------------------------------------------------------------------


class TestGitServicePushPull:
    """Tests for ``push`` and ``pull``."""

    def test_push_to_remote(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        _make_change(work, "pushed.txt", "content")
        svc = GitService(work)
        svc.commit_all("Push test")
        svc.push()  # should not raise

    def test_pull_from_remote(self, mock_git_remote: Path, tmp_path: Path):
        # Clone A pushes a change
        clone_a = _clone_work_dir(mock_git_remote, tmp_path, name="clone_a")
        _make_change(clone_a, "shared.txt", "from_a")
        svc_a = GitService(clone_a)
        svc_a.commit_all("From clone A")
        svc_a.push()

        # Clone B pulls and sees the change
        clone_b = _clone_work_dir(mock_git_remote, tmp_path, name="clone_b")
        svc_b = GitService(clone_b)
        svc_b.pull()
        assert (clone_b / "shared.txt").exists()


# ---------------------------------------------------------------------------
# TestGitServiceChanges
# ---------------------------------------------------------------------------


class TestGitServiceChanges:
    """Tests for ``has_changes``."""

    def test_has_changes_true_when_modified(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        _make_change(work, "dirty.txt", "stuff")
        svc = GitService(work)
        assert svc.has_changes() is True

    def test_has_changes_false_when_clean(self, mock_git_remote: Path, tmp_path: Path):
        work = _clone_work_dir(mock_git_remote, tmp_path)
        svc = GitService(work)
        assert svc.has_changes() is False
