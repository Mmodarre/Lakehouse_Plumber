import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

import git

logger = logging.getLogger(__name__)


@dataclass
class GitStatus:
    """Structured git status."""
    modified: List[str]
    staged: List[str]
    untracked: List[str]
    branch: str
    ahead: int = 0
    behind: int = 0


@dataclass
class GitLogEntry:
    """Single commit log entry."""
    sha: str
    message: str
    author: str
    date: str


class GitService:
    """Git operations for a specific repository directory.

    One instance per workspace. Thread-safe for read operations.
    Write operations (commit, push) should be serialized externally.
    """

    def __init__(self, repo_path: Path):
        self.repo_path = repo_path
        self.repo = git.Repo(repo_path)
        self.logger = logging.getLogger(__name__)

    @classmethod
    def clone(
        cls,
        source_url: str,
        target_path: Path,
        branch: Optional[str] = None,
    ) -> "GitService":
        """Clone a repository and return a GitService for it.

        Args:
            source_url: Remote repository URL (or local path for dev mode)
            target_path: Local directory to clone into
            branch: Optional branch to checkout after cloning
        """
        logger.info(f"Cloning {source_url} → {target_path}")
        repo = git.Repo.clone_from(source_url, str(target_path))

        if branch:
            repo.git.checkout("-b", branch)

        return cls(target_path)

    @classmethod
    def init_local(cls, repo_path: Path) -> "GitService":
        """Wrap an existing local repo (dev mode — no clone needed).

        For local development, the project directory IS the repo.
        Creates an ephemeral branch for the dev user.
        """
        return cls(repo_path)

    def create_branch(self, branch_name: str, checkout_existing: bool = False) -> bool:
        """Create and checkout a new branch from current HEAD.

        Args:
            branch_name: Name of the branch to create.
            checkout_existing: If True, checkout an existing branch instead
                of raising an error. If False (default), raises ValueError
                if branch already exists.

        Returns:
            True if a new branch was created, False if an existing branch
            was checked out (only when checkout_existing=True).
        """
        if branch_name in [h.name for h in self.repo.heads]:
            if not checkout_existing:
                raise ValueError(
                    f"Branch '{branch_name}' already exists. "
                    f"Use checkout_existing=True to switch to it."
                )
            self.repo.git.checkout(branch_name)
            self.logger.info(f"Checked out existing branch: {branch_name}")
            return False

        self.repo.git.checkout("-b", branch_name)
        self.logger.info(f"Created and checked out branch: {branch_name}")
        return True

    def get_status(self) -> GitStatus:
        """Get current git status. Handles empty repos (no commits)."""
        try:
            staged = [item.a_path for item in self.repo.index.diff("HEAD")]
        except git.exc.BadName:
            # Empty repo: no HEAD to diff against
            staged = (
                list(self.repo.index.entries.keys())
                if self.repo.index.entries
                else []
            )

        try:
            branch = self.repo.active_branch.name
        except TypeError:
            # Detached HEAD
            branch = f"(detached at {self.repo.head.commit.hexsha[:8]})"

        # Compute ahead/behind counts against remote tracking branch
        ahead, behind = 0, 0
        try:
            tracking = self.repo.active_branch.tracking_branch()
            if tracking is not None:
                counts = self.repo.git.rev_list(
                    "--left-right", "--count", f"{tracking.name}...HEAD"
                )
                behind_str, ahead_str = counts.split("\t")
                behind = int(behind_str)
                ahead = int(ahead_str)
        except (TypeError, ValueError, git.exc.GitCommandError):
            pass  # No remote tracking branch or detached HEAD -- default to 0

        return GitStatus(
            modified=[item.a_path for item in self.repo.index.diff(None)],
            staged=staged,
            untracked=self.repo.untracked_files,
            branch=branch,
            ahead=ahead,
            behind=behind,
        )

    def get_log(self, max_count: int = 20) -> List[GitLogEntry]:
        """Get recent commit log."""
        entries = []
        for commit in self.repo.iter_commits(max_count=max_count):
            entries.append(
                GitLogEntry(
                    sha=commit.hexsha[:8],
                    message=commit.message.strip(),
                    author=str(commit.author),
                    date=commit.committed_datetime.isoformat(),
                )
            )
        return entries

    def commit_all(self, message: str) -> Optional[str]:
        """Stage all changes and commit.

        Returns commit SHA if changes were committed, None if nothing to commit.
        """
        # Stage all changes
        self.repo.git.add("-A")

        # Check if there's anything to commit
        if not self.repo.is_dirty(index=True):
            self.logger.debug("Nothing to commit")
            return None

        commit = self.repo.index.commit(message)
        self.logger.info(f"Committed: {commit.hexsha[:8]} — {message}")
        return commit.hexsha

    def commit_files(self, files: List[str], message: str) -> Optional[str]:
        """Stage specific files and commit."""
        for f in files:
            self.repo.git.add(f)

        if not self.repo.is_dirty(index=True):
            return None

        commit = self.repo.index.commit(message)
        return commit.hexsha

    def push(self, remote: str = "origin") -> None:
        """Push current branch to remote."""
        branch = self.repo.active_branch.name
        self.repo.git.push(remote, branch, set_upstream=True)
        self.logger.info(f"Pushed {branch} to {remote}")

    def pull(self, remote: str = "origin") -> None:
        """Pull latest from remote for current branch.

        If the current branch doesn't exist on the remote yet (e.g., a
        freshly created workspace branch that hasn't been pushed), this
        is a no-op rather than an error.
        """
        branch = self.repo.active_branch.name
        try:
            self.repo.git.pull(remote, branch)
            self.logger.info("Pulled latest changes")
        except git.exc.GitCommandError as exc:
            if "couldn't find remote ref" in str(exc):
                self.logger.info(
                    f"Branch '{branch}' not on remote yet — nothing to pull"
                )
            else:
                raise

    def get_diff_against(self, base_branch: str = "main") -> str:
        """Get diff of current branch against base branch."""
        return self.repo.git.diff(f"{base_branch}...HEAD")

    def has_changes(self) -> bool:
        """Check if there are any uncommitted changes."""
        return self.repo.is_dirty(untracked_files=True)
