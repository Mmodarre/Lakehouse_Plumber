import asyncio
import logging
import time
from collections import defaultdict
from typing import Callable, Dict, List, Optional, Set

from lhp.api.services.git_service import GitService

logger = logging.getLogger(__name__)


class AutoCommitService:
    """Debounced auto-commit for workspace file changes.

    Tracks changed files per-user and commits only those files
    after a debounce period of inactivity. Uses a git_service_resolver
    callback to look up the git service at commit time (not at
    notification time), preventing stale reference bugs when a
    workspace is deleted while a timer is pending.

    Key design decisions:
    - Per-user asyncio.Lock protects debounce state from concurrent
      notify_change calls.
    - Selective staging: only the files that actually changed are
      committed (via commit_files), not a blanket `git add -A`.
    - shutdown() flushes all pending commits on server stop.
    """

    def __init__(self, debounce_seconds: float = 60.0):
        self.debounce_seconds = debounce_seconds
        self._changed_files: Dict[str, Set[str]] = {}  # user_hash -> set of paths
        self._tasks: Dict[str, asyncio.Task] = {}
        # Safe in asyncio: dictionary operations don't yield, so check-then-create
        # is atomic within the event loop. defaultdict ensures a lock exists for
        # every user_hash without a separate check-and-create step.
        self._locks: Dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
        # Callable to resolve git_service at commit time, not capture time
        self._git_service_resolver: Optional[Callable[[str], Optional[GitService]]] = None

    def set_git_service_resolver(
        self, resolver: Callable[[str], Optional[GitService]]
    ) -> None:
        """Set a callable(user_hash) -> Optional[GitService].

        This avoids capturing a stale GitService reference. The resolver
        is typically workspace_manager.get_git_service_by_hash, which
        returns None if the workspace was deleted.
        """
        self._git_service_resolver = resolver

    def _get_lock(self, user_hash: str) -> asyncio.Lock:
        """Return the per-user lock, creating one if needed.

        Uses defaultdict(asyncio.Lock) so the lookup-or-create is a single
        dictionary operation that cannot yield, making it safe without
        additional synchronization in the asyncio event loop.
        """
        return self._locks[user_hash]

    async def notify_change(
        self, user_hash: str, changed_file: str
    ) -> None:
        """Notify that a specific file was changed in a user's workspace.

        Tracks the individual file path and resets the debounce timer.
        If no further changes within debounce_seconds, auto-commits
        only the tracked files.
        """
        async with self._get_lock(user_hash):
            if user_hash not in self._changed_files:
                self._changed_files[user_hash] = set()
            self._changed_files[user_hash].add(changed_file)

            # Cancel existing timer and restart
            if user_hash in self._tasks:
                self._tasks[user_hash].cancel()

            self._tasks[user_hash] = asyncio.create_task(
                self._debounced_commit(user_hash)
            )

    async def _debounced_commit(self, user_hash: str) -> None:
        """Wait for debounce period, then commit tracked files."""
        try:
            await asyncio.sleep(self.debounce_seconds)
            await self._do_commit(user_hash)
        except asyncio.CancelledError:
            pass  # Timer was reset by a new change

    async def _do_commit(self, user_hash: str) -> Optional[str]:
        """Actually perform the commit with only tracked files."""
        async with self._get_lock(user_hash):
            files = self._changed_files.pop(user_hash, set())
            self._tasks.pop(user_hash, None)

        if not files:
            return None

        # Resolve git_service at commit time (not at notify time)
        if not self._git_service_resolver:
            logger.warning("No git_service_resolver set -- skipping auto-commit")
            return None

        git_svc = self._git_service_resolver(user_hash)
        if git_svc is None:
            logger.warning(
                f"Workspace gone for {user_hash} -- discarding auto-commit"
            )
            return None

        sha = await asyncio.to_thread(
            git_svc.commit_files,
            list(files),
            "Auto-commit: workspace changes",
        )
        if sha:
            logger.info(
                f"Auto-committed {len(files)} file(s) for {user_hash}: {sha[:8]}"
            )

            # Push to remote
            try:
                await asyncio.to_thread(git_svc.push)
                logger.info(f"Auto-pushed for {user_hash}")
            except Exception as e:
                logger.warning(f"Auto-push failed for {user_hash}: {e}")

        return sha

    async def cancel_pending(self, user_hash: str) -> None:
        """Cancel pending auto-commit timer and discard tracked file state.

        Used before a manual commit so the caller can commit all workspace
        changes with a user-provided message. The tracked files are cleared
        because commit_all() (git add -A) will stage them anyway.
        """
        async with self._get_lock(user_hash):
            if user_hash in self._tasks:
                self._tasks[user_hash].cancel()
                self._tasks.pop(user_hash, None)
            self._changed_files.pop(user_hash, None)

    async def flush(self, user_hash: str) -> Optional[str]:
        """Immediately commit pending changes with auto-commit message (bypass debounce).

        Used on workspace deletion and server shutdown to prevent data loss.
        Commits with "Auto-commit: workspace changes" — not for manual commits
        where a user-provided message is available (use cancel_pending instead).
        """
        # Cancel any pending timer
        if user_hash in self._tasks:
            self._tasks[user_hash].cancel()

        return await self._do_commit(user_hash)

    async def shutdown(self) -> None:
        """Flush all pending commits. Call from app shutdown event.

        Iterates over all users with pending changes, cancels their
        debounce timers, and commits immediately. This prevents
        data loss on server stop.
        """
        user_hashes = list(self._tasks.keys())
        for user_hash in user_hashes:
            task = self._tasks.get(user_hash)
            if task:
                task.cancel()
            await self._do_commit(user_hash)
        logger.info("AutoCommitService shutdown complete")
