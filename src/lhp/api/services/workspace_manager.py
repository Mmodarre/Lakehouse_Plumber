import hashlib
import json
import logging
import shutil
import threading
import time
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Dict, List, Optional

from pydantic import BaseModel

from lhp.api.auth import UserContext
from lhp.api.services.git_service import GitService

logger = logging.getLogger(__name__)


class WorkspaceState(str, Enum):
    CREATING = "creating"
    ACTIVE = "active"
    IDLE = "idle"
    STOPPED = "stopped"
    DELETED = "deleted"


class PersistedWorkspaceInfo(BaseModel):
    """Workspace metadata that survives server restarts."""
    user_id: str
    username: str
    state: str  # WorkspaceState value
    workspace_path: str
    branch: str
    created_at: float
    last_activity: float


class WorkspaceStore:
    """Durable workspace metadata storage backed by a JSON file.

    Uses atomic writes (write-to-temp + rename) to prevent corruption.
    This follows the same pattern as LHP's .lhp_state.json for state
    persistence.

    Thread safety: All mutations (put, remove) and file reads (_load) are
    protected by a threading.Lock. This prevents corruption when multiple
    async tasks (running in thread pool) access the store concurrently.
    """

    def __init__(self, workspace_root: Path):
        self._file = workspace_root / ".lhp_workspaces.json"
        self._workspace_root = workspace_root
        self._data: Dict[str, PersistedWorkspaceInfo] = {}
        self._lock = threading.Lock()
        self._load()

    def _load(self) -> None:
        with self._lock:
            if self._file.exists():
                raw = json.loads(self._file.read_text())
                for user_hash, info in raw.items():
                    self._data[user_hash] = PersistedWorkspaceInfo(**info)

    def _save(self) -> None:
        self._file.parent.mkdir(parents=True, exist_ok=True)
        payload = {k: v.model_dump() for k, v in self._data.items()}
        # Atomic write: write to temp, then rename
        tmp = self._file.with_suffix(".tmp")
        tmp.write_text(json.dumps(payload, indent=2))
        tmp.rename(self._file)

    def put(self, user_hash: str, info: PersistedWorkspaceInfo) -> None:
        with self._lock:
            self._data[user_hash] = info
            self._save()

    def get(self, user_hash: str) -> Optional[PersistedWorkspaceInfo]:
        return self._data.get(user_hash)

    def remove(self, user_hash: str) -> None:
        with self._lock:
            self._data.pop(user_hash, None)
            self._save()

    def all_workspaces(self) -> Dict[str, PersistedWorkspaceInfo]:
        return dict(self._data)

    def update_state(self, user_hash: str, state: WorkspaceState) -> None:
        """Update the state of an existing workspace entry."""
        with self._lock:
            info = self._data.get(user_hash)
            if info is not None:
                info.state = state.value
                self._save()

    def update_last_activity(self, user_hash: str, timestamp: float) -> None:
        """Update the last_activity timestamp of an existing workspace entry."""
        with self._lock:
            info = self._data.get(user_hash)
            if info is not None:
                info.last_activity = timestamp
                self._save()


@dataclass
class WorkspaceInfo:
    """Runtime metadata about a user's workspace (returned to callers)."""
    user_id: str
    username: str
    state: WorkspaceState
    workspace_path: Path
    branch: str
    created_at: float
    last_activity: float
    has_uncommitted_changes: bool = False

    @property
    def path(self) -> Path:
        """Alias for workspace_path — used by tests and external callers."""
        return self.workspace_path


class WorkspaceManager:
    """Manage per-user workspace lifecycle.

    Each user gets an isolated git clone at:
        {workspace_root}/{user_id_hash}/workspace/

    The workspace is a full clone of the project repo with an ephemeral
    branch named {username}/workspace.

    Workspace metadata is persisted to .lhp_workspaces.json so that
    workspaces survive server restarts. Git service handles (runtime-only)
    are kept in-memory and re-created on resume.
    """

    def __init__(
        self,
        workspace_root: Path,
        source_repo: str,
        max_workspaces: int = 50,
        idle_ttl_hours: float = 24.0,
        stopped_ttl_hours: float = 168.0,
    ):
        self.workspace_root = workspace_root
        self.source_repo = source_repo
        self.max_workspaces = max_workspaces
        self.idle_ttl_seconds = idle_ttl_hours * 3600
        self.stopped_ttl_seconds = stopped_ttl_hours * 3600
        self._store = WorkspaceStore(workspace_root)
        self._git_services: Dict[str, GitService] = {}  # runtime handles, not persisted
        self.logger = logging.getLogger(__name__)
        self._recover_workspaces()

    def _recover_workspaces(self) -> None:
        """Recover workspace state from persisted store on startup.

        For each persisted workspace entry:
        - If the directory still exists on disk, mark as IDLE (user must re-connect).
        - If the directory is missing, remove the stale metadata entry.

        Auto-commit recovery note: On startup, we do NOT restart auto-commit
        timers. Instead, each recovered workspace is checked for uncommitted
        changes and a warning is logged. The next user action (or an explicit
        POST /workspace/heartbeat) will re-arm the auto-commit timer.
        """
        for user_hash, info in self._store.all_workspaces().items():
            ws_path = Path(info.workspace_path)
            if ws_path.exists() and (ws_path / ".git").exists():
                logger.info(f"Recovered workspace for {info.username} at {ws_path}")
                # Mark as idle (not active) -- user must re-connect
                info.state = WorkspaceState.IDLE.value
                self._store.put(user_hash, info)

                # Check for uncommitted changes and warn
                try:
                    git_svc = GitService(ws_path)
                    if git_svc.has_changes():
                        logger.warning(
                            f"Workspace for {info.username} has uncommitted changes "
                            f"from before restart"
                        )
                except Exception as e:
                    logger.warning(
                        f"Could not check workspace state for {info.username}: {e}"
                    )
            else:
                logger.warning(
                    f"Workspace directory missing for {info.username}, "
                    f"removing stale metadata"
                )
                self._store.remove(user_hash)

    def _user_hash(self, user: UserContext) -> str:
        return hashlib.sha256(user.user_id.encode()).hexdigest()[:16]

    def _workspace_path(self, user: UserContext) -> Path:
        return self.workspace_root / self._user_hash(user) / "workspace"

    def _branch_name(self, user: UserContext) -> str:
        return f"{user.username}/workspace"

    def create_or_resume(self, user: UserContext) -> WorkspaceInfo:
        """Create a new workspace or resume an existing one.

        If the workspace directory exists and is valid, resume it.
        Otherwise, create a new clone.
        """
        workspace_path = self._workspace_path(user)
        branch = self._branch_name(user)
        user_hash = self._user_hash(user)

        # Resume from persisted store
        persisted = self._store.get(user_hash)
        if persisted is not None:
            ws_path = Path(persisted.workspace_path)
            if ws_path.exists() and (ws_path / ".git").exists():
                self.logger.info(
                    f"Resuming workspace for {user.username} at {ws_path}"
                )
                git_svc = GitService(ws_path)
                self._git_services[user_hash] = git_svc

                # Update state to active and persist
                persisted.state = WorkspaceState.ACTIVE.value
                persisted.last_activity = time.time()
                self._store.put(user_hash, persisted)

                return WorkspaceInfo(
                    user_id=user.user_id,
                    username=user.username,
                    state=WorkspaceState.ACTIVE,
                    workspace_path=ws_path,
                    branch=persisted.branch,
                    created_at=persisted.created_at,
                    last_activity=persisted.last_activity,
                    has_uncommitted_changes=git_svc.has_changes(),
                )
            else:
                # Stale entry -- directory is gone
                self._store.remove(user_hash)

        # Create new workspace
        self.logger.info(f"Creating workspace for {user.username} at {workspace_path}")
        workspace_path.parent.mkdir(parents=True, exist_ok=True)

        git_svc = GitService.clone(
            source_url=str(self.source_repo),
            target_path=workspace_path,
            branch=branch,
        )
        self._git_services[user_hash] = git_svc

        now = time.time()
        persisted_info = PersistedWorkspaceInfo(
            user_id=user.user_id,
            username=user.username,
            state=WorkspaceState.ACTIVE.value,
            workspace_path=str(workspace_path),
            branch=branch,
            created_at=now,
            last_activity=now,
        )
        self._store.put(user_hash, persisted_info)

        return WorkspaceInfo(
            user_id=user.user_id,
            username=user.username,
            state=WorkspaceState.ACTIVE,
            workspace_path=workspace_path,
            branch=branch,
            created_at=now,
            last_activity=now,
        )

    def get_workspace(self, user: UserContext) -> Optional[WorkspaceInfo]:
        """Get workspace info for a user, if it exists.

        Checks the in-memory git service (if available) to populate
        has_uncommitted_changes accurately.
        """
        user_hash = self._user_hash(user)
        persisted = self._store.get(user_hash)
        if persisted is None:
            return None

        # Check for uncommitted changes via the cached git service
        has_changes = False
        git_svc = self._git_services.get(user_hash)
        if git_svc is not None:
            try:
                has_changes = git_svc.has_changes()
            except Exception:
                pass  # Git check failed -- report no changes rather than crash

        return WorkspaceInfo(
            user_id=persisted.user_id,
            username=persisted.username,
            state=WorkspaceState(persisted.state),
            workspace_path=Path(persisted.workspace_path),
            branch=persisted.branch,
            created_at=persisted.created_at,
            last_activity=persisted.last_activity,
            has_uncommitted_changes=has_changes,
        )

    def get_git_service(self, user: UserContext) -> Optional[GitService]:
        """Get the GitService for a user's workspace."""
        return self._git_services.get(self._user_hash(user))

    def get_git_service_by_hash(self, user_hash: str) -> Optional[GitService]:
        """Get the GitService for a user's workspace by user hash.

        Used by AutoCommitService's resolver callback, which only has the
        user_hash (not a full UserContext) at commit time.
        """
        return self._git_services.get(user_hash)

    def heartbeat(self, user: UserContext) -> None:
        """Update last_activity timestamp."""
        user_hash = self._user_hash(user)
        persisted = self._store.get(user_hash)
        if persisted is not None:
            persisted.last_activity = time.time()
            persisted.state = WorkspaceState.ACTIVE.value
            self._store.put(user_hash, persisted)

    def stop_workspace(self, user: UserContext) -> None:
        """Soft-stop a workspace (mark as stopped, keep on disk)."""
        user_hash = self._user_hash(user)
        persisted = self._store.get(user_hash)
        if persisted is not None:
            persisted.state = WorkspaceState.STOPPED.value
            self._store.put(user_hash, persisted)
            self._git_services.pop(user_hash, None)

    def delete_workspace(self, user: UserContext) -> None:
        """Hard-delete a workspace (remove from disk)."""
        user_hash = self._user_hash(user)
        workspace_path = self._workspace_path(user)

        if workspace_path.exists():
            shutil.rmtree(workspace_path)
            self.logger.info(f"Deleted workspace at {workspace_path}")

        self._store.remove(user_hash)
        self._git_services.pop(user_hash, None)

    def get_workspace_project_root(self, user: UserContext) -> Optional[Path]:
        """Get the project root path for a user's workspace.

        This is the key integration point with Phase 1's dependency injection.
        The workspace path replaces the static project_root.
        """
        persisted = self._store.get(self._user_hash(user))
        if persisted and persisted.state in (
            WorkspaceState.ACTIVE.value,
            WorkspaceState.IDLE.value,
        ):
            return Path(persisted.workspace_path)
        return None

    def cleanup_expired(self) -> List[str]:
        """Remove workspaces that have exceeded their TTL.

        IDLE workspaces expire after idle_ttl_seconds (default 24h).
        STOPPED workspaces expire after stopped_ttl_seconds (default 7d).
        ACTIVE workspaces are never cleaned up.

        Returns:
            List of user hashes whose workspaces were removed.
        """
        removed: List[str] = []
        now = time.time()

        for user_hash, info in self._store.all_workspaces().items():
            elapsed = now - info.last_activity

            if (
                info.state == WorkspaceState.IDLE.value
                and elapsed > self.idle_ttl_seconds
            ):
                self.logger.info(
                    f"Cleaning up idle workspace for {info.username} "
                    f"(idle {elapsed / 3600:.1f}h)"
                )
            elif (
                info.state == WorkspaceState.STOPPED.value
                and elapsed > self.stopped_ttl_seconds
            ):
                self.logger.info(
                    f"Cleaning up stopped workspace for {info.username} "
                    f"(stopped {elapsed / 3600:.1f}h)"
                )
            else:
                continue

            # Remove workspace directory and metadata
            ws_path = Path(info.workspace_path)
            if ws_path.exists():
                shutil.rmtree(ws_path)
            self._store.remove(user_hash)
            self._git_services.pop(user_hash, None)
            removed.append(user_hash)

        return removed
