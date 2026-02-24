import asyncio
import hashlib
from typing import Dict

from fastapi import APIRouter, Depends, HTTPException

from lhp.api.auth import UserContext, get_current_user
from lhp.api.dependencies import (
    get_auto_commit_service,
    get_git_service,
    get_git_service_readonly,
    get_workspace_manager,
    require_not_dev_mode,
)
from lhp.api.schemas.workspace import (
    CommitRequest,
    CommitResponse,
    GitStatusResponse,
    WorkspaceResponse,
)
from lhp.api.services.auto_commit_service import AutoCommitService
from lhp.api.services.git_service import GitService
from lhp.api.services.workspace_manager import WorkspaceManager

router = APIRouter(prefix="/workspace", tags=["workspace"])


@router.put("", response_model=WorkspaceResponse)
async def ensure_workspace(
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> WorkspaceResponse:
    """#4: Create or resume a workspace (idempotent).

    Uses PUT because the semantics are 'ensure this workspace exists'.
    Returns the workspace status whether it was newly created or resumed.
    """
    info = await asyncio.to_thread(workspace_mgr.create_or_resume, user)
    return WorkspaceResponse(
        state=info.state.value,
        branch=info.branch,
        created_at=info.created_at,
        last_activity=info.last_activity,
        has_uncommitted_changes=info.has_uncommitted_changes,
    )


@router.get("", response_model=WorkspaceResponse)
async def get_workspace_status(
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> WorkspaceResponse:
    """#5: Get workspace status."""
    info = workspace_mgr.get_workspace(user)
    if info is None:
        raise HTTPException(404, "No workspace found")
    return WorkspaceResponse(
        state=info.state.value,
        branch=info.branch,
        created_at=info.created_at,
        last_activity=info.last_activity,
        has_uncommitted_changes=info.has_uncommitted_changes,
    )


@router.post("/heartbeat")
async def heartbeat(
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> Dict:
    """#6: Update last_activity timestamp."""
    workspace_mgr.heartbeat(user)
    return {"ok": True}


@router.post("/stop")
async def stop_workspace(
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
) -> Dict:
    """#7: Soft-stop workspace (keep files on disk, deactivate).

    The workspace can be resumed later with PUT /workspace.
    Stopped workspaces are subject to TTL-based cleanup.
    """
    workspace_mgr.stop_workspace(user)
    return {"stopped": True}


@router.delete("")
async def delete_workspace(
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    workspace_mgr: WorkspaceManager = Depends(get_workspace_manager),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> Dict:
    """#7b: Hard-delete workspace (remove all files from disk).

    Flushes any pending auto-commit BEFORE deleting to prevent data loss.
    After flush, the workspace is irreversibly removed.
    """
    # Flush pending auto-commits before deleting -- prevents data loss
    user_hash = hashlib.sha256(user.user_id.encode()).hexdigest()[:16]
    await auto_commit_service.flush(user_hash)

    workspace_mgr.delete_workspace(user)
    return {"deleted": True}


@router.post("/commit", response_model=CommitResponse)
async def manual_commit(
    body: CommitRequest,
    _guard: None = Depends(require_not_dev_mode),
    user: UserContext = Depends(get_current_user),
    git_svc: GitService = Depends(get_git_service),
    auto_commit_service: AutoCommitService = Depends(get_auto_commit_service),
) -> CommitResponse:
    """#8: Manual commit with user-provided message.

    Cancels any pending auto-commit timer before performing the commit.
    This prevents the auto-commit from firing after the user's commit and
    ensures the user's message (not "Auto-commit: workspace changes") is
    recorded in git history. commit_all() stages all workspace changes, so
    nothing tracked by the auto-commit service is lost.
    """
    user_hash = hashlib.sha256(user.user_id.encode()).hexdigest()[:16]
    await auto_commit_service.cancel_pending(user_hash)

    sha = await asyncio.to_thread(git_svc.commit_all, body.message)
    return CommitResponse(committed=sha is not None, sha=sha)


@router.post("/push")
async def push_branch(
    _guard: None = Depends(require_not_dev_mode),
    git_svc: GitService = Depends(get_git_service),
) -> Dict:
    """#9: Push current branch to remote."""
    await asyncio.to_thread(git_svc.push)
    return {"pushed": True}


@router.post("/pull")
async def pull_latest(
    _guard: None = Depends(require_not_dev_mode),
    git_svc: GitService = Depends(get_git_service),
) -> Dict:
    """#10: Pull latest from remote."""
    await asyncio.to_thread(git_svc.pull)
    return {"pulled": True}


@router.get("/git/status", response_model=GitStatusResponse)
async def git_status(
    git_svc: GitService = Depends(get_git_service_readonly),
) -> GitStatusResponse:
    """#11: Current git status."""
    status = await asyncio.to_thread(git_svc.get_status)
    return GitStatusResponse(
        branch=status.branch,
        modified=status.modified,
        staged=status.staged,
        untracked=status.untracked,
        ahead=status.ahead,
        behind=status.behind,
    )


@router.get("/git/log")
async def git_log(
    max_count: int = 20,
    git_svc: GitService = Depends(get_git_service_readonly),
) -> Dict:
    """#12: Recent commit log."""
    entries = await asyncio.to_thread(git_svc.get_log, max_count)
    return {
        "entries": [
            {
                "sha": e.sha,
                "message": e.message,
                "author": e.author,
                "date": e.date,
            }
            for e in entries
        ]
    }
