from typing import List, Optional

from pydantic import BaseModel, Field


class WorkspaceResponse(BaseModel):
    """Response model for workspace status."""

    state: str
    branch: str
    created_at: float
    last_activity: float
    has_uncommitted_changes: bool


class GitStatusResponse(BaseModel):
    """Response model for git status."""

    branch: str
    modified: List[str]
    staged: List[str]
    untracked: List[str]
    ahead: int = 0
    behind: int = 0


class CommitRequest(BaseModel):
    """Request body for manual commit."""

    message: str = Field(
        ...,
        min_length=1,
        max_length=500,
        description="Commit message describing the changes",
    )


class CommitResponse(BaseModel):
    """Response model for commit operations."""

    committed: bool
    sha: Optional[str] = None
