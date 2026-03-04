import hashlib
import logging
from typing import Optional

from fastapi import HTTPException, Request
from pydantic import BaseModel

logger = logging.getLogger(__name__)


class UserContext(BaseModel):
    """Authenticated user information."""

    email: str
    username: str
    user_id: str
    access_token: Optional[str] = None

    @property
    def user_id_hash(self) -> str:
        """Short hash for workspace directory naming."""
        return hashlib.sha256(self.user_id.encode()).hexdigest()[:16]


# Dev-mode fallback user (used when Databricks headers are absent)
DEV_USER = UserContext(
    email="dev@localhost",
    username="dev",
    user_id="dev-local",
)


async def get_current_user(request: Request) -> UserContext:
    """Extract user from Databricks X-Forwarded-* headers.

    In dev_mode (local development), returns DEV_USER when headers are absent.
    In production (Databricks Apps), requires all three headers.
    """
    settings = request.app.state.settings

    email = request.headers.get("X-Forwarded-Email")
    username = request.headers.get("X-Forwarded-Preferred-Username")
    user_id = request.headers.get("X-Forwarded-User")
    access_token = request.headers.get("X-Forwarded-Access-Token")

    # If all headers present, use them regardless of mode
    if all([email, username, user_id]):
        return UserContext(
            email=email,
            username=username,
            user_id=user_id,
            access_token=access_token,
        )

    # Dev mode: fall back to dev user
    if settings.dev_mode:
        logger.debug("Dev mode: using fallback dev user")
        return DEV_USER

    # Production: missing headers is an auth failure
    raise HTTPException(
        status_code=401,
        detail="Missing authentication headers (X-Forwarded-Email, X-Forwarded-Preferred-Username, X-Forwarded-User)",
    )
