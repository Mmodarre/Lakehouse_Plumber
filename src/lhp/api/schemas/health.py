from typing import Dict, Optional
from pydantic import BaseModel


class HealthResponse(BaseModel):
    status: str = "healthy"
    version: str
    python_version: str
    dev_mode: bool = False


class VersionResponse(BaseModel):
    lhp_version: str
    python_version: str
    dependencies: Dict[str, str]  # package name → installed version


class UserResponse(BaseModel):
    email: str
    username: str
    user_id: str
