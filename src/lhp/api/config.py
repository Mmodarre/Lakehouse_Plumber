from pathlib import Path
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class APISettings(BaseSettings):
    """API configuration loaded from environment variables."""

    model_config = SettingsConfigDict(env_prefix="LHP_")

    # Core settings
    project_root: Path = Field(default_factory=lambda: Path(".").resolve())
    dev_mode: bool = False
    log_level: str = "INFO"

    # Server settings (Databricks Apps uses DATABRICKS_APP_PORT)
    databricks_app_port: Optional[int] = Field(default=None, alias="DATABRICKS_APP_PORT")

    # CORS
    cors_origins: List[str] = ["*"]
    cors_allow_credentials: bool = True

    # Phase 2: Workspace settings (declared here, used later)
    workspace_root: Path = Field(default_factory=lambda: Path("/tmp/lhp-workspaces"))
