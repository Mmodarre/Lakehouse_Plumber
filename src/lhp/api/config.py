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

    # Static file serving (set LHP_STATIC_DIR to serve pre-built React SPA)
    static_dir: Optional[str] = None

    # CORS
    cors_origins: List[str] = ["*"]
    cors_allow_credentials: bool = True

    # Phase 2: Workspace settings (declared here, used later)
    workspace_root: Path = Field(default_factory=lambda: Path("/tmp/lhp-workspaces"))

    # Workspace management settings
    workspace_max_count: int = 50
    workspace_idle_ttl_hours: float = 24.0
    workspace_stopped_ttl_hours: float = 168.0
    source_repo: Optional[str] = None

    # AI Assistant (OpenCode) settings
    ai_enabled: bool = False
    opencode_url: Optional[str] = None
    opencode_port: int = 4096
    opencode_password: Optional[str] = None

    # AI configuration (overridden by LHP_AI_* env vars and ai_config.yaml)
    ai_provider: str = "anthropic"
    ai_model: str = "anthropic/claude-sonnet-4-20250514"
    ai_max_processes: int = 10
    ai_idle_timeout_minutes: float = 30.0
