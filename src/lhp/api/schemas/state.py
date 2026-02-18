from typing import Any, Dict, List, Optional
from pydantic import BaseModel


class FileStateResponse(BaseModel):
    source_yaml: str
    generated_path: str
    checksum: str
    source_yaml_checksum: str
    timestamp: str
    environment: str
    pipeline: str
    flowgroup: str


class StateOverviewResponse(BaseModel):
    """Full state across all environments."""
    version: str
    last_updated: str
    environments: Dict[str, Dict[str, FileStateResponse]]
    total_files: int


class EnvironmentStateResponse(BaseModel):
    """State for a single environment."""
    environment: str
    files: Dict[str, FileStateResponse]
    total_files: int


class StalenessResponse(BaseModel):
    """Staleness analysis result."""
    has_work_to_do: bool
    pipelines_needing_generation: Dict[str, Any]
    pipelines_up_to_date: Dict[str, int]
    has_global_changes: bool
    global_changes: List[str]
    total_new: int
    total_stale: int
    total_up_to_date: int


class CleanupResponse(BaseModel):
    """Result of orphaned file cleanup."""
    cleaned_files: List[str]
    total_cleaned: int
