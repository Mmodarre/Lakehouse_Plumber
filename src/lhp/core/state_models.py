"""State management data models for LakehousePlumber."""

from dataclasses import dataclass
from typing import Dict, Optional, List


@dataclass
class DependencyInfo:
    """Information about a dependency file."""
    
    path: str              # Relative path to dependency file
    checksum: str          # SHA256 checksum of dependency
    type: str             # 'preset', 'template', 'substitution', 'project_config'
    last_modified: str    # ISO timestamp of last modification
    mtime: Optional[float] = None  # Unix timestamp for fast comparison


@dataclass
class GlobalDependencies:
    """Dependencies that affect all files in scope."""
    substitution_file: Optional[DependencyInfo] = None  # Per environment
    project_config: Optional[DependencyInfo] = None     # Global across environments


@dataclass
class FileState:
    """Represents the state of a generated file."""

    source_yaml: str  # Path to the YAML file that generated this
    generated_path: str  # Path to the generated file
    checksum: str  # SHA256 checksum of the generated file
    source_yaml_checksum: str  # SHA256 checksum of the source YAML file
    timestamp: str  # When it was generated
    environment: str  # Environment name
    pipeline: str  # Pipeline name
    flowgroup: str  # FlowGroup name
    
    # New dependency tracking fields
    file_dependencies: Optional[Dict[str, DependencyInfo]] = None  # File-specific dependencies
    file_composite_checksum: str = ""
    used_substitution_keys: Optional[List[str]] = None  # Substitution keys used during generation


@dataclass
class ProjectState:
    """Represents the complete state of a project."""

    version: str = "1.0"
    last_updated: str = ""
    environments: Dict[str, Dict[str, FileState]] = (
        None  # env -> file_path -> FileState
    )
    
    # Global dependencies per environment
    global_dependencies: Optional[Dict[str, GlobalDependencies]] = None  # env -> GlobalDependencies

    def __post_init__(self):
        if self.environments is None:
            self.environments = {}
        if self.global_dependencies is None:
            self.global_dependencies = {}
