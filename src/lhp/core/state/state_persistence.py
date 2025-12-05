"""State persistence service for LakehousePlumber."""

import json
import logging
from pathlib import Path
from typing import Optional
from datetime import datetime
from dataclasses import asdict

# Import state models from separate module
from ..state_models import DependencyInfo, GlobalDependencies, FileState, ProjectState


class StatePersistence:
    """
    Service for state file I/O operations and serialization.
    
    Handles loading and saving .lhp_state.json files with backward compatibility,
    backup management, and proper error handling.
    """
    
    def __init__(self, project_root: Path, state_file_name: str = ".lhp_state.json"):
        """
        Initialize state persistence service.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
            state_file_name: Name of the state file (default: .lhp_state.json)
        """
        self.project_root = project_root
        self.state_file = project_root / state_file_name
        self.logger = logging.getLogger(__name__)
    
    def state_file_exists(self) -> bool:
        """
        Check if the state file exists on the filesystem.
        
        Returns:
            True if state file exists, False otherwise
        """
        return self.state_file.exists()
    
    def load_state(self):
        """
        Load state from file with backward compatibility.
        
        Returns:
            ProjectState object loaded from file, or new empty state if loading fails
        """
        if self.state_file.exists():
            try:
                with open(self.state_file, "r") as f:
                    state_data = json.load(f)

                # Convert dict back to dataclass with backward compatibility
                environments = {}
                for env_name, env_files in state_data.get("environments", {}).items():
                    environments[env_name] = {}
                    for file_path, file_state in env_files.items():
                        # Handle backward compatibility - add missing fields
                        if "source_yaml_checksum" not in file_state:
                            file_state["source_yaml_checksum"] = ""
                        if "file_dependencies" not in file_state:
                            file_state["file_dependencies"] = None
                        if "file_composite_checksum" not in file_state:
                            file_state["file_composite_checksum"] = ""
                        
                        # Convert file_dependencies from dict to DependencyInfo objects
                        if file_state["file_dependencies"]:
                            file_deps = {}
                            for dep_path, dep_info in file_state["file_dependencies"].items():
                                # Normalize dependency keys for cross-platform compatibility
                                # Replace backslashes first (for Unix systems where \ is literal)
                                normalized_dep_key = dep_path.replace('\\', '/')
                                file_deps[normalized_dep_key] = DependencyInfo(**dep_info)
                            file_state["file_dependencies"] = file_deps
                        
                        # Normalize file path key for cross-platform compatibility
                        # Replace backslashes first (for Unix systems where \ is literal)
                        normalized_key = file_path.replace('\\', '/')
                        environments[env_name][normalized_key] = FileState(**file_state)

                # Handle global dependencies
                global_dependencies = {}
                if "global_dependencies" in state_data:
                    for env_name, global_deps in state_data["global_dependencies"].items():
                        substitution_file = None
                        project_config = None
                        
                        if "substitution_file" in global_deps and global_deps["substitution_file"]:
                            substitution_file = DependencyInfo(**global_deps["substitution_file"])
                        if "project_config" in global_deps and global_deps["project_config"]:
                            project_config = DependencyInfo(**global_deps["project_config"])
                        
                        global_dependencies[env_name] = GlobalDependencies(
                            substitution_file=substitution_file,
                            project_config=project_config
                        )

                loaded_state = ProjectState(
                    version=state_data.get("version", "1.0"),
                    last_updated=state_data.get("last_updated", ""),
                    environments=environments,
                    global_dependencies=global_dependencies
                )

                self.logger.info(f"Loaded state from {self.state_file}")
                return loaded_state

            except Exception as e:
                self.logger.warning(f"Failed to load state file {self.state_file}: {e}")
                return ProjectState()
        else:
            return ProjectState()
    
    def save_state(self, state) -> None:
        """
        Save current state to file.
        
        Args:
            state: ProjectState to save
            
        Raises:
            Exception: If save operation fails
        """
        try:
            # Convert to dict for JSON serialization
            state_dict = asdict(state)
            state_dict["last_updated"] = datetime.now().isoformat()

            with open(self.state_file, "w") as f:
                json.dump(state_dict, f, indent=2, sort_keys=True)

            self.logger.debug(f"Saved state to {self.state_file}")

        except Exception as e:
            self.logger.error(f"Failed to save state file {self.state_file}: {e}")
            raise
    
    def backup_state_file(self) -> Optional[Path]:
        """
        Create a backup copy of the state file.
        
        Returns:
            Path to backup file if created, None if no state file exists
        """
        if not self.state_file_exists():
            return None
        
        try:
            backup_path = self.state_file.with_suffix(f".json.backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}")
            
            with open(self.state_file, "r") as src:
                with open(backup_path, "w") as dst:
                    dst.write(src.read())
            
            self.logger.info(f"Created state backup: {backup_path}")
            return backup_path
            
        except Exception as e:
            self.logger.warning(f"Failed to create state backup: {e}")
            return None
    
    def get_state_file_path(self) -> Path:
        """
        Get the path to the state file.
        
        Returns:
            Path to the state file
        """
        return self.state_file
    
    def get_state_file_info(self) -> dict:
        """
        Get information about the state file.
        
        Returns:
            Dictionary with file information
        """
        if not self.state_file_exists():
            return {
                "exists": False,
                "path": str(self.state_file),
                "size": 0,
                "last_modified": None
            }
        
        try:
            stat = self.state_file.stat()
            return {
                "exists": True,
                "path": str(self.state_file),
                "size": stat.st_size,
                "last_modified": datetime.fromtimestamp(stat.st_mtime).isoformat()
            }
        except Exception as e:
            self.logger.warning(f"Failed to get state file info: {e}")
            return {
                "exists": True,
                "path": str(self.state_file),
                "size": 0,
                "last_modified": None
            }
