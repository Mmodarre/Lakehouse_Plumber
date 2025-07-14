"""
VS Code configuration manager for IntelliSense setup.

This module provides utilities to manage VS Code settings.json file
for setting up YAML schema associations.
"""

import json
import platform
import shutil
import datetime
from pathlib import Path
from typing import Dict, Optional, Any, List

from lhp.utils.error_formatter import LHPError, ErrorCategory


class VSCodeConfigError(LHPError):
    """Raised when VS Code configuration operations fail."""
    
    def __init__(self, message: str, suggestions: list = None):
        super().__init__(
            category=ErrorCategory.CONFIG,
            code_number="002",
            title="VS Code Configuration Error",
            details=message,
            suggestions=suggestions or ["Check VS Code installation", "Verify file permissions", "Check settings.json syntax"]
        )


class VSCodeConfigManager:
    """Manages VS Code configuration for IntelliSense setup."""
    
    def __init__(self):
        self._settings_path = self._get_vscode_settings_path()
        self._backup_directory = self._get_backup_directory()
    
    def _get_vscode_settings_path(self) -> Path:
        """Get the platform-specific VS Code settings path."""
        system = platform.system()
        
        if system == "Windows":
            return Path.home() / "AppData" / "Roaming" / "Code" / "User" / "settings.json"
        elif system == "Darwin":
            return Path.home() / "Library" / "Application Support" / "Code" / "User" / "settings.json"
        else:  # Linux and others
            return Path.home() / ".config" / "Code" / "User" / "settings.json"
    
    def _get_backup_directory(self) -> Path:
        """Get the backup directory for VS Code settings."""
        backup_dir = Path.home() / ".lhp" / "vscode_backups"
        backup_dir.mkdir(parents=True, exist_ok=True)
        return backup_dir
    
    def settings_file_exists(self) -> bool:
        """Check if VS Code settings file exists."""
        return self._settings_path.exists()
    
    def read_settings(self) -> Dict[str, Any]:
        """
        Read current VS Code settings.
        
        Returns:
            Dictionary containing VS Code settings
            
        Raises:
            VSCodeConfigError: If settings file cannot be read
        """
        try:
            if not self.settings_file_exists():
                return {}
            
            with open(self._settings_path, 'r', encoding='utf-8') as f:
                return json.load(f)
                
        except json.JSONDecodeError as e:
            raise VSCodeConfigError(f"Invalid JSON in VS Code settings: {str(e)}")
        except Exception as e:
            raise VSCodeConfigError(f"Failed to read VS Code settings: {str(e)}")
    
    def write_settings(self, settings: Dict[str, Any]) -> None:
        """
        Write VS Code settings to file.
        
        Args:
            settings: Dictionary containing VS Code settings
            
        Raises:
            VSCodeConfigError: If settings cannot be written
        """
        try:
            # Ensure parent directory exists
            self._settings_path.parent.mkdir(parents=True, exist_ok=True)
            
            # Write settings with proper formatting
            with open(self._settings_path, 'w', encoding='utf-8') as f:
                json.dump(settings, f, indent=2, ensure_ascii=False)
                
        except Exception as e:
            raise VSCodeConfigError(f"Failed to write VS Code settings: {str(e)}")
    
    def backup_settings(self) -> Optional[Path]:
        """
        Create a backup of current VS Code settings.
        
        Returns:
            Path to the backup file if successful, None if no settings exist
            
        Raises:
            VSCodeConfigError: If backup cannot be created
        """
        if not self.settings_file_exists():
            return None
        
        try:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"settings_backup_{timestamp}.json"
            backup_path = self._backup_directory / backup_filename
            
            shutil.copy2(self._settings_path, backup_path)
            
            return backup_path
            
        except Exception as e:
            raise VSCodeConfigError(f"Failed to create backup: {str(e)}")
    
    def restore_settings(self, backup_path: Path) -> None:
        """
        Restore VS Code settings from a backup.
        
        Args:
            backup_path: Path to the backup file
            
        Raises:
            VSCodeConfigError: If restoration fails
        """
        try:
            if not backup_path.exists():
                raise VSCodeConfigError(f"Backup file not found: {backup_path}")
            
            # Ensure parent directory exists
            self._settings_path.parent.mkdir(parents=True, exist_ok=True)
            
            shutil.copy2(backup_path, self._settings_path)
            
        except Exception as e:
            raise VSCodeConfigError(f"Failed to restore settings: {str(e)}")
    
    def get_yaml_schemas(self) -> Dict[str, str]:
        """
        Get current YAML schema associations from VS Code settings.
        
        Returns:
            Dictionary mapping schema paths to file patterns
        """
        settings = self.read_settings()
        return settings.get("yaml.schemas", {})
    
    def set_yaml_schemas(self, schemas: Dict[str, str]) -> None:
        """
        Set YAML schema associations in VS Code settings.
        
        Args:
            schemas: Dictionary mapping schema paths to file patterns
            
        Raises:
            VSCodeConfigError: If schemas cannot be set
        """
        settings = self.read_settings()
        settings["yaml.schemas"] = schemas
        self.write_settings(settings)
    
    def add_yaml_schema(self, schema_path: str, file_pattern: str) -> None:
        """
        Add a single YAML schema association.
        
        Args:
            schema_path: Path to the schema file
            file_pattern: Glob pattern for files to associate with this schema
            
        Raises:
            VSCodeConfigError: If schema cannot be added
        """
        settings = self.read_settings()
        
        if "yaml.schemas" not in settings:
            settings["yaml.schemas"] = {}
        
        settings["yaml.schemas"][schema_path] = file_pattern
        self.write_settings(settings)
    
    def remove_yaml_schema(self, schema_path: str) -> bool:
        """
        Remove a YAML schema association.
        
        Args:
            schema_path: Path to the schema file to remove
            
        Returns:
            True if schema was removed, False if it didn't exist
            
        Raises:
            VSCodeConfigError: If schema cannot be removed
        """
        settings = self.read_settings()
        
        if "yaml.schemas" not in settings:
            return False
        
        if schema_path in settings["yaml.schemas"]:
            del settings["yaml.schemas"][schema_path]
            self.write_settings(settings)
            return True
        
        return False
    
    def clear_lhp_schemas(self) -> int:
        """
        Remove all LHP-related schema associations.
        
        Returns:
            Number of schemas removed
            
        Raises:
            VSCodeConfigError: If schemas cannot be removed
        """
        settings = self.read_settings()
        
        if "yaml.schemas" not in settings:
            return 0
        
        # Find LHP-related schemas (those containing .lhp or lakehouse-plumber)
        lhp_schemas = []
        for schema_path in settings["yaml.schemas"].keys():
            if ".lhp" in schema_path or "lakehouse-plumber" in schema_path:
                lhp_schemas.append(schema_path)
        
        # Remove LHP schemas
        for schema_path in lhp_schemas:
            del settings["yaml.schemas"][schema_path]
        
        if lhp_schemas:
            self.write_settings(settings)
        
        return len(lhp_schemas)
    
    def get_file_associations(self) -> Dict[str, str]:
        """
        Get current file associations from VS Code settings.
        
        Returns:
            Dictionary mapping file patterns to language identifiers
        """
        settings = self.read_settings()
        return settings.get("files.associations", {})
    
    def ensure_yaml_file_associations(self) -> None:
        """
        Ensure YAML file associations are set up correctly.
        
        Raises:
            VSCodeConfigError: If associations cannot be set
        """
        settings = self.read_settings()
        
        if "files.associations" not in settings:
            settings["files.associations"] = {}
        
        # Ensure YAML and YML files are associated with the yaml language
        yaml_associations = {
            "*.yaml": "yaml",
            "*.yml": "yaml"
        }
        
        settings["files.associations"].update(yaml_associations)
        self.write_settings(settings)
    
    def ensure_yaml_validation_enabled(self) -> None:
        """
        Ensure YAML validation is enabled in VS Code.
        
        Raises:
            VSCodeConfigError: If validation settings cannot be set
        """
        settings = self.read_settings()
        
        # Enable YAML validation and completion
        yaml_settings = {
            "yaml.validate": True,
            "yaml.completion": True,
            "yaml.hover": True,
            "yaml.format.enable": True
        }
        
        settings.update(yaml_settings)
        self.write_settings(settings)
    
    def get_backup_list(self) -> List[Dict[str, Any]]:
        """
        Get list of available backups.
        
        Returns:
            List of backup information dictionaries
        """
        backups = []
        
        if not self._backup_directory.exists():
            return backups
        
        for backup_file in self._backup_directory.glob("settings_backup_*.json"):
            try:
                stat = backup_file.stat()
                backups.append({
                    "path": backup_file,
                    "filename": backup_file.name,
                    "created": datetime.datetime.fromtimestamp(stat.st_ctime),
                    "size": stat.st_size
                })
            except Exception:
                continue
        
        # Sort by creation time (newest first)
        backups.sort(key=lambda x: x["created"], reverse=True)
        
        return backups
    
    def cleanup_old_backups(self, keep_count: int = 5) -> int:
        """
        Remove old backup files, keeping only the most recent ones.
        
        Args:
            keep_count: Number of recent backups to keep
            
        Returns:
            Number of backups removed
        """
        backups = self.get_backup_list()
        
        if len(backups) <= keep_count:
            return 0
        
        removed_count = 0
        for backup in backups[keep_count:]:
            try:
                backup["path"].unlink()
                removed_count += 1
            except Exception:
                continue
        
        return removed_count
    
    def validate_settings_syntax(self) -> bool:
        """
        Validate that the current settings file has valid JSON syntax.
        
        Returns:
            True if settings are valid, False otherwise
        """
        try:
            self.read_settings()
            return True
        except VSCodeConfigError:
            return False
    
    def get_conflicting_yaml_extensions(self) -> List[str]:
        """
        Get a list of potentially conflicting YAML extensions.
        
        This method would need to be implemented with actual VS Code
        extension detection logic.
        
        Returns:
            List of conflicting extension identifiers
        """
        # This is a placeholder implementation
        # In a real implementation, we would check the VS Code extensions
        # directory and look for extensions that might conflict with
        # YAML schema associations
        
        # Common YAML extensions that might conflict
        known_yaml_extensions = [
            "redhat.vscode-yaml",
            "ms-kubernetes-tools.vscode-kubernetes-tools",
            "ms-vscode.vscode-yaml-sort"
        ]
        
        return known_yaml_extensions
    
    def get_settings_path(self) -> Path:
        """Get the path to the VS Code settings file."""
        return self._settings_path
    
    def get_backup_directory_path(self) -> Path:
        """Get the path to the backup directory."""
        return self._backup_directory 