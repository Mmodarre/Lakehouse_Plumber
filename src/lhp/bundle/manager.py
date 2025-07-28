"""
Bundle manager for LHP Databricks Asset Bundle integration.

This module provides the main BundleManager class that coordinates bundle
resource operations including resource file synchronization and management.
"""

import logging
from pathlib import Path
from typing import List, Dict, Set, Optional, Union, Any
import os

from .exceptions import BundleResourceError, YAMLParsingError
from ..core.base_generator import BaseActionGenerator

# TEMPORARY import - remove when Databricks fixes limitation
from .temporary_databricks_headers import add_databricks_notebook_headers


logger = logging.getLogger(__name__)


class BundleManager(BaseActionGenerator):
    """
    Manages Databricks Asset Bundle resource files for LHP.
    
    This class handles synchronization of bundle resource files with generated
    Python files, maintaining consistency between LHP-generated code and
    bundle configurations.
    """
    
    def __init__(self, project_root: Union[Path, str]):
        """
        Initialize the bundle manager.
        
        Args:
            project_root: Path to the project root directory
            
        Raises:
            TypeError: If project_root is None
        """
        # Initialize parent class for Jinja2 template support
        super().__init__()
        
        if project_root is None:
            raise TypeError("project_root cannot be None")
            
        # Convert string to Path if necessary
        if isinstance(project_root, str):
            project_root = Path(project_root)
            
        self.project_root = project_root
        self.resources_dir = project_root / "resources" / "lhp"
        self.logger = logging.getLogger(__name__)

    def generate(self, action, context: Dict[str, Any]) -> str:
        """
        Required implementation for BaseActionGenerator.
        BundleManager uses template rendering for bundle resources, not actions.
        """
        raise NotImplementedError("BundleManager uses render_template() for bundle resources, not generate()")

    def sync_resources_with_generated_files(self, output_dir: Path, env: str):
        """
        Bidirectionally sync bundle resource files with generated Python files.
        
        This method performs complete synchronization:
        - Creates resource files for new pipeline directories
        - Updates resource files when Python files are added/removed
        - Removes resource files for pipeline directories that no longer exist
        
        Args:
            output_dir: Directory containing generated Python files
            env: Environment name for template processing
            
        Raises:
            BundleResourceError: If synchronization fails
        """
        self.logger.info("🔄 Syncing bundle resources with generated files...")
        
        # Ensure resources directory exists
        self._ensure_resources_directory()
        
        # Get current state
        current_pipeline_dirs = self._get_pipeline_directories(output_dir)
        current_pipeline_names = {pipeline_dir.name for pipeline_dir in current_pipeline_dirs}
        existing_resource_files = self._get_existing_resource_files()
        
        updated_count = 0
        removed_count = 0
        
        # Step 1: Create/update resource files for current pipeline directories
        for pipeline_dir in current_pipeline_dirs:
            pipeline_name = pipeline_dir.name
            
            try:
                if self._sync_pipeline_resource(pipeline_name, pipeline_dir, env):
                    updated_count += 1
                    self.logger.debug(f"Successfully synced pipeline: {pipeline_name}")
                    
            except YAMLParsingError as e:
                error_msg = f"YAML processing failed for pipeline '{pipeline_name}': {e}"
                self.logger.error(error_msg)
                raise BundleResourceError(error_msg, e)
                
            except OSError as e:
                error_msg = f"File system error for pipeline '{pipeline_name}': {e}"
                self.logger.error(error_msg)
                raise BundleResourceError(error_msg, e)
                
            except Exception as e:
                error_msg = f"Unexpected error for pipeline '{pipeline_name}': {e}"
                self.logger.error(error_msg)
                raise BundleResourceError(error_msg, e)
        
        # Step 2: Backup resource files for pipeline directories that no longer exist
        # Check ALL files in resources/lhp, not just LHP-generated ones
        all_resource_files = self._get_all_resource_files_in_lhp_directory()
        for resource_file_info in all_resource_files:
            pipeline_name = resource_file_info["pipeline_name"]
            resource_file = resource_file_info["path"]
            
            if pipeline_name not in current_pipeline_names:
                try:
                    self._backup_resource_file(resource_file, pipeline_name)
                    removed_count += 1
                    self.logger.debug(f"Successfully backed up resource file for deleted pipeline: {pipeline_name}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to backup resource file {resource_file}: {e}")
        
        # Log summary
        if updated_count > 0 or removed_count > 0:
            if updated_count > 0 and removed_count > 0:
                self.logger.info(f"✅ Bundle sync completed: updated {updated_count}, removed {removed_count} resource file(s)")
            elif updated_count > 0:
                self.logger.info(f"✅ Updated {updated_count} bundle resource file(s)")
            else:
                self.logger.info(f"✅ Removed {removed_count} bundle resource file(s)")
        else:
            self.logger.info("✅ All bundle resources are up to date")
        
        # TEMPORARY: Add Databricks notebook headers after successful sync
        # This is a workaround for Databricks Asset Bundle limitation
        # Remove when Databricks no longer requires '# Databricks notebook source' headers
        try:
            header_count = add_databricks_notebook_headers(output_dir, env)
            if header_count > 0:
                self.logger.debug(f"TEMPORARY: Added Databricks headers to {header_count} Python file(s)")
        except Exception as e:
            self.logger.warning(f"TEMPORARY: Failed to add Databricks headers: {e}")
        
        return updated_count + removed_count

    def _sync_pipeline_resource(self, pipeline_name: str, pipeline_dir: Path, env: str) -> bool:
        """
        Sync a single pipeline resource file using create-once model.
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_dir: Directory containing pipeline Python files
            env: Environment name
            
        Returns:
            True if resource file was created or updated, False if no changes needed
        """
        # Handle multiple files scenario (E2): Find ALL files for this pipeline
        related_files = self._find_all_resource_files_for_pipeline(pipeline_name)
        
        if related_files:
            # Check if we have any LHP-managed files
            lhp_files = [f for f in related_files if self._is_lhp_generated_file(f)]
            non_lhp_files = [f for f in related_files if not self._is_lhp_generated_file(f)]
            
            # Backup all non-LHP files
            for non_lhp_file in non_lhp_files:
                self._backup_single_file(non_lhp_file, pipeline_name)
            
            if lhp_files:
                # Backup any extra LHP files, keep primary for replacement
                primary_file = lhp_files[0]
                for extra_file in lhp_files[1:]:
                    self._backup_single_file(extra_file, pipeline_name)
                
                # Always recreate the resource file with current template
                self._backup_and_recreate_resource_file(primary_file, pipeline_name, env)
                return True
            else:
                # All files were non-LHP, create new
                self._create_new_resource_file(pipeline_name, env)
                return True
        else:
            # Create new resource file
            self._create_new_resource_file(pipeline_name, env)
            return True

    def _ensure_resources_directory(self):
        """Create resources/lhp directory if it doesn't exist."""
        try:
            self.resources_dir.mkdir(parents=True, exist_ok=True)
            self.logger.debug(f"Ensured LHP resources directory exists: {self.resources_dir}")
        except OSError as e:
            raise BundleResourceError(f"Failed to create resources directory: {e}", e)

    def _get_pipeline_directories(self, output_dir: Path) -> List[Path]:
        """
        Get list of pipeline directories in the output directory.
        
        Args:
            output_dir: Directory to scan for pipeline directories
            
        Returns:
            List of pipeline directory paths in sorted order
            
        Raises:
            BundleResourceError: If directory access fails
        """
        try:
            if not output_dir.exists():
                raise BundleResourceError(f"Output directory does not exist: {output_dir}")
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Cannot access output directory {output_dir}: {e}", e)
        
        try:
            pipeline_dirs = []
            # Sort directories to ensure deterministic processing order across platforms
            for item in sorted(output_dir.iterdir()):
                if item.is_dir():
                    pipeline_dirs.append(item)
                    self.logger.debug(f"Found pipeline directory: {item.name}")
            
            return pipeline_dirs
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Error scanning output directory {output_dir}: {e}", e)



    def _get_resource_file_path(self, pipeline_name: str) -> Path:
        """
        Find or generate resource file path for a pipeline.
        
        This method looks for existing resource files in order of preference:
        1. {pipeline_name}.pipeline.yml (preferred format)
        2. {pipeline_name}.yml (simple format)
        
        If neither exists, returns path for the preferred format.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            Path to the resource file for this pipeline
        """
        # Check for preferred format first
        preferred_path = self.resources_dir / f"{pipeline_name}.pipeline.yml"
        if preferred_path.exists():
            return preferred_path
        
        # Check for simple format
        simple_path = self.resources_dir / f"{pipeline_name}.yml"
        if simple_path.exists():
            return simple_path
        
        # If neither exists, return preferred format for new file creation
        return preferred_path



    def _create_new_resource_file(self, pipeline_name: str, env: str):
        """
        Create new resource file for a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            env: Environment name for template processing
        """
        # Ensure resources directory exists
        self._ensure_resources_directory()
        
        resource_file = self._get_resource_file_path(pipeline_name)
        
        # Generate basic resource file content
        content = self._generate_resource_file_content(pipeline_name)
        
        try:
            resource_file.write_text(content, encoding='utf-8')
            self.logger.info(f"Created new resource file: {resource_file}")
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to create resource file {resource_file}: {e}", e)

    def _backup_and_recreate_resource_file(self, resource_file: Path, pipeline_name: str, env: str):
        """
        Backup existing resource file and create new LHP-managed file.
        
        Args:
            resource_file: Path to existing resource file
            pipeline_name: Name of the pipeline
            env: Environment name for template processing
            
        Raises:
            BundleResourceError: If backup or creation fails
        """
        try:
            # Create backup with .bkup extension
            backup_file = resource_file.with_suffix(resource_file.suffix + '.bkup')
            
            # If backup already exists, find a unique name
            counter = 1
            original_backup = backup_file
            while backup_file.exists():
                backup_file = original_backup.with_suffix(f'.bkup.{counter}')
                counter += 1
            
            # Move original to backup
            resource_file.rename(backup_file)
            self.logger.info(f"📦 Backed up existing file: {resource_file.name} → {backup_file.name}")
            
            # Create new LHP-managed file
            self._create_new_resource_file(pipeline_name, env)
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to backup and recreate resource file {resource_file}: {e}", e)

    def _generate_resource_file_content(self, pipeline_name: str) -> str:
        """
        Generate content for a bundle resource file using Jinja2 template.
        
        Args:
            pipeline_name: Name of the pipeline
            
        Returns:
            YAML content for the resource file
        """
        context = {
            "pipeline_name": pipeline_name
        }
        
        return self.render_template("bundle/pipeline_resource.yml.j2", context)

    def _get_existing_resource_files(self) -> List[Dict[str, Any]]:
        """
        Get list of existing resource files in the resources directory.
        
        Returns:
            List of dictionaries with 'path' and 'pipeline_name' keys
        """
        resource_files = []
        
        if not self.resources_dir.exists():
            return resource_files
            
        try:
            # Look for pipeline resource files (.pipeline.yml and .yml)
            for resource_file in self.resources_dir.glob("*.yml"):
                pipeline_name = self._extract_pipeline_name_from_resource_file(resource_file)
                if pipeline_name:
                    resource_files.append({
                        "path": resource_file,
                        "pipeline_name": pipeline_name
                    })
                    self.logger.debug(f"Found existing resource file: {resource_file.name} for pipeline: {pipeline_name}")
            
            return resource_files
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error scanning resources directory {self.resources_dir}: {e}")
            return []

    def _extract_pipeline_name_from_resource_file(self, resource_file: Path) -> Optional[str]:
        """
        Extract pipeline name from LHP-generated resource file.
        
        Args:
            resource_file: Path to the resource file
            
        Returns:
            Pipeline name if it's an LHP-generated file, None otherwise
        """
        # First check if this is an LHP-generated file
        if not self._is_lhp_generated_file(resource_file):
            self.logger.debug(f"Skipping non-LHP file: {resource_file.name}")
            return None
            
        file_name = resource_file.name
        
        # Handle .pipeline.yml format
        if file_name.endswith(".pipeline.yml"):
            return file_name[:-13]  # Remove ".pipeline.yml"
        
        # Handle .yml format  
        elif file_name.endswith(".yml"):
            return file_name[:-4]   # Remove ".yml"
        
        return None

    def _remove_resource_file(self, resource_file: Path, pipeline_name: str):
        """
        Remove a resource file for a pipeline that no longer exists.
        
        Args:
            resource_file: Path to the resource file to remove
            pipeline_name: Name of the pipeline (for logging)
            
        Raises:
            BundleResourceError: If file removal fails
        """
        try:
            if resource_file.exists():
                resource_file.unlink()
                self.logger.info(f"🗑️  Removed resource file: {resource_file.name} (pipeline '{pipeline_name}' no longer exists)")
            else:
                self.logger.debug(f"Resource file already removed: {resource_file}")
                
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to remove resource file {resource_file}: {e}", e)

    def _is_lhp_generated_file(self, resource_file: Path) -> bool:
        """
        Check if a resource file was generated by LHP by examining its content.
        
        Args:
            resource_file: Path to the resource file to check
            
        Returns:
            True if the file was generated by LHP, False otherwise
        """
        try:
            if not resource_file.exists() or not resource_file.is_file():
                return False
                
            # Read first few lines to check for LHP header
            with open(resource_file, 'r', encoding='utf-8') as f:
                first_lines = []
                for _ in range(5):  # Check first 5 lines
                    line = f.readline()
                    if not line:
                        break
                    first_lines.append(line.strip())
                
                # Look for LHP signature in the first few lines
                content = '\n'.join(first_lines)
                return "Generated by LakehousePlumber" in content
                
        except (OSError, PermissionError, UnicodeDecodeError) as e:
            self.logger.debug(f"Could not read file {resource_file} for LHP detection: {e}")
            return False

    def _get_all_resource_files_in_lhp_directory(self) -> List[Dict[str, Any]]:
        """
        Get ALL resource files in the resources/lhp directory, regardless of headers.
        
        Returns:
            List of dictionaries with 'path' and 'pipeline_name' keys
        """
        resource_files = []
        
        if not self.resources_dir.exists():
            return resource_files
            
        try:
            # Look for ALL pipeline resource files (.pipeline.yml and .yml)
            for resource_file in self.resources_dir.glob("*.yml"):
                # Extract pipeline name from filename (not header check)
                pipeline_name = self._extract_pipeline_name_from_filename(resource_file)
                if pipeline_name:
                    resource_files.append({
                        "path": resource_file,
                        "pipeline_name": pipeline_name
                    })
                    self.logger.debug(f"Found resource file: {resource_file.name} for pipeline: {pipeline_name}")
            
            return resource_files
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error scanning resources directory {self.resources_dir}: {e}")
            return []

    def _extract_pipeline_name_from_filename(self, resource_file: Path) -> Optional[str]:
        """
        Extract pipeline name from resource file name (regardless of header).
        
        Args:
            resource_file: Path to the resource file
            
        Returns:
            Pipeline name extracted from filename, or None if not a pipeline file
        """
        file_name = resource_file.name
        
        # Handle .pipeline.yml format
        if file_name.endswith(".pipeline.yml"):
            return file_name[:-13]  # Remove ".pipeline.yml"
        
        # Handle .yml format  
        elif file_name.endswith(".yml"):
            return file_name[:-4]   # Remove ".yml"
        
        return None

    def _backup_resource_file(self, resource_file: Path, pipeline_name: str):
        """
        Backup a resource file for a pipeline that no longer exists.
        
        Args:
            resource_file: Path to the resource file to backup
            pipeline_name: Name of the pipeline (for logging)
            
        Raises:
            BundleResourceError: If file backup fails
        """
        try:
            if resource_file.exists():
                # Create backup with .bkup extension
                backup_file = resource_file.with_suffix(resource_file.suffix + '.bkup')
                
                # If backup already exists, find a unique name
                counter = 1
                original_backup = backup_file
                while backup_file.exists():
                    backup_file = original_backup.with_suffix(f'.bkup.{counter}')
                    counter += 1
                
                # Move original to backup
                resource_file.rename(backup_file)
                self.logger.info(f"📦 Backed up resource file: {resource_file.name} → {backup_file.name} (pipeline '{pipeline_name}' no longer exists)")
            else:
                self.logger.debug(f"Resource file already removed: {resource_file}")
                
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to backup resource file {resource_file}: {e}", e)

    def _find_all_resource_files_for_pipeline(self, pipeline_name: str) -> List[Path]:
        """
        Find all resource files that might be related to a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline to search for
            
        Returns:
            List of Path objects for all related resource files
        """
        related_files = []
        
        if not self.resources_dir.exists():
            return related_files
        
        try:
            # Look for standard naming patterns
            patterns = [
                f"{pipeline_name}.pipeline.yml",
                f"{pipeline_name}.yml",
                f"{pipeline_name}_*.pipeline.yml",  # Custom suffixes
                f"{pipeline_name}_*.yml"
            ]
            
            for pattern in patterns:
                for file_path in self.resources_dir.glob(pattern):
                    if file_path.is_file() and file_path not in related_files:
                        related_files.append(file_path)
                        self.logger.debug(f"Found related file for pipeline '{pipeline_name}': {file_path.name}")
            
            return related_files
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error finding resource files for pipeline '{pipeline_name}': {e}")
            return []

    def _backup_single_file(self, resource_file: Path, pipeline_name: str):
        """
        Backup a single resource file.
        
        Args:
            resource_file: Path to the resource file to backup
            pipeline_name: Name of the pipeline (for logging)
        """
        try:
            if resource_file.exists():
                # Create backup with .bkup extension
                backup_file = resource_file.with_suffix(resource_file.suffix + '.bkup')
                
                # If backup already exists, find a unique name
                counter = 1
                original_backup = backup_file
                while backup_file.exists():
                    backup_file = original_backup.with_suffix(f'.bkup.{counter}')
                    counter += 1
                
                # Move original to backup
                resource_file.rename(backup_file)
                self.logger.info(f"📦 Backed up file: {resource_file.name} → {backup_file.name}")
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Failed to backup file {resource_file}: {e}") 