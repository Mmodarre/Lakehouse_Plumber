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
from .yaml_processor import YAMLProcessor

# TEMPORARY import - remove when Databricks fixes limitation
from .temporary_databricks_headers import add_databricks_notebook_headers


logger = logging.getLogger(__name__)


class BundleManager:
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
        if project_root is None:
            raise TypeError("project_root cannot be None")
            
        # Convert string to Path if necessary
        if isinstance(project_root, str):
            project_root = Path(project_root)
            
        self.project_root = project_root
        self.resources_dir = project_root / "resources"
        self.logger = logging.getLogger(__name__)
        self.yaml_processor = YAMLProcessor()

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
        
        # Step 2: Remove resource files for pipeline directories that no longer exist
        for resource_file_info in existing_resource_files:
            pipeline_name = resource_file_info["pipeline_name"]
            resource_file = resource_file_info["path"]
            
            if pipeline_name not in current_pipeline_names:
                try:
                    self._remove_resource_file(resource_file, pipeline_name)
                    removed_count += 1
                    self.logger.debug(f"Successfully removed resource file for deleted pipeline: {pipeline_name}")
                    
                except Exception as e:
                    self.logger.warning(f"Failed to remove resource file {resource_file}: {e}")
        
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
        Sync a single pipeline resource file. Returns True if updated.
        
        Args:
            pipeline_name: Name of the pipeline
            pipeline_dir: Directory containing pipeline Python files
            env: Environment name
            
        Returns:
            True if resource file was updated, False if no changes needed
        """
        resource_file = self._get_resource_file_path(pipeline_name)
        
        # Get actual generated files
        actual_notebook_paths = self._get_notebook_paths_for_pipeline(pipeline_dir)
        
        if resource_file.exists():
            # Update existing resource file
            return self._update_existing_resource_file(resource_file, actual_notebook_paths)
        else:
            # Create new resource file
            self._create_new_resource_file(pipeline_name, actual_notebook_paths, env)
            return True

    def _ensure_resources_directory(self):
        """Create resources directory if it doesn't exist."""
        try:
            self.resources_dir.mkdir(exist_ok=True)
            self.logger.debug(f"Ensured resources directory exists: {self.resources_dir}")
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

    def _get_notebook_paths_for_pipeline(self, pipeline_dir: Path) -> List[str]:
        """
        Get list of notebook paths for a pipeline directory.
        
        Args:
            pipeline_dir: Pipeline directory to scan
            
        Returns:
            List of relative notebook paths for bundle configuration
        """
        try:
            notebook_paths = []
            
            # Only look for .py files in the root of the pipeline directory
            for py_file in pipeline_dir.glob("*.py"):
                if py_file.is_file():
                    # Convert to relative path for bundle configuration
                    relative_path = f"../generated/{pipeline_dir.name}/{py_file.name}"
                    notebook_paths.append(relative_path)
                    self.logger.debug(f"Found notebook: {relative_path}")
            
            return sorted(notebook_paths)  # Sort for consistent ordering
            
        except (OSError, PermissionError) as e:
            self.logger.warning(f"Error scanning pipeline directory {pipeline_dir}: {e}")
            return []

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

    def _update_existing_resource_file(self, resource_file: Path, notebook_paths: List[str]) -> bool:
        """
        Update existing resource file. Returns True if changes were made.
        
        Args:
            resource_file: Path to the existing resource file
            notebook_paths: List of current notebook paths
            
        Returns:
            True if file was updated, False if no changes needed
            
        Raises:
            BundleResourceError: If YAML processing fails
        """
        try:
            # Extract current notebook paths from the resource file
            existing_notebook_paths = self.yaml_processor.extract_notebook_paths(resource_file)
            
            # Compare and determine what needs to be updated
            to_add, to_remove = self.yaml_processor.compare_notebook_paths(
                existing_notebook_paths, notebook_paths
            )
            
            # If no changes needed, return False
            if not to_add and not to_remove:
                self.logger.debug(f"No changes needed for resource file: {resource_file}")
                return False
            
            # Update the resource file
            self.yaml_processor.update_resource_file_libraries(resource_file, to_add, to_remove)
            
            self.logger.info(f"Updated resource file: {resource_file} (added: {len(to_add)}, removed: {len(to_remove)})")
            return True
            
        except YAMLParsingError as e:
            # Re-raise YAML errors as bundle resource errors with context
            raise BundleResourceError(f"Failed to update resource file {resource_file}: {e}", e)

    def _create_new_resource_file(self, pipeline_name: str, notebook_paths: List[str], env: str):
        """
        Create new resource file for a pipeline.
        
        Args:
            pipeline_name: Name of the pipeline
            notebook_paths: List of notebook paths to include
            env: Environment name for template processing
        """
        resource_file = self._get_resource_file_path(pipeline_name)
        
        # Generate basic resource file content
        content = self._generate_resource_file_content(pipeline_name, notebook_paths)
        
        try:
            resource_file.write_text(content, encoding='utf-8')
            self.logger.info(f"Created new resource file: {resource_file}")
            
        except (OSError, PermissionError) as e:
            raise BundleResourceError(f"Failed to create resource file {resource_file}: {e}", e)

    def _generate_resource_file_content(self, pipeline_name: str, notebook_paths: List[str]) -> str:
        """
        Generate content for a bundle resource file.
        
        Args:
            pipeline_name: Name of the pipeline
            notebook_paths: List of notebook paths to include
            
        Returns:
            YAML content for the resource file
        """
        # Generate libraries section
        libraries_section = ""
        for notebook_path in notebook_paths:
            libraries_section += f"        - notebook:\n            path: {notebook_path}\n"
        
        # Generate complete resource file content
        content = f"""# Generated by LakehousePlumber - Bundle Resource for {pipeline_name}
resources:
  pipelines:
    {pipeline_name}_pipeline:
      name: {pipeline_name}_pipeline
      catalog: main
      schema: lhp_${{bundle.target}}
      libraries:
{libraries_section}      configuration:
        bundle.sourcePath: ${{workspace.file_path}}/generated
"""
        
        return content

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
        Extract pipeline name from resource file name.
        
        Args:
            resource_file: Path to the resource file
            
        Returns:
            Pipeline name if it can be determined, None otherwise
        """
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