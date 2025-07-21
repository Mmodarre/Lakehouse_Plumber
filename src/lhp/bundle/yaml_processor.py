"""
YAML processing functionality for LHP bundle resource files.

This module handles parsing, validation, and incremental updates of 
Databricks Asset Bundle resource YAML files.
"""

import logging
import yaml
from pathlib import Path
from typing import List, Set, Dict, Any, Union
import copy

from .exceptions import BundleResourceError, YAMLParsingError


logger = logging.getLogger(__name__)


class YAMLProcessor:
    """
    Handles YAML parsing and manipulation for bundle resource files.
    
    This class provides methods for extracting notebook paths from resource files
    and updating the libraries section while preserving user customizations.
    """
    
    def __init__(self):
        """Initialize the YAML processor."""
        self.logger = logging.getLogger(__name__)

    def extract_notebook_paths(self, yaml_file: Path) -> List[str]:
        """
        Extract notebook paths from a bundle resource YAML file.
        
        Args:
            yaml_file: Path to the resource YAML file
            
        Returns:
            List of notebook paths found in the file
            
        Raises:
            YAMLParsingError: If file parsing fails or structure is invalid
        """
        try:
            # Read and parse YAML file
            content = yaml_file.read_text(encoding='utf-8')
            data = yaml.safe_load(content)
            
            # Validate structure
            self._validate_yaml_structure(data, str(yaml_file))
            
            # Extract notebook paths
            return self._extract_notebook_paths_from_data(data)
            
        except FileNotFoundError:
            raise YAMLParsingError("File not found", file_path=str(yaml_file))
        except PermissionError as e:
            raise YAMLParsingError(f"Permission denied: {e}", file_path=str(yaml_file), original_error=e)
        except UnicodeDecodeError as e:
            raise YAMLParsingError(f"File encoding error: {e}", file_path=str(yaml_file), original_error=e)
        except yaml.YAMLError as e:
            # Extract line number if available
            line_number = getattr(e, 'problem_mark', None)
            line_num = line_number.line + 1 if line_number else None
            context = getattr(e, 'problem', None)
            
            raise YAMLParsingError(
                f"Invalid YAML syntax: {e}",
                file_path=str(yaml_file),
                line_number=line_num,
                context=context,
                original_error=e
            )
        except YAMLParsingError:
            # Re-raise our custom errors as-is
            raise
        except Exception as e:
            raise YAMLParsingError(f"Unexpected error: {e}", file_path=str(yaml_file), original_error=e)

    def update_resource_file_libraries(self, yaml_file: Path, to_add: List[str], to_remove: List[str]):
        """
        Update the libraries section of a resource file.
        
        Args:
            yaml_file: Path to the resource YAML file
            to_add: List of notebook paths to add
            to_remove: List of notebook paths to remove
            
        Raises:
            YAMLParsingError: If file operations fail
        """
        # If no changes needed, don't modify the file
        if not to_add and not to_remove:
            return
            
        try:
            # Read and parse current content
            content = yaml_file.read_text(encoding='utf-8')
            data = yaml.safe_load(content)
            
            # Validate structure
            self._validate_yaml_structure(data, str(yaml_file))
            
            # Update the libraries section
            self._update_libraries_section(data, to_add, to_remove)
            
            # Write back to file
            updated_content = yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True)
            yaml_file.write_text(updated_content, encoding='utf-8')
            
            self.logger.debug(f"Updated resource file: {yaml_file}")
            
        except FileNotFoundError:
            raise YAMLParsingError("File not found", file_path=str(yaml_file))
        except PermissionError as e:
            raise YAMLParsingError(f"Permission denied: {e}", file_path=str(yaml_file), original_error=e)
        except OSError as e:
            # Handle disk full, read-only filesystem, etc.
            raise YAMLParsingError(f"File system error: {e}", file_path=str(yaml_file), original_error=e)
        except yaml.YAMLError as e:
            line_number = getattr(e, 'problem_mark', None)
            line_num = line_number.line + 1 if line_number else None
            context = getattr(e, 'problem', None)
            
            raise YAMLParsingError(
                f"Invalid YAML syntax: {e}",
                file_path=str(yaml_file),
                line_number=line_num,
                context=context,
                original_error=e
            )
        except YAMLParsingError:
            # Re-raise our custom errors as-is
            raise
        except Exception as e:
            raise YAMLParsingError(f"Failed to update file: {e}", file_path=str(yaml_file), original_error=e)

    def _validate_yaml_structure(self, data: Any, file_path: str):
        """
        Validate that YAML data has the expected structure.
        
        Args:
            data: Parsed YAML data
            file_path: File path for error reporting
            
        Raises:
            YAMLParsingError: If structure is invalid
        """
        if not isinstance(data, dict):
            raise YAMLParsingError("Root element must be a dictionary", file_path=file_path)
        
        if 'resources' not in data:
            raise YAMLParsingError("Missing 'resources' key", file_path=file_path)
        
        resources = data['resources']
        if not isinstance(resources, dict):
            raise YAMLParsingError("'resources' must be a dictionary", file_path=file_path)
        
        if 'pipelines' not in resources:
            raise YAMLParsingError("Missing 'resources.pipelines' key", file_path=file_path)
        
        pipelines = resources['pipelines']
        if not isinstance(pipelines, dict):
            raise YAMLParsingError(file_path, "'pipelines' must be a dictionary")
        
        if len(pipelines) == 0:
            raise YAMLParsingError(file_path, "No pipelines defined")

    def _extract_notebook_paths_from_data(self, data: Dict[str, Any]) -> List[str]:
        """
        Extract notebook paths from parsed YAML data.
        
        Args:
            data: Parsed YAML data
            
        Returns:
            List of notebook paths
        """
        notebook_paths = []
        
        # Get the first pipeline (we only support one pipeline per resource file)
        pipelines = data['resources']['pipelines']
        pipeline_key = next(iter(pipelines.keys()))
        pipeline_config = pipelines[pipeline_key]
        
        if not isinstance(pipeline_config, dict):
            return notebook_paths
        
        # Get libraries section
        libraries = pipeline_config.get('libraries', [])
        if not isinstance(libraries, list):
            return notebook_paths
        
        # Extract notebook paths
        for library in libraries:
            if isinstance(library, dict) and 'notebook' in library:
                notebook = library['notebook']
                if isinstance(notebook, dict) and 'path' in notebook:
                    notebook_paths.append(notebook['path'])
        
        return sorted(notebook_paths)  # Sort for consistent ordering

    def _update_libraries_section(self, data: Dict[str, Any], to_add: List[str], to_remove: List[str]):
        """
        Update the libraries section in YAML data.
        
        Args:
            data: Parsed YAML data (modified in place)
            to_add: List of notebook paths to add
            to_remove: List of notebook paths to remove
        """
        # Get the first pipeline
        pipelines = data['resources']['pipelines']
        pipeline_key = next(iter(pipelines.keys()))
        pipeline_config = pipelines[pipeline_key]
        
        # Ensure libraries section exists
        if 'libraries' not in pipeline_config:
            pipeline_config['libraries'] = []
        
        libraries = pipeline_config['libraries']
        
        # Remove specified paths
        if to_remove:
            libraries[:] = [
                lib for lib in libraries
                if not (isinstance(lib, dict) and 
                       'notebook' in lib and 
                       isinstance(lib['notebook'], dict) and
                       lib['notebook'].get('path') in to_remove)
            ]
        
        # Add new paths
        for path in to_add:
            notebook_entry = {
                'notebook': {
                    'path': path
                }
            }
            libraries.append(notebook_entry)
        
        self.logger.debug(f"Updated libraries: added {len(to_add)}, removed {len(to_remove)}")

    def compare_notebook_paths(self, existing_paths: List[str], actual_paths: List[str]) -> tuple:
        """
        Compare existing and actual notebook paths to determine changes needed.
        
        Args:
            existing_paths: Currently listed notebook paths
            actual_paths: Actual notebook paths that should be listed
            
        Returns:
            Tuple of (paths_to_add, paths_to_remove)
        """
        existing_set = set(existing_paths)
        actual_set = set(actual_paths)
        
        to_add = actual_set - existing_set
        to_remove = existing_set - actual_set
        
        return sorted(list(to_add)), sorted(list(to_remove)) 