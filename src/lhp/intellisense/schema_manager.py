"""
Schema manager for accessing packaged JSON schema files.

This module provides utilities to access and manage JSON schema files
that are packaged with the Lakehouse Plumber distribution.
"""

import json
import sys
from pathlib import Path
from typing import Dict, List, Optional, Union

if sys.version_info >= (3, 9):
    from importlib import resources
else:
    import importlib_resources as resources

from lhp.utils.error_formatter import LHPError, ErrorCategory


class SchemaManagerError(LHPError):
    """Raised when schema management operations fail."""
    
    def __init__(self, message: str, suggestions: list = None):
        super().__init__(
            category=ErrorCategory.GENERAL,
            code_number="001",
            title="Schema Management Error",
            details=message,
            suggestions=suggestions or ["Check that all required schemas are available", "Verify file permissions"]
        )


class SchemaManager:
    """Manages access to packaged JSON schema files."""
    
    def __init__(self):
        self._schema_package = "lhp.schemas"
        self._schema_files = {
            "flowgroup": "flowgroup.schema.json",
            "template": "template.schema.json",
            "substitution": "substitution.schema.json",
            "project": "project.schema.json",
            "preset": "preset.schema.json"
        }
    
    def get_schema_content(self, schema_name: str) -> str:
        """
        Get the content of a schema file as a string.
        
        Args:
            schema_name: Name of the schema (e.g., 'flowgroup', 'template')
            
        Returns:
            Schema content as a string
            
        Raises:
            SchemaManagerError: If schema file cannot be found or read
        """
        try:
            if schema_name not in self._schema_files:
                available_schemas = ", ".join(self._schema_files.keys())
                raise SchemaManagerError(
                    f"Schema '{schema_name}' not found. Available schemas: {available_schemas}"
                )
            
            schema_filename = self._schema_files[schema_name]
            
            # Use importlib.resources to access packaged files
            try:
                # Python 3.9+ syntax
                schema_files = resources.files(self._schema_package)
                schema_file = schema_files / schema_filename
                return schema_file.read_text(encoding='utf-8')
            except AttributeError:
                # Fallback for older Python versions
                with resources.path(self._schema_package, schema_filename) as schema_path:
                    return schema_path.read_text(encoding='utf-8')
                    
        except Exception as e:
            raise SchemaManagerError(f"Failed to read schema '{schema_name}': {str(e)}")
    
    def get_schema_json(self, schema_name: str) -> Dict:
        """
        Get the parsed JSON content of a schema file.
        
        Args:
            schema_name: Name of the schema (e.g., 'flowgroup', 'template')
            
        Returns:
            Parsed JSON schema as a dictionary
            
        Raises:
            SchemaManagerError: If schema file cannot be found, read, or parsed
        """
        try:
            content = self.get_schema_content(schema_name)
            return json.loads(content)
        except json.JSONDecodeError as e:
            raise SchemaManagerError(f"Failed to parse schema '{schema_name}' as JSON: {str(e)}")
    
    def get_all_schemas(self) -> Dict[str, str]:
        """
        Get all available schemas as a dictionary.
        
        Returns:
            Dictionary mapping schema names to their content
            
        Raises:
            SchemaManagerError: If any schema file cannot be read
        """
        schemas = {}
        for schema_name in self._schema_files.keys():
            schemas[schema_name] = self.get_schema_content(schema_name)
        return schemas
    
    def get_schema_file_path(self, schema_name: str) -> Optional[Path]:
        """
        Get the file path to a schema file if it exists on disk.
        
        This method is primarily used for copying schemas to the user's
        VS Code settings directory.
        
        Args:
            schema_name: Name of the schema (e.g., 'flowgroup', 'template')
            
        Returns:
            Path to the schema file if it exists, None otherwise
            
        Raises:
            SchemaManagerError: If schema cannot be found
        """
        try:
            if schema_name not in self._schema_files:
                available_schemas = ", ".join(self._schema_files.keys())
                raise SchemaManagerError(
                    f"Schema '{schema_name}' not found. Available schemas: {available_schemas}"
                )
            
            schema_filename = self._schema_files[schema_name]
            
            try:
                # Python 3.9+ syntax
                schema_files = resources.files(self._schema_package)
                schema_file = schema_files / schema_filename
                
                # For newer versions, we need to use a context manager
                # to get the actual path
                with resources.as_file(schema_file) as path:
                    return path
            except AttributeError:
                # Fallback for older Python versions
                with resources.path(self._schema_package, schema_filename) as schema_path:
                    return schema_path
                    
        except Exception as e:
            raise SchemaManagerError(f"Failed to get path for schema '{schema_name}': {str(e)}")
    
    def copy_schema_to_directory(self, schema_name: str, target_directory: Path) -> Path:
        """
        Copy a schema file to a target directory.
        
        Args:
            schema_name: Name of the schema to copy
            target_directory: Directory to copy the schema to
            
        Returns:
            Path to the copied schema file
            
        Raises:
            SchemaManagerError: If schema cannot be copied
        """
        try:
            if not target_directory.exists():
                target_directory.mkdir(parents=True, exist_ok=True)
            
            schema_filename = self._schema_files[schema_name]
            target_path = target_directory / schema_filename
            
            # Get schema content and write to target
            content = self.get_schema_content(schema_name)
            target_path.write_text(content, encoding='utf-8')
            
            return target_path
            
        except Exception as e:
            raise SchemaManagerError(f"Failed to copy schema '{schema_name}': {str(e)}")
    
    def copy_all_schemas_to_directory(self, target_directory: Path) -> Dict[str, Path]:
        """
        Copy all schema files to a target directory.
        
        Args:
            target_directory: Directory to copy schemas to
            
        Returns:
            Dictionary mapping schema names to their copied file paths
            
        Raises:
            SchemaManagerError: If any schema cannot be copied
        """
        copied_schemas = {}
        
        for schema_name in self._schema_files.keys():
            copied_path = self.copy_schema_to_directory(schema_name, target_directory)
            copied_schemas[schema_name] = copied_path
        
        return copied_schemas
    
    def get_schema_mapping_for_vscode(self, schema_base_path: Union[str, Path]) -> Dict[str, str]:
        """
        Get schema mapping suitable for VS Code settings.
        
        Args:
            schema_base_path: Base path where schemas are stored
            
        Returns:
            Dictionary mapping schema file paths to glob patterns
        """
        if isinstance(schema_base_path, str):
            schema_base_path = Path(schema_base_path)
        
        # Define glob patterns for each schema type
        schema_patterns = {
            "flowgroup": ["pipelines/**/*.yaml", "pipelines/**/*.yml"],
            "template": ["templates/**/*.yaml", "templates/**/*.yml"],
            "substitution": ["substitutions/**/*.yaml", "substitutions/**/*.yml"],
            "project": ["lhp.yaml", "lhp.yml"],
            "preset": ["presets/**/*.yaml", "presets/**/*.yml"]
        }
        
        mapping = {}
        
        for schema_name, filename in self._schema_files.items():
            schema_path = schema_base_path / filename
            patterns = schema_patterns.get(schema_name, [])
            
            for pattern in patterns:
                mapping[str(schema_path)] = pattern
        
        return mapping
    
    def list_available_schemas(self) -> List[str]:
        """
        List all available schema names.
        
        Returns:
            List of available schema names
        """
        return list(self._schema_files.keys())
    
    def validate_schema_availability(self) -> Dict[str, bool]:
        """
        Validate that all expected schemas are available.
        
        Returns:
            Dictionary mapping schema names to their availability status
        """
        availability = {}
        
        for schema_name in self._schema_files.keys():
            try:
                self.get_schema_content(schema_name)
                availability[schema_name] = True
            except SchemaManagerError:
                availability[schema_name] = False
        
        return availability
    
    def get_schema_info(self) -> Dict[str, Dict[str, str]]:
        """
        Get information about all available schemas.
        
        Returns:
            Dictionary with schema information including filename and description
        """
        info = {}
        
        for schema_name, filename in self._schema_files.items():
            try:
                schema_json = self.get_schema_json(schema_name)
                description = schema_json.get("description", "No description available")
                title = schema_json.get("title", schema_name.capitalize())
                
                info[schema_name] = {
                    "filename": filename,
                    "title": title,
                    "description": description
                }
            except SchemaManagerError:
                info[schema_name] = {
                    "filename": filename,
                    "title": schema_name.capitalize(),
                    "description": "Schema not available"
                }
        
        return info 