"""
DatabricksYAMLManager for LHP Databricks Asset Bundle integration.

This module provides the DatabricksYAMLManager class that handles databricks.yml
file modifications with perfect structure preservation using ruamel.yaml.
This is the ONLY place in LHP that uses ruamel.yaml.
"""

import logging
from pathlib import Path
from typing import Dict, List, Any
from ruamel.yaml import YAML

from .exceptions import BundleResourceError, MissingDatabricksTargetError


logger = logging.getLogger(__name__)


class DatabricksYAMLManager:
    """
    Handles databricks.yml modifications with perfect structure preservation.
    
    IMPORTANT: This class uses ruamel.yaml exclusively for maximum preservation
    of user comments, formatting, and structure. All other YAML operations in
    LHP continue to use PyYAML.
    
    Features:
    - Preserves comments and formatting
    - Only modifies variables sections
    - Validates target existence before modifications
    - Atomic operations for multiple environment updates
    """
    
    def __init__(self, project_root: Path):
        """
        Initialize the DatabricksYAMLManager.
        
        Args:
            project_root: Path to the project root directory
        """
        self.project_root = project_root
        self.databricks_file = project_root / "databricks.yml"
        self.logger = logging.getLogger(__name__)
        
        # Configure ruamel.yaml for preservation
        self.yaml = YAML()
        self.yaml.preserve_quotes = True
        self.yaml.map_indent = 2
        self.yaml.sequence_indent = 4
        self.yaml.width = 4096
        self.yaml.allow_duplicate_keys = False
        
    def validate_targets_exist(self, environments: List[str]) -> None:
        """
        Validate that all required targets exist in databricks.yml.
        
        Args:
            environments: List of environment names that should have targets
            
        Raises:
            FileNotFoundError: If databricks.yml doesn't exist
            MissingDatabricksTargetError: If any targets are missing
        """
        if not self.databricks_file.exists():
            raise FileNotFoundError(f"databricks.yml not found: {self.databricks_file}")
        
        with open(self.databricks_file, 'r', encoding='utf-8') as f:
            data = self.yaml.load(f)
        
        if not data or 'targets' not in data:
            raise MissingDatabricksTargetError(
                "databricks.yml missing 'targets' section. "
                "Please ensure your databricks.yml file has a targets section defined."
            )
        
        targets = data['targets']
        missing = [env for env in environments if env not in targets]
        
        if missing:
            available_targets = list(targets.keys()) if targets else []
            raise MissingDatabricksTargetError(
                f"Missing targets in databricks.yml: {missing}. "
                f"Available targets: {available_targets}. "
                f"Please add the missing targets to your databricks.yml file."
            )
        
        self.logger.debug(f"All required targets exist: {environments}")
    
    def update_target_variables(self, target_environments: List[str], 
                               variables: Dict[str, str]) -> None:
        """
        Update variables section in specified targets with perfect preservation.
        
        Args:
            target_environments: List of target names (e.g., ['dev', 'prod'])
            variables: Variables to add/update in each target
                      (e.g., {'default_pipeline_catalog': 'my_catalog'})
            
        Raises:
            FileNotFoundError: If databricks.yml doesn't exist
            MissingDatabricksTargetError: If any targets are missing
            BundleResourceError: If file operations fail
        """
        if not self.databricks_file.exists():
            raise FileNotFoundError(f"databricks.yml not found: {self.databricks_file}")
        
        try:
            # Read with ruamel.yaml (preserves everything)
            with open(self.databricks_file, 'r', encoding='utf-8') as f:
                data = self.yaml.load(f)
            
            # Validate structure
            if not data or 'targets' not in data:
                raise MissingDatabricksTargetError("databricks.yml missing 'targets' section")
            
            # Validate all targets exist before any modifications
            self.validate_targets_exist(target_environments)
            
            # Update variables for each target
            for env in target_environments:
                target_config = data['targets'][env]
                
                # Create variables section if it doesn't exist
                if 'variables' not in target_config:
                    target_config['variables'] = {}
                
                # Update only the specified variables
                target_config['variables'].update(variables)
                
                self.logger.debug(f"Updated variables for target '{env}': {variables}")
            
            # Write back with perfect preservation
            with open(self.databricks_file, 'w', encoding='utf-8') as f:
                self.yaml.dump(data, f)
            
            self.logger.info(f"Updated databricks.yml variables for targets: {target_environments}")
            
        except MissingDatabricksTargetError:
            # Re-raise MissingDatabricksTargetError as-is (business logic error)
            raise
        except (OSError, PermissionError) as e:
            raise BundleResourceError(
                f"Failed to update databricks.yml: {e}. "
                f"Check file permissions and disk space."
            ) from e
        except Exception as e:
            raise BundleResourceError(f"Unexpected error updating databricks.yml: {e}") from e
    
    def get_target_variables(self, target_name: str) -> Dict[str, Any]:
        """
        Get current variables for a specific target.
        
        Args:
            target_name: Name of the target to get variables for
            
        Returns:
            Dictionary of current variables for the target
            
        Raises:
            FileNotFoundError: If databricks.yml doesn't exist
            MissingDatabricksTargetError: If target doesn't exist
        """
        if not self.databricks_file.exists():
            raise FileNotFoundError(f"databricks.yml not found: {self.databricks_file}")
        
        with open(self.databricks_file, 'r', encoding='utf-8') as f:
            data = self.yaml.load(f)
        
        if not data or 'targets' not in data:
            raise MissingDatabricksTargetError("databricks.yml missing 'targets' section")
        
        if target_name not in data['targets']:
            available_targets = list(data['targets'].keys())
            raise MissingDatabricksTargetError(
                f"Target '{target_name}' not found in databricks.yml. "
                f"Available targets: {available_targets}"
            )
        
        target_config = data['targets'][target_name]
        return target_config.get('variables', {})
    
    def bulk_update_all_targets(self, environments: List[str], 
                               environment_variables: Dict[str, Dict[str, str]]) -> None:
        """
        Update variables for multiple targets with environment-specific values.
        
        Args:
            environments: List of target names to update
            environment_variables: Dictionary mapping environment to its variables
                                  e.g., {'dev': {'default_pipeline_catalog': 'dev_cat'}}
            
        Raises:
            FileNotFoundError: If databricks.yml doesn't exist
            MissingDatabricksTargetError: If any targets are missing
            BundleResourceError: If file operations fail
        """
        if not self.databricks_file.exists():
            raise FileNotFoundError(f"databricks.yml not found: {self.databricks_file}")
        
        try:
            # Read and parse once
            with open(self.databricks_file, 'r', encoding='utf-8') as f:
                data = self.yaml.load(f)
            
            # Validate structure and targets
            if not data or 'targets' not in data:
                raise MissingDatabricksTargetError("databricks.yml missing 'targets' section")
            
            self.validate_targets_exist(environments)
            
            # Update all targets
            for env in environments:
                target_config = data['targets'][env]
                if 'variables' not in target_config:
                    target_config['variables'] = {}
                
                # Update with environment-specific variables
                if env in environment_variables:
                    target_config['variables'].update(environment_variables[env])
                    self.logger.debug(f"Updated target '{env}' with variables: {environment_variables[env]}")
            
            # Write once
            with open(self.databricks_file, 'w', encoding='utf-8') as f:
                self.yaml.dump(data, f)
            
            self.logger.info(f"Bulk updated variables in {len(environments)} targets: {environments}")
            
        except MissingDatabricksTargetError:
            raise
        except (OSError, PermissionError) as e:
            raise BundleResourceError(
                f"Failed to bulk update databricks.yml: {e}. "
                f"Check file permissions and disk space."
            ) from e
        except Exception as e:
            raise BundleResourceError(f"Unexpected error in bulk update: {e}") from e
