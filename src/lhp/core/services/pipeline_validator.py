"""Pipeline validation service for LakehousePlumber."""

import logging
from typing import List, Tuple
from pathlib import Path

from ...models.config import FlowGroup
from ...utils.substitution import EnhancedSubstitutionManager


class PipelineValidator:
    """
    Service for validating pipeline configurations and business rules.
    
    Coordinates existing validation infrastructure and provides pipeline-level
    validation methods for the orchestration layer.
    """
    
    def __init__(self, project_root: Path, config_validator=None, 
                 secret_validator=None):
        """
        Initialize pipeline validator.
        
        Args:
            project_root: Root directory of the LakehousePlumber project  
            config_validator: Config validator for flowgroup validation
            secret_validator: Secret validator for secret reference validation
        """
        self.project_root = project_root
        self.config_validator = config_validator
        self.secret_validator = secret_validator
        self.logger = logging.getLogger(__name__)
    
    def validate_pipeline_by_field(self, pipeline_field: str, env: str, 
                                  discoverer=None, processor=None) -> Tuple[List[str], List[str]]:
        """
        Validate pipeline configuration using pipeline field without generating code.
        
        Args:
            pipeline_field: The pipeline field value to validate
            env: Environment to validate for
            discoverer: FlowgroupDiscoverer service for finding flowgroups
            processor: FlowgroupProcessor service for processing flowgroups
            
        Returns:
            Tuple of (errors, warnings)
        """
        errors = []
        warnings = []
        
        try:
            # Discover flowgroups by pipeline field
            if discoverer:
                flowgroups = discoverer.discover_flowgroups_by_pipeline_field(pipeline_field)
            else:
                # Fallback: use direct discovery (for backward compatibility during transition)
                flowgroups = []
                self.logger.warning("No discoverer provided, cannot validate pipeline")
                return ["No flowgroup discoverer available"], warnings
            
            if not flowgroups:
                errors.append(f"No flowgroups found for pipeline field: {pipeline_field}")
                return errors, warnings
            
            # Load substitution manager
            substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
            substitution_mgr = EnhancedSubstitutionManager(substitution_file, env)
            
            # Validate each flowgroup
            for flowgroup in flowgroups:
                try:
                    if processor:
                        # Use processor to validate flowgroup (includes templates, presets, substitutions)
                        processor.process_flowgroup(flowgroup, substitution_mgr)
                    else:
                        # Fallback: basic validation without processing
                        self._validate_flowgroup_basic(flowgroup)
                    # Note: Validation happens in process_flowgroup method
                    
                except Exception as e:
                    errors.append(f"Flowgroup '{flowgroup.flowgroup}': {e}")
        
        except Exception as e:
            errors.append(f"Pipeline validation failed: {e}")
        
        return errors, warnings
    
    def validate_pipeline_by_directory(self, pipeline_name: str, env: str,
                                      discoverer=None, processor=None) -> Tuple[List[str], List[str]]:
        """
        Validate pipeline configuration by directory name without generating code.
        
        Args:
            pipeline_name: Name of the pipeline directory to validate
            env: Environment to validate for  
            discoverer: FlowgroupDiscoverer service for finding flowgroups
            processor: FlowgroupProcessor service for processing flowgroups
            
        Returns:
            Tuple of (errors, warnings)
        """
        errors = []
        warnings = []
        
        try:
            pipeline_dir = self.project_root / "pipelines" / pipeline_name
            
            # Discover flowgroups in pipeline directory
            if discoverer:
                flowgroups = discoverer.discover_flowgroups(pipeline_dir)
            else:
                # Fallback for backward compatibility
                errors.append("No flowgroup discoverer available")
                return errors, warnings
            
            # Load substitution manager
            substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
            substitution_mgr = EnhancedSubstitutionManager(substitution_file, env)
            
            # Validate each flowgroup
            for flowgroup in flowgroups:
                try:
                    if processor:
                        # Use processor to validate flowgroup
                        processor.process_flowgroup(flowgroup, substitution_mgr)
                    else:
                        # Fallback: basic validation
                        self._validate_flowgroup_basic(flowgroup)
                    # Validation happens in process_flowgroup
                    
                except Exception as e:
                    errors.append(f"Flowgroup '{flowgroup.flowgroup}': {e}")
        
        except Exception as e:
            errors.append(f"Pipeline validation failed: {e}")
        
        return errors, warnings
    
    def _validate_flowgroup_basic(self, flowgroup: FlowGroup) -> None:
        """
        Basic flowgroup validation without full processing.
        
        Args:
            flowgroup: FlowGroup to validate
            
        Raises:
            ValueError: If validation fails
        """
        if self.config_validator:
            errors = self.config_validator.validate_flowgroup(flowgroup)
            if errors:
                raise ValueError(f"Flowgroup validation failed: {errors}")
        else:
            # Minimal validation if no config validator available
            if not flowgroup.actions:
                raise ValueError("Flowgroup must have at least one action")
    
    def validate_action_dependencies(self, actions: List) -> List[str]:
        """
        Validate action dependencies and relationships.
        
        Args:
            actions: List of actions to validate
            
        Returns:
            List of validation errors
        """
        if self.config_validator and hasattr(self.config_validator, 'dependency_resolver'):
            return self.config_validator.dependency_resolver.validate_relationships(actions)
        return []
    
    def validate_table_creation_rules(self, actions: List) -> List[str]:
        """
        Validate table creation rules for write actions.
        
        Args:
            actions: List of actions to validate
            
        Returns:
            List of validation errors
        """
        if self.config_validator:
            try:
                # Use existing validator logic
                return self.config_validator._validate_table_creation_rules(actions)
            except AttributeError:
                # Method might not exist or have different name
                self.logger.warning("Table creation validation method not available")
        return []
