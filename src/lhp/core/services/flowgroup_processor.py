"""Flowgroup processing service for LakehousePlumber."""

import logging
from typing import Dict, Any

from ...models.config import FlowGroup
from ...utils.substitution import EnhancedSubstitutionManager


class FlowgroupProcessor:
    """
    Service for processing flowgroups through templates, presets, and substitutions.
    
    Handles the complete flowgroup processing pipeline including template expansion,
    preset application, substitution processing, and validation.
    """
    
    def __init__(self, template_engine=None, preset_manager=None, 
                 config_validator=None, secret_validator=None):
        """
        Initialize flowgroup processor.
        
        Args:
            template_engine: Template engine for template expansion
            preset_manager: Preset manager for preset chain resolution
            config_validator: Config validator for flowgroup validation
            secret_validator: Secret validator for secret reference validation
        """
        self.template_engine = template_engine
        self.preset_manager = preset_manager
        self.config_validator = config_validator
        self.secret_validator = secret_validator
        self.logger = logging.getLogger(__name__)
    
    def process_flowgroup(self, flowgroup: FlowGroup, 
                         substitution_mgr: EnhancedSubstitutionManager) -> FlowGroup:
        """
        Process flowgroup: expand templates, apply presets, apply substitutions.
        
        Args:
            flowgroup: FlowGroup to process
            substitution_mgr: Substitution manager for the environment
            
        Returns:
            Processed flowgroup
        """
        # Step 1: Expand templates first
        if flowgroup.use_template:
            template_actions = self.template_engine.render_template(
                flowgroup.use_template, flowgroup.template_parameters or {}
            )
            # Add template actions to existing actions
            flowgroup.actions.extend(template_actions)
        
        # Step 2: Apply presets after template expansion
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
            flowgroup = self.apply_preset_config(flowgroup, preset_config)
        
        # Step 3: Apply substitutions
        flowgroup_dict = flowgroup.model_dump()
        substituted_dict = substitution_mgr.substitute_yaml(flowgroup_dict)
        processed_flowgroup = FlowGroup(**substituted_dict)
        
        # Step 4: Validate individual flowgroup
        errors = self.config_validator.validate_flowgroup(processed_flowgroup)
        if errors:
            raise ValueError(f"Flowgroup validation failed: {errors}")
        
        # Step 5: Validate secret references
        secret_errors = self.secret_validator.validate_secret_references(
            substitution_mgr.get_secret_references()
        )
        if secret_errors:
            raise ValueError(f"Secret validation failed: {secret_errors}")
        
        return processed_flowgroup
    
    def apply_preset_config(self, flowgroup: FlowGroup, preset_config: Dict[str, Any]) -> FlowGroup:
        """
        Apply preset configuration to flowgroup.
        
        Args:
            flowgroup: FlowGroup to apply presets to
            preset_config: Resolved preset configuration
            
        Returns:
            FlowGroup with preset defaults applied
        """
        flowgroup_dict = flowgroup.model_dump()
        
        # Apply preset defaults to actions
        for action in flowgroup_dict.get("actions", []):
            action_type = action.get("type")
            
            # Apply type-specific defaults
            if action_type == "load" and "load_actions" in preset_config:
                source_type = action.get("source", {}).get("type")
                if source_type and source_type in preset_config["load_actions"]:
                    # Merge preset defaults with action source
                    preset_defaults = preset_config["load_actions"][source_type]
                    action["source"] = self.deep_merge(
                        preset_defaults, action.get("source", {})
                    )
            
            elif action_type == "transform" and "transform_actions" in preset_config:
                transform_type = action.get("transform_type")
                if transform_type and transform_type in preset_config["transform_actions"]:
                    # Apply transform defaults
                    preset_defaults = preset_config["transform_actions"][transform_type]
                    for key, value in preset_defaults.items():
                        if key not in action:
                            action[key] = value
            
            elif action_type == "write" and "write_actions" in preset_config:
                # For new structure, check write_target
                if action.get("write_target") and isinstance(action["write_target"], dict):
                    target_type = action["write_target"].get("type")
                    if target_type and target_type in preset_config["write_actions"]:
                        # Merge preset defaults with write_target configuration
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["write_target"] = self.deep_merge(
                            preset_defaults, action.get("write_target", {})
                        )
                        
                        # Handle special cases like database_suffix
                        if ("database_suffix" in preset_defaults 
                            and "database" in action["write_target"]):
                            action["write_target"]["database"] += preset_defaults["database_suffix"]
                
                # Handle old structure for backward compatibility during migration
                elif action.get("source") and isinstance(action["source"], dict):
                    target_type = action["source"].get("type")
                    if target_type and target_type in preset_config["write_actions"]:
                        # Merge preset defaults with write configuration
                        preset_defaults = preset_config["write_actions"][target_type]
                        action["source"] = self.deep_merge(
                            preset_defaults, action.get("source", {})
                        )
                        
                        # Handle special cases like database_suffix
                        if ("database_suffix" in preset_defaults 
                            and "database" in action["source"]):
                            action["source"]["database"] += preset_defaults["database_suffix"]
        
        # Apply global preset settings
        if "defaults" in preset_config:
            for key, value in preset_config["defaults"].items():
                if key not in flowgroup_dict:
                    flowgroup_dict[key] = value
        
        return FlowGroup(**flowgroup_dict)
    
    def deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """
        Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Dictionary to override with
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in override.items():
            if (key in result 
                and isinstance(result[key], dict) 
                and isinstance(value, dict)):
                result[key] = self.deep_merge(result[key], value)
            else:
                result[key] = value
        return result
