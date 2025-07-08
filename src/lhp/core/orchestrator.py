"""Main orchestration for LakehousePlumber pipeline generation."""

import logging
from pathlib import Path
from typing import Dict, List, Any, Set, Optional
from collections import defaultdict
from datetime import datetime

from ..parsers.yaml_parser import YAMLParser
from ..presets.preset_manager import PresetManager
from ..core.template_engine import TemplateEngine
from ..utils.substitution import EnhancedSubstitutionManager
from ..core.action_registry import ActionRegistry
from ..core.validator import ConfigValidator
from ..core.secret_validator import SecretValidator
from ..core.dependency_resolver import DependencyResolver
from ..utils.formatter import format_code
from ..models.config import FlowGroup, Action, ActionType, LoadSourceType, TransformType, WriteTargetType
from ..utils.error_formatter import LHPError
from ..utils.smart_file_writer import SmartFileWriter


class ActionOrchestrator:
    """Main orchestration for pipeline generation."""
    
    def __init__(self, project_root: Path):
        """Initialize orchestrator with project root.
        
        Args:
            project_root: Root directory of the LakehousePlumber project
        """
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
        
        # Step 4.5.1: Initialize all components
        self.yaml_parser = YAMLParser()
        self.preset_manager = PresetManager(project_root / "presets")
        self.template_engine = TemplateEngine(project_root / "templates")
        self.action_registry = ActionRegistry()
        self.config_validator = ConfigValidator()
        self.secret_validator = SecretValidator()
        self.dependency_resolver = DependencyResolver()
        
        self.logger.info(f"Initialized ActionOrchestrator with project root: {project_root}")
    
    def generate_pipeline(self, pipeline_name: str, env: str, output_dir: Path = None, state_manager=None, 
                         force_all: bool = False, specific_flowgroups: List[str] = None) -> Dict[str, str]:
        """Generate complete pipeline from YAML configs.
        
        Args:
            pipeline_name: Name of the pipeline to generate
            env: Environment to generate for (e.g., 'dev', 'prod')
            output_dir: Optional output directory for generated files
            state_manager: Optional state manager for tracking generated files
            force_all: If True, generate all flowgroups regardless of changes
            specific_flowgroups: If provided, only generate these specific flowgroups
            
        Returns:
            Dictionary mapping filename to generated code content
        """
        self.logger.info(f"Generating pipeline '{pipeline_name}' for environment '{env}'")
        
        # 1. Parse pipeline configuration
        pipeline_dir = self.project_root / "pipelines" / pipeline_name
        if not pipeline_dir.exists():
            raise ValueError(f"Pipeline directory not found: {pipeline_dir}")
        
        # Step 4.5.2: Implement flowgroup discovery
        all_flowgroups = self._discover_flowgroups(pipeline_dir)
        if not all_flowgroups:
            raise ValueError(f"No flowgroups found in pipeline: {pipeline_name}")
        
        # Smart generation: filter flowgroups based on changes
        flowgroups = all_flowgroups
        if not force_all and state_manager and specific_flowgroups is None:
            # Determine which flowgroups need generation
            generation_info = state_manager.get_files_needing_generation(env, pipeline_name)
            
            # Get flowgroups for new YAML files
            new_flowgroups = set()
            for yaml_path in generation_info['new']:
                try:
                    fg = self.yaml_parser.parse_flowgroup(yaml_path)
                    new_flowgroups.add(fg.flowgroup)
                except Exception as e:
                    self.logger.warning(f"Could not parse new flowgroup {yaml_path}: {e}")
            
            # Get flowgroups for stale files
            stale_flowgroups = {fs.flowgroup for fs in generation_info['stale']}
            
            # Combine new and stale flowgroups
            flowgroups_to_generate = new_flowgroups | stale_flowgroups
            
            if flowgroups_to_generate:
                # Filter to only include flowgroups that need generation
                flowgroups = [fg for fg in all_flowgroups if fg.flowgroup in flowgroups_to_generate]
                self.logger.info(f"Smart generation: processing {len(flowgroups)}/{len(all_flowgroups)} flowgroups")
            else:
                # Nothing to generate
                flowgroups = []
                self.logger.info("Smart generation: no flowgroups need processing")
        
        elif specific_flowgroups:
            # Filter to only specified flowgroups
            flowgroups = [fg for fg in all_flowgroups if fg.flowgroup in specific_flowgroups]
            self.logger.info(f"Generating specific flowgroups: {len(flowgroups)}/{len(all_flowgroups)}")
        
        # 2. Load substitution manager for environment
        substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env)
        
        # 3. Initialize smart file writer
        smart_writer = SmartFileWriter()
        
        # 4. Process all flowgroups first
        processed_flowgroups = []
        for flowgroup in flowgroups:
            self.logger.info(f"Processing flowgroup: {flowgroup.flowgroup}")
            
            try:
                # Step 4.5.3: Process flowgroup
                processed_flowgroup = self._process_flowgroup(flowgroup, substitution_mgr)
                processed_flowgroups.append(processed_flowgroup)
                
            except Exception as e:
                self.logger.error(f"Error processing flowgroup {flowgroup.flowgroup}: {e}")
                raise
        
        # 5. Validate table creation rules across entire pipeline
        table_creation_errors = self.config_validator.validate_table_creation_rules(processed_flowgroups)
        if table_creation_errors:
            raise ValueError(f"Table creation validation failed:\n" + "\n".join(f"  - {error}" for error in table_creation_errors))
        
        # 6. Generate code for each processed flowgroup
        generated_files = {}
        
        for processed_flowgroup in processed_flowgroups:
            self.logger.info(f"Generating code for flowgroup: {processed_flowgroup.flowgroup}")
            
            try:
                # Step 4.5.6: Generate code
                code = self._generate_flowgroup_code(processed_flowgroup, substitution_mgr)
                
                # Format code
                formatted_code = format_code(code)
                
                # Store result
                filename = f"{processed_flowgroup.flowgroup}.py"
                generated_files[filename] = formatted_code
                
                # Step 4.5.7: Write to output directory if specified
                if output_dir:
                    output_file = output_dir / filename
                    file_was_written = smart_writer.write_if_changed(output_file, formatted_code)
                    
                    # Track the generated file in state manager (regardless of whether it was written)
                    if state_manager:
                        # Find the source YAML file for this flowgroup
                        source_yaml = self._find_source_yaml(pipeline_dir, processed_flowgroup.flowgroup)
                        if source_yaml:
                            state_manager.track_generated_file(
                                generated_path=output_file,
                                source_yaml=source_yaml,
                                environment=env,
                                pipeline=pipeline_name,
                                flowgroup=processed_flowgroup.flowgroup
                            )
                
            except Exception as e:
                self.logger.error(f"Error generating code for flowgroup {processed_flowgroup.flowgroup}: {e}")
                raise
        
        # Save state after all files are generated
        if state_manager:
            state_manager.save()
        
        # Log smart file writer statistics
        if output_dir:
            files_written, files_skipped = smart_writer.get_stats()
            self.logger.info(f"Generation complete: {files_written} files written, {files_skipped} files skipped (no changes)")
        
        return generated_files
    
    def _discover_flowgroups(self, pipeline_dir: Path) -> List[FlowGroup]:
        """Step 4.5.2: Discover all flowgroups in pipeline directory.
        
        Args:
            pipeline_dir: Directory containing flowgroup YAML files
            
        Returns:
            List of discovered flowgroups
        """
        flowgroups = []
        
        for yaml_file in pipeline_dir.rglob("*.yaml"):
            try:
                flowgroup = self.yaml_parser.parse_flowgroup(yaml_file)
                flowgroups.append(flowgroup)
                self.logger.debug(f"Discovered flowgroup: {flowgroup.flowgroup} in {yaml_file}")
            except Exception as e:
                self.logger.warning(f"Could not parse flowgroup {yaml_file}: {e}")
        
        return flowgroups
    
    def _find_source_yaml(self, pipeline_dir: Path, flowgroup_name: str) -> Optional[Path]:
        """Find the source YAML file for a given flowgroup name.
        
        Args:
            pipeline_dir: Directory containing flowgroup YAML files
            flowgroup_name: Name of the flowgroup to find
            
        Returns:
            Path to the source YAML file, or None if not found
        """
        for yaml_file in pipeline_dir.rglob("*.yaml"):
            try:
                flowgroup = self.yaml_parser.parse_flowgroup(yaml_file)
                if flowgroup.flowgroup == flowgroup_name:
                    return yaml_file
            except Exception as e:
                self.logger.debug(f"Could not parse flowgroup {yaml_file}: {e}")
        
        return None
    
    def _process_flowgroup(self, flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager) -> FlowGroup:
        """Step 4.5.3: Process flowgroup: apply presets, expand templates, apply substitutions.
        
        Args:
            flowgroup: FlowGroup to process
            substitution_mgr: Substitution manager for the environment
            
        Returns:
            Processed flowgroup
        """
        # Step 4.5.4: Apply presets
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
            flowgroup = self._apply_preset_config(flowgroup, preset_config)
        
        # Step 4.5.5: Expand templates
        if flowgroup.use_template:
            template_actions = self.template_engine.render_template(
                flowgroup.use_template, 
                flowgroup.template_parameters or {}
            )
            # Add template actions to existing actions
            flowgroup.actions.extend(template_actions)
        
        # Apply substitutions
        flowgroup_dict = flowgroup.model_dump()
        substituted_dict = substitution_mgr.substitute_yaml(flowgroup_dict)
        processed_flowgroup = FlowGroup(**substituted_dict)
        
        # Validate individual flowgroup
        errors = self.config_validator.validate_flowgroup(processed_flowgroup)
        if errors:
            raise ValueError(f"Flowgroup validation failed: {errors}")
        
        # Validate secret references
        secret_errors = self.secret_validator.validate_secret_references(
            substitution_mgr.get_secret_references()
        )
        if secret_errors:
            raise ValueError(f"Secret validation failed: {secret_errors}")
        
        return processed_flowgroup
    
    def _apply_preset_config(self, flowgroup: FlowGroup, preset_config: Dict[str, Any]) -> FlowGroup:
        """Apply preset configuration to flowgroup.
        
        Args:
            flowgroup: FlowGroup to apply presets to
            preset_config: Resolved preset configuration
            
        Returns:
            FlowGroup with preset defaults applied
        """
        flowgroup_dict = flowgroup.model_dump()
        
        # Apply preset defaults to actions
        for action in flowgroup_dict.get('actions', []):
            action_type = action.get('type')
            
            # Apply type-specific defaults
            if action_type == 'load' and 'load_actions' in preset_config:
                source_type = action.get('source', {}).get('type')
                if source_type and source_type in preset_config['load_actions']:
                    # Merge preset defaults with action source
                    preset_defaults = preset_config['load_actions'][source_type]
                    action['source'] = self._deep_merge(preset_defaults, action.get('source', {}))
            
            elif action_type == 'transform' and 'transform_actions' in preset_config:
                transform_type = action.get('transform_type')
                if transform_type and transform_type in preset_config['transform_actions']:
                    # Apply transform defaults
                    preset_defaults = preset_config['transform_actions'][transform_type]
                    for key, value in preset_defaults.items():
                        if key not in action:
                            action[key] = value
            
            elif action_type == 'write' and 'write_actions' in preset_config:
                # For new structure, check write_target
                if action.get('write_target') and isinstance(action['write_target'], dict):
                    target_type = action['write_target'].get('type')
                    if target_type and target_type in preset_config['write_actions']:
                        # Merge preset defaults with write_target configuration
                        preset_defaults = preset_config['write_actions'][target_type]
                        action['write_target'] = self._deep_merge(preset_defaults, action.get('write_target', {}))
                        
                        # Handle special cases like database_suffix
                        if 'database_suffix' in preset_defaults and 'database' in action['write_target']:
                            action['write_target']['database'] += preset_defaults['database_suffix']
                # Handle old structure for backward compatibility during migration
                elif action.get('source') and isinstance(action['source'], dict):
                    target_type = action['source'].get('type')
                    if target_type and target_type in preset_config['write_actions']:
                        # Merge preset defaults with write configuration
                        preset_defaults = preset_config['write_actions'][target_type]
                        action['source'] = self._deep_merge(preset_defaults, action.get('source', {}))
                        
                        # Handle special cases like database_suffix
                        if 'database_suffix' in preset_defaults and 'database' in action['source']:
                            action['source']['database'] += preset_defaults['database_suffix']
        
        # Apply global preset settings
        if 'defaults' in preset_config:
            for key, value in preset_config['defaults'].items():
                if key not in flowgroup_dict:
                    flowgroup_dict[key] = value
        
        return FlowGroup(**flowgroup_dict)
    
    def _deep_merge(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Deep merge two dictionaries.
        
        Args:
            base: Base dictionary
            override: Dictionary to override with
            
        Returns:
            Merged dictionary
        """
        result = base.copy()
        for key, value in override.items():
            if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                result[key] = self._deep_merge(result[key], value)
            else:
                result[key] = value
        return result
    
    def _generate_flowgroup_code(self, flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager) -> str:
        """Generate code for a flowgroup."""
        
        # 1. Resolve action dependencies
        ordered_actions = self.dependency_resolver.resolve_dependencies(flowgroup.actions)
        
        # 2. Get preset configuration if any
        preset_config = {}
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
        
        # 3. Group actions by type while preserving order
        action_groups = defaultdict(list)
        for action in ordered_actions:
            action_groups[action.type].append(action)
        
        # 4. Generate code for each action group
        generated_sections = []
        all_imports = set()
        
        # Add base imports
        all_imports.add("import dlt")
        
        # Define section headers
        section_headers = {
            ActionType.LOAD: "SOURCE VIEWS",
            ActionType.TRANSFORM: "TRANSFORMATION VIEWS", 
            ActionType.WRITE: "TARGET TABLES"
        }
        
        # Process each action type in order
        for action_type in [ActionType.LOAD, ActionType.TRANSFORM, ActionType.WRITE]:
            if action_type in action_groups:
                # Add section header
                header_text = section_headers.get(action_type, str(action_type).upper())
                section_header = f"""
# {"=" * 76}
# {header_text}
# {"=" * 76}"""
                generated_sections.append(section_header)
                
                # Special handling for write actions - group by target table
                if action_type == ActionType.WRITE:
                    write_actions = action_groups[action_type]
                    grouped_actions = self._group_write_actions_by_target(write_actions)
                    
                    for target_table, actions in grouped_actions.items():
                        try:
                            # Use the first action to determine sub-type and get generator
                            first_action = actions[0]
                            sub_type = self._determine_action_subtype(first_action)
                            generator = self.action_registry.get_generator(first_action.type, sub_type)
                            
                            # Create a combined action with multiple source views
                            combined_action = self._create_combined_write_action(actions, target_table)
                            
                            # Generate code
                            context = {
                                "flowgroup": flowgroup,
                                "substitution_manager": substitution_mgr,
                                "spec_dir": self.project_root,
                                "preset_config": preset_config
                            }
                            
                            action_code = generator.generate(combined_action, context)
                            generated_sections.append(action_code)
                            
                            # Collect imports
                            all_imports.update(generator.imports)
                            
                        except LHPError:
                            # Re-raise LHPError as-is (it's already well-formatted)
                            raise
                        except Exception as e:
                            action_names = [a.name for a in actions]
                            raise ValueError(f"Error generating code for write actions {action_names}: {e}")
                else:
                    # Generate code for each action in this group (non-write actions)
                    for action in action_groups[action_type]:
                        try:
                            # Determine action sub-type
                            sub_type = self._determine_action_subtype(action)
                            
                            # Get generator
                            generator = self.action_registry.get_generator(action.type, sub_type)
                            
                            # Generate code
                            context = {
                                "flowgroup": flowgroup,
                                "substitution_manager": substitution_mgr,
                                "spec_dir": self.project_root,
                                "preset_config": preset_config  # Add preset config to context
                            }
                            
                            action_code = generator.generate(action, context)
                            generated_sections.append(action_code)
                            
                            # Collect imports
                            all_imports.update(generator.imports)
                            
                        except LHPError:
                            # Re-raise LHPError as-is (it's already well-formatted)
                            raise
                        except Exception as e:
                            raise ValueError(f"Error generating code for action '{action.name}': {e}")
        
        # 5. Apply secret substitutions to generated code
        complete_code = "\n\n".join(generated_sections)
        complete_code = substitution_mgr.replace_secret_placeholders(complete_code)
        
        # 6. Build final file
        imports_section = "\n".join(sorted(all_imports))
        
        # Add pipeline configuration section
        pipeline_config = f"""
# Pipeline Configuration
PIPELINE_ID = "{flowgroup.flowgroup}"
PIPELINE_GROUP = "{flowgroup.pipeline}"
"""
        
        header = f"""# Generated by LakehousePlumber
# Pipeline: {flowgroup.pipeline}
# FlowGroup: {flowgroup.flowgroup}
# Generated: {datetime.now().isoformat()}

{imports_section}
{pipeline_config}"""
        
        return header + complete_code
    
    def _determine_action_subtype(self, action: Action) -> str:
        """Determine the sub-type of an action for generator selection.
        
        Args:
            action: Action to determine sub-type for
            
        Returns:
            Sub-type string for generator selection
        """
        if action.type == ActionType.LOAD:
            if isinstance(action.source, dict):
                return action.source.get("type", "sql")
            else:
                return "sql"  # String source is SQL
                
        elif action.type == ActionType.TRANSFORM:
            return action.transform_type or "sql"
            
        elif action.type == ActionType.WRITE:
            if action.write_target and isinstance(action.write_target, dict):
                return action.write_target.get("type", "streaming_table")
            else:
                return "streaming_table"  # Default to streaming table
        
        else:
            raise ValueError(f"Unknown action type: {action.type}")
    
    def _group_write_actions_by_target(self, write_actions: List[Action]) -> Dict[str, List[Action]]:
        """Group write actions by their target table.
        
        Args:
            write_actions: List of write actions
            
        Returns:
            Dictionary mapping target table names to lists of actions
        """
        grouped = defaultdict(list)
        
        for action in write_actions:
            target_config = action.write_target
            if not target_config:
                # Handle legacy structure
                target_config = action.source if isinstance(action.source, dict) else {}
            
            # Build full table name
            database = target_config.get("database", "")
            table = target_config.get("table") or target_config.get("name", "")
            
            if database and table:
                full_table_name = f"{database}.{table}"
            elif table:
                full_table_name = table
            else:
                # Use action name as fallback
                full_table_name = action.name
            
            grouped[full_table_name].append(action)
        
        return dict(grouped)
    
    def _create_combined_write_action(self, actions: List[Action], target_table: str) -> Action:
        """Create a combined write action with multiple source views.
        
        Args:
            actions: List of write actions targeting the same table
            target_table: Full target table name
            
        Returns:
            Combined action with multiple source views
        """
        # Use the first action as the base
        base_action = actions[0]
        
        # Extract all source views from the actions
        source_views = []
        flow_names = []
        
        for action in actions:
            # Extract source view name
            if isinstance(action.source, str):
                source_views.append(action.source)
            elif isinstance(action.source, dict):
                source_view = action.source.get("view", action.source.get("name", ""))
                if source_view:
                    source_views.append(source_view)
            
            # Extract flow name from action name
            flow_name = action.name.replace("-", "_").replace(" ", "_")
            if flow_name.startswith("write_"):
                flow_name = flow_name[6:]  # Remove "write_" prefix
            flow_name = f"f_{flow_name}" if not flow_name.startswith("f_") else flow_name
            flow_names.append(flow_name)
        
        # Create a new action with combined properties
        combined_action = Action(
            name=f"write_{target_table.replace('.', '_')}",
            type=base_action.type,
            source=source_views,  # Pass as list for multiple append flows
            write_target=base_action.write_target,
            target=base_action.target,
            description=f"Write to {target_table} from multiple sources",
            once=base_action.once
        )
        
        # Store flow names for template use
        combined_action._flow_names = flow_names
        
        return combined_action
    
    def validate_pipeline(self, pipeline_name: str, env: str) -> tuple[List[str], List[str]]:
        """Validate pipeline configuration without generating code.
        
        Args:
            pipeline_name: Name of the pipeline to validate
            env: Environment to validate for
            
        Returns:
            Tuple of (errors, warnings)
        """
        errors = []
        warnings = []
        
        try:
            pipeline_dir = self.project_root / "pipelines" / pipeline_name
            flowgroups = self._discover_flowgroups(pipeline_dir)
            
            substitution_file = self.project_root / "substitutions" / f"{env}.yaml"
            substitution_mgr = EnhancedSubstitutionManager(substitution_file, env)
            
            for flowgroup in flowgroups:
                try:
                    processed_flowgroup = self._process_flowgroup(flowgroup, substitution_mgr)
                    # Validation happens in _process_flowgroup
                    warnings.append(f"Flowgroup '{flowgroup.flowgroup}' validated successfully")
                    
                except Exception as e:
                    errors.append(f"Flowgroup '{flowgroup.flowgroup}': {e}")
                    
        except Exception as e:
            errors.append(f"Pipeline validation failed: {e}")
        
        return errors, warnings 