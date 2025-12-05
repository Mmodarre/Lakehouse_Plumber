"""Code generation service for LakehousePlumber."""

import logging
from typing import Dict, List, Any, Optional, Set, Tuple
from collections import defaultdict
from pathlib import Path

from ...models.config import FlowGroup, Action, ActionType
from ...utils.substitution import EnhancedSubstitutionManager
from ...utils.error_formatter import LHPError
from ...utils.source_extractor import extract_source_views_from_action


class CodeGenerator:
    """
    Service for generating Python code from flowgroup configurations.
    
    Handles the complete code generation pipeline including action processing,
    dependency resolution, import management, and final code assembly.
    """
    
    def __init__(self, action_registry=None, dependency_resolver=None, 
                 preset_manager=None, project_config=None, project_root=None):
        """
        Initialize code generator.
        
        Args:
            action_registry: Action registry for getting generators
            dependency_resolver: Dependency resolver for action ordering
            preset_manager: Preset manager for preset configurations
            project_config: Project configuration for context
            project_root: Project root directory for spec_dir context
        """
        self.action_registry = action_registry
        self.dependency_resolver = dependency_resolver
        self.preset_manager = preset_manager
        self.project_config = project_config
        self.project_root = project_root
        self.logger = logging.getLogger(__name__)
    
    def generate_flowgroup_code(self, flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager,
                               output_dir: Optional[Path] = None, state_manager=None, 
                               source_yaml: Optional[Path] = None, env: Optional[str] = None,
                               include_tests: bool = False, python_file_copier=None) -> str:
        """
        Generate complete Python code for a flowgroup.
        
        Args:
            flowgroup: FlowGroup to generate code for
            substitution_mgr: Substitution manager for the environment
            output_dir: Output directory for generated files
            state_manager: State manager for file tracking
            source_yaml: Source YAML path for file tracking
            env: Environment name for file tracking
            include_tests: Whether to include test actions
            python_file_copier: Thread-safe Python file copier (for parallel mode)
            
        Returns:
            Complete Python code for the flowgroup
        """
        # 1. Resolve action dependencies
        ordered_actions = self.dependency_resolver.resolve_dependencies(flowgroup.actions)
        
        # 2. Get preset configuration if any
        preset_config = {}
        if flowgroup.presets:
            preset_config = self.preset_manager.resolve_preset_chain(flowgroup.presets)
        
        # 3. Check for test-only flowgroups when include_tests is False
        if not include_tests:
            non_test_actions = [action for action in ordered_actions if action.type != ActionType.TEST]
            if not non_test_actions:
                # This is a test-only flowgroup, skip entirely
                self.logger.info(f"Skipping test-only flowgroup: {flowgroup.flowgroup} (--include-tests not specified)")
                return ""  # Return empty string to skip this flowgroup
        
        # 4. Generate code sections
        generated_sections, all_imports, custom_source_sections = self._generate_action_sections(
            flowgroup, ordered_actions, substitution_mgr, preset_config, 
            output_dir, state_manager, source_yaml, env, include_tests, python_file_copier
        )
        
        # 5. Apply secret substitutions to generated code
        complete_code = self._apply_secret_substitutions(generated_sections, substitution_mgr)
        
        # 6. Assemble final code with imports and headers
        return self._assemble_final_code(
            flowgroup, all_imports, custom_source_sections, complete_code
        )
    
    def _generate_action_sections(self, flowgroup: FlowGroup, ordered_actions: List[Action],
                                 substitution_mgr: EnhancedSubstitutionManager, preset_config: Dict[str, Any],
                                 output_dir: Optional[Path], state_manager, source_yaml: Optional[Path],
                                 env: Optional[str], include_tests: bool, python_file_copier=None) -> Tuple[List[str], Set[str], List[Dict]]:
        """Generate code sections for all actions."""
        # Group actions by type while preserving order
        action_groups = defaultdict(list)
        for action in ordered_actions:
            action_groups[action.type].append(action)
        
        # Initialize collections
        generated_sections = []
        all_imports = set()
        custom_source_sections = []
        
        # Add base imports
        all_imports.add("from pyspark import pipelines as dp")
        
        # Define section headers
        section_headers = {
            ActionType.LOAD: "SOURCE VIEWS",
            ActionType.TRANSFORM: "TRANSFORMATION VIEWS", 
            ActionType.WRITE: "TARGET TABLES",
            ActionType.TEST: "DATA QUALITY TESTS",
        }
        
        # Process each action type in order
        action_types = [ActionType.LOAD, ActionType.TRANSFORM, ActionType.WRITE]
        if include_tests:
            action_types.append(ActionType.TEST)
        
        for action_type in action_types:
            if action_type in action_groups:
                # Add section header
                header_text = section_headers.get(action_type, str(action_type).upper())
                section_header = f"""
# {"=" * 76}
# {header_text}
# {"=" * 76}"""
                generated_sections.append(section_header)
                
                # Generate actions for this type
                if action_type == ActionType.WRITE:
                    sections, imports, custom = self._generate_write_actions(
                        action_groups[action_type], flowgroup, substitution_mgr,
                        preset_config, output_dir, state_manager, source_yaml, env, python_file_copier
                    )
                else:
                    sections, imports, custom = self._generate_regular_actions(
                        action_groups[action_type], flowgroup, substitution_mgr,
                        preset_config, output_dir, state_manager, source_yaml, env, python_file_copier
                    )
                
                generated_sections.extend(sections)
                all_imports.update(imports)
                custom_source_sections.extend(custom)
        
        return generated_sections, all_imports, custom_source_sections
    
    def _generate_write_actions(self, write_actions: List[Action], flowgroup: FlowGroup,
                               substitution_mgr: EnhancedSubstitutionManager, preset_config: Dict[str, Any],
                               output_dir: Optional[Path], state_manager, source_yaml: Optional[Path],
                               env: Optional[str], python_file_copier=None) -> Tuple[List[str], Set[str], List[Dict]]:
        """Generate code for write actions with target grouping."""
        sections = []
        imports = set()
        custom_sources = []
        
        # Group write actions by target table
        grouped_actions = self.group_write_actions_by_target(write_actions)
        
        for target_table, actions in grouped_actions.items():
            try:
                # Use the first action to determine sub-type and get generator
                first_action = actions[0]
                sub_type = self.determine_action_subtype(first_action)
                generator = self.action_registry.get_generator(first_action.type, sub_type)
                
                # Create a combined action with multiple source views
                combined_action = self.create_combined_write_action(actions, target_table)
                
                # Generate code
                context = self._build_generation_context(
                    flowgroup, substitution_mgr, preset_config, 
                    output_dir, state_manager, source_yaml, env, python_file_copier
                )
                action_code = generator.generate(combined_action, context)
                sections.append(action_code)
                
                # Collect imports and custom sources
                section_imports, section_custom = self._collect_generator_outputs(generator)
                imports.update(section_imports)
                custom_sources.extend(section_custom)
                
            except LHPError:
                raise  # Re-raise LHPError as-is
            except Exception as e:
                action_names = [a.name for a in actions]
                raise ValueError(f"Error generating code for write actions {action_names}: {e}")
        
        return sections, imports, custom_sources
    
    def _generate_regular_actions(self, actions: List[Action], flowgroup: FlowGroup,
                                 substitution_mgr: EnhancedSubstitutionManager, preset_config: Dict[str, Any],
                                 output_dir: Optional[Path], state_manager, source_yaml: Optional[Path],
                                 env: Optional[str], python_file_copier=None) -> Tuple[List[str], Set[str], List[Dict]]:
        """Generate code for regular (non-write) actions."""
        sections = []
        imports = set()
        custom_sources = []
        
        for action in actions:
            try:
                # Determine action sub-type
                sub_type = self.determine_action_subtype(action)
                
                # Get generator
                generator = self.action_registry.get_generator(action.type, sub_type)
                
                # Generate code
                context = self._build_generation_context(
                    flowgroup, substitution_mgr, preset_config,
                    output_dir, state_manager, source_yaml, env, python_file_copier
                )
                action_code = generator.generate(action, context)
                sections.append(action_code)
                
                # Collect imports and custom sources
                section_imports, section_custom = self._collect_generator_outputs(generator)
                imports.update(section_imports)
                custom_sources.extend(section_custom)
                
            except LHPError:
                raise  # Re-raise LHPError as-is
            except Exception as e:
                raise ValueError(f"Error generating code for action '{action.name}': {e}")
        
        return sections, imports, custom_sources
    
    def _build_generation_context(self, flowgroup: FlowGroup, substitution_mgr: EnhancedSubstitutionManager,
                                 preset_config: Dict[str, Any], output_dir: Optional[Path], 
                                 state_manager, source_yaml: Optional[Path], env: Optional[str],
                                 python_file_copier=None) -> Dict[str, Any]:
        """Build context dictionary for generator execution."""
        project_root = self.project_root or Path.cwd()
        return {
            "flowgroup": flowgroup,
            "substitution_manager": substitution_mgr,
            "spec_dir": project_root,  # For backward compatibility
            "project_root": project_root,  # Explicit project root for external file loading
            "preset_config": preset_config,
            "project_config": self.project_config,
            "output_dir": output_dir,
            "state_manager": state_manager,
            "source_yaml": source_yaml,
            "environment": env,
            "secret_references": set(),  # Track secret references from file processing
            "python_file_copier": python_file_copier,  # Thread-safe copier for parallel mode
        }
    
    def _collect_generator_outputs(self, generator) -> Tuple[Set[str], List[Dict]]:
        """Collect imports and custom source code from generator."""
        imports = set()
        custom_sources = []
        
        # Enhanced import collection - use ImportManager if available
        import_manager = getattr(generator, 'get_import_manager', lambda: None)()
        if import_manager:
            # Generator uses ImportManager - get consolidated imports
            consolidated_imports = import_manager.get_consolidated_imports()
            imports.update(consolidated_imports)
            self.logger.debug(f"Used ImportManager: {len(consolidated_imports)} imports")
        else:
            # Legacy generator - use simple import collection
            imports.update(generator.imports)
        
        # Collect custom source code if available
        if hasattr(generator, 'custom_source_code') and generator.custom_source_code:
            custom_sources.append({
                'content': generator.custom_source_code,
                'source_file': Path(str(generator.source_file_path)).as_posix() if generator.source_file_path else None,
                'action_name': generator.action_name if hasattr(generator, 'action_name') else 'unknown'
            })
        
        # Collect custom sink code if available
        if hasattr(generator, 'custom_sink_code') and generator.custom_sink_code:
            custom_sources.append({
                'content': generator.custom_sink_code,
                'source_file': Path(str(generator.sink_file_path)).as_posix() if generator.sink_file_path else None,
                'action_name': generator.action_name if hasattr(generator, 'action_name') else 'unknown'
            })
        
        return imports, custom_sources
    
    def _apply_secret_substitutions(self, generated_sections: List[str], 
                                   substitution_mgr: EnhancedSubstitutionManager) -> str:
        """Apply secret substitutions to generated code."""
        complete_code = "\n\n".join(generated_sections)
        
        # Use SecretCodeGenerator to convert secret placeholders to valid f-strings
        try:
            from ...utils.secret_code_generator import SecretCodeGenerator
            secret_generator = SecretCodeGenerator()
            complete_code = secret_generator.generate_python_code(
                complete_code, substitution_mgr.get_secret_references()
            )
        except ImportError:
            self.logger.warning("SecretCodeGenerator not available, skipping secret substitutions")
        except Exception as e:
            self.logger.warning(f"Error applying secret substitutions: {e}")
        
        return complete_code
    
    def _assemble_final_code(self, flowgroup: FlowGroup, all_imports: Set[str],
                            custom_source_sections: List[Dict], complete_code: str) -> str:
        """Assemble final Python code with headers and imports."""
        # Build imports section
        imports_section = "\n".join(sorted(all_imports))
        
        # Add pipeline configuration section
        pipeline_config = f"""
# Pipeline Configuration
PIPELINE_ID = "{flowgroup.pipeline}"
FLOWGROUP_ID = "{flowgroup.flowgroup}"
"""
        
        # Build header
        header = f"""# Generated by LakehousePlumber
# Pipeline: {flowgroup.pipeline}
# FlowGroup: {flowgroup.flowgroup}

{imports_section}
{pipeline_config}"""
        
        # FIXED ORDERING: Custom source code FIRST, then main generated code
        final_code = header
        
        # Add custom source code first (so classes are defined before registration)
        if custom_source_sections:
            custom_code_block = self.build_custom_source_block(custom_source_sections)
            final_code += "\n\n" + custom_code_block
        
        # Then add main generated code (registration happens after class definitions)
        final_code += "\n\n" + complete_code
        
        return final_code
    
    def determine_action_subtype(self, action: Action) -> str:
        """
        Determine the sub-type of an action for generator selection.
        
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
        
        elif action.type == ActionType.TEST:
            return action.test_type or "row_count"  # Default to row_count test
        
        else:
            raise ValueError(f"Unknown action type: {action.type}")
    
    def group_write_actions_by_target(self, write_actions: List[Action]) -> Dict[str, List[Action]]:
        """
        Group write actions by their target table.
        
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
    
    def create_combined_write_action(self, actions: List[Action], target_table: str) -> Action:
        """
        Create a combined write action with individual action metadata preserved.
        
        Args:
            actions: List of write actions targeting the same table
            target_table: Full target table name
            
        Returns:
            Combined action with individual action metadata
        """
        # Determine which action should create the table based on existing validation logic
        from ..validators import TableCreationValidator
        table_creator = None
        table_creation_validator = TableCreationValidator()
        for action in actions:
            if table_creation_validator._action_creates_table(action):
                table_creator = action
                break
        
        # If no explicit creator found, use the first action (default behavior)
        if not table_creator:
            table_creator = actions[0]
        
        # Build individual action metadata for each append flow
        action_metadata = []
        for action in actions:
            # Extract source views (can be multiple per action)
            source_views_for_action = self._extract_source_views_from_action(action.source)
            
            # Generate base flow name from action name
            base_flow_name = action.name.replace("-", "_").replace(" ", "_")
            if base_flow_name.startswith("write_"):
                base_flow_name = base_flow_name[6:]  # Remove "write_" prefix
            base_flow_name = f"f_{base_flow_name}" if not base_flow_name.startswith("f_") else base_flow_name
            
            if len(source_views_for_action) > 1:
                # Multiple sources in this action: create separate append flow for each
                for i, source_view in enumerate(source_views_for_action):
                    flow_name = f"{base_flow_name}_{i+1}"
                    action_metadata.append({
                        "action_name": f"{action.name}_{i+1}",
                        "source_view": source_view,
                        "once": action.once or False,
                        "readMode": action.readMode,  # Preserve individual readMode
                        "flow_name": flow_name,
                        "description": action.description or f"Append flow to {target_table} from {source_view}",
                    })
            else:
                # Single source in this action: create one append flow
                source_view = source_views_for_action[0] if source_views_for_action else ""
                action_metadata.append({
                    "action_name": action.name,
                    "source_view": source_view,
                    "once": action.once or False,
                    "readMode": action.readMode,  # Preserve individual readMode
                    "flow_name": base_flow_name,
                    "description": action.description or f"Append flow to {target_table}",
                })
        
        # Create combined action using the table creator as the base
        combined_action = table_creator.model_copy(deep=True)
        
        # Store metadata as private attribute (Pydantic compatible)
        object.__setattr__(combined_action, '_action_metadata', action_metadata)
        object.__setattr__(combined_action, '_table_creator', table_creator)
        
        return combined_action
    
    def build_custom_source_block(self, custom_sections: List[Dict]) -> str:
        """
        Build the custom source code block to append to flowgroup files.
        
        Args:
            custom_sections: List of dictionaries with custom source code info
            
        Returns:
            Formatted custom source code block with headers
        """
        blocks = []
        blocks.append("# " + "="*76)
        blocks.append("# CUSTOM DATA SOURCE IMPLEMENTATIONS")
        blocks.append("# " + "="*76)
        
        for section in custom_sections:
            blocks.append(f"# The following code was automatically copied from: {section['source_file']}")
            blocks.append(f"# Used by action: {section['action_name']}")
            blocks.append("")
            blocks.append(section['content'])
            blocks.append("")
        
        return "\n".join(blocks)
    
    def _extract_source_views_from_action(self, source) -> List[str]:
        """
        Extract all source views from an action source configuration.
        
        Delegates to utility function for consistency across codebase.
        
        Args:
            source: Source configuration (string, list, or dict)
            
        Returns:
            List of source view names
        """
        return extract_source_views_from_action(source)
