"""Show command implementation for LakehousePlumber CLI."""

import sys
import os
import time
from pathlib import Path
from typing import Optional
import click
import yaml

from .base_command import BaseCommand
from ...core.orchestrator import ActionOrchestrator
from ...parsers.yaml_parser import YAMLParser
from ...utils.substitution import EnhancedSubstitutionManager
from ...models.config import ActionType


class ShowCommand(BaseCommand):
    """
    Handles show and info commands for LakehousePlumber CLI.
    
    Provides detailed information about flowgroups, project configuration,
    and resolved configurations with substitutions applied.
    """
    
    def show_flowgroup(self, flowgroup: str, env: str = "dev") -> None:
        """
        Show resolved configuration for a specific flowgroup.
        
        Args:
            flowgroup: Name of the flowgroup to show
            env: Environment to resolve configuration for
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        click.echo(f"🔍 Showing resolved configuration for '{flowgroup}' in environment '{env}'")
        
        # Find the flowgroup file
        flowgroup_file = self._find_flowgroup_file(flowgroup, project_root)
        if not flowgroup_file:
            click.echo(f"❌ Flowgroup '{flowgroup}' not found")
            sys.exit(1)
        
        # Parse and process flowgroup
        fg = self._parse_flowgroup(flowgroup_file)
        substitution_mgr = self._load_substitution_manager(project_root, env)
        processed_fg = self._process_flowgroup(fg, substitution_mgr, project_root)
        
        # Display flowgroup information
        self._display_flowgroup_configuration(processed_fg, flowgroup_file, project_root, env)
        
        # Display actions in detail
        self._display_actions_table(processed_fg)
        self._display_action_details(processed_fg)
        
        # Show secret references and substitution summary
        self._display_secret_references(substitution_mgr)
        self._display_substitution_summary(substitution_mgr)
    
    def show_project_info(self) -> None:
        """Display comprehensive project information and statistics."""
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        click.echo("📊 LakehousePlumber Project Information")
        click.echo("=" * 60)
        
        # Load and display project configuration
        config = self._load_project_config(project_root)
        self._display_project_basic_info(config, project_root)
        
        # Display resource summary
        resource_summary = self._collect_resource_summary(project_root)
        self._display_resource_summary(resource_summary)
        
        # Display environments
        self._display_environments(project_root)
        
        # Display recent activity
        self._display_recent_activity(project_root)
    
    def _find_flowgroup_file(self, flowgroup: str, project_root: Path) -> Optional[Path]:
        """Find the YAML file containing the specified flowgroup.
        
        Supports multi-document (---) and flowgroups array syntax.
        """
        pipelines_dir = project_root / "pipelines"
        
        # Get include patterns and discover files
        include_patterns = self._get_include_patterns(project_root)
        yaml_files = self._discover_yaml_files_with_include(pipelines_dir, include_patterns)
        
        for yaml_file in yaml_files:
            try:
                with open(yaml_file, "r", encoding="utf-8") as f:
                    # Load all documents to support multi-document files
                    documents = list(yaml.safe_load_all(f))
                
                # Check each document
                for doc in documents:
                    if doc is None:
                        continue
                    
                    # Check regular syntax (flowgroup field at document level)
                    if doc.get("flowgroup") == flowgroup:
                        return yaml_file
                    
                    # Check array syntax (flowgroups list)
                    if "flowgroups" in doc:
                        for fg_config in doc["flowgroups"]:
                            if fg_config.get("flowgroup") == flowgroup:
                                return yaml_file
            except Exception:
                continue
        
        return None
    
    def _get_include_patterns(self, project_root: Path) -> list[str]:
        """Get include patterns from project configuration."""
        try:
            from ...core.project_config_loader import ProjectConfigLoader
            config_loader = ProjectConfigLoader(project_root)
            project_config = config_loader.load_project_config()
            
            if project_config and project_config.include:
                return project_config.include
            return []
        except Exception as e:
            self.logger.warning(f"Could not load project config for include patterns: {e}")
            return []
    
    def _discover_yaml_files_with_include(self, pipelines_dir: Path, 
                                         include_patterns: list[str]) -> list[Path]:
        """Discover YAML files with include pattern filtering."""
        if include_patterns:
            from ...utils.file_pattern_matcher import discover_files_with_patterns
            return discover_files_with_patterns(pipelines_dir, include_patterns)
        else:
            yaml_files = []
            yaml_files.extend(pipelines_dir.rglob("*.yaml"))
            yaml_files.extend(pipelines_dir.rglob("*.yml"))
            return yaml_files
    
    def _parse_flowgroup(self, flowgroup_file: Path):
        """Parse flowgroup file."""
        parser = YAMLParser()
        try:
            return parser.parse_flowgroup(flowgroup_file)
        except Exception as e:
            click.echo(f"❌ Error parsing flowgroup: {e}")
            sys.exit(1)
    
    def _load_substitution_manager(self, project_root: Path, env: str):
        """Load substitution manager for environment."""
        substitution_file = project_root / "substitutions" / f"{env}.yaml"
        if not substitution_file.exists():
            click.echo(f"⚠️  Warning: Substitution file not found: {substitution_file}")
            return EnhancedSubstitutionManager(env=env)
        else:
            return EnhancedSubstitutionManager(substitution_file, env)
    
    def _process_flowgroup(self, fg, substitution_mgr, project_root: Path):
        """Process flowgroup with templates and presets."""
        orchestrator = ActionOrchestrator(project_root, enforce_version=False)
        try:
            return orchestrator.process_flowgroup(fg, substitution_mgr)
        except Exception as e:
            click.echo(f"❌ Error processing flowgroup: {e}")
            sys.exit(1)
    
    def _display_flowgroup_configuration(self, processed_fg, flowgroup_file: Path,
                                        project_root: Path, env: str) -> None:
        """Display basic flowgroup configuration information."""
        click.echo("\n📋 FlowGroup Configuration")
        click.echo("─" * 60)
        click.echo(f"Pipeline:    {processed_fg.pipeline}")
        click.echo(f"FlowGroup:   {processed_fg.flowgroup}")
        click.echo(f"Location:    {flowgroup_file.relative_to(project_root)}")
        click.echo(f"Environment: {env}")
        
        if processed_fg.presets:
            click.echo(f"Presets:     {', '.join(processed_fg.presets)}")
        
        if processed_fg.use_template:
            click.echo(f"Template:    {processed_fg.use_template}")
    
    def _display_actions_table(self, processed_fg) -> None:
        """Display actions in table format."""
        click.echo(f"\n📊 Actions ({len(processed_fg.actions)} total)")
        click.echo("─" * 80)
        
        if not processed_fg.actions:
            click.echo("No actions found")
            return
        
        # Calculate column widths
        name_width = max(len(a.name) for a in processed_fg.actions) + 2
        type_width = 12
        target_width = max(len(a.target or "-") for a in processed_fg.actions) + 2
        
        # Header
        click.echo(
            f"{'Name':<{name_width}} │ {'Type':<{type_width}} │ "
            f"{'Target':<{target_width}} │ Description"
        )
        click.echo("─" * 80)
        
        # Actions
        for action in processed_fg.actions:
            name = action.name
            action_type = action.type.value
            target = action.target or "-"
            description = action.description or "-"
            
            # Truncate description if too long
            max_desc_width = 80 - name_width - type_width - target_width - 9
            if len(description) > max_desc_width:
                description = description[:max_desc_width - 3] + "..."
            
            click.echo(
                f"{name:<{name_width}} │ {action_type:<{type_width}} │ "
                f"{target:<{target_width}} │ {description}"
            )
        
        click.echo("─" * 80)
    
    def _display_action_details(self, processed_fg) -> None:
        """Display detailed action information."""
        click.echo("\n📝 Action Details:")
        for i, action in enumerate(processed_fg.actions):
            click.echo(f"\n{i+1}. {action.name} ({action.type.value})")
            
            # Show source configuration
            if action.source:
                click.echo("   Source:")
                if isinstance(action.source, str):
                    click.echo(f"      {action.source}")
                elif isinstance(action.source, list):
                    for src in action.source:
                        click.echo(f"      • {src}")
                elif isinstance(action.source, dict):
                    for key, value in action.source.items():
                        # Show values, keeping secret placeholders
                        if isinstance(value, str) and "${secret:" in value:
                            click.echo(f"      {key}: {value}")
                        else:
                            click.echo(f"      {key}: {value}")
            
            # Show additional properties
            if action.type == ActionType.TRANSFORM and action.transform_type:
                click.echo(f"   Transform Type: {action.transform_type}")
            
            if hasattr(action, "sql") and action.sql:
                sql_preview = action.sql[:100] + "..." if len(action.sql) > 100 else action.sql
                click.echo(f"   SQL: {sql_preview}")
            
            if hasattr(action, "sql_path") and action.sql_path:
                click.echo(f"   SQL Path: {action.sql_path}")
    
    def _display_secret_references(self, substitution_mgr) -> None:
        """Display secret references found in configuration."""
        secret_refs = substitution_mgr.get_secret_references()
        if secret_refs:
            click.echo(f"\n🔐 Secret References ({len(secret_refs)} found)")
            click.echo("─" * 60)
            for ref in sorted(secret_refs, key=lambda r: f"{r.scope}/{r.key}"):
                click.echo(f"   ${{{ref.scope}/{ref.key}}}")
    
    def _display_substitution_summary(self, substitution_mgr) -> None:
        """Display substitution token summary."""
        if substitution_mgr.mappings:
            click.echo(f"\n🔄 Token Substitutions ({len(substitution_mgr.mappings)} found)")
            click.echo("─" * 60)
            for token, value in list(substitution_mgr.mappings.items())[:10]:
                # Truncate long values for display
                display_value = str(value)
                if len(display_value) > 40:
                    display_value = display_value[:37] + "..."
                click.echo(f"   {{{token}}} → {display_value}")
            
            if len(substitution_mgr.mappings) > 10:
                click.echo(f"   ... and {len(substitution_mgr.mappings) - 10} more")
    
    def _load_project_config(self, project_root: Path) -> dict:
        """Load project configuration from lhp.yaml."""
        config_file = project_root / "lhp.yaml"
        if not config_file.exists():
            return {}
        
        try:
            with open(config_file, "r") as f:
                return yaml.safe_load(f) or {}
        except Exception as e:
            self.logger.warning(f"Could not load project config: {e}")
            return {}
    
    def _display_project_basic_info(self, config: dict, project_root: Path) -> None:
        """Display basic project information."""
        click.echo(f"Name:        {config.get('name', 'Unknown')}")
        click.echo(f"Version:     {config.get('version', 'Unknown')}")
        click.echo(f"Description: {config.get('description', 'No description')}")
        click.echo(f"Author:      {config.get('author', 'Unknown')}")
        click.echo(f"Location:    {project_root}")
    
    def _collect_resource_summary(self, project_root: Path) -> dict:
        """Collect summary statistics about project resources."""
        pipelines_dir = project_root / "pipelines"
        presets_dir = project_root / "presets"
        templates_dir = project_root / "templates"
        
        # Count pipelines and flowgroups
        pipeline_count = 0
        flowgroup_count = 0
        if pipelines_dir.exists():
            pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]
            pipeline_count = len(pipeline_dirs)
            
            for pipeline_dir in pipeline_dirs:
                yaml_files = list(pipeline_dir.rglob("*.yaml"))
                flowgroup_count += len(yaml_files)
        
        # Count other resources
        preset_count = len(list(presets_dir.glob("*.yaml"))) if presets_dir.exists() else 0
        template_count = len(list(templates_dir.glob("*.yaml"))) if templates_dir.exists() else 0
        
        return {
            "pipeline_count": pipeline_count,
            "flowgroup_count": flowgroup_count,
            "preset_count": preset_count,
            "template_count": template_count,
        }
    
    def _display_resource_summary(self, summary: dict) -> None:
        """Display resource summary statistics."""
        click.echo("\n📈 Resource Summary:")
        click.echo(f"   Pipelines:  {summary['pipeline_count']}")
        click.echo(f"   FlowGroups: {summary['flowgroup_count']}")
        click.echo(f"   Presets:    {summary['preset_count']}")
        click.echo(f"   Templates:  {summary['template_count']}")
    
    def _display_environments(self, project_root: Path) -> None:
        """Display available environments."""
        substitutions_dir = project_root / "substitutions"
        if substitutions_dir.exists():
            env_files = [f.stem for f in substitutions_dir.glob("*.yaml")]
            if env_files:
                click.echo(f"\n🌍 Environments: {', '.join(env_files)}")
    
    def _display_recent_activity(self, project_root: Path) -> None:
        """Display recent activity information."""
        click.echo("\n📅 Recent Activity:")
        
        # Find most recently modified flowgroup
        pipelines_dir = project_root / "pipelines"
        recent_files = []
        
        if pipelines_dir.exists():
            for yaml_file in pipelines_dir.rglob("*.yaml"):
                mtime = os.path.getmtime(yaml_file)
                recent_files.append((yaml_file, mtime))
        
        if recent_files:
            recent_files.sort(key=lambda x: x[1], reverse=True)
            most_recent = recent_files[0]
            time_str = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(most_recent[1]))
            rel_path = most_recent[0].relative_to(project_root)
            click.echo(f"   Last modified: {rel_path} ({time_str})")
    
    def show_substitutions(self, env: str = "dev") -> None:
        """
        Show available substitution tokens for an environment.
        
        Args:
            env: Environment to show substitutions for
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()
        
        # Load substitutions
        sub_file = project_root / "substitutions" / f"{env}.yaml"
        if not sub_file.exists():
            click.echo(f"❌ Substitution file not found: {sub_file}")
            sys.exit(1)
        
        mgr = EnhancedSubstitutionManager(sub_file, env=env)
        
        # Display
        click.echo(f"\n📋 Available Substitutions for Environment: {env}")
        click.echo("═" * 60)
        
        # Separate simple tokens and maps
        simple_tokens = {}
        maps = {}
        reserved = {}
        
        for key, value in mgr.mappings.items():
            if key in ["workspace_env", "logical_env"]:
                reserved[key] = value
            elif isinstance(value, dict):
                maps[key] = value
            else:
                simple_tokens[key] = value
        
        # Display simple tokens
        if simple_tokens:
            click.echo("\n✨ Simple Tokens:")
            for key, value in sorted(simple_tokens.items()):
                click.echo(f"  {{<KEY>}}: \"{value}\"".replace("<KEY>", key))
        
        # Display maps (with tree structure)
        if maps:
            click.echo("\n📦 Maps:")
            for map_name, map_value in sorted(maps.items()):
                click.echo(f"  {map_name}:")
                self._display_dict_tree(map_value, indent=4)
        
        # Display reserved
        if reserved:
            click.echo("\n🔒 Reserved Tokens:")
            for key, value in sorted(reserved.items()):
                click.echo(f"  {{<KEY>}}: \"{value}\"".replace("<KEY>", key))
        
        click.echo("")
    
    def _display_dict_tree(self, data: dict, indent: int = 0, is_last: bool = True) -> None:
        """Display dict as tree structure."""
        items = list(data.items())
        for i, (key, value) in enumerate(items):
            is_last_item = (i == len(items) - 1)
            prefix = " " * indent + ("└─ " if is_last_item else "├─ ")
            
            if isinstance(value, dict):
                click.echo(f"{prefix}{key}:")
                self._display_dict_tree(value, indent + 2, is_last_item)
            else:
                click.echo(f"{prefix}{key}: \"{value}\"")