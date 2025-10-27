"""List commands implementation for LakehousePlumber CLI."""

import sys
import logging
from pathlib import Path
from typing import List, Dict, Any
import click

from .base_command import BaseCommand
from ...parsers.yaml_parser import YAMLParser

logger = logging.getLogger(__name__)


class ListCommand(BaseCommand):
    """
    Handles list commands for presets and templates.
    
    Provides listing and detailed information about available
    presets and templates in the project.
    """
    
    def list_presets(self) -> None:
        """List all available presets with detailed information."""
        self.setup_from_context()
        project_root = self.ensure_project_root()
        presets_dir = project_root / "presets"
        
        click.echo("📋 Available presets:")
        
        if not presets_dir.exists():
            click.echo("❌ No presets directory found")
            sys.exit(1)
        
        # Discover preset files
        preset_files = list(presets_dir.glob("*.yaml")) + list(presets_dir.glob("*.yml"))
        
        if not preset_files:
            click.echo("📭 No presets found")
            click.echo("\n💡 Create a preset file in the 'presets' directory")
            click.echo("   Example: presets/bronze_layer.yaml")
            return
        
        # Parse preset information
        presets_info = self._parse_preset_information(preset_files)
        
        # Display presets table
        self._display_presets_table(presets_info)
        
        # Show descriptions
        self._display_preset_descriptions(presets_info)
        
        click.echo(f"\n📊 Total presets: {len(presets_info)}")
    
    def list_templates(self) -> None:
        """List all available templates with detailed parameter information."""
        self.setup_from_context()
        project_root = self.ensure_project_root()
        templates_dir = project_root / "templates"
        
        click.echo("📋 Available templates:")
        
        if not templates_dir.exists():
            click.echo("❌ No templates directory found")
            sys.exit(1)
        
        # Discover template files
        template_files = list(templates_dir.glob("*.yaml")) + list(templates_dir.glob("*.yml"))
        
        if not template_files:
            click.echo("📭 No templates found")
            click.echo("\n💡 Create a template file in the 'templates' directory")
            click.echo("   Example: templates/standard_ingestion.yaml")
            return
        
        # Parse template information
        templates_info = self._parse_template_information(template_files)
        
        # Display templates table
        self._display_templates_table(templates_info)
        
        # Show detailed template information
        self._display_template_details(template_files)
        
        click.echo(f"\n📊 Total templates: {len(templates_info)}")
        self._display_template_usage_help()
    
    def _parse_preset_information(self, preset_files: List[Path]) -> List[Dict[str, Any]]:
        """Parse preset files and extract information."""
        parser = YAMLParser()
        presets_info = []
        
        for preset_file in sorted(preset_files):
            try:
                preset = parser.parse_preset(preset_file)
                presets_info.append({
                    "name": preset.name,
                    "file": preset_file.name,
                    "version": preset.version,
                    "extends": preset.extends,
                    "description": preset.description or "No description",
                })
            except Exception as e:
                logger.warning(f"Could not parse preset {preset_file}: {e}")
                presets_info.append({
                    "name": preset_file.stem,
                    "file": preset_file.name,
                    "version": "?",
                    "extends": "?",
                    "description": f"Error: {e}",
                })
        
        return presets_info
    
    def _parse_template_information(self, template_files: List[Path]) -> List[Dict[str, Any]]:
        """Parse template files and extract information."""
        parser = YAMLParser()
        templates_info = []
        
        for template_file in sorted(template_files):
            try:
                template = parser.parse_template_raw(template_file)
                # Count parameters
                required_params = sum(1 for p in template.parameters if p.get("required", False))
                total_params = len(template.parameters)
                
                templates_info.append({
                    "name": template.name,
                    "file": template_file.name,
                    "version": template.version,
                    "params": f"{required_params}/{total_params}",
                    "actions": len(template.actions),
                    "description": template.description or "No description",
                })
            except Exception as e:
                logger.warning(f"Could not parse template {template_file}: {e}")
                templates_info.append({
                    "name": template_file.stem,
                    "file": template_file.name,
                    "version": "?",
                    "params": "?",
                    "actions": "?",
                    "description": f"Error: {e}",
                })
        
        return templates_info
    
    def _display_presets_table(self, presets_info: List[Dict[str, Any]]) -> None:
        """Display presets in table format."""
        if not presets_info:
            return
        
        # Calculate column widths
        name_width = max(len(p["name"]) for p in presets_info) + 2
        file_width = max(len(p["file"]) for p in presets_info) + 2
        version_width = 10
        extends_width = max(len(str(p["extends"] or "-")) for p in presets_info) + 2
        
        # Header
        total_width = name_width + file_width + version_width + extends_width + 9
        click.echo("\n" + "─" * total_width)
        click.echo(
            f"{'Name':<{name_width}} │ {'File':<{file_width}} │ "
            f"{'Version':<{version_width}} │ {'Extends':<{extends_width}}"
        )
        click.echo("─" * total_width)
        
        # Rows
        for preset in presets_info:
            name = preset["name"]
            file = preset["file"]
            version = preset["version"]
            extends = preset["extends"] or "-"
            click.echo(
                f"{name:<{name_width}} │ {file:<{file_width}} │ "
                f"{version:<{version_width}} │ {extends:<{extends_width}}"
            )
        
        click.echo("─" * total_width)
    
    def _display_preset_descriptions(self, presets_info: List[Dict[str, Any]]) -> None:
        """Display preset descriptions."""
        click.echo("\n📝 Descriptions:")
        for preset in presets_info:
            if preset["description"] != "No description":
                click.echo(f"\n{preset['name']}:")
                click.echo(f"   {preset['description']}")
    
    def _display_templates_table(self, templates_info: List[Dict[str, Any]]) -> None:
        """Display templates in table format."""
        if not templates_info:
            return
        
        # Calculate column widths
        name_width = max(len(t["name"]) for t in templates_info) + 2
        file_width = max(len(t["file"]) for t in templates_info) + 2
        version_width = 10
        params_width = 12
        actions_width = 10
        
        # Header
        total_width = name_width + file_width + version_width + params_width + actions_width + 12
        click.echo("\n" + "─" * total_width)
        click.echo(
            f"{'Name':<{name_width}} │ {'File':<{file_width}} │ "
            f"{'Version':<{version_width}} │ {'Params':<{params_width}} │ {'Actions':<{actions_width}}"
        )
        click.echo("─" * total_width)
        
        # Rows
        for template in templates_info:
            name = template["name"]
            file = template["file"] 
            version = template["version"]
            params = template["params"]
            actions = str(template["actions"])
            click.echo(
                f"{name:<{name_width}} │ {file:<{file_width}} │ "
                f"{version:<{version_width}} │ {params:<{params_width}} │ {actions:<{actions_width}}"
            )
        
        click.echo("─" * total_width)
    
    def _display_template_details(self, template_files: List[Path]) -> None:
        """Display detailed template information including parameters."""
        parser = YAMLParser()
        
        click.echo("\n📝 Template Details:")
        for template_file in sorted(template_files):
            try:
                template = parser.parse_template_raw(template_file)
                click.echo(f"\n{template.name}:")
                if template.description:
                    click.echo(f"   Description: {template.description}")
                
                if template.parameters:
                    click.echo("   Parameters:")
                    for param in template.parameters:
                        param_name = param.get("name", "unknown")
                        param_type = param.get("type", "string")
                        param_required = "required" if param.get("required", False) else "optional"
                        param_desc = param.get("description", "")
                        default = param.get("default")
                        
                        click.echo(f"      • {param_name} ({param_type}, {param_required})")
                        if param_desc:
                            click.echo(f"        {param_desc}")
                        if default is not None:
                            click.echo(f"        Default: {default}")
                
            except Exception:
                pass  # Already logged during parsing
    
    def _display_template_usage_help(self) -> None:
        """Display template usage help information."""
        click.echo("\n💡 Use templates in your flowgroup configuration:")
        click.echo("   use_template: template_name")
        click.echo("   template_parameters:")
        click.echo("     param1: value1")
