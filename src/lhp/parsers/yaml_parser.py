import yaml
from pathlib import Path
from typing import Dict, Any, List
from ..models.config import FlowGroup, Template, Preset
from ..utils.error_formatter import LHPError


class YAMLParser:
    """Parse and validate YAML configuration files."""

    def __init__(self):
        pass

    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a single YAML file."""
        # Import here to avoid circular imports
        try:
            from ..utils.error_formatter import LHPError
        except ImportError:
            LHPError = None
            
        from ..utils.yaml_loader import load_yaml_file
        try:
            content = load_yaml_file(file_path, error_context=f"YAML file {file_path}")
            return content or {}
        except Exception as e:
            # Check if it's an LHPError that should be re-raised
            if LHPError and isinstance(e, LHPError):
                raise  # Re-raise LHPError as-is
            elif isinstance(e, ValueError):
                # For backward compatibility, convert back to generic error for non-LHPErrors
                if "File not found" in str(e):
                    raise ValueError(f"Error reading {file_path}: {e}")
                raise  # Re-raise ValueError as-is for YAML errors
            else:
                raise ValueError(f"Error reading {file_path}: {e}")

    def parse_flowgroup(self, file_path: Path) -> FlowGroup:
        """Parse a FlowGroup YAML file."""
        content = self.parse_file(file_path)
        return FlowGroup(**content)

    def parse_template(self, file_path: Path) -> Template:
        """Parse a Template YAML file.
        
        .. deprecated:: 0.6.3
            Use :func:`parse_template_raw` instead. This method will be removed in v0.7.0.
        """
        import warnings
        warnings.warn(
            "parse_template() is deprecated and will be removed in v0.7.0. "
            "Use parse_template_raw() instead.",
            DeprecationWarning,
            stacklevel=2
        )
        content = self.parse_file(file_path)
        return Template(**content)
    
    def parse_template_raw(self, file_path: Path) -> Template:
        """Parse a Template YAML file with raw actions (no Action object creation).
        
        This is used during template loading to avoid validation of template syntax
        like {{ table_properties }}. Actions will be validated later during rendering
        when actual parameter values are available.
        """
        content = self.parse_file(file_path)
        
        # Create template with raw actions
        raw_actions = content.pop('actions', [])
        template = Template(**content, actions=raw_actions)
        template._raw_actions = True  # Set flag after creation
        return template

    def parse_preset(self, file_path: Path) -> Preset:
        """Parse a Preset YAML file."""
        content = self.parse_file(file_path)
        return Preset(**content)

    def discover_flowgroups(self, pipelines_dir: Path, include_patterns: List[str] = None) -> List[FlowGroup]:
        """Discover all FlowGroup files in pipelines directory.
        
        Args:
            pipelines_dir: Directory containing flowgroup YAML files
            include_patterns: Optional list of glob patterns to filter files
            
        Returns:
            List of discovered flowgroups
        """
        flowgroups = []
        
        if include_patterns:
            # Use include filtering
            from ..utils.file_pattern_matcher import discover_files_with_patterns
            yaml_files = discover_files_with_patterns(pipelines_dir, include_patterns)
        else:
            # No include patterns, discover all YAML files (backwards compatibility)
            yaml_files = []
            yaml_files.extend(pipelines_dir.rglob("*.yaml"))
            yaml_files.extend(pipelines_dir.rglob("*.yml"))
        
        for yaml_file in yaml_files:
            if yaml_file.is_file():
                try:
                    flowgroup = self.parse_flowgroup(yaml_file)
                    flowgroups.append(flowgroup)
                except Exception as e:
                    print(f"Warning: Could not parse {yaml_file}: {e}")
        return flowgroups

    def discover_templates(self, templates_dir: Path) -> List[Template]:
        """Discover all Template files.
        
        .. deprecated:: 0.6.3
            This method is unused and will be removed in v0.7.0.
        """
        import warnings
        warnings.warn(
            "discover_templates() is deprecated and will be removed in v0.7.0.",
            DeprecationWarning,
            stacklevel=2
        )
        templates = []
        for yaml_file in templates_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    template = self.parse_template(yaml_file)
                    templates.append(template)
                except Exception as e:
                    print(f"Warning: Could not parse template {yaml_file}: {e}")
        return templates

    def discover_presets(self, presets_dir: Path) -> List[Preset]:
        """Discover all Preset files."""
        presets = []
        for yaml_file in presets_dir.glob("*.yaml"):
            if yaml_file.is_file():
                try:
                    preset = self.parse_preset(yaml_file)
                    presets.append(preset)
                except Exception as e:
                    print(f"Warning: Could not parse preset {yaml_file}: {e}")
        return presets
