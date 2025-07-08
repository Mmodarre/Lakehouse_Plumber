import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from ..models.config import FlowGroup, Template, Preset
from ..utils.error_formatter import LHPError

class YAMLParser:
    """Parse and validate YAML configuration files."""
    
    def __init__(self):
        self.loaded_configs = {}
    
    def parse_file(self, file_path: Path) -> Dict[str, Any]:
        """Parse a single YAML file."""
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = yaml.safe_load(f)
            return content or {}
        except yaml.YAMLError as e:
            raise ValueError(f"Invalid YAML in {file_path}: {e}")
        except LHPError:
            # Re-raise LHPError as-is (it's already well-formatted)
            raise
        except Exception as e:
            raise ValueError(f"Error reading {file_path}: {e}")
    
    def parse_flowgroup(self, file_path: Path) -> FlowGroup:
        """Parse a FlowGroup YAML file."""
        content = self.parse_file(file_path)
        return FlowGroup(**content)
    
    def parse_template(self, file_path: Path) -> Template:
        """Parse a Template YAML file."""
        content = self.parse_file(file_path)
        return Template(**content)
    
    def parse_preset(self, file_path: Path) -> Preset:
        """Parse a Preset YAML file."""
        content = self.parse_file(file_path)
        return Preset(**content)
    
    def discover_flowgroups(self, pipelines_dir: Path) -> List[FlowGroup]:
        """Discover all FlowGroup files in pipelines directory."""
        flowgroups = []
        for yaml_file in pipelines_dir.rglob("*.yaml"):
            if yaml_file.is_file():
                try:
                    flowgroup = self.parse_flowgroup(yaml_file)
                    flowgroups.append(flowgroup)
                except Exception as e:
                    print(f"Warning: Could not parse {yaml_file}: {e}")
        return flowgroups
    
    def discover_templates(self, templates_dir: Path) -> List[Template]:
        """Discover all Template files."""
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