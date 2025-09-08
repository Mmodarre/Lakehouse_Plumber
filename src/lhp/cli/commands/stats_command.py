"""Stats command implementation for LakehousePlumber CLI."""

import logging
from typing import Optional, Dict, Set
from collections import defaultdict
from pathlib import Path
import click

from .base_command import BaseCommand
from ...parsers.yaml_parser import YAMLParser

logger = logging.getLogger(__name__)


class StatsCommand(BaseCommand):
    """
    Handles pipeline statistics and complexity metrics command.
    
    Analyzes pipeline configurations to provide statistics on flowgroups,
    actions, complexity metrics, and resource usage patterns.
    """
    
    def execute(self, pipeline: Optional[str] = None) -> None:
        """
        Execute the stats command.
        
        Args:
            pipeline: Specific pipeline to analyze (optional)
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()
        parser = YAMLParser()
        
        click.echo("📊 Pipeline Statistics")
        click.echo("=" * 60)
        
        # Determine which pipelines to analyze
        pipeline_dirs = self._get_pipeline_directories(project_root, pipeline)
        if not pipeline_dirs:
            return
        
        # Collect statistics across all pipelines
        total_stats = self._initialize_stats(len(pipeline_dirs))
        
        # Get include patterns for YAML file filtering
        include_patterns = self._get_include_patterns(project_root)
        
        # Analyze each pipeline
        for pipeline_dir in pipeline_dirs:
            self._analyze_pipeline(
                pipeline_dir, parser, include_patterns, total_stats,
                single_pipeline=len(pipeline_dirs) == 1
            )
        
        # Display comprehensive statistics
        self._display_statistics_summary(total_stats)
    
    def _get_pipeline_directories(self, project_root: Path, 
                                 pipeline: Optional[str]) -> list[Path]:
        """Get pipeline directories to analyze."""
        pipelines_dir = project_root / "pipelines"
        if not pipelines_dir.exists():
            click.echo("❌ No pipelines directory found")
            return []
        
        if pipeline:
            pipeline_dir = pipelines_dir / pipeline
            if not pipeline_dir.exists():
                click.echo(f"❌ Pipeline '{pipeline}' not found")
                return []
            return [pipeline_dir]
        else:
            pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]
            if not pipeline_dirs:
                click.echo("❌ No pipeline directories found")
                return []
            return pipeline_dirs
    
    def _initialize_stats(self, pipeline_count: int) -> Dict:
        """Initialize statistics collection dictionary."""
        return {
            "pipelines": pipeline_count,
            "flowgroups": 0,
            "actions": 0,
            "load_actions": 0,
            "transform_actions": 0,
            "write_actions": 0,
            "secret_refs": 0,
            "templates_used": set(),
            "presets_used": set(),
            "action_types": defaultdict(int),
        }
    
    def _get_include_patterns(self, project_root: Path) -> list[str]:
        """Get include patterns from project configuration."""
        try:
            from ...core.project_config_loader import ProjectConfigLoader
            config_loader = ProjectConfigLoader(project_root)
            project_config = config_loader.load_project_config()
            
            if project_config and project_config.include:
                return project_config.include
            else:
                return []
        except Exception as e:
            self.logger.warning(f"Could not load project config for include patterns: {e}")
            return []
    
    def _discover_yaml_files_with_include(self, pipeline_dir: Path, 
                                         include_patterns: list[str]) -> list[Path]:
        """Discover YAML files with include pattern filtering."""
        if include_patterns:
            # Use include filtering
            from ...utils.file_pattern_matcher import discover_files_with_patterns
            return discover_files_with_patterns(pipeline_dir, include_patterns)
        else:
            # No include patterns, discover all YAML files
            yaml_files = []
            yaml_files.extend(pipeline_dir.rglob("*.yaml"))
            yaml_files.extend(pipeline_dir.rglob("*.yml"))
            return yaml_files
    
    def _analyze_pipeline(self, pipeline_dir: Path, parser: YAMLParser,
                         include_patterns: list[str], total_stats: Dict,
                         single_pipeline: bool = False) -> None:
        """Analyze a single pipeline directory and update statistics."""
        pipeline_name = pipeline_dir.name
        flowgroup_files = self._discover_yaml_files_with_include(pipeline_dir, include_patterns)
        
        if single_pipeline:
            click.echo(f"\n📁 Pipeline: {pipeline_name}")
            click.echo("-" * 40)
        
        pipeline_actions = 0
        
        for yaml_file in flowgroup_files:
            try:
                flowgroup = parser.parse_flowgroup(yaml_file)
                total_stats["flowgroups"] += 1
                
                # Count actions by type
                for action in flowgroup.actions:
                    total_stats["actions"] += 1
                    pipeline_actions += 1
                    
                    # Count by action type
                    if action.type.value == "load":
                        total_stats["load_actions"] += 1
                    elif action.type.value == "transform":
                        total_stats["transform_actions"] += 1
                    elif action.type.value == "write":
                        total_stats["write_actions"] += 1
                    
                    # Track action subtypes for detailed breakdown
                    if action.type.value == "load" and isinstance(action.source, dict):
                        subtype = action.source.get("type", "unknown")
                        total_stats["action_types"][f"load_{subtype}"] += 1
                    elif action.type.value == "transform" and action.transform_type:
                        total_stats["action_types"][f"transform_{action.transform_type}"] += 1
                
                # Track presets and templates used
                if flowgroup.presets:
                    for preset in flowgroup.presets:
                        total_stats["presets_used"].add(preset)
                
                if flowgroup.use_template:
                    total_stats["templates_used"].add(flowgroup.use_template)
                
                # Show flowgroup details for single pipeline analysis
                if single_pipeline:
                    click.echo(
                        f"   FlowGroup: {flowgroup.flowgroup} ({len(flowgroup.actions)} actions)"
                    )
                    
            except Exception as e:
                logger.warning(f"Could not parse {yaml_file}: {e}")
                continue
        
        if single_pipeline:
            click.echo(f"   Total actions: {pipeline_actions}")
    
    def _display_statistics_summary(self, total_stats: Dict) -> None:
        """Display comprehensive statistics summary."""
        # Main statistics
        click.echo("\n📈 Summary Statistics:")
        click.echo(f"   Total pipelines: {total_stats['pipelines']}")
        click.echo(f"   Total flowgroups: {total_stats['flowgroups']}")
        click.echo(f"   Total actions: {total_stats['actions']}")
        click.echo(f"      • Load actions: {total_stats['load_actions']}")
        click.echo(f"      • Transform actions: {total_stats['transform_actions']}")
        click.echo(f"      • Write actions: {total_stats['write_actions']}")
        
        # Action type breakdown
        if total_stats["action_types"]:
            click.echo("\n📊 Action Type Breakdown:")
            for action_type, count in sorted(total_stats["action_types"].items()):
                click.echo(f"   {action_type}: {count}")
        
        # Resources used
        if total_stats["presets_used"]:
            click.echo(f"\n🔧 Presets Used: {', '.join(sorted(total_stats['presets_used']))}")
        
        if total_stats["templates_used"]:
            click.echo(f"\n📝 Templates Used: {', '.join(sorted(total_stats['templates_used']))}")
        
        # Complexity metrics
        if total_stats["flowgroups"] > 0:
            self._display_complexity_metrics(total_stats)
    
    def _display_complexity_metrics(self, total_stats: Dict) -> None:
        """Display complexity metrics based on action patterns."""
        avg_actions_per_flowgroup = total_stats["actions"] / total_stats["flowgroups"]
        click.echo("\n🧮 Complexity Metrics:")
        click.echo(f"   Average actions per flowgroup: {avg_actions_per_flowgroup:.1f}")
        
        # Determine complexity level
        if avg_actions_per_flowgroup < 3:
            complexity = "Low"
        elif avg_actions_per_flowgroup < 7:
            complexity = "Medium"
        else:
            complexity = "High"
        
        click.echo(f"   Overall complexity: {complexity}")
