"""LakehousePlumber CLI - Main entry point."""

import sys
import logging
from pathlib import Path
from typing import Optional, List
import click

# Import for dynamic version detection
try:
    from importlib.metadata import version
except ImportError:
    # Fallback for older Python versions where importlib.metadata is not available
    try:
        from importlib_metadata import version  # type: ignore
    except Exception:  # pragma: no cover - best-effort fallback
        def version(package: str) -> str:  # type: ignore
            return "0.0.0"


def get_version():
    """Get the package version dynamically from package metadata."""
    try:
        # Try to get version from installed package metadata
        return version("lakehouse-plumber")
    except Exception:
        try:
            # Fallback: try reading from pyproject.toml (for development)
            import re
            from pathlib import Path

            # Find pyproject.toml - look up the directory tree
            current_dir = Path(__file__).parent
            for _ in range(5):  # Look up to 5 levels
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    # Use regex to find version = "x.y.z"
                    version_match = re.search(r'version\s*=\s*["\']([^"\']+)["\']', content)
                    if version_match:
                        return version_match.group(1)
                    break
                current_dir = current_dir.parent
        except Exception:
            pass

        # Final fallback
        return "0.2.11"


def configure_logging(verbose: bool, project_root: Optional[Path] = None):
    """Configure logging for LakehousePlumber.
    
    Returns:
        str: Path to log file if created, None otherwise
    """
    # Clear any existing handlers from previous runs
    cleanup_logging()

    # Configure root logger
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG if verbose else logging.INFO)

    # Console handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.WARNING if not verbose else logging.DEBUG)
    console_formatter = logging.Formatter("%(levelname)s: %(message)s")
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # File handler (only if project root available)
    log_file_path = None
    if project_root:
        log_dir = project_root / ".lhp" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)
        log_file_path = log_dir / "lhp.log"
        
        file_handler = logging.FileHandler(log_file_path, mode='a', encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        file_handler.setFormatter(file_formatter)
        root_logger.addHandler(file_handler)

    return str(log_file_path) if log_file_path else None


def cleanup_logging():
    """Clean up logging handlers."""
    root_logger = logging.getLogger()
    # Remove all existing handlers
    for handler in root_logger.handlers[:]:
        handler.close()
        root_logger.removeHandler(handler)


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for lhp.yaml."""
    current = Path.cwd().resolve()

    # Check current directory and parent directories
    for path in [current] + list(current.parents):
        if (path / "lhp.yaml").exists():
            return path

    return None


def _ensure_project_root() -> Path:
    """Find project root or exit with error."""
    project_root = _find_project_root()
    if not project_root:
        click.echo("❌ Not in a LakehousePlumber project directory")
        click.echo("💡 Run 'lhp init <project_name>' to create a new project")
        click.echo("💡 Or navigate to an existing project directory")
        sys.exit(1)

    return project_root


def _load_project_config(project_root: Path) -> dict:
    """Load project configuration from lhp.yaml."""
    import yaml
    
    config_file = project_root / "lhp.yaml"
    if not config_file.exists():
        return {}

    from ..utils.yaml_loader import safe_load_yaml_with_fallback
    return safe_load_yaml_with_fallback(
        config_file, 
        fallback_value={},
        error_context="project configuration",
        log_errors=True
    )


def _get_include_patterns(project_root: Path) -> List[str]:
    """Get include patterns from project configuration."""
    try:
        from ..core.project_config_loader import ProjectConfigLoader
        config_loader = ProjectConfigLoader(project_root)
        project_config = config_loader.load_project_config()
        
        if project_config and project_config.include:
            return project_config.include
        else:
            return []
    except Exception as e:
        logging.getLogger(__name__).warning(f"Could not load project config for include patterns: {e}")
        return []


def _discover_yaml_files_with_include(pipelines_dir: Path, include_patterns: List[str] = None) -> List[Path]:
    """Discover YAML files with optional include pattern filtering."""
    if include_patterns:
        from ..utils.file_pattern_matcher import discover_files_with_patterns
        return discover_files_with_patterns(pipelines_dir, include_patterns)
    else:
        yaml_files = []
        yaml_files.extend(pipelines_dir.rglob("*.yaml"))
        yaml_files.extend(pipelines_dir.rglob("*.yml"))
        return yaml_files


# ============================================================================
# CLI Command Group and Routing
# ============================================================================

@click.group()
@click.version_option(version=get_version(), prog_name="lhp")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose):
    """LakehousePlumber - Generate Lakeflow pipelines from YAML configs."""
    # Try to find project root for better logging setup
    project_root = _find_project_root()
    log_file = configure_logging(verbose, project_root)

    # Store logging info in context for subcommands
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["log_file"] = log_file


# ============================================================================
# Command Routing - Delegate to Command Classes
# ============================================================================

@cli.command()
@click.argument("project_name")
@click.option("--bundle", is_flag=True, help="Initialize as a Databricks Asset Bundle project")
def init(project_name, bundle):
    """Initialize a new LakehousePlumber project with automatic VS Code IntelliSense setup"""
    from .commands.init_command import InitCommand
    InitCommand().execute(project_name, bundle)


@cli.command()
@click.option("--env", "-e", required=True, help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to generate")
@click.option("--output", "-o", help="Output directory (defaults to generated/{env})")
@click.option("--dry-run", is_flag=True, help="Preview without generating files")
@click.option("--no-cleanup", is_flag=True, help="Disable cleanup of generated files when source YAML files are removed.")
@click.option("--force", "-f", is_flag=True, help="Force regeneration of all files, even if unchanged")
@click.option("--no-bundle", is_flag=True, help="Disable bundle support even if databricks.yml exists")
@click.option("--include-tests", is_flag=True, default=False, help="Include test actions in generation (skipped by default for faster builds)")
@click.option("--pipeline-config", "-pc", help="Custom pipeline config file path (relative to project root)")
def generate(env, pipeline, output, dry_run, no_cleanup, force, no_bundle, include_tests, pipeline_config):
    """Generate DLT pipeline code"""
    from .commands.generate_command import GenerateCommand
    GenerateCommand().execute(env, pipeline, output, dry_run, no_cleanup, force, no_bundle, include_tests, pipeline_config)


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to validate")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def validate(env, pipeline, verbose):
    """Validate pipeline configurations"""
    from .commands.validate_command import ValidateCommand
    ValidateCommand().execute(env, pipeline, verbose)


@cli.command()
@click.option("--env", "-e", help="Environment to show state for")
@click.option("--pipeline", "-p", help="Specific pipeline to show state for")
@click.option("--orphaned", is_flag=True, help="Show only orphaned files")
@click.option("--stale", is_flag=True, help="Show only stale files (YAML changed)")
@click.option("--new", is_flag=True, help="Show only new/untracked YAML files")
@click.option("--dry-run", is_flag=True, help="Preview cleanup without actually deleting files")
@click.option("--cleanup", is_flag=True, help="Clean up orphaned files")
@click.option("--regen", is_flag=True, help="Regenerate stale files")
def state(env, pipeline, orphaned, stale, new, dry_run, cleanup, regen):
    """Show or manage the current state of generated files."""
    from .commands.state_command import StateCommand
    StateCommand().execute(env, pipeline, orphaned, stale, new, dry_run, cleanup, regen)


@cli.command()
@click.option("--pipeline", "-p", help="Specific pipeline to analyze")
def stats(pipeline):
    """Display pipeline statistics and complexity metrics."""
    from .commands.stats_command import StatsCommand
    StatsCommand().execute(pipeline)


@cli.command()
def list_presets():
    """List available presets"""
    from .commands.list_commands import ListCommand
    ListCommand().list_presets()


@cli.command()
def list_templates():
    """List available templates"""
    from .commands.list_commands import ListCommand
    ListCommand().list_templates()


@cli.command()
@click.argument("flowgroup")
@click.option("--env", "-e", default="dev", help="Environment")
def show(flowgroup, env):
    """Show resolved configuration for a flowgroup in table format"""
    from .commands.show_command import ShowCommand
    ShowCommand().show_flowgroup(flowgroup, env)


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
def substitutions(env):
    """Show available substitution tokens for an environment"""
    from .commands.show_command import ShowCommand
    ShowCommand().show_substitutions(env)


@cli.command()
def info():
    """Display project information and statistics."""
    from .commands.show_command import ShowCommand
    ShowCommand().show_project_info()


@cli.command()
@click.option("--format", "-f", type=click.Choice(["dot", "json", "text", "job", "all"], case_sensitive=False),
              default="all", help="Output format(s) to generate (dot=GraphViz, json=structured data, text=readable report, job=orchestration job)")
@click.option("--output", "-o", type=click.Path(),
              help="Output directory (defaults to .lhp/dependencies/)")
@click.option("--pipeline", "-p", help="Analyze specific pipeline only")
@click.option("--job-name", "-j", help="Custom name for generated orchestration job (only used with job format)")
@click.option("--job-config", "-jc", help="Custom job config file path (relative to project root, defaults to templates/bundle/job_config.yaml)")
@click.option("--bundle-output", "-b", is_flag=True, help="Save job file to resources/ directory for Databricks bundle integration")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
def deps(format, output, pipeline, job_name, job_config, bundle_output, verbose):
    """Analyze and visualize pipeline dependencies for orchestration planning."""
    from .commands.dependencies_command import DependenciesCommand
    DependenciesCommand().execute(format, output, pipeline, job_name, job_config, bundle_output, verbose)


# ============================================================================
# Entry Point
# ============================================================================

if __name__ == "__main__":
    cli()