"""LakehousePlumber CLI - Main entry point."""

# JUSTIFIED: §3.3 — this module is oversize (>500 lines) pending the CLI presenter
# extraction tracked as CLI-PRESENTER-DEFER (see CHANGELOG [Unreleased]). It was
# not refactored in the architecture-residual-cleanup change. Remove this block
# once the presenter extraction lands and the file drops back under 500 lines.

import logging

import rich_click as click

from ._project_root import _find_project_root
from .error_boundary import cli_error_boundary
from .logging_config import configure_logging

try:
    from importlib.metadata import version
except ImportError:
    try:
        from importlib_metadata import version  # type: ignore
    except Exception:  # pragma: no cover - best-effort fallback

        def version(package: str) -> str:  # type: ignore
            return "0.0.0"


def get_version():
    try:
        return version("lakehouse-plumber")
    except Exception:
        try:
            import re
            from pathlib import Path

            current_dir = Path(__file__).parent
            for _ in range(5):
                pyproject_path = current_dir / "pyproject.toml"
                if pyproject_path.exists():
                    with open(pyproject_path, "r") as f:
                        content = f.read()
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                    break
                current_dir = current_dir.parent
        except Exception as e:
            logging.getLogger(__name__).debug(
                f"Could not read version from pyproject.toml: {e}"
            )

        return "0.2.11"


@click.group()
@click.version_option(version=get_version(), prog_name="lhp")
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Enable verbose console logging",
)
@click.option(
    "--log-file",
    is_flag=True,
    help="Write a detailed DEBUG log to .lhp/logs/lhp.log (off by default).",
)
@click.option("--perf", is_flag=True, hidden=True)
def cli(verbose, perf, log_file):
    """LakehousePlumber - Generate Lakeflow pipelines from YAML configs."""
    project_root = _find_project_root()
    log_file_path = configure_logging(verbose, project_root, log_to_file=log_file)

    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["log_file"] = log_file_path
    ctx.obj["perf"] = perf

    if perf:
        from ..utils.performance_timer import enable_perf_timing

        enable_perf_timing(project_root)


@cli.command()
@click.argument("project_name")
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Skip Databricks Asset Bundle setup (bundle is enabled by default)",
)
@cli_error_boundary("Project initialization")
def init(project_name, no_bundle):
    """Initialize a new LakehousePlumber project in the current directory.

    PROJECT_NAME is used for template rendering (e.g. bundle name, lhp.yaml).
    All files are created in the current working directory.
    """
    from .commands.init_command import InitCommand

    InitCommand().execute(project_name, bundle=not no_bundle)


@cli.group("skill")
def skill() -> None:
    """Manage the LHP Claude Code skill installation."""


@skill.command("install")
@click.option(
    "--user",
    is_flag=True,
    help="Install to ~/.claude/skills/lhp/ instead of <cwd>/.claude/skills/lhp/",
)
@click.option(
    "--force",
    is_flag=True,
    help="Overwrite an existing install without prompting",
)
@cli_error_boundary("Skill management")
def skill_install(user: bool, force: bool) -> None:
    """Install the LHP Claude Code skill."""
    from .commands.skill_command import SkillCommand

    SkillCommand().install(user=user, force=force)


@skill.command("update")
@click.option(
    "--user",
    is_flag=True,
    help="Update the install at ~/.claude/skills/lhp/",
)
@click.option(
    "--yes",
    is_flag=True,
    help="Skip confirmation when installed version is newer than the CLI",
)
@cli_error_boundary("Skill management")
def skill_update(user: bool, yes: bool) -> None:
    """Update the installed LHP skill to the current LHP version."""
    from .commands.skill_command import SkillCommand

    SkillCommand().update(user=user, yes=yes)


@skill.command("status")
@click.option(
    "--user",
    is_flag=True,
    help="Check the install at ~/.claude/skills/lhp/",
)
@cli_error_boundary("Skill management")
def skill_status(user: bool) -> None:
    """Show installed skill version and drift state."""
    from .commands.skill_command import SkillCommand

    SkillCommand().status(user=user)


@skill.command("uninstall")
@click.option(
    "--user",
    is_flag=True,
    help="Remove the install at ~/.claude/skills/lhp/",
)
@click.option(
    "--force",
    is_flag=True,
    help="Skip the confirmation prompt",
)
@cli_error_boundary("Skill management")
def skill_uninstall(user: bool, force: bool) -> None:
    """Remove the LHP Claude Code skill installation."""
    from .commands.skill_command import SkillCommand

    SkillCommand().uninstall(user=user, force=force)


@cli.command()
@click.option("--env", "-e", required=True, help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to generate")
@click.option("--output", "-o", help="Output directory (defaults to generated/{env})")
@click.option("--dry-run", is_flag=True, help="Preview without generating files")
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Deprecated no-op; retained for backwards compatibility.",
)
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Disable bundle support even if databricks.yml exists",
)
@click.option(
    "--include-tests",
    is_flag=True,
    default=False,
    help="Include test actions in generation (skipped by default for faster builds)",
)
@click.option(
    "--no-format",
    "no_format",
    is_flag=True,
    default=False,
    help=(
        "Skip the terminal code-formatting pass over generated Python "
        "(faster builds). Overrides the lhp.yaml 'apply_formatting' key. "
        "The generated-code validity guard (LHP-CFG-031) always runs."
    ),
)
@click.option(
    "--pipeline-config",
    "-pc",
    help="Custom pipeline config file path (relative to project root)",
)
@click.option(
    "--max-workers",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Maximum worker processes. Default: ~80 percent of detected CPU count "
        "(honors cgroup limits on Linux), capped at the workload size. "
        "Override with the LHP_MAX_WORKERS env var. Use 1 for sequential."
    ),
)
@click.option(
    "--show-all",
    "-a",
    is_flag=True,
    default=False,
    help=(
        "Show every pipeline in the summary table (default: failed pipelines "
        "only). Note: --verbose/-v controls console verbosity, not the "
        "summary table."
    ),
)
@click.option(
    "--no-state",
    "no_state",
    is_flag=True,
    default=False,
    help="Deprecated no-op; retained for backwards compatibility.",
)
@cli_error_boundary("Code generation")
def generate(
    env,
    pipeline,
    output,
    dry_run,
    force,
    no_bundle,
    include_tests,
    no_format,
    pipeline_config,
    max_workers,
    show_all,
    no_state,
):
    """Generate DLT pipeline code"""
    from .commands.generate_command import GenerateCommand

    GenerateCommand().execute(
        env,
        pipeline,
        output,
        dry_run,
        no_bundle,
        include_tests,
        pipeline_config,
        max_workers=max_workers,
        show_all=show_all,
        force=force,
        no_state=no_state,
        no_format=no_format,
    )


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to validate")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
@click.option(
    "--include-tests",
    is_flag=True,
    default=False,
    help="Include test actions in validation (matches generate behavior)",
)
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Disable bundle support even if databricks.yml exists",
)
@click.option(
    "--pipeline-config",
    "-pc",
    help="Custom pipeline config file path (relative to project root)",
)
@click.option(
    "--max-workers",
    type=click.IntRange(min=1),
    default=None,
    help=(
        "Maximum worker processes. Default: ~80 percent of detected CPU count "
        "(honors cgroup limits on Linux), capped at the workload size. "
        "Override with the LHP_MAX_WORKERS env var. Use 1 for sequential."
    ),
)
@click.option(
    "--show-all",
    "-a",
    is_flag=True,
    default=False,
    help=(
        "Show every pipeline in the summary table (default: failed pipelines "
        "only). Note: --verbose/-v controls console verbosity, not the "
        "summary table."
    ),
)
@cli_error_boundary("Pipeline validation")
def validate(
    env,
    pipeline,
    verbose,
    include_tests,
    no_bundle,
    pipeline_config,
    max_workers,
    show_all,
):
    """Validate pipeline configurations"""
    from .commands.validate_command import ValidateCommand

    ValidateCommand().execute(
        env,
        pipeline,
        verbose,
        include_tests,
        no_bundle,
        pipeline_config,
        max_workers=max_workers,
        show_all=show_all,
    )


@cli.command()
@click.option("--pipeline", "-p", help="Specific pipeline to analyze")
@cli_error_boundary("Pipeline statistics")
def stats(pipeline):
    """Display pipeline statistics and complexity metrics."""
    from .commands.stats_command import StatsCommand

    StatsCommand().execute(pipeline)


@cli.command()
@cli_error_boundary("List presets")
def list_presets():
    """List available presets"""
    from .commands.list_commands import ListCommand

    ListCommand().list_presets()


@cli.command()
@cli_error_boundary("List templates")
def list_templates():
    """List available templates"""
    from .commands.list_commands import ListCommand

    ListCommand().list_templates()


@cli.command()
@click.option(
    "--verbose",
    "-v",
    is_flag=True,
    help="Show each instance and the resolved pipelines it produces",
)
@cli_error_boundary("List blueprints")
def list_blueprints(verbose):
    """List available blueprints with parameter and instance counts."""
    from .commands.list_commands import ListCommand

    ListCommand().list_blueprints(verbose=verbose)


@cli.command()
@click.argument("flowgroup", required=False)
@click.option("--env", "-e", default="dev", help="Environment")
@click.option(
    "--instance",
    "instance_path",
    default=None,
    help=(
        "Path to a blueprint instance file; expands only that instance and "
        "displays the resolved flowgroups (M4)."
    ),
)
@cli_error_boundary("Show flowgroup")
def show(flowgroup, env, instance_path):
    """Show resolved configuration for a flowgroup or blueprint instance."""
    from .commands.show_command import ShowCommand

    ShowCommand().execute(flowgroup, env, instance_path)


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@cli_error_boundary("Show substitutions")
def substitutions(env):
    """Show available substitution tokens for an environment"""
    from .commands.show_command import ShowCommand

    ShowCommand().show_substitutions(env)


@cli.command()
@cli_error_boundary("Project info")
def info():
    """Display project information and statistics."""
    from .commands.show_command import ShowCommand

    ShowCommand().show_project_info()


@cli.command()
@click.option(
    "--format",
    "-f",
    type=click.Choice(["dot", "json", "text", "job", "all"], case_sensitive=False),
    default="all",
    help="Output format(s) to generate (dot=GraphViz, json=structured data, text=readable report, job=orchestration job)",
)
@click.option(
    "--output",
    "-o",
    type=click.Path(),
    help="Output directory (defaults to .lhp/dependencies/)",
)
@click.option("--pipeline", "-p", help="Analyze specific pipeline only")
@click.option(
    "--job-name",
    "-j",
    help="Custom name for generated orchestration job (only used with job format)",
)
@click.option(
    "--job-config",
    "-jc",
    help="Custom job config file path (relative to project root, defaults to templates/bundle/job_config.yaml)",
)
@click.option(
    "--bundle-output",
    "-b",
    is_flag=True,
    help="Save job file to resources/ directory for Databricks bundle integration",
)
@click.option(
    "--expand-blueprints",
    is_flag=True,
    default=False,
    help=(
        "Emit one flowgroup per (blueprint x instance x spec) instead of "
        "deduping. Default behavior collapses synthetic flowgroups by "
        "(blueprint_name, spec_index) to keep the graph readable at scale."
    ),
)
@click.option(
    "--blueprint",
    "blueprint_name",
    help=(
        "Restrict the dependency graph to flowgroups expanded from the named "
        "blueprint. Combine with --expand-blueprints to see per-instance edges."
    ),
)
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose output")
@cli_error_boundary("Dependency analysis")
def deps(
    format,
    output,
    pipeline,
    job_name,
    job_config,
    bundle_output,
    expand_blueprints,
    blueprint_name,
    verbose,
):
    """Analyze and visualize pipeline dependencies for orchestration planning."""
    from .commands.dependencies_command import DependenciesCommand

    DependenciesCommand().execute(
        format,
        output,
        pipeline,
        job_name,
        job_config,
        bundle_output,
        verbose,
        expand_blueprints=expand_blueprints,
        blueprint_filter=blueprint_name,
    )


if __name__ == "__main__":
    cli()
