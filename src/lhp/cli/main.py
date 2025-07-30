"""LakehousePlumber CLI - Main entry point."""

import sys
import logging
from pathlib import Path
from typing import Optional, List
from collections import defaultdict
import click
import yaml

from ..core.orchestrator import ActionOrchestrator
from ..core.state_manager import StateManager
from ..core.init_template_loader import InitTemplateLoader
from ..core.init_template_context import InitTemplateContext
from ..utils.substitution import EnhancedSubstitutionManager
from ..parsers.yaml_parser import YAMLParser
from ..models.config import ActionType
from ..utils.error_handler import ErrorHandler
from ..utils.bundle_detection import should_enable_bundle_support
from ..bundle.manager import BundleManager
from ..bundle.exceptions import BundleResourceError

# Import for dynamic version detection
try:
    from importlib.metadata import version
except ImportError:
    # Fallback for Python < 3.8
    from importlib_metadata import version


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
                    version_match = re.search(
                        r'version\s*=\s*["\']([^"\']+)["\']', content
                    )
                    if version_match:
                        return version_match.group(1)
                current_dir = current_dir.parent
        except Exception:
            pass

        # Final fallback
        return "0.2.11"


# Configure logging
logger = logging.getLogger(__name__)


def configure_logging(verbose: bool, project_root: Optional[Path] = None):
    """Configure logging with clean console output and detailed file logging."""

    # Create logs directory in project if project_root is provided
    if project_root:
        logs_dir = project_root / ".lhp" / "logs"
        logs_dir.mkdir(parents=True, exist_ok=True)
        log_file = logs_dir / "lhp.log"
    else:
        # Fallback to temp directory if no project root
        import tempfile

        log_file = Path(tempfile.gettempdir()) / "lhp.log"

    # Remove any existing handlers
    root_logger = logging.getLogger()
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Set root logger level to capture everything
    root_logger.setLevel(logging.DEBUG)

    # File handler - logs everything with detailed format
    file_handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    # Console handler - only warnings and errors by default
    console_handler = logging.StreamHandler()
    if verbose:
        console_handler.setLevel(logging.INFO)
        console_formatter = logging.Formatter("🔧 %(levelname)s: %(message)s")
    else:
        console_handler.setLevel(logging.WARNING)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")

    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)

    # Log the setup
    logger.info(
        f"Logging initialized - File: {log_file}, Console level: {'INFO' if verbose else 'WARNING'}"
    )

    return log_file


def cleanup_logging():
    """Clean up logging handlers to ensure proper file closure on Windows."""
    root_logger = logging.getLogger()
    
    # Close and remove all handlers
    for handler in root_logger.handlers[:]:
        try:
            handler.close()
        except Exception:
            pass  # Ignore errors during cleanup
        root_logger.removeHandler(handler)


@click.group()
@click.version_option(version=get_version(), prog_name="lhp")
@click.option("--verbose", "-v", is_flag=True, help="Enable verbose logging")
def cli(verbose):
    """LakehousePlumber - Generate Lakeflow Pipeliness pipelines from YAML configs."""
    # Try to find project root for better logging setup
    project_root = _find_project_root()
    log_file = configure_logging(verbose, project_root)

    # Store logging info in context for subcommands
    ctx = click.get_current_context()
    ctx.ensure_object(dict)
    ctx.obj["verbose"] = verbose
    ctx.obj["log_file"] = log_file


@cli.command()
@click.argument("project_name")
@click.option(
    "--bundle",
    is_flag=True,
    help="Initialize as a Databricks Asset Bundle project",
)
def init(project_name, bundle):
    """Initialize a new LakehousePlumber project with automatic VS Code IntelliSense setup"""
    project_path = Path(project_name)
    if project_path.exists():
        click.echo(f"❌ Directory {project_name} already exists")
        sys.exit(1)

    # Create project structure
    project_path.mkdir()

    # Create directories
    directories = [
        "presets",
        "templates",
        "pipelines",
        "substitutions",
        "schemas",
        "expectations",
        "generated",
    ]
    for dir_name in directories:
        (project_path / dir_name).mkdir()

    # Add resources directory for bundle projects
    if bundle:
        resources_lhp_dir = project_path / "resources" / "lhp"
        resources_lhp_dir.mkdir(parents=True, exist_ok=True)

    # Create template context
    context = InitTemplateContext.create(
        project_name=project_name,
        bundle_enabled=bundle,
        author=""  # Empty by default as in original code
    )

    # Initialize template loader and create all project files
    try:
        template_loader = InitTemplateLoader()
        template_loader.create_project_files(project_path, context)
    except Exception as e:
        click.echo(f"❌ Failed to create project files: {e}")
        # Clean up on failure
        import shutil
        if project_path.exists():
            shutil.rmtree(project_path)
        sys.exit(1)

    # Display success message
    if bundle:
        click.echo(f"✅ Initialized Databricks Asset Bundle project: {project_name}")
        click.echo(f"📁 Created directories: {', '.join(directories)}, resources")
        click.echo(
            "📄 Created example files: presets/bronze_layer.yaml, templates/standard_ingestion.yaml, databricks.yml"
        )
        click.echo("🔧 VS Code IntelliSense automatically configured for YAML files")
        click.echo("\n🚀 Next steps:")
        click.echo(f"   cd {project_name}")
        click.echo("   # Create your first pipeline")
        click.echo("   mkdir pipelines/my_pipeline")
        click.echo("   # Add flowgroup configurations")
        click.echo("   # Deploy bundle with: databricks bundle deploy")
    else:
        click.echo(f"✅ Initialized LakehousePlumber project: {project_name}")
        click.echo(f"📁 Created directories: {', '.join(directories)}")
        click.echo(
            "📄 Created example files: presets/bronze_layer.yaml, templates/standard_ingestion.yaml"
        )
        click.echo("🔧 VS Code IntelliSense automatically configured for YAML files")
        click.echo("\n🚀 Next steps:")
        click.echo(f"   cd {project_name}")
        click.echo("   # Create your first pipeline")
        click.echo("   mkdir pipelines/my_pipeline")
        click.echo("   # Add flowgroup configurations")


@cli.command()
@click.option("--env", "-e", default="dev", help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to validate")
@click.option("--verbose", "-v", is_flag=True, help="Verbose output")
def validate(env, pipeline, verbose):
    """Validate pipeline configurations"""
    project_root = _ensure_project_root()

    # Get context info
    ctx = click.get_current_context()
    log_file = ctx.obj.get("log_file") if ctx.obj else None

    click.echo(f"🔍 Validating pipeline configurations for environment: {env}")
    if verbose and log_file:
        click.echo(f"📝 Detailed logs: {log_file}")

    # Check if substitution file exists
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        click.echo(f"❌ Substitution file not found: {substitution_file}")
        sys.exit(1)

    # Initialize orchestrator instead of validator
    orchestrator = ActionOrchestrator(project_root)

    # Determine which pipelines to validate (using pipeline field, not directory names)
    pipelines_to_validate = []
    if pipeline:
        # Check if pipeline field exists in flowgroups
        all_flowgroups = orchestrator.discover_all_flowgroups()
        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        
        if pipeline not in pipeline_fields:
            click.echo(f"❌ Pipeline field '{pipeline}' not found in any flowgroup")
            if pipeline_fields:
                click.echo(f"💡 Available pipeline fields: {sorted(pipeline_fields)}")
            sys.exit(1)
        pipelines_to_validate = [pipeline]
    else:
        # Discover all pipeline fields from flowgroups
        all_flowgroups = orchestrator.discover_all_flowgroups()
        if not all_flowgroups:
            click.echo("❌ No flowgroups found in project")
            sys.exit(1)

        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        pipelines_to_validate = sorted(pipeline_fields)

    # Track validation results
    total_errors = 0
    total_warnings = 0
    validated_pipelines = 0

    # Validate each pipeline
    for pipeline_name in pipelines_to_validate:
        click.echo(f"\n🔧 Validating pipeline: {pipeline_name}")

        try:
            # Validate pipeline using orchestrator by field
            errors, warnings = orchestrator.validate_pipeline_by_field(pipeline_name, env)

            validated_pipelines += 1
            pipeline_errors = len(errors)
            pipeline_warnings = len(warnings)
            total_errors += pipeline_errors
            total_warnings += pipeline_warnings

            # Show results
            if pipeline_errors == 0 and pipeline_warnings == 0:
                click.echo(f"✅ Pipeline '{pipeline_name}' is valid")
            else:
                if pipeline_errors > 0:
                    click.echo(
                        f"❌ Pipeline '{pipeline_name}' has {pipeline_errors} error(s)"
                    )
                    if verbose:
                        for error in errors:
                            click.echo(f"   Error: {error}")

                if pipeline_warnings > 0:
                    click.echo(
                        f"⚠️  Pipeline '{pipeline_name}' has {pipeline_warnings} warning(s)"
                    )
                    if verbose:
                        for warning in warnings:
                            click.echo(f"   Warning: {warning}")

                if not verbose:
                    click.echo("   Use --verbose flag to see detailed messages")

        except Exception as e:
            error_handler = ErrorHandler(verbose)
            error_handler.with_pipeline_context(pipeline_name, env).handle_cli_error(
                e, f"Validation for pipeline '{pipeline_name}'"
            )
            if log_file:
                click.echo(f"📝 Check detailed logs: {log_file}")
            total_errors += 1

    # Summary
    click.echo("\n📊 Validation Summary:")
    click.echo(f"   Environment: {env}")
    click.echo(f"   Pipelines validated: {validated_pipelines}")
    click.echo(f"   Total errors: {total_errors}")
    click.echo(f"   Total warnings: {total_warnings}")

    if total_errors == 0:
        click.echo("\n✅ All configurations are valid")
        sys.exit(0)
    else:
        click.echo(f"\n❌ Validation failed with {total_errors} error(s)")
        sys.exit(1)


@cli.command()
@click.option("--env", "-e", required=True, help="Environment")
@click.option("--pipeline", "-p", help="Specific pipeline to generate")
@click.option("--output", "-o", default="generated", help="Output directory")
@click.option("--dry-run", is_flag=True, help="Preview without generating files")
@click.option("--format", is_flag=True, help="Format generated code with Black")
@click.option(
    "--no-cleanup",
    is_flag=True,
    help="Disable cleanup of generated files when source YAML files are removed.",
)
@click.option(
    "--force",
    "-f",
    is_flag=True,
    help="Force regeneration of all files, even if unchanged",
)
@click.option(
    "--no-bundle",
    is_flag=True,
    help="Disable bundle support even if databricks.yml exists",
)
def generate(env, pipeline, output, dry_run, format, no_cleanup, force, no_bundle):
    """Generate DLT pipeline code"""
    project_root = _ensure_project_root()

    # Get context info
    ctx = click.get_current_context()
    verbose = ctx.obj.get("verbose", False) if ctx.obj else False
    log_file = ctx.obj.get("log_file") if ctx.obj else None

    click.echo(f"🚀 Generating pipeline code for environment: {env}")
    if verbose and log_file:
        click.echo(f"📝 Detailed logs: {log_file}")

    # Check if substitution file exists
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        click.echo(f"❌ Substitution file not found: {substitution_file}")
        sys.exit(1)

    # Initialize orchestrator and state manager
    if verbose:
        click.echo("🔧 Initializing orchestrator and state manager...")
    orchestrator = ActionOrchestrator(project_root)
    state_manager = StateManager(project_root) if not no_cleanup else None

    # Determine which pipelines to generate (using pipeline field, not directory names)
    pipelines_to_generate = []
    if pipeline:
        # Check if pipeline field exists in flowgroups
        all_flowgroups = orchestrator.discover_all_flowgroups()
        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        
        if pipeline not in pipeline_fields:
            click.echo(f"❌ Pipeline field '{pipeline}' not found in any flowgroup")
            if pipeline_fields:
                click.echo(f"💡 Available pipeline fields: {sorted(pipeline_fields)}")
            sys.exit(1)
        pipelines_to_generate = [pipeline]
    else:
        # Discover all pipeline fields from flowgroups
        all_flowgroups = orchestrator.discover_all_flowgroups()
        if not all_flowgroups:
            click.echo("❌ No flowgroups found in project")
            sys.exit(1)

        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        pipelines_to_generate = sorted(pipeline_fields)

    # Set output directory
    output_dir = project_root / output

    # Handle cleanup if enabled (default behavior)
    if not no_cleanup and state_manager:
        click.echo(f"🧹 Checking for orphaned files in environment: {env}")

        # Check for fresh start scenario (no state file exists)
        if not state_manager.state_file_exists():
            click.echo("🆕 Fresh start detected: no state file exists")
            if not dry_run:
                # Perform filesystem-aware cleanup for fresh start
                deleted_files = state_manager.cleanup_untracked_files(output_dir, env)
                if deleted_files:
                    click.echo(f"🧹 Fresh start cleanup: removed {len(deleted_files)} orphaned file(s):")
                    for deleted_file in deleted_files:
                        click.echo(f"   • Deleted: {deleted_file}")
                else:
                    click.echo("✨ Fresh start cleanup: no orphaned files found")
            else:
                # Dry-run: show what would be cleaned up
                existing_files = state_manager.scan_generated_directory(output_dir)
                expected_files = state_manager.calculate_expected_files(output_dir, env)
                orphaned_files_fs = existing_files - expected_files
                lhp_orphaned = [f for f in orphaned_files_fs if state_manager.is_lhp_generated_file(f)]
                
                if lhp_orphaned:
                    click.echo(f"📋 Fresh start cleanup would remove {len(lhp_orphaned)} orphaned file(s):")
                    for file_path in sorted(lhp_orphaned):
                        try:
                            rel_path = file_path.relative_to(project_root)
                            click.echo(f"   • {rel_path}")
                        except ValueError:
                            click.echo(f"   • {file_path}")
                else:
                    click.echo("📋 Fresh start cleanup: no orphaned files would be removed")

        # Find orphaned files (tracked files)
        orphaned_files = state_manager.find_orphaned_files(env)

        if orphaned_files:
            if dry_run:
                click.echo(f"📋 Would clean up {len(orphaned_files)} orphaned file(s):")
                for file_state in orphaned_files:
                    click.echo(
                        f"   • {file_state.generated_path} (from {file_state.source_yaml})"
                    )
            else:
                click.echo(f"🗑️  Cleaning up {len(orphaned_files)} orphaned file(s):")
                deleted_files = state_manager.cleanup_orphaned_files(env, dry_run=False)
                for deleted_file in deleted_files:
                    click.echo(f"   • Deleted: {deleted_file}")
        else:
            click.echo("✅ No orphaned files found")

    # Smart generation: determine what needs to be generated
    pipelines_needing_generation = {}
    if not no_cleanup and state_manager and not force:
        click.echo(f"🔍 Analyzing changes in environment: {env}")

        # Get detailed staleness information
        staleness_info = state_manager.get_detailed_staleness_info(env)
        
        # Show global dependency changes if any
        if staleness_info["global_changes"]:
            click.echo("🌍 Global dependency changes detected:")
            for change in staleness_info["global_changes"]:
                click.echo(f"   • {change}")
            click.echo("   → All files will be regenerated")

        for pipeline_name in pipelines_to_generate:
            generation_info = state_manager.get_files_needing_generation(
                env, pipeline_name
            )

            new_count = len(generation_info["new"])
            stale_count = len(generation_info["stale"])
            up_to_date_count = len(generation_info["up_to_date"])

            if new_count > 0 or stale_count > 0:
                pipelines_needing_generation[pipeline_name] = generation_info
                status_parts = []
                if new_count > 0:
                    status_parts.append(f"{new_count} new")
                if stale_count > 0:
                    status_parts.append(f"{stale_count} stale")
                click.echo(f"   📁 {pipeline_name}: {', '.join(status_parts)} file(s)")
                
                # Show detailed dependency changes for verbose mode
                if verbose and stale_count > 0:
                    for file_state in generation_info["stale"]:
                        file_path = file_state.generated_path
                        if file_path in staleness_info["files"]:
                            file_info = staleness_info["files"][file_path]
                            click.echo(f"      • {file_path}:")
                            for detail in file_info["details"]:
                                click.echo(f"        - {detail}")
            else:
                click.echo(
                    f"   ✅ {pipeline_name}: {up_to_date_count} file(s) up-to-date"
                )

        if not pipelines_needing_generation:
            click.echo("✨ All files are up-to-date! Nothing to generate.")
            click.echo("💡 Use --force flag to regenerate all files anyway.")
            # Don't return early - still need to run bundle sync
            pipelines_to_generate = []  # Set to empty list to skip generation loop
        else:
            # Update pipelines_to_generate to only process those that need it
            original_count = len(pipelines_to_generate)
            pipelines_to_generate = list(pipelines_needing_generation.keys())
            skipped_count = original_count - len(pipelines_to_generate)

            if skipped_count > 0:
                click.echo(
                    f"⚡ Smart generation: processing {len(pipelines_to_generate)}/{original_count} pipelines"
                )
    elif force:
        click.echo("🔄 Force mode: regenerating all files regardless of changes")
    else:
        click.echo("📝 State tracking disabled: generating all files")

    # Track generated files
    total_files = 0
    all_generated_files = {}

    # Generate each pipeline
    for pipeline_name in pipelines_to_generate:
        click.echo(f"\n🔧 Processing pipeline: {pipeline_name}")
        click.echo("   FlowGroups:")

        try:
            # Generate pipeline by field
            pipeline_output_dir = output_dir if not dry_run else None
            generated_files = orchestrator.generate_pipeline_by_field(
                pipeline_name,
                env,
                pipeline_output_dir,
                state_manager=state_manager,
                force_all=force or no_cleanup,
            )

            # Track files
            all_generated_files[pipeline_name] = generated_files
            total_files += len(generated_files)

            if dry_run:
                click.echo(f"📄 Would generate {len(generated_files)} file(s):")
                for filename in sorted(generated_files.keys()):
                    click.echo(f"   • {filename}")

                # Show preview of first file if verbose
                if generated_files and logger.isEnabledFor(logging.DEBUG):
                    first_file = next(iter(generated_files.values()))
                    click.echo("\n📄 Preview of generated code:")
                    click.echo("─" * 60)
                    # Show first 50 lines
                    lines = first_file.split("\n")[:50]
                    for line in lines:
                        click.echo(line)
                    if len(first_file.split("\n")) > 50:
                        click.echo("... (truncated)")
                    click.echo("─" * 60)
            else:
                click.echo(
                    f"✅ Generated {len(generated_files)} file(s) in {output_dir / pipeline_name}"
                )
                for filename in sorted(generated_files.keys()):
                    file_path = output_dir / pipeline_name / filename
                    click.echo(f"   • {file_path.relative_to(project_root)}")

        except ValueError as e:
            if "No flowgroups found in pipeline" in str(e):
                # This is expected when YAML files are removed - handle cleanup
                click.echo(f"📭 No flowgroups found in pipeline: {pipeline_name}")

                # Still run cleanup if enabled (default behavior)
                if not no_cleanup and state_manager:
                    click.echo(
                        f"🧹 Checking for orphaned files from pipeline: {pipeline_name}"
                    )

                    # Find orphaned files for this specific pipeline
                    all_orphaned = state_manager.find_orphaned_files(env)
                    pipeline_orphaned = [
                        f for f in all_orphaned if f.pipeline == pipeline_name
                    ]

                    if pipeline_orphaned:
                        click.echo(
                            f"🗑️  Found {len(pipeline_orphaned)} orphaned file(s) from {pipeline_name}"
                        )
                        if not dry_run:
                            # Clean up orphaned files for this pipeline
                            for file_state in pipeline_orphaned:
                                generated_path = (
                                    project_root / file_state.generated_path
                                )
                                if generated_path.exists():
                                    generated_path.unlink()
                                    click.echo(
                                        f"   • Deleted: {file_state.generated_path}"
                                    )

                                # Remove from state
                                if (
                                    file_state.generated_path
                                    in state_manager._state.environments.get(env, {})
                                ):
                                    del state_manager._state.environments[env][
                                        file_state.generated_path
                                    ]

                            # Clean up empty directories
                            state_manager._cleanup_empty_directories(env)
                            click.echo(
                                f"✅ Cleaned up {len(pipeline_orphaned)} orphaned file(s)"
                            )
                        else:
                            click.echo(
                                f"📋 Would clean up {len(pipeline_orphaned)} orphaned file(s) (dry-run)"
                            )
                    else:
                        click.echo("✅ No orphaned files found for this pipeline")
                else:
                    click.echo(
                        "💡 Cleanup is enabled by default. Use --no-cleanup to disable automatic cleanup"
                    )

                # Track empty result
                all_generated_files[pipeline_name] = {}
            elif "validation failed" in str(e) and "❌ Error [LHP-" in str(e):
                # This is a validation error that should be handled by the error handler
                error_handler = ErrorHandler(verbose)
                error_handler.with_pipeline_context(
                    pipeline_name, env
                ).handle_cli_error(e, f"Generation for pipeline '{pipeline_name}'")
                if log_file:
                    click.echo(f"📝 Check detailed logs: {log_file}")
                sys.exit(1)
            else:
                # Other ValueError, re-raise
                raise

        except Exception as e:
            error_handler = ErrorHandler(verbose)
            error_handler.with_pipeline_context(pipeline_name, env).handle_cli_error(
                e, f"Generation for pipeline '{pipeline_name}'"
            )
            if log_file:
                click.echo(f"📝 Check detailed logs: {log_file}")
            sys.exit(1)

    # Save state if cleanup is enabled (default behavior)
    if not no_cleanup and state_manager:
        state_manager.save()

    # Bundle synchronization (if enabled)
    try:
        bundle_enabled = should_enable_bundle_support(project_root, cli_no_bundle=no_bundle)
        if bundle_enabled:
            if verbose:
                click.echo("🔗 Bundle support detected - syncing resource files...")
            
            bundle_manager = BundleManager(project_root)
            bundle_manager.sync_resources_with_generated_files(output_dir, env)
            
            if verbose:
                click.echo("✅ Bundle resource files synchronized")
    except BundleResourceError as e:
        click.echo(f"⚠️  Bundle sync warning: {e}")
        if verbose and log_file:
            click.echo(f"📝 Bundle details in logs: {log_file}")
    except Exception as e:
        click.echo(f"⚠️  Bundle sync failed: {e}")
        if verbose and log_file:
            click.echo(f"📝 Bundle error details in logs: {log_file}")

    # Summary
    click.echo("\n📊 Generation Summary:")
    click.echo(f"   Environment: {env}")
    click.echo(f"   Pipelines processed: {len(pipelines_to_generate)}")
    click.echo(f"   Total files generated: {total_files}")

    if not dry_run:
        if total_files > 0:
            click.echo(f"   Output location: {output_dir.relative_to(project_root)}")

        # Show cleanup information if enabled (default behavior)
        if not no_cleanup and state_manager:
            click.echo("   State tracking: Enabled (.lhp_state.json)")

        if total_files > 0:
            click.echo("\n✅ Code generation completed successfully")
            click.echo("\n🚀 Next steps:")
            click.echo("   1. Review the generated code")
            click.echo("   2. Copy to your Databricks workspace")
            click.echo("   3. Create a DLT pipeline with the generated notebooks")
        else:
            click.echo("\n✅ Pipeline processing completed")
            if not no_cleanup:
                click.echo("   • Cleanup operations were performed")
            click.echo("   • No files were generated (no flowgroups found)")
    else:
        click.echo("\n✨ Dry run completed - no files were written")
        click.echo("   Remove --dry-run flag to generate files")


@cli.command()
def list_presets():
    """List available presets"""
    project_root = _ensure_project_root()
    presets_dir = project_root / "presets"

    click.echo("📋 Available presets:")

    if not presets_dir.exists():
        click.echo("❌ No presets directory found")
        sys.exit(1)

    preset_files = list(presets_dir.glob("*.yaml")) + list(presets_dir.glob("*.yml"))

    if not preset_files:
        click.echo("📭 No presets found")
        click.echo("\n💡 Create a preset file in the 'presets' directory")
        click.echo("   Example: presets/bronze_layer.yaml")
        return

    # Parse and display preset information
    parser = YAMLParser()
    presets_info = []

    for preset_file in sorted(preset_files):
        try:
            preset = parser.parse_preset(preset_file)
            presets_info.append(
                {
                    "name": preset.name,
                    "file": preset_file.name,
                    "version": preset.version,
                    "extends": preset.extends,
                    "description": preset.description or "No description",
                }
            )
        except Exception as e:
            logger.warning(f"Could not parse preset {preset_file}: {e}")
            presets_info.append(
                {
                    "name": preset_file.stem,
                    "file": preset_file.name,
                    "version": "?",
                    "extends": "?",
                    "description": f"Error: {e}",
                }
            )

    # Display as table
    if presets_info:
        # Calculate column widths
        name_width = max(len(p["name"]) for p in presets_info) + 2
        file_width = max(len(p["file"]) for p in presets_info) + 2
        version_width = 10
        extends_width = max(len(str(p["extends"] or "-")) for p in presets_info) + 2

        # Header
        click.echo(
            "\n" + "─" * (name_width + file_width + version_width + extends_width + 9)
        )
        click.echo(
            f"{'Name':<{name_width}} │ {'File':<{file_width}} │ {'Version':<{version_width}} │ {'Extends':<{extends_width}}"
        )
        click.echo("─" * (name_width + file_width + version_width + extends_width + 9))

        # Rows
        for preset in presets_info:
            name = preset["name"]
            file = preset["file"]
            version = preset["version"]
            extends = preset["extends"] or "-"
            click.echo(
                f"{name:<{name_width}} │ {file:<{file_width}} │ {version:<{version_width}} │ {extends:<{extends_width}}"
            )

        click.echo("─" * (name_width + file_width + version_width + extends_width + 9))

        # Show descriptions
        click.echo("\n📝 Descriptions:")
        for preset in presets_info:
            if preset["description"] != "No description":
                click.echo(f"\n{preset['name']}:")
                click.echo(f"   {preset['description']}")

    click.echo(f"\n📊 Total presets: {len(presets_info)}")


@cli.command()
def list_templates():
    """List available templates"""
    project_root = _ensure_project_root()
    templates_dir = project_root / "templates"

    click.echo("📋 Available templates:")

    if not templates_dir.exists():
        click.echo("❌ No templates directory found")
        sys.exit(1)

    template_files = list(templates_dir.glob("*.yaml")) + list(
        templates_dir.glob("*.yml")
    )

    if not template_files:
        click.echo("📭 No templates found")
        click.echo("\n💡 Create a template file in the 'templates' directory")
        click.echo("   Example: templates/standard_ingestion.yaml")
        return

    # Parse and display template information
    parser = YAMLParser()
    templates_info = []

    for template_file in sorted(template_files):
        try:
            template = parser.parse_template(template_file)
            # Count parameters
            required_params = sum(
                1 for p in template.parameters if p.get("required", False)
            )
            total_params = len(template.parameters)

            templates_info.append(
                {
                    "name": template.name,
                    "file": template_file.name,
                    "version": template.version,
                    "params": f"{required_params}/{total_params}",
                    "actions": len(template.actions),
                    "description": template.description or "No description",
                }
            )
        except Exception as e:
            logger.warning(f"Could not parse template {template_file}: {e}")
            templates_info.append(
                {
                    "name": template_file.stem,
                    "file": template_file.name,
                    "version": "?",
                    "params": "?",
                    "actions": "?",
                    "description": f"Error: {e}",
                }
            )

    # Display as table
    if templates_info:
        # Calculate column widths
        name_width = max(len(t["name"]) for t in templates_info) + 2
        file_width = max(len(t["file"]) for t in templates_info) + 2
        version_width = 10
        params_width = 12
        actions_width = 10

        # Header
        total_width = (
            name_width + file_width + version_width + params_width + actions_width + 12
        )
        click.echo("\n" + "─" * total_width)
        click.echo(
            f"{'Name':<{name_width}} │ {'File':<{file_width}} │ {'Version':<{version_width}} │ {'Params':<{params_width}} │ {'Actions':<{actions_width}}"
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
                f"{name:<{name_width}} │ {file:<{file_width}} │ {version:<{version_width}} │ {params:<{params_width}} │ {actions:<{actions_width}}"
            )

        click.echo("─" * total_width)

        # Show descriptions and parameters
        click.echo("\n📝 Template Details:")
        for i, template_file in enumerate(sorted(template_files)):
            try:
                template = parser.parse_template(template_file)
                click.echo(f"\n{template.name}:")
                if template.description:
                    click.echo(f"   Description: {template.description}")

                if template.parameters:
                    click.echo("   Parameters:")
                    for param in template.parameters:
                        param_name = param.get("name", "unknown")
                        param_type = param.get("type", "string")
                        param_required = (
                            "required" if param.get("required", False) else "optional"
                        )
                        param_desc = param.get("description", "")
                        default = param.get("default")

                        click.echo(
                            f"      • {param_name} ({param_type}, {param_required})"
                        )
                        if param_desc:
                            click.echo(f"        {param_desc}")
                        if default is not None:
                            click.echo(f"        Default: {default}")

            except Exception:
                pass  # Already logged above

    click.echo(f"\n📊 Total templates: {len(templates_info)}")
    click.echo("\n💡 Use templates in your flowgroup configuration:")
    click.echo("   use_template: template_name")
    click.echo("   template_parameters:")
    click.echo("     param1: value1")


@cli.command()
@click.argument("flowgroup")
@click.option("--env", "-e", default="dev", help="Environment")
def show(flowgroup, env):
    """Show resolved configuration for a flowgroup in table format"""
    project_root = _ensure_project_root()

    click.echo(
        f"🔍 Showing resolved configuration for '{flowgroup}' in environment '{env}'"
    )

    # Find the flowgroup file
    flowgroup_file = None
    pipelines_dir = project_root / "pipelines"
    
    # Get include patterns and discover files accordingly
    include_patterns = _get_include_patterns(project_root)
    yaml_files = _discover_yaml_files_with_include(pipelines_dir, include_patterns)

    for yaml_file in yaml_files:
        try:
            with open(yaml_file, "r") as f:
                content = yaml.safe_load(f)
            if content.get("flowgroup") == flowgroup:
                flowgroup_file = yaml_file
                break
        except Exception:
            continue

    if not flowgroup_file:
        click.echo(f"❌ Flowgroup '{flowgroup}' not found")
        sys.exit(1)

    # Parse flowgroup
    parser = YAMLParser()
    try:
        fg = parser.parse_flowgroup(flowgroup_file)
    except Exception as e:
        click.echo(f"❌ Error parsing flowgroup: {e}")
        sys.exit(1)

    # Load substitution manager
    substitution_file = project_root / "substitutions" / f"{env}.yaml"
    if not substitution_file.exists():
        click.echo(f"⚠️  Warning: Substitution file not found: {substitution_file}")
        substitution_mgr = EnhancedSubstitutionManager(env=env)
    else:
        substitution_mgr = EnhancedSubstitutionManager(substitution_file, env)

    # Process flowgroup with presets and templates
    orchestrator = ActionOrchestrator(project_root)
    try:
        processed_fg = orchestrator._process_flowgroup(fg, substitution_mgr)
    except Exception as e:
        click.echo(f"❌ Error processing flowgroup: {e}")
        sys.exit(1)

    # Display flowgroup information
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

    # Display actions in table format
    click.echo(f"\n📊 Actions ({len(processed_fg.actions)} total)")
    click.echo("─" * 80)

    if processed_fg.actions:
        # Calculate column widths
        name_width = max(len(a.name) for a in processed_fg.actions) + 2
        type_width = 12
        target_width = max(len(a.target or "-") for a in processed_fg.actions) + 2

        # Header
        click.echo(
            f"{'Name':<{name_width}} │ {'Type':<{type_width}} │ {'Target':<{target_width}} │ Description"
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
                description = description[: max_desc_width - 3] + "..."

            click.echo(
                f"{name:<{name_width}} │ {action_type:<{type_width}} │ {target:<{target_width}} │ {description}"
            )

    click.echo("─" * 80)

    # Show action details
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
            click.echo(
                f"   SQL: {action.sql[:100]}..."
                if len(action.sql) > 100
                else f"   SQL: {action.sql}"
            )

        if hasattr(action, "sql_path") and action.sql_path:
            click.echo(f"   SQL Path: {action.sql_path}")

    # Show secret references
    secret_refs = substitution_mgr.get_secret_references()
    if secret_refs:
        click.echo(f"\n🔐 Secret References ({len(secret_refs)} found)")
        click.echo("─" * 60)
        for ref in sorted(secret_refs, key=lambda r: f"{r.scope}/{r.key}"):
            click.echo(f"   ${{{ref.scope}/{ref.key}}}")

    # Show substitution summary
    if substitution_mgr.mappings:
        click.echo(
            f"\n🔄 Token Substitutions ({len(substitution_mgr.mappings)} tokens)"
        )
        click.echo("─" * 60)
        for token, value in sorted(substitution_mgr.mappings.items())[:10]:
            if len(value) > 40:
                value = value[:37] + "..."
            click.echo(f"   {{{token}}} → {value}")
        if len(substitution_mgr.mappings) > 10:
            click.echo(f"   ... and {len(substitution_mgr.mappings) - 10} more")


@cli.command()
def info():
    """Display project information and statistics."""
    project_root = _ensure_project_root()

    # Load project configuration
    config = _load_project_config(project_root)

    click.echo("📊 LakehousePlumber Project Information")
    click.echo("=" * 60)

    # Basic info
    click.echo(f"Name:        {config.get('name', 'Unknown')}")
    click.echo(f"Version:     {config.get('version', 'Unknown')}")
    click.echo(f"Description: {config.get('description', 'No description')}")
    click.echo(f"Author:      {config.get('author', 'Unknown')}")
    click.echo(f"Location:    {project_root}")

    # Count resources
    pipelines_dir = project_root / "pipelines"
    presets_dir = project_root / "presets"
    templates_dir = project_root / "templates"

    # Count pipelines
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
    template_count = (
        len(list(templates_dir.glob("*.yaml"))) if templates_dir.exists() else 0
    )

    click.echo("\n📈 Resource Summary:")
    click.echo(f"   Pipelines:  {pipeline_count}")
    click.echo(f"   FlowGroups: {flowgroup_count}")
    click.echo(f"   Presets:    {preset_count}")
    click.echo(f"   Templates:  {template_count}")

    # Check for environments
    substitutions_dir = project_root / "substitutions"
    if substitutions_dir.exists():
        env_files = [f.stem for f in substitutions_dir.glob("*.yaml")]
        if env_files:
            click.echo(f"\n🌍 Environments: {', '.join(env_files)}")

    # Recent activity
    import os
    import time

    click.echo("\n📅 Recent Activity:")

    # Find most recently modified flowgroup
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


@cli.command()
@click.option("--pipeline", "-p", help="Specific pipeline to analyze")
def stats(pipeline):
    """Display pipeline statistics and complexity metrics."""
    project_root = _ensure_project_root()
    parser = YAMLParser()

    click.echo("📊 Pipeline Statistics")
    click.echo("=" * 60)

    # Determine which pipelines to analyze
    pipelines_dir = project_root / "pipelines"
    if not pipelines_dir.exists():
        click.echo("❌ No pipelines directory found")
        return

    pipeline_dirs = []
    if pipeline:
        pipeline_dir = pipelines_dir / pipeline
        if not pipeline_dir.exists():
            click.echo(f"❌ Pipeline '{pipeline}' not found")
            return
        pipeline_dirs = [pipeline_dir]
    else:
        pipeline_dirs = [d for d in pipelines_dir.iterdir() if d.is_dir()]

    # Collect statistics
    total_stats = {
        "pipelines": len(pipeline_dirs),
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

    # Get include patterns for filtering
    include_patterns = _get_include_patterns(project_root)
    
    # Analyze each pipeline
    for pipeline_dir in pipeline_dirs:
        pipeline_name = pipeline_dir.name
        flowgroup_files = _discover_yaml_files_with_include(pipeline_dir, include_patterns)

        if pipeline_dirs and len(pipeline_dirs) == 1:
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

                    if action.type.value == "load":
                        total_stats["load_actions"] += 1
                    elif action.type.value == "transform":
                        total_stats["transform_actions"] += 1
                    elif action.type.value == "write":
                        total_stats["write_actions"] += 1

                    # Track action subtypes
                    if action.type.value == "load" and isinstance(action.source, dict):
                        subtype = action.source.get("type", "unknown")
                        total_stats["action_types"][f"load_{subtype}"] += 1
                    elif action.type.value == "transform" and action.transform_type:
                        total_stats["action_types"][
                            f"transform_{action.transform_type}"
                        ] += 1

                # Track presets and templates used
                if flowgroup.presets:
                    for preset in flowgroup.presets:
                        total_stats["presets_used"].add(preset)

                if flowgroup.use_template:
                    total_stats["templates_used"].add(flowgroup.use_template)

                if pipeline_dirs and len(pipeline_dirs) == 1:
                    click.echo(
                        f"   FlowGroup: {flowgroup.flowgroup} ({len(flowgroup.actions)} actions)"
                    )

            except Exception as e:
                logger.warning(f"Could not parse {yaml_file}: {e}")
                continue

        if pipeline_dirs and len(pipeline_dirs) == 1:
            click.echo(f"   Total actions: {pipeline_actions}")

    # Display summary statistics
    click.echo("\n📈 Summary Statistics:")
    click.echo(f"   Total pipelines: {total_stats['pipelines']}")
    click.echo(f"   Total flowgroups: {total_stats['flowgroups']}")
    click.echo(f"   Total actions: {total_stats['actions']}")
    click.echo(f"      • Load actions: {total_stats['load_actions']}")
    click.echo(f"      • Transform actions: {total_stats['transform_actions']}")
    click.echo(f"      • Write actions: {total_stats['write_actions']}")

    # Show action type breakdown
    if total_stats["action_types"]:
        click.echo("\n📊 Action Type Breakdown:")
        for action_type, count in sorted(total_stats["action_types"].items()):
            click.echo(f"   {action_type}: {count}")

    # Show resources used
    if total_stats["presets_used"]:
        click.echo(
            f"\n🔧 Presets Used: {', '.join(sorted(total_stats['presets_used']))}"
        )

    if total_stats["templates_used"]:
        click.echo(
            f"\n📝 Templates Used: {', '.join(sorted(total_stats['templates_used']))}"
        )

    # Calculate complexity metrics
    if total_stats["flowgroups"] > 0:
        avg_actions_per_flowgroup = total_stats["actions"] / total_stats["flowgroups"]
        click.echo("\n🧮 Complexity Metrics:")
        click.echo(f"   Average actions per flowgroup: {avg_actions_per_flowgroup:.1f}")

        if avg_actions_per_flowgroup < 3:
            complexity = "Low"
        elif avg_actions_per_flowgroup < 7:
            complexity = "Medium"
        else:
            complexity = "High"

        click.echo(f"   Overall complexity: {complexity}")











@cli.command()
@click.option("--env", "-e", help="Environment to show state for")
@click.option("--pipeline", "-p", help="Specific pipeline to show state for")
@click.option("--orphaned", is_flag=True, help="Show only orphaned files")
@click.option("--stale", is_flag=True, help="Show only stale files (YAML changed)")
@click.option("--new", is_flag=True, help="Show only new/untracked YAML files")
@click.option(
    "--dry-run", is_flag=True, help="Preview cleanup without actually deleting files"
)
@click.option("--cleanup", is_flag=True, help="Clean up orphaned files")
@click.option("--regen", is_flag=True, help="Regenerate stale files")
def state(env, pipeline, orphaned, stale, new, dry_run, cleanup, regen):
    """Show or manage the current state of generated files."""
    from ..services.state_display_service import StateDisplayService
    from ..services.state_display_utils import StateDisplayUtils
    
    project_root = _ensure_project_root()

    # Get context info for verbose logging
    ctx = click.get_current_context()
    verbose = ctx.obj.get("verbose", False) if ctx.obj else False
    log_file = ctx.obj.get("log_file") if ctx.obj else None

    state_manager = StateManager(project_root)
    
    # Initialize service layer
    service = StateDisplayService(state_manager, project_root, verbose, log_file)

    if verbose and log_file:
        click.echo(f"📝 Detailed logs: {log_file}")

    # Handle no environment specified - show overall stats
    if not env:
        stats = service.get_overall_stats()
        if stats is None:
            StateDisplayUtils.display_no_tracked_files_message()
        else:
            StateDisplayUtils.display_overall_stats(stats)
        return

    # Handle environment-specific display
    StateDisplayUtils.display_environment_header(env)

    # Check if any tracked files exist for this environment/pipeline
    tracked_files = service.get_tracked_files(env, pipeline)

    if not tracked_files and not new:
        StateDisplayUtils.display_missing_tracked_files(env, pipeline)
        return

    # Handle specific flag-based operations
    if orphaned:
        orphaned_files = service.get_orphaned_files(env, pipeline)
        StateDisplayUtils.display_orphaned_files(orphaned_files, cleanup, dry_run)
        
        if cleanup and orphaned_files and not dry_run:
            deleted_files = service.cleanup_orphaned_files(env, dry_run)
            StateDisplayUtils.display_cleanup_results(deleted_files)
        return

    if stale:
        stale_files, staleness_info = service.get_stale_files(env, pipeline)
        StateDisplayUtils.display_stale_files(stale_files, staleness_info, regen, dry_run)
        
        if regen and stale_files and not dry_run:
            try:
                regenerated_count = service.regenerate_stale_files(env, stale_files, dry_run)
                StateDisplayUtils.display_regeneration_results(regenerated_count)
            except Exception:
                # Error already handled by service layer
                pass
        return

    if new:
        new_files = service.get_new_files(env, pipeline)
        by_pipeline = service.group_files_by_pipeline(new_files)
        StateDisplayUtils.display_new_files(new_files, project_root, env, by_pipeline)
        return

    # Default comprehensive view
    StateDisplayUtils.display_tracked_files(
        tracked_files, project_root, service.calculate_file_status
    )

    # Get new files for summary
    new_files = service.get_new_files(env, pipeline)
    
    # Display comprehensive summary
    counts = service.calculate_summary_counts(env, pipeline)
    by_pipeline = service.group_files_by_pipeline(new_files)
    
    StateDisplayUtils.display_new_files_in_summary(new_files, project_root, env, by_pipeline)
    StateDisplayUtils.display_comprehensive_summary(counts, env)


def _find_project_root() -> Optional[Path]:
    """Find the project root by looking for lhp.yaml."""
    current = Path.cwd()

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
    config_file = project_root / "lhp.yaml"
    if not config_file.exists():
        return {}

    try:
        with open(config_file, "r") as f:
            return yaml.safe_load(f) or {}
    except Exception as e:
        logger.warning(f"Could not load project config: {e}")
        return {}


def _get_include_patterns(project_root: Path) -> List[str]:
    """Get include patterns from project configuration.
    
    Args:
        project_root: Project root directory
        
    Returns:
        List of include patterns, or empty list if none specified
    """
    try:
        from ..core.project_config_loader import ProjectConfigLoader
        config_loader = ProjectConfigLoader(project_root)
        project_config = config_loader.load_project_config()
        
        if project_config and project_config.include:
            return project_config.include
        else:
            return []
    except Exception as e:
        logger.warning(f"Could not load project config for include patterns: {e}")
        return []


def _discover_yaml_files_with_include(pipelines_dir: Path, include_patterns: List[str] = None) -> List[Path]:
    """Discover YAML files in pipelines directory with optional include filtering.
    
    Args:
        pipelines_dir: Directory to search in
        include_patterns: Optional list of include patterns
        
    Returns:
        List of YAML files
    """
    if include_patterns:
        from ..utils.file_pattern_matcher import discover_files_with_patterns
        return discover_files_with_patterns(pipelines_dir, include_patterns)
    else:
        # No include patterns, discover all YAML files (backwards compatibility)
        yaml_files = []
        yaml_files.extend(pipelines_dir.rglob("*.yaml"))
        yaml_files.extend(pipelines_dir.rglob("*.yml"))
        return yaml_files





if __name__ == "__main__":
    cli()
