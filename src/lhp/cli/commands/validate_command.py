"""Validate command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import List, Optional, Tuple

import click

from ...core.orchestrator import ActionOrchestrator
from ...utils.exit_codes import ExitCode
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class ValidateCommand(BaseCommand):
    """
    Handles pipeline configuration validation command.

    Validates YAML pipeline configurations for syntax, structure,
    and business logic rules across specified environments.
    """

    def execute(
        self,
        env: str = "dev",
        pipeline: Optional[str] = None,
        verbose: bool = False,
        include_tests: bool = False,
        *,
        max_workers: Optional[int] = None,
    ) -> None:
        """
        Execute the validate command.

        Args:
            env: Environment to validate against
            pipeline: Specific pipeline to validate (optional)
            verbose: Enable verbose output
            include_tests: Include test reporting validation
            max_workers: Maximum worker threads for the cross-pipeline
                flat pool used by Phase A validation. ``None`` falls back
                to ``min(cpu_count, 8)`` inside the orchestrator
                constructor. The raw user value (possibly ``None``) is
                forwarded through the application facade so the
                resolution rule lives in one place.
        """
        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Override verbose setting if provided directly
        if verbose:
            self.verbose = verbose

        logger.debug(
            f"Validation request: env={env}, pipeline={pipeline}, verbose={verbose}"
        )

        click.echo(f"🔍 Validating pipeline configurations for environment: {env}")
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")

        # Validate blueprint/instance files up front so codes 040–055, 059
        # surface cleanly before the orchestrator wraps them in discovery
        # context. Without this, the same errors would still be raised by
        # discover_all_flowgroups later, just less cleanly.
        self._validate_blueprints_and_instances(project_root)

        # Check if substitution file exists
        self.check_substitution_file(env)

        # Initialize orchestrator
        orchestrator = ActionOrchestrator(project_root, max_workers=max_workers)

        # Determine which pipelines to validate (also returns discovered flowgroups)
        pipelines_to_validate, all_flowgroups = self._determine_pipelines_to_validate(
            pipeline, orchestrator
        )

        # Validate all pipelines (raw user value passed through facade)
        total_errors, total_warnings = self._validate_all_pipelines(
            pipelines_to_validate,
            env,
            orchestrator,
            include_tests=include_tests,
            all_flowgroups=all_flowgroups,
            max_workers=max_workers,
        )

        # Validate test reporting configuration (reuses already-discovered flowgroups)
        tr_errors = self._validate_test_reporting(
            orchestrator, all_flowgroups, pipelines_to_validate, include_tests
        )
        total_errors += len(tr_errors)

        # Display summary
        self._display_validation_summary(
            env, len(pipelines_to_validate), total_errors, total_warnings
        )

        # Exit with appropriate code (let error boundary handle it)
        if total_errors > 0:
            raise SystemExit(ExitCode.DATA_ERROR)

    def _validate_blueprints_and_instances(self, project_root: Path) -> None:
        """Validate every blueprint and instance file before pipeline validation.

        Reuses the runtime parser/discoverer/expander stack so the contract
        is identical to ``lhp generate``: any error that would fail generation
        also fails validation, just earlier and with a clearer rendering path.

        - No blueprint files AND no instance files → silent no-op (the entire
          feature is opt-in via file presence; same contract as the
          orchestrator's ``_expand_blueprints``).
        - One or more blueprint files exist → parse all (codes 046, 047–050,
          plus Pydantic-level shape errors) and resolve them into a
          name-keyed registry.
        - One or more instance files exist → parse each against the registry
          (codes 041–043, 051–054).
        - If both populations are non-empty, run cross-file expansion so codes
          044, 045 and 055 surface here rather than mid-generation.

        All errors are raised as ``LHPError`` and formatted by the existing
        CLI error boundary.
        """
        from ...core.project_config_loader import ProjectConfigLoader
        from ...core.services.blueprint_discoverer import BlueprintDiscoverer
        from ...core.services.blueprint_expander import BlueprintExpander
        from ...parsers.blueprint_parser import BlueprintParser

        project_config = ProjectConfigLoader(project_root).load_project_config()
        discoverer = BlueprintDiscoverer(
            project_root,
            project_config=project_config,
            blueprint_parser=BlueprintParser(),
        )

        blueprints = discoverer.discover_blueprints()
        instances = discoverer.discover_instances(blueprints)

        if not blueprints and not instances:
            logger.debug(
                "No blueprint or instance files found; skipping blueprint validation."
            )
            return

        expanded_count = 0
        if blueprints and instances:
            expanded_flowgroups, _provenance = BlueprintExpander().expand(
                blueprints, instances
            )
            expanded_count = len(expanded_flowgroups)
        elif blueprints and not instances:
            logger.info(
                f"Found {len(blueprints)} blueprint(s) but no instance files; "
                "no flowgroups will be expanded."
            )

        if self.verbose:
            click.echo(
                f"   ✅ Validated {len(blueprints)} blueprint(s), "
                f"{len(instances)} instance(s), "
                f"{expanded_count} expanded flowgroup(s)"
            )

    def _determine_pipelines_to_validate(
        self, pipeline: Optional[str], orchestrator: ActionOrchestrator
    ) -> Tuple[List[str], list]:
        """Determine which pipelines to validate based on user input.

        Returns:
            Tuple of (pipeline names to validate, all discovered flowgroups)
        """
        from ...utils.error_formatter import ErrorCategory, LHPConfigError

        all_flowgroups = orchestrator.discover_all_flowgroups()

        if pipeline:
            logger.debug(f"Validating specific pipeline: {pipeline}")
            pipeline_fields = {fg.pipeline for fg in all_flowgroups}

            if pipeline not in pipeline_fields:
                suggestions = [
                    "Check the pipeline name for typos",
                ]
                if pipeline_fields:
                    suggestions.insert(
                        0,
                        f"Available pipelines: {', '.join(sorted(pipeline_fields))}",
                    )
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="015",
                    title=f"Pipeline '{pipeline}' not found",
                    details=f"No flowgroup with pipeline field '{pipeline}' was found.",
                    suggestions=suggestions,
                    context={"Pipeline": pipeline},
                )
            return [pipeline], all_flowgroups
        else:
            if not all_flowgroups:
                raise LHPConfigError(
                    category=ErrorCategory.CONFIG,
                    code_number="014",
                    title="No flowgroups found in project",
                    details="No flowgroup YAML files were found in the pipelines/ directory.",
                    suggestions=[
                        "Create flowgroup YAML files in pipelines/<pipeline_name>/",
                        "Check that pipeline YAML files have the correct extension (.yaml or .yml)",
                        "Run 'lhp init <name>' to create a new project with example files",
                    ],
                )

            pipeline_fields = {fg.pipeline for fg in all_flowgroups}
            return sorted(pipeline_fields), all_flowgroups

    def _validate_all_pipelines(
        self,
        pipelines_to_validate: List[str],
        env: str,
        orchestrator: ActionOrchestrator,
        include_tests: bool = True,
        all_flowgroups: Optional[list] = None,
        *,
        max_workers: Optional[int] = None,
    ) -> Tuple[int, int]:
        """
        Validate all specified pipelines through the application facade.

        Routes the validate path through
        :meth:`LakehousePlumberApplicationFacade.validate_pipelines` so the
        layered dependency direction matches the generate command
        (CLI → facade → orchestrator). The per-pipeline display fires via
        the ``on_pipeline_complete`` callback on the main thread (display
        order is completion order, not input order — acceptable because
        each line is self-contained).

        Args:
            pipelines_to_validate: List of pipeline names to validate.
            env: Environment name.
            orchestrator: Action orchestrator instance (wrapped in a
                facade internally; passed in to reuse the caller's
                already-constructed instance).
            include_tests: If False, skip test actions during validation.
            all_flowgroups: Pre-discovered flowgroups to avoid redundant scans.
            max_workers: Raw user-supplied thread-pool size. ``None``
                lets the orchestrator's constructor default
                (``min(cpu_count, 8)``) take over.

        Returns:
            Tuple of (total_errors, total_warnings).
        """
        from ...core.layers import (
            LakehousePlumberApplicationFacade,
            ValidationResponse,
        )

        # Wrap the orchestrator in a facade. Validation never touches state,
        # so state_manager=None is correct here.
        application_facade = LakehousePlumberApplicationFacade(orchestrator, None)

        total_errors = 0
        total_warnings = 0

        click.echo(
            f"\n🔧 Validating {len(pipelines_to_validate)} pipeline(s) in parallel"
        )

        def _on_complete(pipeline_name: str, response: "ValidationResponse") -> None:
            nonlocal total_errors, total_warnings
            pipeline_errors = len(response.errors)
            pipeline_warnings = len(response.warnings)
            total_errors += pipeline_errors
            total_warnings += pipeline_warnings

            # Display per-pipeline result on the main thread, in completion
            # order (the facade forwards callbacks one-at-a-time).
            self._display_pipeline_validation_results(
                pipeline_name,
                pipeline_errors,
                pipeline_warnings,
                list(response.errors),
                list(response.warnings),
            )

        batch_response = application_facade.validate_pipelines(
            pipeline_fields=pipelines_to_validate,
            env=env,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=all_flowgroups,
            max_workers=max_workers,
            on_pipeline_complete=_on_complete,
        )

        if batch_response.original_error is not None:
            logger.warning(f"Batch validation raised: {batch_response.original_error}")
            click.echo(f"❌ Batch validation failed: {batch_response.original_error}")
            if self.log_file:
                click.echo(f"📝 Check detailed logs: {self.log_file}")
            total_errors += 1

        return total_errors, total_warnings

    def _display_pipeline_validation_results(
        self,
        pipeline_name: str,
        pipeline_errors: int,
        pipeline_warnings: int,
        errors: List[str],
        warnings: List[str],
    ) -> None:
        """Display validation results for a single pipeline.

        Always emits the error code + title header for every error so users
        can see *what* is wrong without re-running with ``--verbose``. The
        full multi-line LHPError block (details, context, suggestions, doc
        link) is gated behind ``--verbose`` to keep the default output
        scannable when validating many pipelines at once.
        """
        if pipeline_errors == 0 and pipeline_warnings == 0:
            click.echo(f"✅ Pipeline '{pipeline_name}' is valid")
            return

        if pipeline_errors > 0:
            click.echo(f"❌ Pipeline '{pipeline_name}' has {pipeline_errors} error(s)")
            for error in errors:
                if self.verbose:
                    # Full formatted LHPError block (or raw string for non-LHP errors).
                    click.echo(f"   {error}")
                else:
                    click.echo(f"   {self._summarize_error(error)}")

        if pipeline_warnings > 0:
            click.echo(
                f"⚠️  Pipeline '{pipeline_name}' has {pipeline_warnings} warning(s)"
            )
            for warning in warnings:
                if self.verbose:
                    click.echo(f"   {warning}")
                else:
                    click.echo(f"   {self._summarize_error(warning)}")

        if not self.verbose:
            click.echo("   (use --verbose for full details, suggestions, and context)")

    @staticmethod
    def _summarize_error(message: str) -> str:
        """Extract a single-line summary from a (possibly multi-line) error.

        LHPError stringifies to a block whose header line is
        ``❌ Error [CODE]: Title``. Upstream code may prefix that with a
        flowgroup label (e.g. ``"Flowgroup 'foo': <block>"``), so we scan
        for the ``❌ Error`` header line and prepend the upstream prefix
        (the first non-empty line, when it differs from the header). For
        plain-string errors with no LHP header, we return the first
        non-empty line.
        """
        first_non_empty: Optional[str] = None
        for line in message.splitlines():
            stripped = line.strip()
            if not stripped:
                continue
            if stripped.startswith("❌ Error ["):
                if first_non_empty and first_non_empty != stripped:
                    return f"{first_non_empty} {stripped}"
                return stripped
            if first_non_empty is None:
                first_non_empty = stripped
        if first_non_empty is not None:
            return first_non_empty
        return message.strip()

    def _validate_test_reporting(
        self,
        orchestrator: ActionOrchestrator,
        all_flowgroups: list,
        pipelines_to_validate: List[str],
        include_tests: bool,
    ) -> List[str]:
        """Validate test reporting configuration if present.

        Args:
            orchestrator: Action orchestrator instance
            all_flowgroups: Pre-discovered flowgroups (avoids redundant discovery)
            pipelines_to_validate: Pipeline names being validated
            include_tests: Whether to perform extended test-level validation

        Returns:
            List of error messages
        """
        project_config = orchestrator.project_config
        if not project_config or not project_config.test_reporting:
            return []

        from ...core.services.tst_reporting_hook_generator import (
            TestReportingHookGenerator,
        )

        click.echo("\n🔧 Validating test reporting configuration")

        processed_flowgroups = None
        if include_tests:
            pipeline_set = set(pipelines_to_validate)
            processed_flowgroups = [
                fg for fg in all_flowgroups if fg.pipeline in pipeline_set
            ]

        generator = TestReportingHookGenerator(
            project_config, orchestrator.project_root
        )
        errors = generator.validate(
            processed_flowgroups=processed_flowgroups,
            include_tests=include_tests,
        )

        if errors:
            for err in errors:
                click.echo(f"   ❌ {err}")
        else:
            click.echo("   ✅ Test reporting configuration is valid")

        return errors

    def _display_validation_summary(
        self, env: str, pipelines_validated: int, total_errors: int, total_warnings: int
    ) -> None:
        """Display validation summary and exit with appropriate code."""
        logger.info(
            f"Validation summary: env={env}, pipelines={pipelines_validated}, "
            f"errors={total_errors}, warnings={total_warnings}"
        )

        click.echo("\n📊 Validation Summary:")
        click.echo(f"   Environment: {env}")
        click.echo(f"   Pipelines validated: {pipelines_validated}")
        click.echo(f"   Total errors: {total_errors}")
        click.echo(f"   Total warnings: {total_warnings}")

        if total_errors == 0:
            click.echo("\n✅ All configurations are valid")
        else:
            click.echo(f"\n❌ Validation failed with {total_errors} error(s)")
