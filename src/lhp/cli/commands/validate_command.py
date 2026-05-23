"""Validate command implementation for LakehousePlumber CLI."""

import logging
from pathlib import Path
from typing import TYPE_CHECKING, Callable, Dict, List, Optional, Tuple

import click

from ...core.orchestrator import ActionOrchestrator
from ...utils.exit_codes import ExitCode
from ..warning_collector import WarningCollector
from ..yaml_scanner import emit_deprecation_warning_if_needed
from .base_command import BaseCommand

if TYPE_CHECKING:
    from ...core.layers import BatchValidationResponse, ValidationResponse

logger = logging.getLogger(__name__)


class ValidateCommand(BaseCommand):
    """Pipeline configuration validation."""

    def execute(
        self,
        env: str = "dev",
        pipeline: Optional[str] = None,
        verbose: bool = False,
        include_tests: bool = False,
        *,
        max_workers: Optional[int] = None,
        show_all: bool = False,
    ) -> None:
        """Run the validate command.

        Mirrors the generate command's Live-panel orchestration: outer
        ``rich_handler_attached`` scope → inner ``Live`` frame →
        completion callback mutates ``records`` (no inline ``print``) →
        post-Live summary table + per-failure ``LHPError`` panels →
        end-of-run warning panel. The symmetric teardown ensures any
        final ``SystemExit`` propagates through clean stderr with no
        Rich machinery attached.

        ``max_workers=None`` defers to the orchestrator constructor's
        resolution order (``LHP_MAX_WORKERS`` env var →
        :func:`_auto_max_workers`). ``show_all=False`` (default)
        suppresses the summary table on a full-success run.
        """
        from time import perf_counter

        from rich.live import Live
        from rich.text import Text

        from .. import console as _console_module
        from ..live_panel import (
            ActivityTail,
            HeaderContext,
            OverallProgress,
            PhaseTracker,
            PipelineRecord,
            render_live_frame,
            rich_handler_attached,
        )
        from ..validate_summary import print_validate_summary_table

        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Per-run accumulator for non-fatal warnings (e.g. deprecated
        # bare ``{token}`` substitution syntax). Rendered as a single
        # yellow-bordered Rich Panel after the Live frame exits.
        warning_collector = WarningCollector()

        if verbose:
            self.verbose = verbose

        logger.debug(
            f"Validation request: env={env}, pipeline={pipeline}, verbose={verbose}"
        )

        self.echo_verbose_info(f"Detailed logs: {self.log_file}")

        # Validate blueprint/instance files up front so codes 040–055, 059
        # surface cleanly before the orchestrator wraps them in discovery
        # context. Without this, the same errors would still be raised by
        # discover_all_flowgroups later, just less cleanly.
        self._validate_blueprints_and_instances(project_root)

        self.check_substitution_file(env)

        orchestrator = ActionOrchestrator(project_root, max_workers=max_workers)

        records: Dict[str, PipelineRecord] = {}
        failure_lines: List[Text] = []
        run_start = perf_counter()
        phase_tracker = PhaseTracker()
        activity_tail = ActivityTail(max_entries=5)
        overall_progress = OverallProgress(
            "Validating pipelines", total=0, console=_console_module.console
        )
        header_ctx = HeaderContext(
            command_name="validate",
            env=env,
            total_pipelines=0,
        )
        # TODO(ui-consolidation): this Live-frame closure duplicates ~80% of
        # generate_command.py. Consolidate into a shared LiveFrameRunner.

        # Populated inside the Live frame; referenced by the post-Live
        # rendering path so they must be defined here. ``with`` does NOT
        # introduce a scope, but pre-binding makes the post-Live access
        # robust against exceptions raised mid-frame.
        batch_response: Optional["BatchValidationResponse"] = None
        tr_errors: List[str] = []
        pipelines_to_validate: List[str] = []
        all_flowgroups: list = []
        # Set inside the Live frame when ``_determine_pipelines_to_validate``
        # returns an empty pipeline list (edge case where flowgroups exist
        # but resolve to zero pipeline names). The post-Live block and the
        # final SystemExit are then skipped — the warning is emitted and
        # the command returns cleanly with exit code 0. This intentionally
        # diverges from ``lhp generate``, which raises ``LHPConfigError-014``
        # in the analogous situation; for validate the user-facing contract
        # is "warn, don't fail".
        empty_no_op = False

        def _elapsed() -> str:
            s = int(perf_counter() - run_start)
            return f"{s // 60:02d}:{s % 60:02d}"

        def _render():
            failed_count = sum(1 for r in records.values() if r.success is False)
            return render_live_frame(
                phase_tracker,
                overall_progress,
                activity_tail,
                failure_lines,
                header_context=header_ctx,
                elapsed_text=_elapsed(),
                show_progress=phase_tracker.active == "Validation",
                failed_count=failed_count,
                console_width=_console_module.console.width,
            )

        with rich_handler_attached(_console_module.err_console):
            try:
                phase_tracker.start("Discovering")
                with Live(
                    _render(),
                    console=_console_module.console,
                    refresh_per_second=10,
                    redirect_stdout=True,
                    redirect_stderr=True,
                ) as live:
                    pipelines_to_validate, all_flowgroups = (
                        self._determine_pipelines_to_validate(pipeline, orchestrator)
                    )
                    phase_tracker.complete(
                        "Discovering",
                        label=f"Discovering ({len(all_flowgroups)} flowgroups)",
                    )
                    live.update(_render())

                    # Empty-pipelines guard. ``_determine_pipelines_to_validate``
                    # already raises ``LHPConfigError-014`` when no flowgroups
                    # exist at all, but an empty pipeline list can still occur
                    # in edge cases (e.g. all discovered flowgroups have a
                    # ``pipeline`` field that resolves to nothing usable). In
                    # those cases, exit cleanly with a warning rather than
                    # opening the Validation phase on an empty list and
                    # printing an empty summary table.
                    if not pipelines_to_validate:
                        empty_no_op = True
                        warning_collector.add(
                            "no-op",
                            "No pipelines found to validate.",
                        )
                        live.update(_render())

                    # Pre-pool deprecation scan: workers are silenced
                    # (NullHandler only) so the in-worker SubstitutionManager
                    # warning cannot reach the user. Resolve each discovered
                    # flowgroup's source YAML on the main thread and record
                    # the warning on ``warning_collector``; the panel renders
                    # after Live exits. Hoisted above the ``empty_no_op``
                    # gate so users with deprecated bare-``{token}`` syntax
                    # still get warned when ``pipelines_to_validate`` is
                    # empty but ``all_flowgroups`` is non-empty (e.g.
                    # ``--pipeline foo`` filter resolves to no flowgroups).
                    if all_flowgroups:
                        emit_deprecation_warning_if_needed(
                            warning_collector,
                            (
                                orchestrator._find_source_yaml_for_flowgroup(fg)
                                for fg in all_flowgroups
                            ),
                        )

                    if not empty_no_op:
                        # Seed records up front so the spinner shows
                        # ``0 of N pipelines done`` immediately.
                        for pipeline_name in pipelines_to_validate:
                            records[pipeline_name] = PipelineRecord(pipeline_name)

                        # Now that the real pipeline count is known, bind the
                        # ``OverallProgress`` total and rebuild the (frozen)
                        # ``HeaderContext`` with the real total. Start the
                        # Progress object so it begins ticking elapsed time.
                        overall_progress.set_total(len(pipelines_to_validate))
                        overall_progress.start()
                        header_ctx = HeaderContext(
                            command_name="validate",
                            env=env,
                            total_pipelines=len(pipelines_to_validate),
                        )
                        live.update(_render())

                        # Main-thread per-pipeline callback fired by the
                        # facade in completion order. Mutates record state
                        # ONLY (no direct output); failure rendering is
                        # deferred to the post-Live phase.
                        def _on_complete(
                            pipeline_name: str, response: "ValidationResponse"
                        ) -> None:
                            rec = records[pipeline_name]
                            rec.kind = "validate"
                            rec.success = bool(response.success)
                            rec.errors_count = response.error_count
                            rec.warnings_count = response.warning_count
                            if not response.success:
                                # First LHPError code goes on the inline
                                # failure line; ``—`` covers plain string
                                # failures with no LHPError attached.
                                first_lhp = next(
                                    (
                                        i.lhp_error
                                        for i in response.issues
                                        if i.lhp_error is not None
                                    ),
                                    None,
                                )
                                rec.error_code = first_lhp.code if first_lhp else None
                                failure_lines.append(
                                    Text.assemble(
                                        ("  ", "default"),
                                        ("✗ ", "bold red"),
                                        (pipeline_name, "default"),
                                        (" failed  ", "default"),
                                        (rec.error_code or "—", "red"),
                                    )
                                )
                            activity_tail.append(rec)
                            overall_progress.advance()
                            live.update(_render())

                        phase_tracker.start("Validation")
                        live.update(_render())
                        batch_response = self._validate_all_pipelines(
                            pipelines_to_validate,
                            env,
                            orchestrator,
                            include_tests=include_tests,
                            all_flowgroups=all_flowgroups,
                            max_workers=max_workers,
                            warning_collector=warning_collector,
                            on_pipeline_complete=_on_complete,
                        )

                        if not batch_response.is_successful():
                            phase_tracker.complete("Validation", success=False)
                        else:
                            phase_tracker.complete("Validation")
                        live.update(_render())
            finally:
                overall_progress.stop()

            # Live frame has closed. We are still inside the
            # ``rich_handler_attached`` context so any logger.warning
            # emitted by post-Live code still routes through Rich,
            # but the parent console is no longer in Live-redirect mode.

            if empty_no_op:
                # Render the per-run warning panel (yellow border) for the
                # no-op notice, then skip the summary table, per-failure
                # panels and SystemExit. Validate returns cleanly with
                # exit code 0 — generate raises ``LHPConfigError-014`` in
                # the analogous situation, so this is intentionally
                # asymmetric.
                warning_collector.render(_console_module.err_console)
                logger.info("Validate no-op: no pipelines to validate")
                return None

            # Computed silently so ``failed_overall`` is known before the
            # summary table; the inline ✓/✗ line is printed AFTER the table
            # to keep the visual order table → tr lines → warnings.
            tr_result = self._validate_test_reporting(
                orchestrator,
                all_flowgroups,
                pipelines_to_validate,
                include_tests,
            )
            tr_errors = tr_result if tr_result is not None else []

            failed_overall = (
                batch_response is None
                or not batch_response.is_successful()
                or bool(tr_errors)
            )

            print_validate_summary_table(
                records,
                failed=failed_overall,
                show_all=show_all,
                warning_count=warning_collector.count,
            )

            self._print_test_reporting_status(tr_result)

            # Plain-string failures already appeared as the inline failure
            # line inside Live; the panels here surface the structured
            # ``LHP-XXX-NNN`` errors via ``LHPError.__rich__``.
            if batch_response is not None:
                for response in batch_response.pipeline_responses.values():
                    for issue in response.issues:
                        if issue.lhp_error is not None:
                            _console_module.err_console.print(issue.lhp_error)

            # Render the per-run warning panel (yellow border) after
            # the summary table and per-pipeline panels.
            warning_collector.render(_console_module.err_console)

            if batch_response is not None and batch_response.original_error is not None:
                logger.warning(
                    f"Batch validation raised: {batch_response.original_error}"
                )
                _console_module.err_console.print(
                    f"Batch validation failed: {batch_response.original_error}"
                )
                if self.log_file:
                    logger.debug(f"Check detailed logs: {self.log_file}")

        # Both context managers have exited: stderr stream handlers
        # restored, no Rich Live frame attached. ANY raise from here on
        # propagates through clean stderr.
        if batch_response is None:
            raise RuntimeError("validate batch did not complete")

        total_errors = sum(
            r.error_count for r in batch_response.pipeline_responses.values()
        ) + len(tr_errors)
        if batch_response.original_error is not None:
            total_errors += 1
        logger.info(
            "Validation summary: passed=%d, failed=%d, warnings=%d",
            sum(1 for r in batch_response.pipeline_responses.values() if r.success),
            sum(1 for r in batch_response.pipeline_responses.values() if not r.success)
            + len(tr_errors),
            sum(r.warning_count for r in batch_response.pipeline_responses.values()),
        )
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
                f"   Validated {len(blueprints)} blueprint(s), "
                f"{len(instances)} instance(s), "
                f"{expanded_count} expanded flowgroup(s)"
            )

    def _determine_pipelines_to_validate(
        self, pipeline: Optional[str], orchestrator: ActionOrchestrator
    ) -> Tuple[List[str], list]:
        """Returns ``(pipeline names to validate, all discovered flowgroups)``."""
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
        include_tests: bool = False,
        all_flowgroups: Optional[list] = None,
        *,
        max_workers: Optional[int] = None,
        warning_collector: Optional[WarningCollector] = None,
        on_pipeline_complete: Optional[
            "Callable[[str, ValidationResponse], None]"
        ] = None,
    ) -> "BatchValidationResponse":
        """Validate via :meth:`LakehousePlumberApplicationFacade.validate_pipelines`.

        Layered direction matches generate (CLI → facade → orchestrator).
        The ``on_pipeline_complete`` callback is passed in by the Live-frame
        caller so this method stays free of direct output.
        """
        from ...core.layers import LakehousePlumberApplicationFacade

        application_facade = LakehousePlumberApplicationFacade(orchestrator)

        batch_response = application_facade.validate_pipelines(
            pipeline_fields=pipelines_to_validate,
            env=env,
            include_tests=include_tests,
            pre_discovered_all_flowgroups=all_flowgroups,
            max_workers=max_workers,
            on_pipeline_complete=on_pipeline_complete,
            warning_collector=warning_collector,
        )

        return batch_response

    def _validate_test_reporting(
        self,
        orchestrator: ActionOrchestrator,
        all_flowgroups: list,
        pipelines_to_validate: List[str],
        include_tests: bool,
    ) -> Optional[List[str]]:
        """Validate test reporting config.

        Returns errors when configured (empty list on success), or ``None``
        when not configured. The tri-state lets the printer skip output on
        the not-configured path while still distinguishing "configured +
        clean" (``[]``) from "not configured" (``None``).
        """
        project_config = orchestrator.project_config
        if not project_config or not project_config.test_reporting:
            return None

        from ...core.services.tst_reporting_hook_generator import (
            TestReportingHookGenerator,
        )

        processed_flowgroups = None
        if include_tests:
            pipeline_set = set(pipelines_to_validate)
            processed_flowgroups = [
                fg for fg in all_flowgroups if fg.pipeline in pipeline_set
            ]

        generator = TestReportingHookGenerator(
            project_config, orchestrator.project_root
        )
        return generator.validate(
            processed_flowgroups=processed_flowgroups,
            include_tests=include_tests,
        )

    def _print_test_reporting_status(self, errors: Optional[List[str]]) -> None:
        """Render the test_reporting inline ✓/✗ status line(s).

        Called AFTER ``print_validate_summary_table`` so the visual order is
        summary → tr lines → warning panel. ``errors=None`` means
        test_reporting was not configured; no line is emitted.
        """
        if errors is None:
            return

        from rich.text import Text

        from .. import console as _console_module

        if errors:
            for err in errors:
                _console_module.console.print(
                    Text.assemble(
                        ("  ", "default"),
                        ("✗ ", "bold red"),
                        (f"test_reporting: {err}", "default"),
                    )
                )
        else:
            _console_module.console.print(
                Text.assemble(
                    ("  ", "default"),
                    ("✓ ", "bold green"),
                    ("test_reporting", "green"),
                )
            )
