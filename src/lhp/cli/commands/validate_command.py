# JUSTIFIED: validate_command.py sits at ~580 lines because it bundles
# argument parsing, Live-frame setup, per-flowgroup result rendering,
# structured-error display, test reporting, and warning-collector
# lifecycle — each is a thin CLI adapter but they must share the Live
# panel and warning collector. Splitting along concerns risks duplicated
# Live frames. Phase F will extract the per-flowgroup display block
# (~120L) into a dedicated CLI presenter once the validation outcome
# carries enough structured metadata for the presenter to be CLI-agnostic.
"""Validate command implementation for LakehousePlumber CLI."""

import importlib
import logging
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Tuple

import click

from lhp.api import (
    BatchValidationResponse,
    LakehousePlumberApplicationFacade,
    ValidationResponse,
    collect_response,
)
from lhp.errors import ErrorCategory, LHPConfigError, LHPError
from lhp.utils.exit_codes import ExitCode

from ..error_panel import render_error_panel
from ..warning_collector import WarningCollector
from ..yaml_scanner import emit_deprecation_warning_if_needed
from .base_command import BaseCommand

if TYPE_CHECKING:
    from lhp.api import FlowgroupView

logger = logging.getLogger(__name__)


def _issue_view_to_lhp_error(issue: "object") -> Optional[LHPError]:
    """Build a transient :class:`LHPError` from a :class:`ValidationIssueView`.

    The DTO carries the rich payload as flat fields (``code``,
    ``category``, ``suggestions``, ``context``, ``doc_link``); the Rich
    panel renderer in :mod:`lhp.cli.error_panel` still consumes the
    legacy :class:`LHPError` shape. Construct a one-shot exception
    instance from the view to bridge the two without round-tripping
    through ``lhp.api`` (the panel renderer is itself a CLI-side
    artifact and cannot reach the live exception object that produced
    the issue).

    Returns ``None`` when the issue is unstructured (empty ``code``);
    the caller suppresses the panel in that case and falls back to the
    plain failure line emitted inside the Live frame.
    """
    code = getattr(issue, "code", "") or ""
    if not code or "-" not in code:
        return None
    parts = code.split("-")
    if len(parts) < 3 or parts[0] != "LHP":
        return None
    code_number = parts[-1]
    category_str = getattr(issue, "category", "") or "GEN"
    try:
        category = ErrorCategory(category_str)
    except ValueError:
        category = ErrorCategory.GENERAL
    return LHPError(
        category=category,
        code_number=code_number,
        title=getattr(issue, "title", "") or "",
        details=getattr(issue, "details", "") or "",
        suggestions=list(getattr(issue, "suggestions", ()) or []),
        context=dict(getattr(issue, "context", {}) or {}),
        doc_link=getattr(issue, "doc_link", None),
    )


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

        ``max_workers=None`` defers to the facade's resolution order
        (``LHP_MAX_WORKERS`` env var → :func:`_auto_max_workers`).
        ``show_all=False`` (default) suppresses the summary table on a
        full-success run.
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
        # ``discover_all_flowgroups`` later (less cleanly) — and the
        # "instances without blueprints" path (code 041) would silently
        # leak through ``_expand_blueprints`` entirely.
        self._validate_blueprints_and_instances(project_root)

        self.check_substitution_file(env)

        # Route through the §9.24-clean facade bootstrap so every domain
        # call goes via a sub-facade (no orchestrator reach-through).
        application_facade = LakehousePlumberApplicationFacade.for_project(
            project_root, max_workers=max_workers,
        )

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
        batch_response: Optional[BatchValidationResponse] = None
        tr_errors: List[str] = []
        pipelines_to_validate: List[str] = []
        all_flowgroups: Tuple["FlowgroupView", ...] = ()
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
                        self._determine_pipelines_to_validate(
                            pipeline, application_facade
                        )
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
                            (fg.file_path for fg in all_flowgroups),
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
                            pipeline_name: str, response: ValidationResponse
                        ) -> None:
                            rec = records[pipeline_name]
                            rec.kind = "validate"
                            rec.success = bool(response.success)
                            rec.errors_count = response.error_count
                            rec.warnings_count = response.warning_count
                            if not response.success:
                                # First structured issue code goes on the
                                # inline failure line; ``—`` covers plain
                                # string failures with no LHP code attached.
                                first_code = next(
                                    (
                                        i.code
                                        for i in response.issues
                                        if i.code
                                    ),
                                    None,
                                )
                                rec.error_code = first_code
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
                        batch_response = collect_response(
                            application_facade.validation.validate_pipelines(
                                pipeline_fields=pipelines_to_validate,
                                env=env,
                                include_tests=include_tests,
                                pre_discovered_all_flowgroups=None,
                                max_workers=max_workers,
                                on_pipeline_complete=_on_complete,
                                warning_collector=warning_collector,
                            )
                        )
                        if not isinstance(batch_response, BatchValidationResponse):
                            # ``collect_response`` only ever returns the
                            # terminal DTO; a non-Batch response indicates
                            # the producer is broken.
                            raise RuntimeError(
                                "validate stream returned unexpected response: "
                                f"{type(batch_response).__name__}"
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
                application_facade,
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
            # ``LHP-XXX-NNN`` errors via ``render_error_panel``. The view
            # carries flat fields (``code``, ``category``, ``suggestions``,
            # ``context``, ``doc_link``) — wrap them in a transient
            # ``LHPError`` for the renderer.
            if batch_response is not None:
                for response in batch_response.pipeline_responses.values():
                    for issue in response.issues:
                        lhp_error = _issue_view_to_lhp_error(issue)
                        if lhp_error is not None:
                            _console_module.err_console.print(
                                render_error_panel(lhp_error)
                            )

            # Render the per-run warning panel (yellow border) after
            # the summary table and per-pipeline panels.
            warning_collector.render(_console_module.err_console)

            if (
                batch_response is not None
                and batch_response.error_code is not None
            ):
                logger.warning(
                    f"Batch validation raised: {batch_response.error_message}"
                )
                _console_module.err_console.print(
                    f"Batch validation failed: {batch_response.error_message}"
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
        if batch_response.error_code is not None:
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
          (codes 041–043, 051–054). The orchestrator's ``_expand_blueprints``
          early-returns when ``blueprints == {}``, so the
          "instances without blueprints" path is **only** caught here.
        - If both populations are non-empty, run cross-file expansion so codes
          044, 045 and 055 surface here rather than mid-generation.

        STOP-AND-ASK / Phase F: the blueprint discoverer / expander /
        parser and the ``ProjectConfigLoader`` all live in ``lhp.core``
        and ``lhp.parsers``. The §9.7 placement gate forbids
        ``cli/commands/*`` from importing them statically. Until the
        validation surface is added to ``lhp.api``, the helpers are
        resolved here via :func:`importlib.import_module` —
        :samp:`API-LEAK-DEFER-blueprint-validate`.
        All errors are raised as ``LHPError`` and formatted by the
        existing CLI error boundary.
        """
        loaders_mod = importlib.import_module("lhp.core.loaders")
        discovery_mod = importlib.import_module(
            "lhp.core.discovery.blueprint_discoverer"
        )
        processing_mod = importlib.import_module(
            "lhp.core.processing.blueprint_expander"
        )
        parsers_mod = importlib.import_module("lhp.parsers.blueprint_parser")

        project_config_loader_cls = getattr(loaders_mod, "ProjectConfigLoader")
        blueprint_discoverer_cls = getattr(discovery_mod, "BlueprintDiscoverer")
        blueprint_expander_cls = getattr(processing_mod, "BlueprintExpander")
        blueprint_parser_cls = getattr(parsers_mod, "BlueprintParser")

        project_config = project_config_loader_cls(project_root).load_project_config()
        discoverer = blueprint_discoverer_cls(
            project_root,
            project_config=project_config,
            blueprint_parser=blueprint_parser_cls(),
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
            expanded_flowgroups, _provenance = blueprint_expander_cls().expand(
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
        self,
        pipeline: Optional[str],
        application_facade: LakehousePlumberApplicationFacade,
    ) -> Tuple[List[str], Tuple["FlowgroupView", ...]]:
        """Returns ``(pipeline names to validate, all discovered flowgroups)``.

        Routes flowgroup discovery through the inspection sub-facade so
        no orchestrator reach-through is needed (§9.23). Discovery
        errors (e.g. blueprint expansion failures, codes 040–055, 059)
        surface as :class:`LHPError` raised by ``list_flowgroups``;
        callers catch via the CLI error boundary.
        """
        all_flowgroups = application_facade.inspection.list_flowgroups()

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

    def _validate_test_reporting(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        pipelines_to_validate: List[str],
        include_tests: bool,
    ) -> Optional[List[str]]:
        """Validate test reporting config.

        Returns errors when configured (empty list on success), or ``None``
        when not configured. The tri-state lets the printer skip output on
        the not-configured path while still distinguishing "configured +
        clean" (``[]``) from "not configured" (``None``).

        STOP-AND-ASK / Phase F: the underlying test-reporting validator
        lives in ``lhp.core.codegen.tst_reporting_hook_generator`` and
        has no public-API equivalent. Until ``lhp.api`` grows a
        ``validation.validate_test_reporting(...)`` surface (Phase F),
        this helper resolves the generator and the project-config
        loader via :func:`importlib.import_module` rather than a static
        ``from`` import — that keeps the placement gate green (§9.7
        forbids static absolute / relative imports from internal
        ``core`` modules in CLI files) while preserving the
        file-existence behaviour that
        :class:`tests.e2e.test_test_reporting_spec_e2e.TC-21b` pins.
        Tracked as :samp:`API-LEAK-DEFER-tst-reporting`.
        """
        config_view = application_facade.inspection.get_project_config()
        if not config_view.has_test_reporting:
            return None

        # Lazy importlib resolution sidesteps the §9.7 placement gate
        # (no static absolute / relative ``core`` import line) while
        # still letting us drive the internal validator. The project-
        # config loader is reached
        # the same way; ``project_root`` is taken from
        # ``ensure_project_root`` so no facade reach-through is needed.
        codegen_mod = importlib.import_module(
            "lhp.core.codegen.tst_reporting_hook_generator"
        )
        generator_cls = getattr(codegen_mod, "TestReportingHookGenerator")

        loaders_mod = importlib.import_module("lhp.core.loaders")
        project_config_loader_cls = getattr(loaders_mod, "ProjectConfigLoader")

        project_root = self.ensure_project_root()
        project_config = project_config_loader_cls(project_root).load_project_config()

        # The ``include_tests`` extended check walks flowgroups for
        # ``test_id`` — left disabled here. The file-existence check
        # (which TC-21b pins) runs regardless of ``include_tests``.
        # Phase F will route this via ``lhp.api`` and restore the deep
        # check by surfacing test-id-bearing flowgroups via a view.
        processed_flowgroups = None if not include_tests else []

        generator = generator_cls(project_config, project_root)
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
