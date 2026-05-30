# JUSTIFIED: generate_command.py sits at ~720 lines because it bundles the
# entire CLI-side generate pipeline: argument parsing, bundle preflight,
# warning collection, Live-frame render setup, parallel pool wiring,
# monitoring artifact post-processing, and summary table emission. Each
# concern is a thin adapter over the public facade (no business logic),
# but the adapters must coexist here to share the Live panel and
# warning-collector lifecycle. Splitting along concerns risks N+1 Live
# frames or duplicated warning collectors.
# TODO(Phase 9.2): extract monitoring post-processing + Live-frame setup + bundle preflight into cli/presenters/ modules per LOCAL/REMAINING_WORK.md §9.2.
"""Clean Architecture generate command implementation."""

import logging
import shutil
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Optional, Sequence

from ...api import (
    BatchGenerationResponse,
    ErrorEmitted,
    FlowgroupView,
    GenerationCompleted,
    GenerationResponse,
    LakehousePlumberApplicationFacade,
    OperationStarted,
    WarningCollector,
    collect_response,
    should_enable_bundle_support,
)
from ...errors import ErrorCategory, LHPConfigError, LHPError
from ...utils.performance_timer import log_perf_summary, perf_timer
from ..generate_summary import print_summary_table
from ..live_panel import (
    ActivityTail,
    HeaderContext,
    OverallProgress,
    PhaseTracker,
    PipelineRecord,
    _LiveUpdateCoalescer,
    render_live_frame,
    rich_handler_attached,
)
from ..warning_panel import render_warning_panel
from ..yaml_scanner import emit_deprecation_warning_if_needed
from .base_command import BaseCommand

logger = logging.getLogger(__name__)

_CATALOG_SCHEMA_DOC_LINK = (
    "https://lakehouse-plumber.readthedocs.io/en/latest/configure_catalog_schema.html"
)


class GenerateCommand(BaseCommand):
    """Pipeline code generation command.

    Run output is rendered via a single ``rich.live.Live`` status panel
    during generation, followed by a per-pipeline summary table. Phase
    markers (Discovery / Preflight / Bundle sync) are only shown when the
    phase takes longer than 250ms.
    """

    def execute(
        self,
        env: str,
        pipeline: Optional[str] = None,
        output: Optional[str] = None,
        dry_run: bool = False,
        no_bundle: bool = False,
        include_tests: bool = False,
        pipeline_config: Optional[str] = None,
        *,
        max_workers: Optional[int] = None,
        show_all: bool = False,
        force: bool = False,
        no_state: bool = False,
    ) -> None:
        """Run the generate command.

        ``show_all=False`` (default) suppresses the summary table on a
        full-success run and only lists failed pipelines otherwise.
        ``force`` and ``no_state`` are deprecated no-ops that route a
        deprecation entry through ``warning_collector`` when truthy.
        """
        from rich.live import Live
        from rich.text import Text

        from .. import console as _console_module

        self.setup_from_context()
        project_root = self.ensure_project_root()

        # Per-run accumulator for non-fatal warnings (e.g. deprecated
        # bare ``{token}`` substitution syntax). Rendered as a single
        # yellow-bordered Rich Panel after the Live frame exits.
        warning_collector = WarningCollector()

        if force or no_state:
            warning_collector.add(
                "deprecation",
                "--force and --no-state are deprecated and will be removed "
                "in a future release; their previous behavior is now the default.",
            )

        logger.debug(
            f"Generate request: env={env}, pipeline={pipeline}, "
            f"dry_run={dry_run}, include_tests={include_tests}, no_bundle={no_bundle}"
        )

        if output is None:
            output = f"generated/{env}"
        output_dir = project_root / output

        _remove_legacy_state_artifacts(project_root)

        self.check_substitution_file(env)
        self.echo_verbose_info(f"Detailed logs: {self.log_file}")

        # Refuse to proceed if bundle support is on and no pipeline_config
        # was supplied. Done BEFORE any side effects (including opening
        # the Live frame below) so a bad invocation cannot wipe
        # resources/lhp/ or generated/<env>/, and so the LHPConfigError
        # propagates through clean stderr — no Live teardown in between.
        bundle_enabled = should_enable_bundle_support(project_root, no_bundle)
        _require_pipeline_config_flag(
            bundle_enabled=bundle_enabled,
            pipeline_config_path=pipeline_config,
        )

        records: Dict[str, PipelineRecord] = {}
        failure_lines: List[Text] = []
        run_start = perf_counter()
        phase_tracker = PhaseTracker()
        activity_tail = ActivityTail(max_entries=5)
        # TODO(ui-consolidation): this Live-frame closure duplicates ~80% of
        # validate_command.py. Consolidate into a shared LiveFrameRunner.

        # ``batch_response`` is referenced by the post-Live raise path; the
        # ``None`` sentinel covers "Live exited via an unrelated exception
        # before the batch was attempted".
        batch_response: Optional[BatchGenerationResponse] = None
        # ``deferred_error`` captures the LHPError raised through the
        # event stream so it can be re-raised AFTER both the Live frame
        # and ``rich_handler_attached`` contexts have exited — required
        # by ``cli_error_boundary`` to render a clean Panel on stderr.
        deferred_error: Optional[LHPError] = None
        total_files = 0
        all_generated_filenames: List[str] = []

        # ``total=0`` up front so the first frame (rendered before discovery)
        # has a valid Progress to paint; the real total is bound after
        # discovery via ``set_total``. ``header_ctx`` is frozen, so it's
        # re-bound (not mutated) once the pipeline count is known.
        overall_progress = OverallProgress(
            "Generating pipelines", total=0, console=_console_module.console
        )
        header_ctx = HeaderContext(
            command_name="generate",
            env=env,
            total_pipelines=0,
        )

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
                show_progress=phase_tracker.active == "Generation",
                failed_count=failed_count,
                console_width=_console_module.console.width,
            )

        # Defensive RichHandler scope: route stray WARNING+ records
        # through Rich so they don't tear the Live frame, then restore
        # the original stream handlers on exit. The handler is detached
        # BEFORE the failure raise propagates so cli_error_boundary
        # prints its Panel to a stderr with no Rich machinery still
        # attached -- that's what prevents the "failed  LHP-XXX-NNN<-
        # box-edge-glue" rendering bug.
        with rich_handler_attached(_console_module.err_console):
            try:
                phase_tracker.start("Orchestrator init")
                with Live(
                    _render(),
                    console=_console_module.console,
                    refresh_per_second=10,
                    redirect_stdout=True,
                    redirect_stderr=True,
                ) as live:
                    # Coalesce the submit-loop ``live.update`` storm: the
                    # facade fires ``on_pipeline_start`` once per pipeline
                    # synchronously on the main thread, so for N=100 we'd
                    # paint the Panel 100 times before any worker finishes.
                    # The coalescer drops calls inside a 100ms window;
                    # completion callbacks still use direct ``live.update``.
                    coalesced = _LiveUpdateCoalescer(live, _render)
                    # Triggers the lazy ``ActionOrchestrator`` import (~25
                    # transitive modules) inside the Live frame so the first
                    # frame draws before the heavy import resolves and the
                    # user sees the spinner immediately.
                    with perf_timer("Orchestrator init", phase=True):
                        application_facade = self._create_application_facade(
                            project_root,
                            pipeline_config,
                            max_workers=max_workers,
                        )
                    phase_tracker.complete("Orchestrator init")
                    live.update(_render())

                    phase_tracker.start("Discovering")
                    with perf_timer("Pipeline discovery", phase=True):
                        all_flowgroups = application_facade.inspection.list_flowgroups()
                        duplicate_response = (
                            application_facade.inspection.validate_duplicate_flowgroups(
                                all_flowgroups
                            )
                        )
                        if not duplicate_response.success:
                            _raise_duplicate_flowgroups(duplicate_response)
                        pipelines_to_generate = self._get_pipeline_names(
                            pipeline, all_flowgroups
                        )

                    if not pipelines_to_generate:
                        raise LHPConfigError(
                            category=ErrorCategory.CONFIG,
                            code_number="014",
                            title="No flowgroups found in project",
                            details=(
                                "No flowgroup YAML files were found in the "
                                "pipelines/ directory."
                            ),
                            suggestions=[
                                "Create flowgroup YAML files in pipelines/<pipeline_name>/",
                                "Check that pipeline YAML files have the correct extension (.yaml or .yml)",
                                "Run 'lhp init <name>' to create a new project with example files",
                            ],
                        )

                    phase_tracker.complete(
                        "Discovering",
                        label=f"Discovering ({len(all_flowgroups)} flowgroups)",
                    )
                    live.update(_render())

                    logger.debug(
                        f"Pipelines discovered for generation: {pipelines_to_generate}"
                    )

                    # Seed records up front so the spinner shows
                    # "0 of N pipelines done" immediately.
                    for pipeline_name in pipelines_to_generate:
                        records[pipeline_name] = PipelineRecord(pipeline_name)

                    # Now that the real pipeline count is known, bind the
                    # ``OverallProgress`` total and rebuild the (frozen)
                    # ``HeaderContext`` with the real total. Start the
                    # Progress object so it begins ticking elapsed time.
                    overall_progress.set_total(len(pipelines_to_generate))
                    overall_progress.start()
                    header_ctx = HeaderContext(
                        command_name="generate",
                        env=env,
                        total_pipelines=len(pipelines_to_generate),
                    )
                    live.update(_render())

                    # Pre-pool deprecation scan: workers are silenced (NullHandler
                    # only) so the in-worker SubstitutionManager warning cannot
                    # reach the user. ``FlowgroupView.file_path`` already carries
                    # the resolved source YAML for each discovered flowgroup, so
                    # no facade round-trip is needed here.
                    phase_tracker.start("Deprecation scan")
                    emit_deprecation_warning_if_needed(
                        warning_collector,
                        (fg.file_path for fg in all_flowgroups),
                    )
                    phase_tracker.complete("Deprecation scan")
                    live.update(_render())

                    # Main-thread per-pipeline callbacks fired by the facade.
                    # ``_on_pipeline_start`` is idempotent under concurrent
                    # worker starts because PhaseTracker.start is keyed by name.
                    def _on_pipeline_start(name: str) -> None:
                        if phase_tracker.active != "Generation":
                            phase_tracker.start("Generation")
                        coalesced.update()

                    def _on_pipeline_complete(
                        name: str, response: GenerationResponse
                    ) -> None:
                        rec = records[name]
                        rec.success = bool(response.success)
                        rec.files = response.files_written if response.success else 0
                        rec.duration_s = response.duration_s
                        if not response.success:
                            rec.error_code = response.error_code
                            failure_lines.append(
                                Text.assemble(
                                    ("  ", "default"),
                                    ("✗ ", "bold red"),
                                    (name, "default"),
                                    (" failed  ", "default"),
                                    (rec.error_code or "—", "red"),
                                )
                            )
                        activity_tail.append(records[name])
                        overall_progress.advance()
                        live.update(_render())

                    # Option B (per-event stream consumption): the Live
                    # frame integrates per-pipeline callbacks fired by
                    # the facade. ``GenerationCompleted`` carries the
                    # terminal ``BatchGenerationResponse``; on LHPError,
                    # an ``ErrorEmitted`` precedes the raise — captured
                    # here and re-raised AFTER both context managers
                    # exit so ``cli_error_boundary`` sees clean stderr.
                    with perf_timer(
                        f"Batch pipeline generation [{len(pipelines_to_generate)}]",
                        phase=True,
                    ):
                        try:
                            for (
                                event
                            ) in application_facade.generation.generate_pipelines(
                                pipeline_fields=pipelines_to_generate,
                                env=env,
                                output_dir=output_dir if not dry_run else None,
                                specific_flowgroups=None,
                                include_tests=include_tests,
                                # §9.24: thread the real bundle flag so the
                                # shared facade preflight runs the bundle
                                # catalog/schema check (→ LHP-CFG-026).
                                bundle_enabled=bundle_enabled,
                                # ``all_flowgroups`` is a tuple of public
                                # ``FlowgroupView`` records that the facade
                                # cannot accept here (the orchestrator
                                # consumes the internal pydantic
                                # ``FlowGroup`` type via attribute access).
                                # Passing ``None`` triggers re-discovery
                                # inside the facade; the cost is a second
                                # discovery pass.
                                pre_discovered_all_flowgroups=None,
                                max_workers=max_workers,
                                on_pipeline_complete=_on_pipeline_complete,
                                on_pipeline_start=_on_pipeline_start,
                                warning_collector=warning_collector,
                            ):
                                if isinstance(event, OperationStarted):
                                    # First event — Live frame already
                                    # initialized; no extra work needed.
                                    continue
                                if isinstance(event, ErrorEmitted):
                                    # The next iteration step raises; the
                                    # event is a hook to inspect the error
                                    # if needed. Capture not required since
                                    # the raise is handled below.
                                    continue
                                if isinstance(event, GenerationCompleted):
                                    batch_response = event.response
                        except LHPError as exc:
                            deferred_error = exc
                        coalesced.update(force=True)

                    if deferred_error is not None:
                        phase_tracker.complete("Generation", success=False)
                        live.update(_render())
                    elif (
                        batch_response is not None
                        and not batch_response.is_successful()
                    ):
                        phase_tracker.complete("Generation", success=False)
                        live.update(_render())
                    elif batch_response is not None:
                        phase_tracker.complete("Generation")
                        total_files = batch_response.total_files_written
                        all_generated_filenames = (
                            batch_response.aggregate_generated_filenames
                        )

                        if not dry_run:
                            phase_tracker.start("Monitoring artifacts")
                            with perf_timer("Monitoring artifacts", phase=True):
                                application_facade.generation.finalize_monitoring_artifacts(
                                    env, output_dir
                                )
                            phase_tracker.complete("Monitoring artifacts")
                            live.update(_render())

                        # ``should_enable_bundle_support`` already factors in
                        # ``no_bundle`` (returns False when --no-bundle is set).
                        if bundle_enabled:
                            bundle_phase_label = (
                                "Bundle sync"
                                if not dry_run
                                else "Bundle sync (dry-run)"
                            )
                            phase_tracker.start(bundle_phase_label)
                            with perf_timer("Bundle sync", phase=True):
                                if not dry_run:
                                    self._handle_bundle_operations(
                                        application_facade,
                                        output_dir,
                                        env,
                                    )
                                else:
                                    logger.debug(
                                        "Dry run: Bundle sync would be performed"
                                    )
                            phase_tracker.complete(bundle_phase_label)
                            live.update(_render())
            finally:
                overall_progress.stop()

            # Live frame has closed cleanly. We are still inside the
            # ``rich_handler_attached`` context so any logger.warning
            # emitted by summary-table code still routes through Rich,
            # but the parent console is no longer in Live-redirect mode.
            log_perf_summary()

            if (
                deferred_error is None
                and batch_response is not None
                and batch_response.is_successful()
            ):
                logger.info(
                    f"Generation complete: {total_files} file(s) generated, "
                    f"{len(all_generated_filenames)} total output file(s)"
                )

            print_summary_table(
                records,
                dry_run,
                failed=(
                    deferred_error is not None
                    or batch_response is None
                    or not batch_response.is_successful()
                ),
                show_all=show_all,
                warning_count=warning_collector.count,
                elapsed_s=perf_counter() - run_start,
            )

            # Render the per-run warning panel (yellow border) after the
            # summary table. Rendered while still inside the
            # ``rich_handler_attached`` context so the surrounding handler
            # discipline is preserved; the Live frame has already exited
            # so the panel is not redirected into a transient state.
            _panel = render_warning_panel(warning_collector)
            if _panel is not None:
                _console_module.err_console.print(_panel)

        # Both context managers have exited: stderr stream handlers
        # restored, no Rich Live frame attached, so any raise from here
        # propagates through clean stderr (what ``cli_error_boundary``
        # needs to render its Panel cleanly).
        if deferred_error is not None:
            raise deferred_error
        if batch_response is None:
            # Unreachable in practice; defensive guard for the type checker.
            raise RuntimeError("generate batch did not complete")
        if not batch_response.is_successful():
            raise RuntimeError(
                batch_response.error_message or "Batch pipeline generation failed"
            )

    def _create_application_facade(
        self,
        project_root: Path,
        pipeline_config_path: Optional[str] = None,
        *,
        max_workers: Optional[int] = None,
    ) -> LakehousePlumberApplicationFacade:
        """Build the facade via the §9.24-clean ``for_project`` classmethod."""
        return LakehousePlumberApplicationFacade.for_project(
            project_root,
            pipeline_config_path=pipeline_config_path,
            max_workers=max_workers,
        )

    @staticmethod
    def _get_pipeline_names(
        pipeline: Optional[str], all_flowgroups: Sequence[FlowgroupView]
    ) -> List[str]:
        if pipeline:
            return [pipeline]
        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        return list(pipeline_fields) if pipeline_fields else []



    def _handle_bundle_operations(
        self,
        application_facade: LakehousePlumberApplicationFacade,
        output_dir: Path,
        env: str,
    ) -> None:
        """Sync bundle resources for the generated output via the public facade.

        Delegates to :meth:`BundleFacade.sync_resources` with
        ``wipe=True``: the facade clears ``resources/lhp/`` before
        re-syncing in a single call, honoring the wipe-and-regenerate
        contract on :class:`BundleManager`. Bundle activity is rendered
        as a phase marker by the outer ``execute()``; detail here routes
        to ``logger.debug``. Caller must gate on ``bundle_enabled`` and
        on ``not dry_run`` — this method assumes both checks have run.
        """
        sync_result = collect_response(
            application_facade.bundle.sync_resources(env, output_dir, wipe=True)
        )
        # ``collect_response`` returns ``object``; narrow for attribute
        # access. ``BundleSyncCompleted.response`` is a ``BundleSyncResult``
        # per :mod:`lhp.api.events`.
        success = bool(getattr(sync_result, "success", False))
        if not success:
            error_message = getattr(sync_result, "error_message", None) or (
                "Bundle sync failed"
            )
            error_code = getattr(sync_result, "error_code", None)
            raise LHPConfigError(
                category=ErrorCategory.CONFIG,
                code_number=_strip_lhp_prefix(error_code) or "027",
                title="Bundle resource sync failed",
                details=error_message,
                suggestions=[
                    "Check the logs for details about the bundle sync failure.",
                    "Verify that resources/lhp/ is writable and not a symlink.",
                ],
            )
        logger.debug("Bundle resource files synchronized")


def _remove_legacy_state_artifacts(project_root: Path) -> None:
    legacy_file = project_root / ".lhp_state.json"
    legacy_dir = project_root / ".lhp_state"
    if legacy_file.exists():
        legacy_file.unlink()
    if legacy_dir.is_dir():
        shutil.rmtree(legacy_dir)


def _require_pipeline_config_flag(
    *,
    bundle_enabled: bool,
    pipeline_config_path: Optional[str],
) -> None:
    """Enforce ``--pipeline-config`` when bundle support is enabled.

    Raises ``LHPConfigError`` with code ``LHP-CFG-023`` when bundle support
    is on but no ``--pipeline-config`` flag was supplied. Without that
    flag, pipeline config loading would fall back to empty defaults and
    every pipeline would fail catalog/schema validation later — so we
    surface the actionable error up-front, before any wipes occur. Inlined
    here per the §5 boundary cutover (CLI can't import from
    ``lhp.bundle.preflight``).
    """
    if not bundle_enabled or pipeline_config_path:
        return
    raise LHPConfigError(
        category=ErrorCategory.CONFIG,
        code_number="023",
        title="--pipeline-config is required when bundle support is enabled",
        details=(
            "databricks.yml is present (bundle support is enabled) but the "
            "--pipeline-config / -pc flag was not supplied. Bundle resource "
            "generation requires a pipeline_config.yaml that defines `catalog` "
            "and `schema` either per-pipeline or under `project_defaults`."
        ),
        suggestions=[
            "Pass --pipeline-config (or -pc) pointing at your pipeline_config.yaml",
            "Or use --no-bundle to skip bundle resource generation",
        ],
        doc_link=_CATALOG_SCHEMA_DOC_LINK,
    )


def _raise_duplicate_flowgroups(response) -> None:
    """Re-raise a duplicate-flowgroup ``ValidationResponse`` as an LHPError.

    ``InspectionFacade.validate_duplicate_flowgroups`` returns a DTO
    rather than raising; the CLI surfaces the failure as an
    :class:`LHPValidationError` so the existing
    :func:`render_error_panel` path renders the structured panel users
    saw under the legacy orchestrator raise.
    """
    issues = response.issues
    first_issue = issues[0] if issues else None
    title = (
        first_issue.title
        if first_issue is not None
        else "Duplicate flowgroup name detected"
    )
    details_lines: List[str] = []
    for issue in issues:
        details_lines.append(f"{issue.title}")
        if issue.details:
            details_lines.append(f"  {issue.details}")
        first = issue.context.get("first") if issue.context else None
        duplicate = issue.context.get("duplicate") if issue.context else None
        if first:
            details_lines.append(f"  first occurrence: {first}")
        if duplicate:
            details_lines.append(f"  duplicate: {duplicate}")
    raise LHPConfigError(
        category=ErrorCategory.VALIDATION,
        code_number="DUPFG",
        title=title,
        details="\n".join(details_lines) or "Duplicate flowgroup name detected.",
        suggestions=[
            "Rename one of the duplicate flowgroups.",
            "Or move one of them to a different pipeline.",
        ],
    )


def _strip_lhp_prefix(error_code: Optional[str]) -> Optional[str]:
    """Return the trailing code-number from a full LHP error code.

    ``"LHP-CFG-023"`` → ``"023"``. Returns ``None`` if the input is
    ``None`` or doesn't fit the expected ``LHP-<CAT>-<NUM>`` shape;
    callers fall back to a hard-coded code_number in that case.
    """
    if not error_code:
        return None
    parts = error_code.split("-")
    if len(parts) != 3:
        return None
    return parts[2]
