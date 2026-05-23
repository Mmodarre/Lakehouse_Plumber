"""Clean Architecture generate command implementation."""

import logging
import shutil
from pathlib import Path
from time import perf_counter
from typing import Dict, List, Optional

from ...bundle.exceptions import BundleResourceError
from ...bundle.manager import BundleManager
from ...bundle.preflight import (
    require_pipeline_config_flag,
    validate_catalog_schema,
)
from ...core.layers import (
    GenerationResponse,
    LakehousePlumberApplicationFacade,
)
from ...core.services.monitoring_pipeline_builder import (
    resolve_monitoring_pipeline_name,
)
from ...models.config import FlowGroup, ProjectConfig
from ...utils.bundle_detection import should_enable_bundle_support
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
from ..warning_collector import WarningCollector
from ..yaml_scanner import emit_deprecation_warning_if_needed
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


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

        from ...core.layers import BatchGenerationResponse
        from ...utils.error_formatter import LHPError
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
        require_pipeline_config_flag(
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
                        all_flowgroups = (
                            application_facade.orchestrator.discover_all_flowgroups()
                        )
                        application_facade.orchestrator.validate_duplicate_pipeline_flowgroup_combinations(
                            all_flowgroups
                        )
                        pipelines_to_generate = self._get_pipeline_names(
                            pipeline, all_flowgroups
                        )

                    if not pipelines_to_generate:
                        from ...utils.error_formatter import (
                            ErrorCategory,
                            LHPConfigError,
                        )

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

                    if bundle_enabled:
                        phase_tracker.start("Preflight")
                        self._run_catalog_schema_preflight(
                            bundle_enabled=bundle_enabled,
                            project_root=project_root,
                            pipeline_config=pipeline_config,
                            pipelines_to_generate=pipelines_to_generate,
                            env=env,
                            project_config=application_facade.orchestrator.project_config,
                        )
                        phase_tracker.complete("Preflight")
                        live.update(_render())

                    # Every run is a full regenerate: wipe env-specific
                    # generated/. resources/lhp/ wipe is gated on bundle so
                    # non-bundle projects don't materialize it just to delete.
                    if not dry_run:
                        phase_tracker.start("Cleanup")
                        with perf_timer("Cleanup operations", phase=True):
                            self._wipe_generated_directory(output_dir.parent, env)
                            if bundle_enabled:
                                _wipe_resources_lhp_directory(project_root)
                        phase_tracker.complete("Cleanup")
                        live.update(_render())

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
                    # reach the user. Resolve each discovered flowgroup's source
                    # YAML on the main thread and record the warning on the
                    # per-run ``WarningCollector`` here; the collector dedups
                    # and the final yellow Rich Panel renders after Live exits.
                    phase_tracker.start("Deprecation scan")
                    emit_deprecation_warning_if_needed(
                        warning_collector,
                        (
                            application_facade.orchestrator._find_source_yaml_for_flowgroup(
                                fg
                            )
                            for fg in all_flowgroups
                        ),
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
                            orig = getattr(response, "original_error", None)
                            rec.error_code = (
                                orig.code if isinstance(orig, LHPError) else None
                            )
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

                    with perf_timer(
                        f"Batch pipeline generation [{len(pipelines_to_generate)}]",
                        phase=True,
                    ):
                        batch_response = application_facade.generate_pipelines(
                            pipeline_fields=pipelines_to_generate,
                            env=env,
                            output_dir=output_dir if not dry_run else None,
                            specific_flowgroups=None,
                            include_tests=include_tests,
                            pre_discovered_all_flowgroups=all_flowgroups,
                            max_workers=max_workers,
                            on_pipeline_complete=_on_pipeline_complete,
                            on_pipeline_start=_on_pipeline_start,
                            warning_collector=warning_collector,
                        )
                        coalesced.update(force=True)

                    if not batch_response.is_successful():
                        phase_tracker.complete("Generation", success=False)
                        live.update(_render())
                    else:
                        phase_tracker.complete("Generation")
                        total_files = batch_response.total_files_written
                        all_generated_filenames = (
                            batch_response.aggregate_generated_filenames
                        )

                        if not dry_run:
                            phase_tracker.start("Monitoring artifacts")
                            with perf_timer("Monitoring artifacts", phase=True):
                                application_facade.orchestrator.finalize_monitoring_artifacts(
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
                                self._handle_bundle_operations(
                                    project_root,
                                    output_dir,
                                    env,
                                    dry_run,
                                    pipeline_config,
                                    project_config=application_facade.orchestrator.project_config,
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

            if batch_response is not None and batch_response.is_successful():
                logger.info(
                    f"Generation complete: {total_files} file(s) generated, "
                    f"{len(all_generated_filenames)} total output file(s)"
                )

            print_summary_table(
                records,
                dry_run,
                failed=(batch_response is None) or (not batch_response.is_successful()),
                show_all=show_all,
                warning_count=warning_collector.count,
                elapsed_s=perf_counter() - run_start,
            )

            # Render the per-run warning panel (yellow border) after the
            # summary table. Rendered while still inside the
            # ``rich_handler_attached`` context so the surrounding handler
            # discipline is preserved; the Live frame has already exited
            # so the panel is not redirected into a transient state.
            warning_collector.render(_console_module.err_console)

        # Both context managers have exited: stderr stream handlers
        # restored, no Rich Live frame attached, so any raise from here
        # propagates through clean stderr (what ``cli_error_boundary``
        # needs to render its Panel cleanly).
        if batch_response is None:
            # Unreachable in practice; defensive guard for the type checker.
            raise RuntimeError("generate batch did not complete")
        if not batch_response.is_successful():
            if batch_response.original_error is not None:
                raise batch_response.original_error
            raise RuntimeError(
                batch_response.error_message or "Batch pipeline generation failed"
            )

    def _run_catalog_schema_preflight(
        self,
        *,
        bundle_enabled: bool,
        project_root: Path,
        pipeline_config: Optional[str],
        pipelines_to_generate: List[str],
        env: str,
        project_config: Optional[ProjectConfig],
    ) -> None:
        """Run catalog/schema preflight when bundle support is enabled.

        ``require_pipeline_config_flag`` guarantees ``pipeline_config`` is
        non-None whenever ``bundle_enabled`` is True; the assertion
        documents that invariant for mypy.
        """
        if not bundle_enabled:
            return
        assert pipeline_config is not None, (
            "pipeline_config must be non-None when bundle_enabled is True; "
            "require_pipeline_config_flag enforces this invariant."
        )
        with perf_timer("Preflight catalog/schema validation", phase=True):
            monitoring_name = resolve_monitoring_pipeline_name(project_config)
            validate_catalog_schema(
                project_root=project_root,
                pipeline_config_path=pipeline_config,
                pipeline_names=pipelines_to_generate,
                env=env,
                monitoring_pipeline_name=monitoring_name,
            )

    def _create_application_facade(
        self,
        project_root: Path,
        pipeline_config_path: Optional[str] = None,
        *,
        max_workers: Optional[int] = None,
    ) -> LakehousePlumberApplicationFacade:
        """``ActionOrchestrator`` is imported lazily here because it pulls
        ~25 transitive modules (including ``concurrent.futures`` worker
        plumbing) into cold start; deferring the import shifts the cost
        inside the Live frame so the spinner draws first.
        """
        from ...core.orchestrator import ActionOrchestrator

        orchestrator = ActionOrchestrator(
            project_root,
            pipeline_config_path=pipeline_config_path,
            max_workers=max_workers,
        )
        return LakehousePlumberApplicationFacade(orchestrator)

    @staticmethod
    def _get_pipeline_names(
        pipeline: Optional[str], all_flowgroups: List[FlowGroup]
    ) -> List[str]:
        if pipeline:
            return [pipeline]
        pipeline_fields = {fg.pipeline for fg in all_flowgroups}
        return list(pipeline_fields) if pipeline_fields else []

    def _wipe_generated_directory(self, generated_dir: Path, env: str) -> None:
        env_dir = generated_dir / env
        if env_dir.exists():
            shutil.rmtree(env_dir)
        env_dir.mkdir(parents=True, exist_ok=True)

    def _handle_bundle_operations(
        self,
        project_root: Path,
        output_dir: Path,
        env: str,
        dry_run: bool,
        pipeline_config_path: Optional[str] = None,
        project_config=None,
    ) -> None:
        """Sync bundle resources for the generated output.

        Bundle activity is rendered as a phase marker by the outer
        ``execute()``; detail here is routed to ``logger.debug``.
        Caller must gate on ``bundle_enabled`` — this method assumes
        bundle support is on and does not recompute that decision.
        """
        if pipeline_config_path is not None:
            logger.debug(
                "Regenerating pipeline YAML files because a pipeline-config "
                "override was supplied"
            )

        if dry_run:
            logger.debug("Dry run: Bundle sync would be performed")
            return

        bundle_manager = BundleManager(
            project_root,
            pipeline_config_path,
            project_config=project_config,
        )
        bundle_manager.sync_resources_with_generated_files(
            output_dir,
            env,
        )
        logger.debug("Bundle resource files synchronized")


def _remove_legacy_state_artifacts(project_root: Path) -> None:
    legacy_file = project_root / ".lhp_state.json"
    legacy_dir = project_root / ".lhp_state"
    if legacy_file.exists():
        legacy_file.unlink()
    if legacy_dir.is_dir():
        shutil.rmtree(legacy_dir)


def _wipe_resources_lhp_directory(project_root: Path) -> None:
    resources_lhp = project_root / "resources" / "lhp"
    if resources_lhp.is_symlink():
        raise BundleResourceError(
            f"resources/lhp is a symlink; refusing to delete: {resources_lhp}. "
            f"Remove the symlink and let LHP manage this directory directly."
        )
    if resources_lhp.exists():
        shutil.rmtree(resources_lhp)
    resources_lhp.mkdir(parents=True, exist_ok=True)
