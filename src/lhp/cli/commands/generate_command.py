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
    PipelineRecord,
    append_phase_marker_line,
    render_status_group,
    rich_handler_attached,
)
from ..warning_collector import WarningCollector
from ..yaml_scanner import emit_deprecation_warning_if_needed
from .base_command import BaseCommand

logger = logging.getLogger(__name__)


class GenerateCommand(BaseCommand):
    """
    Pipeline code generation command.

    This command follows Clean Architecture principles:
    - Pure presentation layer (no business logic)
    - Uses DTOs for layer communication
    - Delegates all business logic to application facade
    - Focused only on user interaction and display

    The user-facing run output is rendered via a single ``rich.live.Live``
    status panel during generation, followed by a per-pipeline summary
    table. Phase markers (Discovery / Preflight / Bundle sync) are only
    shown when the phase takes longer than 250ms.
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
        """Execute the generate command using clean architecture.

        Args:
            env: Environment to generate for
            pipeline: Specific pipeline to generate (optional)
            output: Output directory (defaults to generated/{env})
            dry_run: Preview without generating files
            no_bundle: Disable bundle support
            include_tests: Include test actions in generation
            pipeline_config: Custom pipeline config file path (relative to project root)
            max_workers: Override the worker pool size; ``None`` defers to auto-detection.
            show_all: When True, the post-run summary table includes every
                pipeline. Default False — only failed pipelines are shown,
                and on a full-success run the table is suppressed in favor
                of a single-line footer.
            force: Deprecated no-op flag (legacy ``--force``); when True,
                routes a deprecation entry through ``warning_collector``.
            no_state: Deprecated no-op flag (legacy ``--no-state``); when
                True, routes a deprecation entry through ``warning_collector``.
        """
        from rich.console import Group
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

        # State for the Live status panel.
        records: Dict[str, PipelineRecord] = {}
        phase_lines: List[Text] = []
        failure_lines: List[Text] = []
        start_times: Dict[str, float] = {}
        run_start = perf_counter()

        # ``batch_response`` is referenced by the post-Live raise path,
        # so it must be defined before the context managers open. The
        # ``None`` sentinel covers "Live exited via an unrelated exception
        # before the batch was attempted".
        batch_response: Optional[BatchGenerationResponse] = None
        total_files = 0
        all_generated_filenames: List[str] = []

        def _maybe_phase_line(
            label: str,
            duration_s: float,
            *,
            force: bool = False,
            success: bool = True,
        ) -> None:
            """Append a phase marker to the closure-local ``phase_lines``.

            Thin wrapper around :func:`append_phase_marker_line` that
            pre-binds the closure-local list. The threshold suppression
            and force override live in the module-level helper so they
            can be unit-tested without instantiating the command.
            """
            append_phase_marker_line(
                phase_lines,
                label,
                duration_s,
                force=force,
                success=success,
            )

        def _elapsed() -> str:
            s = int(perf_counter() - run_start)
            return f"{s // 60:02d}:{s % 60:02d}"

        def _render() -> Group:
            return render_status_group(
                records,
                phase_lines,
                failure_lines,
                elapsed_text=_elapsed(),
            )

        # Defensive RichHandler scope: route stray WARNING+ records
        # through Rich so they don't tear the Live frame, then restore
        # the original stream handlers on exit. The handler is detached
        # BEFORE the failure raise propagates so cli_error_boundary
        # prints its Panel to a stderr with no Rich machinery still
        # attached -- that's what prevents the "failed  LHP-XXX-NNN<-
        # box-edge-glue" rendering bug.
        with rich_handler_attached(_console_module.err_console):
            with Live(
                _render(),
                console=_console_module.console,
                refresh_per_second=10,
                redirect_stdout=True,
                redirect_stderr=True,
            ) as live:
                # === Orchestrator init ===
                # ``_create_application_facade`` triggers the lazy
                # ``ActionOrchestrator`` import (~25 transitive modules,
                # including the worker pool plumbing). Doing it INSIDE
                # the Live frame means the first frame draws before the
                # heavy import resolves, so the user sees the spinner
                # immediately rather than staring at a blank terminal.
                init_start = perf_counter()
                with perf_timer("Orchestrator init", phase=True):
                    application_facade = self._create_application_facade(
                        project_root,
                        pipeline_config,
                        max_workers=max_workers,
                    )
                _maybe_phase_line("Orchestrator init", perf_counter() - init_start)
                live.update(_render())

                # === Discovery ===
                disc_start = perf_counter()
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
                    from ...utils.error_formatter import ErrorCategory, LHPConfigError

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

                _maybe_phase_line(
                    f"Discovering ({len(all_flowgroups)} flowgroups)",
                    perf_counter() - disc_start,
                )
                live.update(_render())

                logger.debug(
                    f"Pipelines discovered for generation: {pipelines_to_generate}"
                )

                # === Preflight (only if bundle enabled) ===
                if bundle_enabled:
                    pf_start = perf_counter()
                    self._run_catalog_schema_preflight(
                        bundle_enabled=bundle_enabled,
                        project_root=project_root,
                        pipeline_config=pipeline_config,
                        pipelines_to_generate=pipelines_to_generate,
                        env=env,
                        project_config=application_facade.orchestrator.project_config,
                    )
                    _maybe_phase_line("Preflight", perf_counter() - pf_start)
                    live.update(_render())

                # Always wipe the env-specific generated directory: every
                # run is a full regenerate after the V0.8.7 state-tracking
                # removal. The resources/lhp/ wipe is gated on bundle so
                # non-bundle projects don't materialize the directory just
                # to delete it.
                if not dry_run:
                    with perf_timer("Cleanup operations", phase=True):
                        self._wipe_generated_directory(output_dir.parent, env)
                        if bundle_enabled:
                            _wipe_resources_lhp_directory(project_root)

                # Seed records up front so the spinner shows
                # "0 of N pipelines done" immediately.
                for pipeline_name in pipelines_to_generate:
                    records[pipeline_name] = PipelineRecord(pipeline_name)
                    start_times[pipeline_name] = perf_counter()
                live.update(_render())

                # Pre-pool deprecation scan: workers are silenced (NullHandler
                # only) so the in-worker SubstitutionManager warning cannot
                # reach the user. Resolve each discovered flowgroup's source
                # YAML on the main thread and record the warning on the
                # per-run ``WarningCollector`` here; the collector dedups
                # and the final yellow Rich Panel renders after Live exits.
                emit_deprecation_warning_if_needed(
                    warning_collector,
                    (
                        application_facade.orchestrator._find_source_yaml_for_flowgroup(
                            fg
                        )
                        for fg in all_flowgroups
                    ),
                )

                # === Generation ===
                # Main-thread per-pipeline callback fired by the facade in
                # completion order. Updates record state and appends an
                # inline failure line for failures; the Live panel re-renders.
                def _on_pipeline_complete(
                    name: str, response: GenerationResponse
                ) -> None:
                    rec = records[name]
                    rec.success = bool(response.success)
                    rec.files = response.files_written if response.success else 0
                    rec.duration_s = perf_counter() - start_times[name]
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
                    live.update(_render())

                gen_start = perf_counter()
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
                        warning_collector=warning_collector,
                    )
                gen_duration = perf_counter() - gen_start

                if not batch_response.is_successful():
                    # Forced phase marker (overrides the 250ms threshold).
                    # A fast-failing Generation phase still needs a
                    # diagnostic line so the user sees WHICH phase
                    # failed in the Live panel.
                    _maybe_phase_line(
                        "Generation",
                        gen_duration,
                        force=True,
                        success=False,
                    )
                    live.update(_render())
                else:
                    total_files = batch_response.total_files_written
                    all_generated_filenames = (
                        batch_response.aggregate_generated_filenames
                    )

                    # Finalize monitoring artifacts after all pipelines complete.
                    if not dry_run:
                        with perf_timer("Monitoring artifacts", phase=True):
                            application_facade.orchestrator.finalize_monitoring_artifacts(
                                env, output_dir
                            )

                    # === Bundle sync (success-only path) ===
                    # ``should_enable_bundle_support`` already factors in
                    # ``no_bundle`` (returns False when --no-bundle is
                    # set), so ``bundle_enabled`` alone is sufficient.
                    if bundle_enabled:
                        bs_start = perf_counter()
                        with perf_timer("Bundle sync", phase=True):
                            self._handle_bundle_operations(
                                project_root,
                                output_dir,
                                env,
                                dry_run,
                                pipeline_config,
                                project_config=application_facade.orchestrator.project_config,
                            )
                        _maybe_phase_line(
                            "Bundle sync" if not dry_run else "Bundle sync (dry-run)",
                            perf_counter() - bs_start,
                        )
                        live.update(_render())

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
            )

            # Render the per-run warning panel (yellow border) after the
            # summary table. Rendered while still inside the
            # ``rich_handler_attached`` context so the surrounding handler
            # discipline is preserved; the Live frame has already exited
            # so the panel is not redirected into a transient state.
            warning_collector.render(_console_module.err_console)

        # Both context managers have exited: stderr stream handlers
        # restored, no Rich Live frame attached. ANY raise from here on
        # propagates through clean stderr, which is what
        # ``cli_error_boundary`` needs to render its Panel cleanly.
        if batch_response is None:
            # Unreachable in practice: if Live exited normally, the
            # batch was attempted. Defensive guard for the type checker
            # and against future restructures.
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
        """Run catalog/schema preflight validation when bundle support is enabled.

        ``require_pipeline_config_flag`` (called earlier in ``execute``)
        guarantees ``pipeline_config`` is non-None whenever
        ``bundle_enabled`` is True; the assertion documents that invariant
        for mypy.
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
        """Create application facade for business layer access.

        ``ActionOrchestrator`` is imported lazily here (rather than at
        module top) because it pulls ~25 transitive modules — including
        ``concurrent.futures`` worker plumbing — into the cold start of
        every CLI invocation. Deferring the import shaves measurable
        latency off ``lhp generate`` startup, where the cost is paid
        inside the Live frame as the first phase instead of before any
        UI has rendered.
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
        """Extract pipeline names from discovered flowgroups, or return specific pipeline."""
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
        """Handle bundle operations - coordinate with bundle management.

        Bundle-related user-facing output is suppressed here because the
        outer ``execute()`` renders bundle activity as a phase marker in
        the Live status panel. Verbose detail is routed through
        ``logger.debug`` so it shows up in the rotating log file but not
        in the interactive run output.

        Caller is expected to gate on ``bundle_enabled`` (computed once
        at the top of ``execute()``); this method assumes bundle support
        is on and does not recompute that decision.
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
