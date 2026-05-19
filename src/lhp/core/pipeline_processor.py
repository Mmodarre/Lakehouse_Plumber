"""Per-worker, per-pipeline generation processor.

One :class:`PipelineProcessor` runs inside one worker process and owns
the full generation flow for one pipeline: Phase A (parse, codegen,
format) and the intra-pipeline Phase B work (cross-flowgroup
validation, ``.py`` writes, copied-module application, state tracking
via a worker-local :class:`PipelineStateManager`, test-reporting hook
generation, and the final shard save).

Atomicity: ``.py`` files are written first, state mutations happen
next in-memory, and the shard is saved last via ``os.replace``. A
crash before save leaves orphan ``.py`` files but no shard, so the
next ``lhp generate`` regenerates cleanly.

The processor never receives a :class:`ProjectStateManager`; it
constructs its own :class:`PipelineStateManager` scoped to one
pipeline + one environment from ``state_dir`` + ``build_state``.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Dict, List, Optional, Sequence, Tuple

from ..generators.python_file_copier import CopiedModuleRecord, PythonFileCopier
from ..utils.error_formatter import ErrorCategory, LHPValidationError
from ..utils.performance_timer import perf_timer
from ..utils.smart_file_writer import SmartFileWriter
from .services.test_reporting import generate_test_reporting_hook
from .state.pipeline_state_manager import PipelineStateManager
from .state_models import PipelineDelta
from .validators.cdc_fanin_compatibility_validator import (
    CdcFanInCompatibilityValidator,
)
from .validators.table_creation_validator import TableCreationValidator

if TYPE_CHECKING:
    from ..models.config import FlowGroup, ProjectConfig
    from ..utils.formatter import CodeFormatter
    from ..utils.substitution import EnhancedSubstitutionManager
    from .services.blueprint_expander import BlueprintProvenance
    from .services.code_generator import CodeGenerator
    from .services.flowgroup_processor import FlowgroupProcessor


logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class FlowgroupResult:
    """Result of Phase A processing for a single flowgroup."""

    pipeline: str
    flowgroup_name: str
    processed_flowgroup: Optional["FlowGroup"]
    code: str
    formatted_code: str
    source_yaml: Optional[Path]
    success: bool
    copied_modules: Tuple[CopiedModuleRecord, ...] = ()
    error: Optional[BaseException] = None


@dataclass(frozen=True, slots=True)
class ProcessingContext:
    """Per-pipeline worker collaborators bundled into a single argument.

    Constructed per pipeline by the dispatcher: ``substitution_mgr`` is
    scoped to a specific pipeline + env, and ``include_tests`` gates
    both code-generation behaviour and test-reporting hook emission.
    """

    processor: "FlowgroupProcessor"
    code_generator: "CodeGenerator"
    formatter: "CodeFormatter"
    substitution_mgr: "EnhancedSubstitutionManager"
    include_tests: bool


class PipelineProcessor:
    """Per-worker processor that owns one pipeline's full generation flow.

    Construct one inside a worker, then call :meth:`run` with the
    pipeline's flowgroups. The processor never receives a project-wide
    state manager; it constructs its own :class:`PipelineStateManager`
    from ``state_dir`` + ``build_state``.

    :meth:`run` lifecycle: Phase A per flowgroup (parse, codegen,
    format) → cross-flowgroup validation → apply copy records → write
    ``.py`` files → emit test-reporting hook → save shard. Any
    exception is caught at the :meth:`run` boundary and packed into a
    :class:`PipelineDelta` failure.
    """

    def __init__(
        self,
        *,
        pipeline_name: str,
        environment: str,
        output_dir: Optional[Path],
        state_dir: Optional[Path],
        project_root: Path,
        project_config: "ProjectConfig",
        context: ProcessingContext,
        build_state: bool,
        blueprint_provenance: Optional[
            Dict[Tuple[str, str], "BlueprintProvenance"]
        ] = None,
    ):
        """Initialise per-pipeline worker state.

        Args:
            pipeline_name: Identifier for the pipeline being generated.
                Used as the shard's filename stem when ``build_state``.
            environment: Environment being generated for (e.g. ``"dev"``).
                Mutations on the shard are restricted to this env's slice.
            output_dir: Pipeline output directory. ``None`` for dry-run
                (no ``.py`` writes, no hook generation).
            state_dir: ``<project_root>/.lhp_state`` directory. May be
                ``None`` only when ``build_state`` is False; otherwise
                required.
            project_root: Project root, forwarded to
                :class:`PipelineStateManager` for relative-path resolution.
            project_config: Project config. Read for ``test_reporting``;
                otherwise opaque to the processor.
            context: :class:`ProcessingContext` bundling the per-pipeline
                collaborators (processor, code generator, formatter,
                substitution manager) and the ``include_tests`` flag.
            build_state: When False, the processor skips constructing a
                state manager and writes no shard. Equivalent to the
                ``--no-state`` CLI flag.
        """
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.output_dir = output_dir
        self.state_dir = state_dir
        self.project_root = project_root
        self.project_config = project_config
        self.processor = context.processor
        self.code_generator = context.code_generator
        self.formatter = context.formatter
        self.substitution_mgr = context.substitution_mgr
        self.include_tests = context.include_tests
        self.build_state = build_state

        self.copier = PythonFileCopier()
        self.smart_writer = SmartFileWriter()

        self._table_creation_validator = TableCreationValidator()
        self._cdc_fanin_validator = CdcFanInCompatibilityValidator()

        self.state_manager: Optional[PipelineStateManager] = None
        if self.build_state:
            if state_dir is None:
                raise ValueError(
                    "PipelineProcessor requires state_dir when build_state=True"
                )
            self.state_manager = PipelineStateManager(
                state_dir=state_dir,
                pipeline_name=pipeline_name,
                environment=environment,
                project_root=project_root,
            )
            # Provenance does not cross the process boundary; re-inject here.
            if blueprint_provenance:
                self.state_manager.set_blueprint_provenance(blueprint_provenance)

        self._artifacts_count = 0

    # ------------------------------------------------------------------ public

    def run(self, flowgroups: Sequence["FlowGroup"]) -> PipelineDelta:
        """Run Phase A + intra-pipeline Phase B for the given flowgroups.

        Any exception — validation error, file write failure, state save
        failure — is caught here and serialised into the returned
        :class:`PipelineDelta` so the worker can never crash the pool.

        Args:
            flowgroups: Flowgroups belonging to this pipeline. Empty
                sequences short-circuit to a success delta with zero
                counts.

        Returns:
            :class:`PipelineDelta` describing success/failure plus
            file-count rollups, plus the ``{relative_path: formatted_code}``
            map the facade surfaces as ``aggregate_generated_files``.
            Tracebacks are pre-formatted strings.
        """
        if not flowgroups:
            return PipelineDelta.success_(self.pipeline_name)

        try:
            with perf_timer(f"pipeline_processor [{self.pipeline_name}]"):
                phase_a_results = [self._phase_a(fg) for fg in flowgroups]
                self._raise_for_phase_a_failures(phase_a_results)

                processed_flowgroups = [
                    r.processed_flowgroup
                    for r in phase_a_results
                    if r.processed_flowgroup is not None
                ]
                self._validate_cross_fg(processed_flowgroups)

                self._apply_copy_records(phase_a_results)
                self._write_python_files(phase_a_results)
                self._generate_test_hook(processed_flowgroups)

                if self.state_manager is not None:
                    with perf_timer(f"state_save [{self.pipeline_name}]"):
                        self.state_manager.save()

            files_written, files_skipped = self.smart_writer.get_stats()
            generated_files = self._collect_generated_files(phase_a_results)
            return PipelineDelta.success_(
                self.pipeline_name,
                files_written=files_written,
                files_skipped=files_skipped,
                artifacts_count=self._artifacts_count,
                generated_files=generated_files,
            )
        except BaseException as exc:
            logger.error(
                f"PipelineProcessor failed for pipeline {self.pipeline_name}: {exc}"
            )
            return PipelineDelta.failure(self.pipeline_name, exc)

    def _collect_generated_files(
        self, results: List[FlowgroupResult]
    ) -> Dict[str, str]:
        """Build the ``{filename: formatted_code}`` map surfaced as
        ``aggregate_generated_files``.

        Keyed by the bare flowgroup filename. Empty flowgroups are
        skipped. Dry-run (``output_dir is None``) still populates the
        map for "would generate" listings.
        """
        out: Dict[str, str] = {}
        for r in results:
            if not r.formatted_code.strip():
                continue
            filename = f"{r.flowgroup_name}.py"
            out[filename] = r.formatted_code
        return out

    # ------------------------------------------------------------------ Phase A

    def _phase_a(self, fg: "FlowGroup") -> FlowgroupResult:
        """Process + codegen + format one flowgroup.

        Captures user-module ``CopiedModuleRecord`` instances for
        replay by :meth:`_apply_copy_records`. State tracking happens
        in Phase B after files are on disk, so the code generator is
        invoked with ``state_manager=None`` here.
        """
        records: List[CopiedModuleRecord] = []
        try:
            with perf_timer(
                f"process_flowgroup [{fg.flowgroup}]",
                category="process_flowgroup",
            ):
                processed = self.processor.process_flowgroup(
                    fg, self.substitution_mgr, include_tests=self.include_tests
                )

            if fg._auxiliary_files:
                processed._auxiliary_files = fg._auxiliary_files
            processed._has_original_test_actions = fg._has_original_test_actions

            source_yaml: Optional[Path] = getattr(fg, "_source_yaml", None)

            with perf_timer(
                f"generate_code [{fg.flowgroup}]",
                category="generate_code",
            ):
                code = self.code_generator.generate_flowgroup_code(
                    processed,
                    self.substitution_mgr,
                    self.output_dir,
                    None,
                    source_yaml,
                    self.environment,
                    self.include_tests,
                    None,
                    phase_a_records=records,
                )

            with perf_timer(
                f"format_code [{fg.flowgroup}]",
                category="format_code",
            ):
                formatted = self.formatter.format_code(code)

            return FlowgroupResult(
                pipeline=fg.pipeline,
                flowgroup_name=fg.flowgroup,
                processed_flowgroup=processed,
                code=code,
                formatted_code=formatted,
                source_yaml=source_yaml,
                success=True,
                copied_modules=tuple(records),
            )
        except BaseException as exc:
            logger.error(
                f"Phase A worker failed for flowgroup {fg.flowgroup} "
                f"in pipeline {fg.pipeline}: {exc}"
            )
            return FlowgroupResult(
                pipeline=fg.pipeline,
                flowgroup_name=fg.flowgroup,
                processed_flowgroup=None,
                code="",
                formatted_code="",
                source_yaml=None,
                success=False,
                copied_modules=tuple(records),
                error=exc,
            )

    # ------------------------------------------------------------------ Phase B

    def _raise_for_phase_a_failures(self, results: List[FlowgroupResult]) -> None:
        """Fail fast: re-raise the first Phase-A exception encountered."""
        for result in results:
            if not result.success and result.error is not None:
                raise result.error

    def _validate_cross_fg(self, flowgroups: List["FlowGroup"]) -> None:
        """Run cross-flowgroup validators and raise the first failure.

        Each validator returns a list of error strings. Non-empty
        lists are wrapped into an :class:`LHPValidationError`.
        """
        with perf_timer("validate_table_creation_rules"):
            errors = self._table_creation_validator.validate(flowgroups)
        if errors:
            self._raise_validation_error(
                code_number="009",
                title="Table creation validation failed",
                errors=errors,
                suggestions=[
                    "Ensure each target table has exactly one action "
                    "with create_table: true",
                    "Check for conflicting table creation settings "
                    "across flowgroups",
                    "Run 'lhp validate' for detailed diagnostics",
                ],
            )

        with perf_timer("validate_cdc_fanin_compatibility"):
            cdc_errors = self._cdc_fanin_validator.validate(flowgroups)
        if cdc_errors:
            self._raise_validation_error(
                code_number="010",
                title="CDC fan-in compatibility validation failed",
                errors=cdc_errors,
                suggestions=[
                    "All CDC actions sharing a target must agree on "
                    "table-level and CDC-key fields (keys, sequence_by, "
                    "stored_as_scd_type, track_history_*, "
                    "partition_columns, table_properties, etc.)",
                    "Fields allowed to differ per flow: source, once, "
                    "ignore_null_updates, apply_as_deletes, "
                    "apply_as_truncates, column_list, except_column_list",
                    "Run 'lhp validate' for detailed diagnostics",
                ],
            )

    def _raise_validation_error(
        self,
        *,
        code_number: str,
        title: str,
        errors: List[str],
        suggestions: List[str],
    ) -> None:
        """Wrap a list of validator-produced strings into an LHPValidationError."""
        raise LHPValidationError(
            category=ErrorCategory.VALIDATION,
            code_number=code_number,
            title=title,
            details=f"{title}:\n" + "\n".join(f"  - {e}" for e in errors),
            suggestions=suggestions,
            context={
                "Pipeline": self.pipeline_name,
                "Error Count": len(errors),
            },
        )

    def _apply_copy_records(self, results: List[FlowgroupResult]) -> None:
        """Replay Phase-A copy records and track each copied file.

        The lock inside ``copier`` deduplicates intra-pipeline copies
        within this worker process.
        """
        for result in results:
            if result.processed_flowgroup is None or not result.copied_modules:
                continue
            for record in result.copied_modules:
                entries = self.copier.apply_copy_record(
                    record,
                    source_yaml=result.source_yaml,
                    flowgroup=result.processed_flowgroup,
                )
                if self.state_manager is None:
                    continue
                for entry in entries:
                    # Auto-generated pipelines (e.g. event-log monitoring)
                    # emit copies with no source YAML / flowgroup; skip
                    # tracking for those — there is no shard entry to write.
                    if entry.source_yaml is None or entry.flowgroup is None:
                        continue
                    self.state_manager.track_generated_file(
                        generated_path=entry.dest_path,
                        source_yaml=entry.source_yaml,
                        flowgroup=entry.flowgroup,
                    )

    def _write_python_files(self, results: List[FlowgroupResult]) -> None:
        """Write the per-flowgroup ``.py`` files and any auxiliary files.

        Empty flowgroups have their output file removed and dropped
        from the shard. Dry-run (``output_dir is None``) skips disk
        writes and state tracking.
        """
        if self.output_dir is None:
            for result in results:
                logger.info(f"Would generate: {result.flowgroup_name}.py")
            return

        for result in results:
            filename = f"{result.flowgroup_name}.py"
            fg = result.processed_flowgroup
            output_file = self.output_dir / filename

            if not result.formatted_code.strip():
                try:
                    output_file.unlink(missing_ok=True)
                except OSError as exc:
                    logger.error(
                        f"Failed to delete empty flowgroup file {output_file}: {exc}"
                    )
                    raise
                # Drop the shard entry so the next run does not see drift.
                if self.state_manager is not None:
                    self.state_manager.untrack_generated_file(output_file)
                if fg is not None:
                    logger.info(
                        f"Skipping empty flowgroup: {fg.flowgroup} (no content)"
                    )
                continue

            with perf_timer(
                f"write_file [{result.flowgroup_name}]", category="write_file"
            ):
                self.smart_writer.write_if_changed(output_file, result.formatted_code)

            if (
                self.state_manager is not None
                and result.source_yaml is not None
                and fg is not None
            ):
                self.state_manager.track_generated_file(
                    generated_path=output_file,
                    source_yaml=result.source_yaml,
                    flowgroup=fg.flowgroup,
                )

            if fg is not None and fg._auxiliary_files:
                for aux_name, aux_content in fg._auxiliary_files.items():
                    aux_file = self.output_dir / aux_name
                    self.smart_writer.write_if_changed(aux_file, aux_content)
                    logger.info(f"Generated auxiliary: {aux_file}")

    def _generate_test_hook(self, flowgroups: List["FlowGroup"]) -> None:
        """Emit the test-reporting hook when configured."""
        if self.output_dir is None:
            return

        added = generate_test_reporting_hook(
            pipeline_name=self.pipeline_name,
            flowgroups=flowgroups,
            output_dir=self.output_dir,
            project_config=self.project_config,
            project_root=self.project_root,
            state_manager=self.state_manager,
            smart_writer=self.smart_writer,
            include_tests=self.include_tests,
            substitution_mgr=self.substitution_mgr,
        )
        self._artifacts_count += added
