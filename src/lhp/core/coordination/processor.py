"""Per-worker, per-pipeline generation processor.

One :class:`PipelineProcessor` runs inside one worker process and owns
the full generation flow for one pipeline: Phase A (parse, codegen,
format) and the intra-pipeline Phase B work (cross-flowgroup
validation, ``.py`` writes, copied-module application, and
test-reporting hook generation).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, List, Optional, Sequence, Tuple

from ...errors import ErrorCategory, LHPValidationError
from ...generators.python_file_copier import CopiedModuleRecord, PythonFileCopier
from ...models.processing import PipelineDelta
from ...utils.file_header import write_normalized
from ...utils.performance_timer import perf_timer
from ..codegen.test_reporting import generate_test_reporting_hook
from .validation_service import ValidationService

if TYPE_CHECKING:
    from ...models.config import FlowGroup, FlowGroupContext, ProjectConfig
    from ...utils.formatter import CodeFormatter
    from ..codegen.coordinator import CodeGenerationService
    from ..processing.flowgroup_resolver import FlowgroupResolutionService
    from ..processing.substitution import EnhancedSubstitutionManager


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
    auxiliary_files: Tuple[Tuple[str, str], ...] = ()
    error: Optional[BaseException] = None


@dataclass(frozen=True, slots=True)
class ProcessingContext:
    """Per-pipeline worker collaborators bundled into a single argument.

    Constructed per pipeline by the dispatcher: ``substitution_mgr`` is
    scoped to a specific pipeline + env, and ``include_tests`` gates
    both code-generation behaviour and test-reporting hook emission.
    """

    processor: "FlowgroupResolutionService"
    code_generator: "CodeGenerationService"
    formatter: "CodeFormatter"
    substitution_mgr: "EnhancedSubstitutionManager"
    include_tests: bool


class PipelineProcessor:
    """Per-worker processor that owns one pipeline's full generation flow.

    Construct one inside a worker, then call :meth:`run` with the
    pipeline's flowgroups.

    :meth:`run` lifecycle: Phase A per flowgroup (parse, codegen,
    format) → cross-flowgroup validation → apply copy records → write
    ``.py`` files → emit test-reporting hook. Any exception is caught
    at the :meth:`run` boundary and packed into a
    :class:`PipelineDelta` failure.
    """

    def __init__(
        self,
        *,
        pipeline_name: str,
        environment: str,
        output_dir: Optional[Path],
        project_root: Path,
        project_config: Optional["ProjectConfig"],
        context: ProcessingContext,
    ):
        """Initialise per-pipeline worker state.

        Args:
            pipeline_name: Identifier for the pipeline being generated.
            environment: Environment being generated for (e.g. ``"dev"``).
            output_dir: Pipeline output directory. ``None`` for dry-run
                (no ``.py`` writes, no hook generation).
            project_root: Project root for relative-path resolution.
            project_config: Project config. Read for ``test_reporting``;
                otherwise opaque to the processor.
            context: :class:`ProcessingContext` bundling the per-pipeline
                collaborators (processor, code generator, formatter,
                substitution manager) and the ``include_tests`` flag.
        """
        self.pipeline_name = pipeline_name
        self.environment = environment
        self.output_dir = output_dir
        self.project_root = project_root
        self.project_config = project_config
        self.processor = context.processor
        self.code_generator = context.code_generator
        self.formatter = context.formatter
        self.substitution_mgr = context.substitution_mgr
        self.include_tests = context.include_tests

        self.copier = PythonFileCopier()

        self._artifacts_count = 0

    def run(self, contexts: Sequence["FlowGroupContext"]) -> PipelineDelta:
        """Run Phase A + intra-pipeline Phase B for the given flowgroups.

        Any exception — validation error, file write failure — is caught
        here and serialised into the returned :class:`PipelineDelta` so
        the worker can never crash the pool.

        Args:
            contexts: FlowGroupContext envelopes belonging to this pipeline.
                Empty sequences short-circuit to a success delta with zero
                counts.

        Returns:
            :class:`PipelineDelta` describing success/failure plus
            file-count rollups, plus the ordered tuple of filenames the
            facade surfaces as ``aggregate_generated_filenames``.
            Tracebacks are pre-formatted strings.
        """
        if not contexts:
            return PipelineDelta.success_(self.pipeline_name)

        try:
            with perf_timer(f"pipeline_processor [{self.pipeline_name}]"):
                phase_a_results = [self._phase_a(ctx) for ctx in contexts]
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

            generated_filenames = self._collect_generated_filenames(phase_a_results)
            files_written = len(generated_filenames)
            return PipelineDelta.success_(
                self.pipeline_name,
                files_written=files_written,
                artifacts_count=self._artifacts_count,
                generated_filenames=generated_filenames,
            )
        except Exception as exc:
            logger.exception(
                "PipelineProcessor failed for pipeline %s: %s",
                self.pipeline_name,
                type(exc).__name__,
            )
            return PipelineDelta.failure(self.pipeline_name, exc)

    def _collect_generated_filenames(
        self, results: List[FlowgroupResult]
    ) -> tuple[str, ...]:
        """Build the ordered tuple of filenames surfaced as
        ``aggregate_generated_filenames``.

        Filenames are bare flowgroup-derived (``{flowgroup_name}.py``).
        Empty flowgroups are skipped. Dry-run (``output_dir is None``)
        still populates the tuple for "would generate" listings.

        Insertion order is preserved (matches the ``contexts`` argument
        order to :meth:`run`) so dry-run output is deterministic.
        """
        out: List[str] = []
        for r in results:
            if not r.formatted_code.strip():
                continue
            out.append(f"{r.flowgroup_name}.py")
        return tuple(out)

    def _phase_a(self, ctx_in: "FlowGroupContext") -> FlowgroupResult:
        """Process + codegen + format one flowgroup.

        Captures user-module ``CopiedModuleRecord`` instances for
        replay by :meth:`_apply_copy_records`.
        """
        fg = ctx_in.flowgroup
        records: List[CopiedModuleRecord] = []
        try:
            with perf_timer(
                f"process_flowgroup [{fg.flowgroup}]",
                category="process_flowgroup",
            ):
                ctx_out = self.processor.process_flowgroup(
                    ctx_in, self.substitution_mgr, include_tests=self.include_tests
                )

            processed = ctx_out.flowgroup
            source_yaml: Optional[Path] = ctx_in.source_yaml

            with perf_timer(
                f"generate_code [{fg.flowgroup}]",
                category="generate_code",
            ):
                code = self.code_generator.generate_flowgroup_code(
                    processed,
                    self.substitution_mgr,
                    self.output_dir,
                    source_yaml,
                    self.environment,
                    self.include_tests,
                    phase_a_records=records,
                    auxiliary_files=ctx_out.auxiliary_files,
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
                auxiliary_files=tuple(ctx_out.auxiliary_files.items()),
            )
        except Exception as exc:
            logger.exception(
                "Phase A worker failed for flowgroup %s in pipeline %s: %s",
                fg.flowgroup,
                fg.pipeline,
                type(exc).__name__,
            )
            # copied_modules retained on failure-result for debug telemetry
            # only; never applied because _raise_for_phase_a_failures precedes
            # _apply_copy_records.
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

    def _raise_for_phase_a_failures(self, results: List[FlowgroupResult]) -> None:
        """Fail fast: re-raise the first Phase-A exception encountered."""
        for result in results:
            if not result.success and result.error is not None:
                raise result.error

    def _validate_cross_fg(self, flowgroups: List["FlowGroup"]) -> None:
        """Run cross-flowgroup validators and raise the first failure.

        Constructs a local :class:`ValidationService` per worker
        invocation and routes the cross-flowgroup compatibility checks
        through it (§9.24). Non-empty error lists are wrapped into an
        :class:`LHPValidationError`. LHPError raised inside the
        underlying validators (e.g. CDC fan-in shared-field mismatch)
        propagates unchanged.

        Local construction (vs. holding a ``ValidationService`` on
        ``self``) keeps the worker safe under ``spawn`` pickling and
        matches the §9.24 worker contract.
        """
        validation = ValidationService(self.project_root, self.project_config)
        with perf_timer("validate_cross_flowgroup"):
            result = validation.validate_cross_flowgroup(flowgroups)

        if result.table_creation_errors:
            self._raise_validation_error(
                code_number="009",
                title="Table creation validation failed",
                errors=result.table_creation_errors,
                suggestions=[
                    "Ensure each target table has exactly one action "
                    "with create_table: true",
                    "Check for conflicting table creation settings across flowgroups",
                    "Run 'lhp validate' for detailed diagnostics",
                ],
            )

        if result.cdc_fanin_errors:
            self._raise_validation_error(
                code_number="010",
                title="CDC fan-in compatibility validation failed",
                errors=result.cdc_fanin_errors,
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
        """Replay Phase-A copy records.

        The lock inside ``copier`` deduplicates intra-pipeline copies
        within this worker process.
        """
        for result in results:
            if result.processed_flowgroup is None or not result.copied_modules:
                continue
            for record in result.copied_modules:
                self.copier.apply_copy_record(record)

    def _write_python_files(self, results: List[FlowgroupResult]) -> None:
        """Write the per-flowgroup ``.py`` files and any auxiliary files.

        Empty flowgroups have their output file removed. Dry-run
        (``output_dir is None``) skips disk writes.

        Atomicity invariant: PipelineProcessor failures may leave a
        partial directory. The main thread's aggregate raise
        short-circuits state persistence but not file writes — earlier
        flowgroups in the same pipeline may already be written to disk
        before a later flowgroup's delete fails.
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
                    logger.exception(
                        "Failed to delete empty flowgroup file %s: %s: %s",
                        output_file,
                        type(exc).__name__,
                        exc.strerror or str(exc),
                    )
                    raise
                if fg is not None:
                    logger.info(
                        f"Skipping empty flowgroup: {fg.flowgroup} (no content)"
                    )
                continue

            with perf_timer(
                f"write_file [{result.flowgroup_name}]", category="write_file"
            ):
                write_normalized(output_file, result.formatted_code)

            if result.auxiliary_files:
                for aux_name, aux_content in result.auxiliary_files:
                    aux_file = self.output_dir / aux_name
                    write_normalized(aux_file, aux_content)
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
            include_tests=self.include_tests,
            substitution_mgr=self.substitution_mgr,
        )
        self._artifacts_count += added
