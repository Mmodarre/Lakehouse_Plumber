"""Clean Architecture layer definitions for LakehousePlumber."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Literal,
    Optional,
    Sequence,
)

from ..utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from ..cli.warning_collector import WarningCollector
    from ..utils.error_formatter import LHPError

# ============================================================================
# DATA TRANSFER OBJECTS (DTOs) - Cross-layer communication
# ============================================================================


@dataclass
class GenerationResponse:
    """DTO for generation responses from application to presentation layer."""

    success: bool
    generated_filenames: tuple[str, ...]  # ordered filenames, no content
    files_written: int
    total_flowgroups: int
    output_location: Optional[Path]
    performance_info: Dict[str, Any]
    error_message: Optional[str] = None
    # Preserved so the CLI can re-raise the original (typically LHPError) at
    # the fail-fast boundary, where ``cli_error_boundary`` formats it and
    # maps the LHP code to a POSIX exit code via ``ExitCode.from_lhp_error``.
    original_error: Optional[Exception] = None
    # Per-pipeline worker work-time in seconds, stamped at the worker
    # dispatch boundary in ``_dispatch_pipeline_for_generate``. Defaults to
    # ``0.0`` for infrastructural failures synthesized on the main thread
    # (``executor.submit`` raising, ``fut.result()`` unpickling errors) and
    # for no-op success deltas from empty pipelines — neither consumed
    # measurable worker work-time.
    duration_s: float = 0.0

    def is_successful(self) -> bool:
        return self.success


@dataclass
class BatchGenerationResponse:
    """Aggregate response for a multi-pipeline generation run.

    The flat-pool architecture raises aggregate errors only at the end, so
    some pipelines may have completed successfully — their state is already
    persisted via per-pipeline atomic save. ``original_error`` carries the
    underlying exception for the fail-fast boundary to re-raise.
    """

    success: bool
    pipeline_responses: Dict[str, "GenerationResponse"]
    total_files_written: int
    aggregate_generated_filenames: tuple[str, ...]
    output_location: Optional[Path]
    error_message: Optional[str] = None
    original_error: Optional[Exception] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass(frozen=True)
class ValidationIssue:
    """A single validation diagnostic — error or warning.

    Constructed by the validation service from LHPError instances (for
    errors) or directly at warning-emission sites. Consumers filter by
    severity to compute counts and render per-pipeline displays.

    ``lhp_error`` carries the live exception instance when the issue
    originates from a structured LHPError raised by a validation
    worker. It survives the worker→main IPC boundary via the LHPError
    ``__reduce__`` contract (preserves subclass identity, ``code``,
    ``context``, ``suggestions``). The CLI uses it to render the rich
    yellow ``Panel`` via :meth:`LHPError.__rich__` after the Live frame
    exits. ``None`` for issues sourced from plain strings (legacy CDC
    fan-in errors, discovery errors, deprecation warnings).
    """

    code: str  # e.g. "LHP-VAL-021"; "" for issues without a code
    severity: Literal["error", "warning"]
    title: str
    details: str = ""
    location: Optional[str] = None  # flowgroup name or file path
    lhp_error: Optional["LHPError"] = None


@dataclass
class ValidationResponse:
    """Per-pipeline validation outcome.

    Issues are a flat list of :class:`ValidationIssue` records carrying
    severity, code, title, details, and location. Convenience properties
    expose per-severity counts; ``has_errors`` / ``has_warnings`` remain
    available for predicate use.
    """

    success: bool
    issues: List[ValidationIssue]
    validated_pipelines: List[str]
    error_message: Optional[str] = None

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "error")

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == "warning")

    def has_errors(self) -> bool:
        return self.error_count > 0

    def has_warnings(self) -> bool:
        return self.warning_count > 0


@dataclass
class BatchValidationResponse:
    """Aggregate response for a multi-pipeline validation run.

    Mirrors :class:`BatchGenerationResponse`. ``original_error`` carries any
    batch-level exception while ``pipeline_responses`` preserves the
    per-pipeline outcomes captured before the failure.
    """

    success: bool
    pipeline_responses: Dict[str, "ValidationResponse"]
    total_errors: int
    total_warnings: int
    validated_pipelines: List[str]
    error_message: Optional[str] = None
    original_error: Optional[Exception] = None

    def is_successful(self) -> bool:
        return self.success


# ============================================================================
# APPLICATION FACADE - Implements application layer interface
# ============================================================================


class LakehousePlumberApplicationFacade:
    """Application-layer facade over the orchestrator for the CLI."""

    def __init__(self, orchestrator):
        self.orchestrator = orchestrator
        self.logger = logging.getLogger(__name__)

    def generate_pipelines(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        output_dir: Optional[Path],
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        pre_discovered_all_flowgroups=None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
        on_pipeline_start: Optional[Callable[[str], None]] = None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchGenerationResponse":
        """Coordinate batch (multi-pipeline) generation through the per-pipeline pool.

        Translates the orchestrator's per-pipeline :class:`PipelineDelta`
        into the presentation-layer :class:`GenerationResponse`, forwarding
        each one to the optional ``on_pipeline_complete`` callback in
        completion order. After the pool returns (or raises the aggregate
        error), builds the :class:`BatchGenerationResponse` with aggregate
        stats.

        Failed-pipeline shards are NOT written by the worker on failure
        (atomicity invariant of :class:`PipelineProcessor`); the main
        thread's aggregate raise short-circuits global state writes for
        failed batches but successful pipelines keep their shards.

        Args:
            pipeline_fields: Pipeline names to generate.
            env: Environment name.
            output_dir: Output root (``None`` for dry-run).
            specific_flowgroups: Optional whitelist.
            include_tests: Include test actions.
            pre_discovered_all_flowgroups: Re-use caller's one-shot discovery.
            max_workers: Override pool size; ``None`` falls back to the
                orchestrator's constructor value.
            on_pipeline_complete: Main-thread callback fired once per pipeline
                with ``(pipeline_name, GenerationResponse)``. Failures are
                caught and forwarded as ``GenerationResponse(success=False,
                original_error=...)``.

        Returns:
            :class:`BatchGenerationResponse` with per-pipeline responses,
            aggregate totals, and (on failure) the underlying aggregate
            exception preserved for re-raise at the CLI fail-fast boundary.
        """
        from ..models.processing import PipelineDelta

        pipeline_responses: Dict[str, "GenerationResponse"] = {}

        def _on_delta(delta: "PipelineDelta") -> None:
            from ..utils.error_formatter import lhp_error_from_worker_failure

            original_error: Optional[BaseException] = None
            error_message: Optional[str] = None
            if not delta.success:
                error_message = (
                    f"{delta.error_type}: {delta.error_message}"
                    if delta.error_type
                    else delta.error_message
                )
                if delta.lhp_error is not None:
                    original_error = delta.lhp_error
                else:
                    # Synthesize an LHPError that preserves dual-inheritance
                    # subclass identity for stdlib types (ValueError →
                    # LHPValidationError, FileNotFoundError → LHPFileError, …)
                    # so callers' ``except ValueError:`` / ``except
                    # FileNotFoundError:`` keep catching worker failures the
                    # same way they catch main-thread failures.
                    original_error = lhp_error_from_worker_failure(
                        pipeline_name=delta.pipeline_name,
                        error_type=delta.error_type or "UnknownError",
                        error_message=delta.error_message or "(no message)",
                        error_traceback=delta.error_traceback or "",
                    )

            response = GenerationResponse(
                success=delta.success,
                generated_filenames=delta.generated_filenames,
                files_written=delta.files_written,
                total_flowgroups=len(delta.generated_filenames),
                output_location=output_dir,
                performance_info={
                    "dry_run": output_dir is None,
                },
                error_message=error_message,
                original_error=original_error,
                duration_s=delta.duration_s,
            )
            pipeline_responses[delta.pipeline_name] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(delta.pipeline_name, response)
                except Exception as cb_exc:
                    self.logger.warning(
                        f"on_pipeline_complete callback raised "
                        f"for {delta.pipeline_name}: {cb_exc}"
                    )

        try:
            with perf_timer(f"facade.generate_pipelines [{len(pipeline_fields)}]"):
                self.orchestrator.generate_pipelines_by_fields(
                    pipeline_fields=list(pipeline_fields),
                    env=env,
                    output_dir=output_dir,
                    specific_flowgroups=specific_flowgroups,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_delta,
                    on_pipeline_start=on_pipeline_start,
                    warning_collector=warning_collector,
                )

            aggregate: tuple[str, ...] = ()
            total_written = 0
            for r in pipeline_responses.values():
                aggregate = aggregate + r.generated_filenames
                total_written += r.files_written

            return BatchGenerationResponse(
                success=True,
                pipeline_responses=pipeline_responses,
                total_files_written=total_written,
                aggregate_generated_filenames=aggregate,
                output_location=output_dir,
            )

        except Exception as exc:
            from ..utils.error_formatter import LHPError

            if isinstance(exc, LHPError):
                self.logger.debug(
                    f"Batch pipeline generation failed: "
                    f"{len(pipeline_responses)} pipeline(s) had outcomes captured"
                )
            else:
                self.logger.error(f"Batch pipeline generation failed: {exc}")

            aggregate = ()
            total_written = 0
            for r in pipeline_responses.values():
                if r.success:
                    aggregate = aggregate + r.generated_filenames
                    total_written += r.files_written

            return BatchGenerationResponse(
                success=False,
                pipeline_responses=pipeline_responses,
                total_files_written=total_written,
                aggregate_generated_filenames=aggregate,
                output_location=None,
                error_message=str(exc),
                original_error=exc,
            )

    def validate_pipelines(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "ValidationResponse"], None]
        ] = None,
        include_tests: bool = True,
        pre_discovered_all_flowgroups=None,
        warning_collector: Optional["WarningCollector"] = None,
    ) -> "BatchValidationResponse":
        """Coordinate batch (multi-pipeline) validation through the flat-pool engine.

        Mirrors :meth:`generate_pipelines` for the validate path: same
        layered dependency direction (CLI → facade → orchestrator), same
        per-pipeline callback semantics, same aggregate-error surface.
        Validation has no Phase B file writes or state save, so the DTO
        is simpler — only error / warning tallies.

        Failed-pipeline state is irrelevant here (validation never persists
        anything), so the aggregate-error path simply preserves whatever
        per-pipeline outcomes were captured before the underlying call
        raised.

        Args:
            pipeline_fields: Pipeline names to validate.
            env: Environment name.
            max_workers: Override thread-pool size; ``None`` falls back to
                the orchestrator's constructor value.
            on_pipeline_complete: Main-thread callback fired once per
                pipeline with ``(pipeline_name, ValidationResponse)`` in
                completion order. Failures are caught and logged; the
                batch is not torn down by a bad callback.
            include_tests: When False, test actions are filtered before
                processing (matches the orchestrator's default behaviour).
            pre_discovered_all_flowgroups: Re-use the caller's one-shot
                discovery results to avoid scanning twice.

        Returns:
            :class:`BatchValidationResponse` with per-pipeline responses,
            aggregate totals, and (on failure) the underlying aggregate
            exception preserved for re-raise at the CLI fail-fast
            boundary.
        """
        from .pipeline_executor import PipelineValidationOutcome

        pipeline_responses: Dict[str, "ValidationResponse"] = {}

        def _on_outcome(outcome: "PipelineValidationOutcome") -> None:
            # Structured LHPError instances → render via __rich__ as
            # yellow Panels at the validate CLI; plain-string errors
            # continue to surface as flat rows. ``outcome.lhp_errors``
            # and ``outcome.errors`` are NOT alternatives — they can
            # coexist within a single outcome when an LHPError-raising
            # flowgroup sits in the same pipeline as a string-only
            # discovery or CDC failure.
            issues: List[ValidationIssue] = []
            for lhp_err in outcome.lhp_errors:
                issues.append(
                    ValidationIssue(
                        code=lhp_err.code,
                        severity="error",
                        title=lhp_err.title,
                        details=lhp_err.details,
                        location=outcome.pipeline,
                        lhp_error=lhp_err,
                    )
                )
            for err in outcome.errors:
                issues.append(
                    ValidationIssue(
                        code="",
                        severity="error",
                        title=err,
                        location=outcome.pipeline,
                    )
                )
            for warn in outcome.warnings:
                issues.append(
                    ValidationIssue(
                        code="",
                        severity="warning",
                        title=warn,
                        location=outcome.pipeline,
                    )
                )
            response = ValidationResponse(
                success=outcome.success,
                issues=issues,
                validated_pipelines=[outcome.pipeline],
            )
            pipeline_responses[outcome.pipeline] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(outcome.pipeline, response)
                except Exception as cb_exc:
                    # The orchestrator already logs callback exceptions; we
                    # add a facade-level safety net so a buggy CLI display
                    # never tears down the batch.
                    self.logger.warning(
                        f"on_pipeline_complete callback raised "
                        f"for {outcome.pipeline}: {cb_exc}"
                    )

        try:
            with perf_timer(f"facade.validate_pipelines [{len(pipeline_fields)}]"):
                self.orchestrator.validate_pipelines_by_fields(
                    pipeline_fields=list(pipeline_fields),
                    env=env,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_outcome,
                    warning_collector=warning_collector,
                )

            total_errors = sum(r.error_count for r in pipeline_responses.values())
            total_warnings = sum(r.warning_count for r in pipeline_responses.values())

            return BatchValidationResponse(
                success=total_errors == 0,
                pipeline_responses=pipeline_responses,
                total_errors=total_errors,
                total_warnings=total_warnings,
                validated_pipelines=list(pipeline_fields),
            )

        except Exception as exc:
            # Aggregate error path: surface what completed and preserve
            # the underlying exception for the CLI's fail-fast boundary.
            from ..utils.error_formatter import LHPError

            if isinstance(exc, LHPError):
                self.logger.debug(
                    f"Batch pipeline validation failed: "
                    f"{len(pipeline_responses)} pipeline(s) had outcomes captured"
                )
            else:
                self.logger.error(f"Batch pipeline validation failed: {exc}")

            total_errors = sum(r.error_count for r in pipeline_responses.values())
            total_warnings = sum(r.warning_count for r in pipeline_responses.values())

            return BatchValidationResponse(
                success=False,
                pipeline_responses=pipeline_responses,
                total_errors=total_errors,
                total_warnings=total_warnings,
                validated_pipelines=list(pipeline_fields),
                error_message=str(exc),
                original_error=exc,
            )
