"""Clean Architecture layer definitions for LakehousePlumber."""

import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    List,
    Optional,
    Sequence,
    Set,
    Tuple,
)

from ..utils.performance_timer import perf_timer

if TYPE_CHECKING:
    from .services.generation_planning_service import GenerationPlan
    from .state_manager import ProjectStateManager


# ============================================================================
# DATA TRANSFER OBJECTS (DTOs) - Cross-layer communication
# ============================================================================


@dataclass
class PipelineGenerationRequest:
    """DTO for pipeline generation requests from presentation to application layer."""

    pipeline_identifier: str
    environment: str
    include_tests: bool = False
    force_all: bool = False
    specific_flowgroups: Optional[List[str]] = None
    output_directory: Optional[Path] = None
    dry_run: bool = False
    no_cleanup: bool = False
    pipeline_config_path: Optional[str] = None


@dataclass
class PipelineValidationRequest:
    """DTO for pipeline validation requests."""

    pipeline_identifier: str
    environment: str
    verbose: bool = False
    include_tests: bool = True


@dataclass
class StalenessAnalysisRequest:
    """DTO for staleness analysis requests."""

    pipeline_names: List[str]
    environment: str
    include_tests: bool = False
    force: bool = False


@dataclass
class GenerationResponse:
    """DTO for generation responses from application to presentation layer."""

    success: bool
    generated_files: Dict[str, str]  # filename -> content
    files_written: int
    total_flowgroups: int
    output_location: Optional[Path]
    performance_info: Dict[str, Any]
    error_message: Optional[str] = None
    # Preserved so the CLI can re-raise the original (typically LHPError) at
    # the fail-fast boundary, where ``cli_error_boundary`` formats it and
    # maps the LHP code to a POSIX exit code via ``ExitCode.from_lhp_error``.
    original_error: Optional[Exception] = None

    def is_successful(self) -> bool:
        """Check if generation was successful."""
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
    aggregate_generated_files: Dict[str, str]
    output_location: Optional[Path]
    error_message: Optional[str] = None
    original_error: Optional[Exception] = None

    def is_successful(self) -> bool:
        return self.success


@dataclass
class ValidationResponse:
    """DTO for validation responses."""

    success: bool
    errors: List[str]
    warnings: List[str]
    validated_pipelines: List[str]
    error_message: Optional[str] = None

    def has_errors(self) -> bool:
        """Check if validation found errors."""
        return len(self.errors) > 0

    def has_warnings(self) -> bool:
        """Check if validation found warnings."""
        return len(self.warnings) > 0


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


@dataclass
class AnalysisResponse:
    """DTO for staleness analysis responses."""

    success: bool
    pipelines_needing_generation: Dict[str, Dict]
    pipelines_up_to_date: Dict[str, int]
    has_global_changes: bool
    global_changes: List[str]
    include_tests_context_applied: bool
    total_new_files: int
    total_stale_files: int
    total_up_to_date_files: int
    error_message: Optional[str] = None

    # Env-wide generation-context change flag + human-readable diff (e.g.
    # "include_tests: False -> True"). When True, all pipelines regenerate.
    context_changed: bool = False
    context_changes: List[str] = field(default_factory=list)

    def has_work_to_do(self) -> bool:
        """Check if any generation work needs to be done."""
        return len(self.pipelines_needing_generation) > 0


# ============================================================================
# LAYER INTERFACES - Define contracts between layers
# ============================================================================


class ApplicationLayer(ABC):
    """Interface for the application layer - coordinates use cases."""

    @abstractmethod
    def generate_pipeline(
        self, request: PipelineGenerationRequest
    ) -> GenerationResponse:
        """Coordinate pipeline generation use case."""
        pass

    @abstractmethod
    def validate_pipeline(
        self, request: PipelineValidationRequest
    ) -> ValidationResponse:
        """Coordinate pipeline validation use case."""
        pass

    @abstractmethod
    def analyze_staleness(self, request: StalenessAnalysisRequest) -> AnalysisResponse:
        """Coordinate staleness analysis use case."""
        pass


class BusinessLayer(ABC):
    """Interface for the business layer - contains business rules."""

    @abstractmethod
    def create_generation_plan(
        self, env: str, pipeline_identifier: str, include_tests: bool, **kwargs
    ) -> "GenerationPlan":
        """Create generation plan based on business rules."""
        pass

    @abstractmethod
    def execute_generation_strategy(self, strategy_type: str, context: Any) -> Any:
        """Execute generation strategy based on business logic."""
        pass

    @abstractmethod
    def validate_configuration(self, pipeline_identifier: str, env: str) -> tuple:
        """Validate configuration based on business rules."""
        pass


class DataLayer(ABC):
    """Interface for the data layer - handles data access and persistence."""

    @abstractmethod
    def get_generation_state(self, env: str, pipeline: str = None) -> Dict[str, List]:
        """Get current generation state from persistence."""
        pass

    @abstractmethod
    def track_generated_file(self, file_path: Path, metadata: Dict[str, Any]) -> None:
        """Track generated file in persistent state."""
        pass

    @abstractmethod
    def cleanup_orphaned_files(
        self,
        env: str,
        dry_run: bool = False,
        active_flowgroups: Optional[Set[Tuple[str, str]]] = None,
    ) -> List[str]:
        """Clean up orphaned files from persistence."""
        pass


class PresentationLayer(ABC):
    """Interface for the presentation layer - handles user interaction."""

    @abstractmethod
    def display_generation_results(self, response: GenerationResponse) -> None:
        """Display generation results to user."""
        pass

    @abstractmethod
    def display_validation_results(self, response: ValidationResponse) -> None:
        """Display validation results to user."""
        pass

    @abstractmethod
    def display_analysis_results(self, response: AnalysisResponse) -> None:
        """Display analysis results to user."""
        pass

    @abstractmethod
    def get_user_input(self, prompt: str) -> str:
        """Get input from user."""
        pass


# ============================================================================
# APPLICATION FACADE - Implements application layer interface
# ============================================================================


class LakehousePlumberApplicationFacade(ApplicationLayer):
    """
    Application layer facade providing clean interface to business layer.

    This facade abstracts the complexity of the orchestrator and provides
    a clean, testable interface for the CLI layer.
    """

    def __init__(self, orchestrator, state_manager: Optional["ProjectStateManager"] = None):
        """
        Initialize application facade.

        Args:
            orchestrator: Business layer orchestrator
            state_manager: Optional state manager for data layer
        """
        self.orchestrator = orchestrator
        self.state_manager = state_manager
        self.logger = logging.getLogger(__name__)

    def generate_pipeline(
        self,
        request: PipelineGenerationRequest,
        pre_discovered_all_flowgroups=None,
    ) -> GenerationResponse:
        """
        Coordinate pipeline generation use case.

        Translates presentation layer request into business layer operations
        and returns structured response for presentation layer.
        """
        try:
            # Execute generation through orchestrator
            with perf_timer(
                f"facade.generate_pipeline [{request.pipeline_identifier}]"
            ):
                generated_files = self.orchestrator.generate_pipeline_by_field(
                    pipeline_field=request.pipeline_identifier,
                    env=request.environment,
                    output_dir=(
                        request.output_directory if not request.dry_run else None
                    ),
                    state_manager=(
                        self.state_manager if not request.no_cleanup else None
                    ),
                    force_all=request.force_all,
                    specific_flowgroups=request.specific_flowgroups,
                    include_tests=request.include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                )

            return GenerationResponse(
                success=True,
                generated_files=generated_files,
                files_written=len(generated_files) if not request.dry_run else 0,
                total_flowgroups=len(generated_files),
                output_location=request.output_directory,
                performance_info={
                    "dry_run": request.dry_run,
                    "force_all": request.force_all,
                    "include_tests": request.include_tests,
                },
            )

        except Exception as e:
            # Log brief context without full error details (avoids duplication)
            from ..utils.error_formatter import LHPError

            if isinstance(e, LHPError):
                # LHPError already has formatted details, just log context
                self.logger.debug(
                    f"Pipeline generation failed for {request.pipeline_identifier}"
                )
            else:
                # Regular exception - log full details
                self.logger.error(f"Pipeline generation failed: {e}")

            # Build the failure response so the CLI can still print a per-pipeline
            # status line, but attach the original exception so the CLI can
            # re-raise at the fail-fast boundary (where cli_error_boundary takes
            # over formatting and exit-code mapping).
            return GenerationResponse(
                success=False,
                generated_files={},
                files_written=0,
                total_flowgroups=0,
                output_location=None,
                performance_info={},
                error_message=str(e),
                original_error=e,
            )

    def generate_pipelines(
        self,
        *,
        pipeline_fields: Sequence[str],
        env: str,
        output_dir: Optional[Path],
        force_all: bool = False,
        specific_flowgroups: Optional[List[str]] = None,
        include_tests: bool = False,
        no_cleanup: bool = False,
        pre_discovered_all_flowgroups=None,
        max_workers: Optional[int] = None,
        on_pipeline_complete: Optional[
            Callable[[str, "GenerationResponse"], None]
        ] = None,
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
            force_all: Bypass smart staleness filtering.
            specific_flowgroups: Optional whitelist.
            include_tests: Include test actions.
            no_cleanup: When True, state_manager is not used (no state tracking).
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
        from .state_models import PipelineDelta

        pipeline_responses: Dict[str, "GenerationResponse"] = {}

        def _on_delta(delta: "PipelineDelta") -> None:
            # Build per-pipeline GenerationResponse from the worker's
            # PipelineDelta. Worker exceptions are surfaced as strings
            # (error_type/message/traceback) because live BaseExceptions
            # can't safely cross the spawn boundary; we reconstruct an
            # LHPError on the response so display code has a real
            # exception object to format.
            from ..utils.error_formatter import LHPError

            original_error: Optional[BaseException] = None
            error_message: Optional[str] = None
            if not delta.success:
                error_message = (
                    f"{delta.error_type}: {delta.error_message}"
                    if delta.error_type
                    else delta.error_message
                )
                original_error = LHPError.from_worker_exception(
                    pipeline_name=delta.pipeline_name,
                    error_type=delta.error_type or "UnknownError",
                    error_message=delta.error_message or "(no message)",
                    error_traceback=delta.error_traceback or "",
                )

            response = GenerationResponse(
                success=delta.success,
                generated_files=dict(delta.generated_files),
                files_written=delta.files_written,
                # Match the legacy `generate_pipeline` semantics: this field
                # is displayed as "Would generate N file(s)" in dry-run, so
                # it is the count of generated FILES (not the count of
                # flowgroups processed). Empty flowgroups produce 0 files.
                total_flowgroups=len(delta.generated_files),
                output_location=output_dir,
                performance_info={
                    "dry_run": output_dir is None,
                    "force_all": force_all,
                    "include_tests": include_tests,
                    "files_skipped": delta.files_skipped,
                    "artifacts_count": delta.artifacts_count,
                },
                error_message=error_message,
                original_error=original_error,
            )
            pipeline_responses[delta.pipeline_name] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(delta.pipeline_name, response)
                except BaseException as cb_exc:
                    # The orchestrator already logs callback exceptions; we
                    # add a facade-level safety net so a buggy CLI display
                    # never tears down the batch.
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
                    state_manager=self.state_manager if not no_cleanup else None,
                    force_all=force_all,
                    specific_flowgroups=specific_flowgroups,
                    include_tests=include_tests,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                    max_workers=max_workers,
                    on_pipeline_complete=_on_delta,
                )

            aggregate: Dict[str, str] = {}
            total_written = 0
            for r in pipeline_responses.values():
                aggregate.update(r.generated_files)
                total_written += r.files_written

            return BatchGenerationResponse(
                success=True,
                pipeline_responses=pipeline_responses,
                total_files_written=total_written,
                aggregate_generated_files=aggregate,
                output_location=output_dir,
            )

        except Exception as exc:
            # Aggregate error path: the orchestrator raised after some
            # pipelines may already have succeeded. Surface what completed.
            from ..utils.error_formatter import LHPError

            if isinstance(exc, LHPError):
                self.logger.debug(
                    f"Batch pipeline generation failed: "
                    f"{len(pipeline_responses)} pipeline(s) had outcomes captured"
                )
            else:
                self.logger.error(f"Batch pipeline generation failed: {exc}")

            aggregate: Dict[str, str] = {}
            total_written = 0
            for r in pipeline_responses.values():
                if r.success:
                    aggregate.update(r.generated_files)
                    total_written += r.files_written

            return BatchGenerationResponse(
                success=False,
                pipeline_responses=pipeline_responses,
                total_files_written=total_written,
                aggregate_generated_files=aggregate,
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
            response = ValidationResponse(
                success=outcome.success,
                errors=list(outcome.errors),
                warnings=list(outcome.warnings),
                validated_pipelines=[outcome.pipeline],
            )
            pipeline_responses[outcome.pipeline] = response
            if on_pipeline_complete is not None:
                try:
                    on_pipeline_complete(outcome.pipeline, response)
                except BaseException as cb_exc:
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
                )

            total_errors = sum(len(r.errors) for r in pipeline_responses.values())
            total_warnings = sum(len(r.warnings) for r in pipeline_responses.values())

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

            total_errors = sum(len(r.errors) for r in pipeline_responses.values())
            total_warnings = sum(len(r.warnings) for r in pipeline_responses.values())

            return BatchValidationResponse(
                success=False,
                pipeline_responses=pipeline_responses,
                total_errors=total_errors,
                total_warnings=total_warnings,
                validated_pipelines=list(pipeline_fields),
                error_message=str(exc),
                original_error=exc,
            )

    def validate_pipeline(
        self, request: PipelineValidationRequest
    ) -> ValidationResponse:
        """Coordinate pipeline validation use case."""
        try:
            errors, warnings = self.orchestrator.validate_pipeline_by_field(
                pipeline_field=request.pipeline_identifier,
                env=request.environment,
                include_tests=request.include_tests,
            )

            return ValidationResponse(
                success=len(errors) == 0,
                errors=errors,
                warnings=warnings,
                validated_pipelines=[request.pipeline_identifier],
            )

        except Exception as e:
            self.logger.error(f"Pipeline validation failed: {e}")
            return ValidationResponse(
                success=False,
                errors=[str(e)],
                warnings=[],
                validated_pipelines=[],
                error_message=str(e),
            )

    def analyze_staleness(
        self,
        request: StalenessAnalysisRequest,
        pre_discovered_all_flowgroups=None,
    ) -> AnalysisResponse:
        """Coordinate staleness analysis use case."""
        try:
            with perf_timer("facade.analyze_staleness"):
                analysis = self.orchestrator.analyze_generation_requirements(
                    env=request.environment,
                    pipeline_names=request.pipeline_names,
                    include_tests=request.include_tests,
                    force=request.force,
                    state_manager=self.state_manager,
                    pre_discovered_all_flowgroups=pre_discovered_all_flowgroups,
                )

            return AnalysisResponse(
                success=True,
                pipelines_needing_generation=analysis.pipelines_needing_generation,
                pipelines_up_to_date=analysis.pipelines_up_to_date,
                has_global_changes=analysis.has_global_changes,
                global_changes=analysis.global_changes,
                include_tests_context_applied=analysis.include_tests_context_applied,
                total_new_files=analysis.total_new_files,
                total_stale_files=analysis.total_stale_files,
                total_up_to_date_files=analysis.total_up_to_date_files,
                context_changed=analysis.context_changed,
                context_changes=list(analysis.context_changes),
            )

        except Exception as e:
            self.logger.error(f"Staleness analysis failed: {e}")
            return AnalysisResponse(
                success=False,
                pipelines_needing_generation={},
                pipelines_up_to_date={},
                has_global_changes=False,
                global_changes=[],
                include_tests_context_applied=False,
                total_new_files=0,
                total_stale_files=0,
                total_up_to_date_files=0,
                error_message=str(e),
            )
