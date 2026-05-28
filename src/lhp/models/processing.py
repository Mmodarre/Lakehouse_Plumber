"""Data structures used during code generation."""

import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Mapping, Optional, Sequence, Tuple, Union

if TYPE_CHECKING:
    from lhp.core.processing.substitution import EnhancedSubstitutionManager
    from lhp.errors import LHPError
    from lhp.models import FlowGroupContext


# JSON-safe projection type for ``PipelineWorkUnit.to_dict`` and other telemetry
# / event-bus surfaces. Locally defined here (rather than under ``lhp/api/``)
# so the DTOs in this module stay in one place. Constitution §4.8 forbids
# ``Any`` outside an explicit ``JSONValue`` alias — this is that exemption.
JSONValue = Union[
    None,
    bool,
    int,
    float,
    str,
    Sequence["JSONValue"],
    Mapping[str, "JSONValue"],
]


@dataclass(frozen=True, slots=True)
class PipelineDelta:
    """Worker → main-thread report for ONE pipeline's generate run.

    Carries the small bit of state the main thread needs after a worker
    finishes a whole pipeline: a success flag, file-count rollups for the
    summary line, the ordered tuple of generated filenames the facade
    surfaces as ``aggregate_generated_filenames``, and — on failure — the
    exception in two forms.

    For :class:`~lhp.errors.LHPError` instances, the live
    exception travels in ``lhp_error`` (pickled via the class's
    :meth:`__reduce__` so subclass identity, ``code``, ``context``,
    ``suggestions`` are preserved verbatim) and the main thread re-raises
    it unchanged. For non-LHP exceptions, only the string projection
    (``error_type`` / ``error_message`` / ``error_traceback``) is
    available across the spawn boundary — tracebacks come through
    pre-formatted via :func:`traceback.format_exception` so the main
    thread can wrap a fresh :class:`LHPError` with the full chained
    context. ``error_message=str(exc)`` is always populated (even when
    ``lhp_error`` is set) so legacy log lines, summary rows, and tests
    that read it keep working.

    Only filenames travel back (no file *contents*) — on the performance
    project this keeps the worker→main pickle to ~50 KB. Consumers that
    need the formatted code (none currently do) must read it from disk.

    Why ``frozen=True, slots=True``:
      - Immutability across the spawn boundary: workers can't accidentally
        mutate a delta after it's been queued for the main thread.
      - Small per-instance overhead: ``slots=True`` drops the per-instance
        ``__dict__``.
    """

    pipeline_name: str
    success: bool
    files_written: int = 0
    artifacts_count: int = 0
    generated_filenames: tuple[str, ...] = ()
    duration_s: float = 0.0
    lhp_error: Optional["LHPError"] = None
    error_type: Optional[str] = None
    error_message: Optional[str] = None
    error_traceback: Optional[str] = None

    @classmethod
    def success_(
        cls,
        pipeline_name: str,
        *,
        files_written: int = 0,
        artifacts_count: int = 0,
        generated_filenames: Sequence[str] = (),
        duration_s: float = 0.0,
    ) -> "PipelineDelta":
        """Build a success delta. The trailing underscore on the name avoids
        shadowing the ``success`` field while still reading naturally at
        call sites (``PipelineDelta.success_(...)``).
        """
        return cls(
            pipeline_name=pipeline_name,
            success=True,
            files_written=files_written,
            artifacts_count=artifacts_count,
            generated_filenames=tuple(generated_filenames),
            duration_s=duration_s,
        )

    @classmethod
    def failure(
        cls,
        pipeline_name: str,
        exc: BaseException,
        *,
        duration_s: float = 0.0,
    ) -> "PipelineDelta":
        """Build a failure delta from a live exception.

        For LHPError instances, the live exception is carried via
        ``lhp_error`` so the main thread can re-raise it unchanged
        (preserving subclass identity, code, context, suggestions).
        For non-LHP exceptions, the string projection
        (``error_type``/``error_message``/``error_traceback``) is the
        only surface available across the spawn boundary.

        ``error_message=str(exc)`` is preserved for both kinds so legacy
        log lines, summary rows, and tests that read it keep working;
        the unwrap consumer prefers ``lhp_error`` when present.

        ``duration_s`` defaults to ``0.0``. The worker dispatch boundary
        stamps a measured value when the failure originates from actual
        work in :func:`_dispatch_pipeline_for_generate`. Failures
        synthesized on the main thread for infrastructural reasons
        (``executor.submit`` raising, ``fut.result()`` unpickling errors,
        worker crashes before t0 is captured) intentionally leave this
        at ``0.0`` — those did not consume measurable worker work-time.
        """
        from lhp.errors import LHPError  # lazy to avoid cycle

        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lhp_err = exc if isinstance(exc, LHPError) else None
        return cls(
            pipeline_name=pipeline_name,
            success=False,
            duration_s=duration_s,
            lhp_error=lhp_err,
            error_type=type(exc).__name__,
            error_message=str(exc),
            error_traceback=tb,
        )


@dataclass(frozen=True, slots=True)
class PipelineWorkUnit:
    """Inputs for one pipeline's execution slice across the worker boundary.

    Carries the per-pipeline data that varies across
    :class:`PipelineExecutionService` invocations. Heavyweight pool-constant
    state (worker collaborators, environment, project_root, project_config,
    include_tests, callbacks, max_workers) is captured constructor-time on
    :class:`PipelineExecutionService` via :class:`_GenerateWorkerState` /
    :class:`_ValidateWorkerState`.

    :stability: provisional
    """

    pipeline_name: str
    flowgroups: Tuple["FlowGroupContext", ...]
    substitution_manager: Optional["EnhancedSubstitutionManager"] = None
    output_dir: Optional[Path] = None
    discovery_error: Optional[str] = None

    def to_dict(self) -> Mapping[str, JSONValue]:
        """JSON-safe projection for telemetry / event-bus surfaces.

        Picklability across the spawn boundary uses the frozen-dataclass
        default; this method is for non-pickle serializations (event-bus
        payload, structured logging).

        ``substitution_manager`` is intentionally absent from the dict
        payload (None passthrough only): it is a live collaborator with
        non-JSON state. ``flowgroups`` projects to the flowgroup-name tuple
        only. ``output_dir`` is stringified.
        """
        return {
            "pipeline_name": self.pipeline_name,
            "flowgroup_names": tuple(
                ctx.flowgroup.flowgroup for ctx in self.flowgroups
            ),
            "substitution_manager": None,
            "output_dir": (
                str(self.output_dir) if self.output_dir is not None else None
            ),
            "discovery_error": self.discovery_error,
        }

    @classmethod
    def from_dict(
        cls,
        data: Mapping[str, JSONValue],
        *,
        flowgroups: Tuple["FlowGroupContext", ...] = (),
        substitution_manager: Optional["EnhancedSubstitutionManager"] = None,
    ) -> "PipelineWorkUnit":
        """Rehydrate a unit from its JSON projection plus live collaborators.

        ``flowgroups`` and ``substitution_manager`` must be supplied
        explicitly because they cannot round-trip through JSON. This
        constructor exists for symmetry with :meth:`to_dict` (event-bus
        replay, fixture tests); production code constructs units directly.
        Defaults of ``()`` and ``None`` make a fixture round-trip
        ``cls.from_dict(unit.to_dict())`` valid for the JSON-safe subset.
        """
        output_dir_raw = data.get("output_dir")
        discovery_error_raw = data.get("discovery_error")
        return cls(
            pipeline_name=str(data["pipeline_name"]),
            flowgroups=flowgroups,
            substitution_manager=substitution_manager,
            output_dir=(
                Path(output_dir_raw) if isinstance(output_dir_raw, str) else None
            ),
            discovery_error=(
                str(discovery_error_raw) if discovery_error_raw is not None else None
            ),
        )
