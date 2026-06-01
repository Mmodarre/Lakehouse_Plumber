"""Data structures used during code generation."""

import traceback
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, Optional, Sequence, Tuple

if TYPE_CHECKING:
    from lhp.errors import LHPError
    from lhp.models import FlowGroup


@dataclass(frozen=True, slots=True)
class CopiedModuleRecord:
    """Pure-compute description of a user Python module copy.

    Produced in Phase A by :func:`compute_copy_records` with no side
    effects — no filesystem writes, no state mutation. Replayed in
    Phase B by :meth:`PythonFileCopier.apply_copy_record`.
    """

    source_path: str
    dest_path: Path
    content: str
    module_path: str
    custom_functions_dir: Path


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

        ``duration_s`` defaults to ``0.0``; the per-pipeline commit step
        stamps a measured value when one is available. Failures
        synthesized for infrastructural reasons (``executor.submit``
        raising, ``fut.result()`` unpickling errors, worker crashes before
        a timer is captured) intentionally leave this at ``0.0`` — those
        did not consume measurable worker work-time.
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
class FlowgroupOutcome:
    """Worker → main-thread report for ONE flowgroup's worker run.

    The picklable, replay-ready slice the main thread needs after a worker
    finishes a single flowgroup — the resolved flowgroup, the
    formatted code (when generating), the auxiliary files and copied-module
    records replayed to disk on commit, and — on failure — the error in
    two forms.

    Dual-channel error transport (as on :class:`PipelineDelta`). For
    :class:`~lhp.errors.LHPError` instances the live exception travels in
    ``lhp_error`` (pickled via the class's ``__reduce__`` so subclass
    identity, ``code``, ``context``, ``suggestions`` are preserved
    verbatim) and the main thread re-raises it unchanged. When the live
    object cannot cross the spawn boundary, the degraded string channel
    ``errors`` carries the human-readable projection instead.

    ``auxiliary_files`` is ``Tuple[Tuple[str, str], ...]`` (each pair is
    ``(path, content)``): dropping it would silently lose the monitoring
    flowgroup's extra modules across the process boundary.

    ``perf`` is an optional in-worker timing/event-export payload attached
    later by the worker pool (via ``dataclasses.replace``); ``None`` by default.

    ``frozen=True, slots=True`` for immutability across the spawn boundary
    and a smaller per-instance footprint (no per-instance ``__dict__``).
    """

    pipeline: str
    flowgroup_name: str
    success: bool
    resolved_flowgroup: Optional["FlowGroup"] = None
    formatted_code: Optional[str] = None
    auxiliary_files: Tuple[Tuple[str, str], ...] = ()
    copy_records: Tuple[CopiedModuleRecord, ...] = ()
    lhp_error: Optional["LHPError"] = None
    errors: Tuple[str, ...] = ()
    perf: Optional[Dict[str, Any]] = None

    @classmethod
    def ok(
        cls,
        pipeline: str,
        flowgroup_name: str,
        *,
        resolved_flowgroup: Optional["FlowGroup"] = None,
        formatted_code: Optional[str] = None,
        auxiliary_files: Sequence[Tuple[str, str]] = (),
        copy_records: Sequence[CopiedModuleRecord] = (),
    ) -> "FlowgroupOutcome":
        """Build a success outcome.

        A success outcome carries neither ``lhp_error`` nor ``errors``.
        ``formatted_code`` is ``None`` for validate-mode runs (no code is
        generated).
        """
        return cls(
            pipeline=pipeline,
            flowgroup_name=flowgroup_name,
            success=True,
            resolved_flowgroup=resolved_flowgroup,
            formatted_code=formatted_code,
            auxiliary_files=tuple(auxiliary_files),
            copy_records=tuple(copy_records),
            lhp_error=None,
            errors=(),
        )

    @classmethod
    def failure(
        cls,
        pipeline: str,
        flowgroup_name: str,
        *,
        lhp_error: Optional["LHPError"] = None,
        errors: Optional[Sequence[str]] = None,
    ) -> "FlowgroupOutcome":
        """Build a failure outcome. MUST be total — it NEVER raises.

        Called from inside a worker's ``except`` block, so a raise here
        would cross the process boundary as an unhandled worker exception
        (§5.6). Accordingly:

          - If ``lhp_error`` is given, the live exception travels in
            ``lhp_error`` for verbatim re-raise on the main thread.
          - If ``errors`` is given, the degraded string channel is
            populated.
          - If both are given, both are stored (no xor enforcement).
          - If neither is given, the string channel degrades to a generic
            ``("unknown error",)`` rather than raising.
        """
        error_strings = tuple(errors) if errors else ()
        if lhp_error is None and not error_strings:
            error_strings = ("unknown error",)
        return cls(
            pipeline=pipeline,
            flowgroup_name=flowgroup_name,
            success=False,
            resolved_flowgroup=None,
            formatted_code=None,
            lhp_error=lhp_error,
            errors=error_strings,
        )
