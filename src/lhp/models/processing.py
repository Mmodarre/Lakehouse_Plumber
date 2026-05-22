"""Data structures used during code generation."""

import traceback
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional, Sequence

if TYPE_CHECKING:
    from lhp.utils.error_formatter import LHPError


@dataclass(frozen=True, slots=True)
class PipelineDelta:
    """Worker → main-thread report for ONE pipeline's generate run.

    Carries the small bit of state the main thread needs after a worker
    finishes a whole pipeline: a success flag, file-count rollups for the
    summary line, the ordered tuple of generated filenames the facade
    surfaces as ``aggregate_generated_filenames``, and — on failure — the
    exception in two forms.

    For :class:`~lhp.utils.error_formatter.LHPError` instances, the live
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
        )

    @classmethod
    def failure(cls, pipeline_name: str, exc: BaseException) -> "PipelineDelta":
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
        """
        from lhp.utils.error_formatter import LHPError  # lazy to avoid cycle

        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        lhp_err = exc if isinstance(exc, LHPError) else None
        return cls(
            pipeline_name=pipeline_name,
            success=False,
            lhp_error=lhp_err,
            error_type=type(exc).__name__,
            error_message=str(exc),
            error_traceback=tb,
        )
