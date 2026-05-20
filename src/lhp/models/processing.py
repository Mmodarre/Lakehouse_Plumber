"""Data structures used during code generation."""

import traceback
from dataclasses import dataclass
from typing import Optional, Sequence


@dataclass(frozen=True, slots=True)
class PipelineDelta:
    """Worker → main-thread report for ONE pipeline's generate run.

    Carries the small bit of state the main thread needs after a worker
    finishes a whole pipeline: a success flag, file-count rollups for the
    summary line, the ordered tuple of generated filenames the facade
    surfaces as ``aggregate_generated_filenames``, and — on failure — the
    exception serialized as plain strings. No live ``BaseException`` is
    shipped across the process boundary; tracebacks come through
    pre-formatted via :func:`traceback.format_exception` so the main thread
    can re-raise a fresh :class:`LHPError` with full chained context.

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

        Captures ``type(exc).__name__``, ``str(exc)``, and a fully-formatted
        traceback (chained ``__cause__``/``__context__`` walked by
        :func:`traceback.format_exception`). The exception object itself
        is never stored — only its string projection — so the delta
        survives pickling across the spawn boundary intact.
        """
        tb = "".join(traceback.format_exception(type(exc), exc, exc.__traceback__))
        return cls(
            pipeline_name=pipeline_name,
            success=False,
            error_type=type(exc).__name__,
            error_message=str(exc),
            error_traceback=tb,
        )
