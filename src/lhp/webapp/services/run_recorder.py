"""Record a validate/generate NDJSON frame stream into SQLite run history.

:func:`record` wraps the frame stream produced by
:mod:`lhp.webapp.services.stream_adapter` IN THE ROUTER (the adapter itself
stays a pure sync→async bridge). It forwards every frame downstream
unchanged while:

* minting a ``run_id`` and inserting a ``running`` row up front,
* buffering each frame as ``(seq, frame_json)`` and flushing in batches,
* classifying the terminal frame into a run status + extracted issues:

  - ``{"type": "error", ...}`` → ``failed`` (one issue from the error frame),
  - ``ValidationCompleted`` / ``GenerationCompleted`` → ``completed`` when
    ``response.success`` is truthy, else ``failed``; issues are pulled from
    the per-pipeline responses (``issues`` views for validation, the
    ``error`` view / ``error_message`` for generation),
  - no terminal seen (client disconnect, upstream crash) → ``failed``,

* publishing ``run-updated`` bus events at start and at terminal, and
* pruning old history after each run.

All DB work is the synchronous :mod:`run_history` layer bridged through
``asyncio.to_thread`` so the event loop never blocks on SQLite.

:func:`record_ndjson` is the byte-level wrapper the streaming router uses:
``stream_events`` yields ready-encoded NDJSON lines (one frame per chunk),
so it decodes each line for recording and re-encodes with the identical
``json.dumps(..., separators=(",", ":"))`` — byte-identical on the wire.
"""

from __future__ import annotations

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any, AsyncIterator, Optional

from lhp.webapp.services import run_history
from lhp.webapp.services.event_bus import EventBus
from lhp.webapp.services.run_history import IssueRecord

logger = logging.getLogger(__name__)

#: Frames buffered between ``run_events`` batch inserts.
_FLUSH_BATCH_SIZE = 25

#: Terminal success event frame types (the adapter's ``{"type": <ClassName>}``).
_COMPLETED_FRAME_TYPES = ("ValidationCompleted", "GenerationCompleted")


@dataclass(frozen=True)
class _TerminalOutcome:
    """Classified terminal frame: run status, compact summary, extracted issues."""

    status: str
    summary: dict[str, Any]
    issues: tuple[IssueRecord, ...]


def _run_updated(run_id: str, kind: str, status: str) -> dict[str, Any]:
    """Build the ``run-updated`` bus event."""
    return {
        "event": "run-updated",
        "data": {"run_id": run_id, "kind": kind, "status": status},
    }


def _issue_from_view(view: dict[str, Any]) -> IssueRecord:
    """Map a serialized ``ValidationIssueView`` dict onto an :class:`IssueRecord`.

    Tolerant of absent fields: ``code`` may be the empty string (unstructured
    issues) → stored as ``NULL``; the view carries no line number today, so
    ``line`` is taken only if a future ``line`` field appears as an int.
    """
    title = str(view.get("title") or "")
    details = view.get("details")
    message = f"{title}: {details}" if details else (title or "unknown issue")
    code = view.get("code") or None
    file_path = view.get("file_path")
    line = view.get("line")
    return IssueRecord(
        severity=str(view.get("severity") or "error"),
        code=str(code) if code else None,
        message=message,
        file=str(file_path) if file_path else None,
        line=line if isinstance(line, int) else None,
    )


def _error_outcome(frame: dict[str, Any]) -> _TerminalOutcome:
    """Terminal ``{"type": "error"}`` frame → failed run + one issue row."""
    title = str(frame.get("title") or "")
    details = frame.get("details")
    message = f"{title}: {details}" if details else (title or "run failed")
    code = frame.get("code") or None
    issue = IssueRecord(
        severity="error", code=str(code) if code else None, message=message
    )
    return _TerminalOutcome(
        status="failed",
        summary={"success": False, "error_code": code},
        issues=(issue,),
    )


def _validation_outcome(response: dict[str, Any]) -> _TerminalOutcome:
    """``ValidationCompleted.response`` (``BatchValidationResponse``) → outcome."""
    success = bool(response.get("success"))
    pipeline_responses = response.get("pipeline_responses")
    if not isinstance(pipeline_responses, dict):
        pipeline_responses = {}
    issues: list[IssueRecord] = []
    for pipeline_response in pipeline_responses.values():
        if not isinstance(pipeline_response, dict):
            continue
        for view in pipeline_response.get("issues") or ():
            if isinstance(view, dict):
                issues.append(_issue_from_view(view))
    summary = {
        "success": success,
        "total_errors": response.get("total_errors"),
        "total_warnings": response.get("total_warnings"),
        "pipeline_count": len(pipeline_responses),
    }
    return _TerminalOutcome(
        status="completed" if success else "failed",
        summary=summary,
        issues=tuple(issues),
    )


def _generation_outcome(response: dict[str, Any]) -> _TerminalOutcome:
    """``GenerationCompleted.response`` (``BatchGenerationResponse``) → outcome."""
    success = bool(response.get("success"))
    pipeline_responses = response.get("pipeline_responses")
    if not isinstance(pipeline_responses, dict):
        pipeline_responses = {}
    issues: list[IssueRecord] = []
    for pipeline_response in pipeline_responses.values():
        if not isinstance(pipeline_response, dict):
            continue
        error_view = pipeline_response.get("error")
        if isinstance(error_view, dict):
            issues.append(_issue_from_view(error_view))
        elif pipeline_response.get("error_message"):
            error_code = pipeline_response.get("error_code") or None
            issues.append(
                IssueRecord(
                    severity="error",
                    code=str(error_code) if error_code else None,
                    message=str(pipeline_response["error_message"]),
                )
            )
    summary = {
        "success": success,
        "total_files_written": response.get("total_files_written"),
        "pipeline_count": len(pipeline_responses),
    }
    return _TerminalOutcome(
        status="completed" if success else "failed",
        summary=summary,
        issues=tuple(issues),
    )


def _classify_terminal(frame: dict[str, Any]) -> Optional[_TerminalOutcome]:
    """Return the run outcome if ``frame`` is terminal, else ``None``."""
    frame_type = frame.get("type")
    if frame_type == "error":
        return _error_outcome(frame)
    if frame_type in _COMPLETED_FRAME_TYPES:
        response = frame.get("response")
        if not isinstance(response, dict):
            return _TerminalOutcome("failed", {"success": False}, ())
        if frame_type == "ValidationCompleted":
            return _validation_outcome(response)
        return _generation_outcome(response)
    return None


async def _aclose_upstream(iterator: object) -> None:
    """Close an upstream async generator deterministically (no-op otherwise).

    ``async for`` does NOT close its iterator on early exit, so each layer of
    the recorder chain closes its upstream explicitly — this is what carries a
    client disconnect all the way down to ``stream_events``'s worker join.
    """
    aclose = getattr(iterator, "aclose", None)
    if aclose is not None:
        await aclose()


async def record(
    frames: AsyncIterator[dict[str, Any]],
    *,
    project_root: Path,
    event_bus: EventBus,
    kind: str,
    env: str,
    pipeline: Optional[str] = None,
) -> AsyncIterator[dict[str, Any]]:
    """Forward ``frames`` unchanged while recording the run into SQLite.

    Frames are never altered, reordered, or dropped. The run row is created
    (status ``running``) before the first frame is pulled; the terminal state
    is written in the ``finally`` block so a disconnect or upstream crash
    still closes the run out as ``failed``.
    """
    run_id = str(uuid.uuid4())
    await asyncio.to_thread(
        run_history.create_run, project_root, run_id, kind, env, pipeline
    )
    event_bus.publish(_run_updated(run_id, kind, "running"))

    buffer: list[tuple[int, str]] = []
    seq = 0
    outcome: Optional[_TerminalOutcome] = None
    try:
        async for frame in frames:
            seq += 1
            buffer.append((seq, json.dumps(frame, separators=(",", ":"))))
            terminal = _classify_terminal(frame)
            if terminal is not None:
                outcome = terminal
            if len(buffer) >= _FLUSH_BATCH_SIZE:
                batch, buffer = buffer, []
                await asyncio.to_thread(
                    run_history.append_events, project_root, run_id, batch
                )
            yield frame
    finally:
        await _aclose_upstream(frames)
        if buffer:
            await asyncio.to_thread(
                run_history.append_events, project_root, run_id, buffer
            )
        status = outcome.status if outcome is not None else "failed"
        summary_json = json.dumps(outcome.summary) if outcome is not None else None
        await asyncio.to_thread(
            run_history.complete_run, project_root, run_id, status, summary_json
        )
        if outcome is not None and outcome.issues:
            await asyncio.to_thread(
                run_history.add_issues, project_root, run_id, outcome.issues
            )
        event_bus.publish(_run_updated(run_id, kind, status))
        await asyncio.to_thread(run_history.prune, project_root)


async def record_ndjson(
    lines: AsyncIterator[bytes],
    *,
    project_root: Path,
    event_bus: EventBus,
    kind: str,
    env: str,
    pipeline: Optional[str] = None,
) -> AsyncIterator[bytes]:
    """Byte-level recorder around an already-encoded NDJSON line stream.

    ``stream_events`` yields exactly one NDJSON line (one frame) per chunk,
    already encoded with ``json.dumps(frame, separators=(",", ":"))``. Each
    line is decoded for :func:`record` and re-encoded identically on the way
    out, so the wire bytes are byte-for-byte what the adapter produced
    (``dumps`` is deterministic and ``loads`` preserves key order).
    """

    async def _decoded() -> AsyncIterator[dict[str, Any]]:
        try:
            async for line in lines:
                yield json.loads(line)
        finally:
            await _aclose_upstream(lines)

    recorder = record(
        _decoded(),
        project_root=project_root,
        event_bus=event_bus,
        kind=kind,
        env=env,
        pipeline=pipeline,
    )
    try:
        async for frame in recorder:
            yield json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
    finally:
        await _aclose_upstream(recorder)
