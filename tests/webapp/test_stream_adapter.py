"""Tests for the sync→async NDJSON stream adapter.

Self-sufficient: every test drives :func:`lhp.webapp.services.stream_adapter`
with a FAKE sync event iterator (no real LHP project, no facade, no shared
conftest). ``pytest-asyncio`` is NOT a project dependency, so the test
functions are plain ``def`` and drive the async generator through
``asyncio.run`` via the :func:`drain` helper.

The four scenarios exercise the pinned frame protocol, the §5.7 ordering
guarantee, the terminal-error path, the soft-cap info frame, and the
single-run lock.

Each NDJSON line yielded by ``stream_events`` is one JSON object + ``\\n``; the
helper :func:`drain` decodes them into a list of dicts.
"""

from __future__ import annotations

import asyncio
import json
import threading
import time
from typing import Any, Iterator, List

import pytest

from lhp.api import (
    ErrorEmitted,
    LHPEvent,
    OperationStarted,
    PhaseStarted,
    PipelineCompleted,
    ProgressSink,
    ValidationCompleted,
    WarningEmitted,
)
from lhp.errors import ErrorCategory, LHPError
from lhp.webapp.services import stream_adapter
from lhp.webapp.services.stream_adapter import stream_events

pytestmark = pytest.mark.webapp

FakeRun = Any  # Callable[[ProgressSink], Iterator[LHPEvent]]


async def _collect(run: FakeRun) -> List[dict[str, Any]]:
    frames: List[dict[str, Any]] = []
    async for line in stream_events(run):
        assert isinstance(line, bytes)
        assert line.endswith(b"\n")
        frames.append(json.loads(line))
    return frames


def drain(run: FakeRun) -> List[dict[str, Any]]:
    """Run ``stream_events(run)`` to completion and decode every NDJSON line."""
    return asyncio.run(_collect(run))


def _make_error() -> LHPError:
    return LHPError(
        category=ErrorCategory.VALIDATION,
        code_number="021",
        title="bad config",
        details="the flowgroup is malformed",
        suggestions=["fix the YAML", "run lhp validate"],
        context={"file": "bronze.yaml"},
    )


def test_happy_path_orders_frames_and_interleaves_progress() -> None:
    """Events + sink-fired progress frames appear in exact production order."""

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        yield PhaseStarted("discover")
        # Sink fires HERE, between PhaseStarted and PipelineCompleted — the
        # progress frames must land at exactly this point in the stream.
        sink.on_total(1)
        sink.on_advance("bronze")
        yield PipelineCompleted("bronze", duration_s=0.5, files_written=3)
        yield ValidationCompleted(response={"ok": True})

    frames = drain(run)
    types = [f["type"] for f in frames]

    # Two progress frames (on_total then on_advance) interleave between the
    # PhaseStarted and the PipelineCompleted event.
    assert types == [
        "OperationStarted",
        "PhaseStarted",
        "progress",
        "progress",
        "PipelineCompleted",
        "ValidationCompleted",
    ]

    # First frame: OperationStarted carries its fields.
    assert frames[0] == {
        "type": "OperationStarted",
        "operation_name": "validate",
        "env": "dev",
    }

    # The two progress snapshots reflect on_total (done still 0) then
    # on_advance (done 1, current set).
    assert frames[2] == {"type": "progress", "total": 1, "done": 0, "current": None}
    assert frames[3] == {"type": "progress", "total": 1, "done": 1, "current": "bronze"}

    # Terminal frame is the completion event carrying its response DTO.
    assert frames[-1] == {"type": "ValidationCompleted", "response": {"ok": True}}


def test_failure_path_emits_terminal_error_and_joins_worker() -> None:
    """ErrorEmitted → terminal error frame; iterator re-raise does not leak."""
    err = _make_error()
    raised_after = {"hit": False}

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        yield ErrorEmitted(err)
        # §1.4 protocol: the iterator re-raises the same LHPError after the
        # ErrorEmitted rendezvous. If the adapter kept pulling past the error
        # frame this RuntimeError would surface — assert it never does.
        raised_after["hit"] = True
        raise RuntimeError("iterator must not be pulled past the error frame")

    frames = drain(run)
    types = [f["type"] for f in frames]

    # OperationStarted, then a single terminal error frame; nothing after.
    assert types == ["OperationStarted", "error"]
    err_frame = frames[-1]
    assert err_frame == {
        "type": "error",
        "code": "LHP-VAL-021",
        "title": "bad config",
        "details": "the flowgroup is malformed",
        "suggestions": ["fix the YAML", "run lhp validate"],
        "context": {"file": "bronze.yaml"},
        "doc_link": err.doc_link,
    }

    # The adapter stopped at the error frame; it never advanced the iterator
    # to the line that would raise RuntimeError.
    assert raised_after["hit"] is False

    # Worker thread joined: no lingering lhp-stream-worker thread.
    assert not any(t.name == "lhp-stream-worker" for t in threading.enumerate())


def test_failure_path_bare_raise_without_error_emitted() -> None:
    """An LHPError raised WITHOUT a preceding ErrorEmitted becomes the error frame."""
    err = _make_error()

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        raise err  # e.g. a preflight failure with no ErrorEmitted rendezvous

    frames = drain(run)
    assert [f["type"] for f in frames] == ["OperationStarted", "error"]
    assert frames[-1]["code"] == "LHP-VAL-021"
    assert not any(t.name == "lhp-stream-worker" for t in threading.enumerate())


def test_unexpected_exception_wrapped_as_error_frame() -> None:
    """A non-LHP exception is wrapped via from_unexpected_exception (LHP-GEN-902)."""

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        raise ValueError("boom")

    frames = drain(run)
    assert [f["type"] for f in frames] == ["OperationStarted", "error"]
    assert frames[-1]["code"] == "LHP-GEN-902"


def test_soft_cap_warning_becomes_info_frame() -> None:
    """Soft-cap WarningEmitted → info frame; ordinary WarningEmitted → event frame."""

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        yield WarningEmitted("a normal warning", code="LHP-VAL-099", category="cfg")
        yield WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")
        yield ValidationCompleted(response={"ok": True})

    frames = drain(run)
    assert [f["type"] for f in frames] == [
        "OperationStarted",
        "WarningEmitted",
        "info",
        "ValidationCompleted",
    ]
    # Ordinary warning keeps its full event shape.
    assert frames[1]["message"] == "a normal warning"
    assert frames[1]["code"] == "LHP-VAL-099"
    # Soft-cap collapses to the pinned info shape.
    assert frames[2] == {
        "type": "info",
        "code": "LHP-EVT-SOFT-CAP",
        "message": "event buffer near limit",
    }


def test_runs_are_serialized_second_waits_for_first() -> None:
    """Two concurrent streams: the second's first frame waits for the first to finish."""

    async def scenario() -> None:
        # Order in which frames were observed across both consumers.
        observed: List[str] = []
        # Gate the FIRST run's iterator (worker thread) until released.
        first_may_finish = threading.Event()
        first_started = asyncio.Event()

        def make_run(label: str, gated: bool) -> FakeRun:
            def run(sink: ProgressSink) -> Iterator[LHPEvent]:
                yield OperationStarted(label, env="dev")
                if gated:
                    # Park the worker thread until the test releases it.
                    # threading.Event.wait is safe to call from this thread.
                    while not first_may_finish.is_set():
                        time.sleep(0.005)
                yield ValidationCompleted(response={"label": label})

            return run

        async def consume(label: str, gated: bool) -> List[str]:
            local: List[str] = []
            async for line in stream_events(make_run(label, gated)):
                frame = json.loads(line)
                tag = f"{label}:{frame['type']}"
                observed.append(tag)
                local.append(tag)
                if label == "first" and frame["type"] == "OperationStarted":
                    first_started.set()
            return local

        # Launch the gated FIRST run, wait until it has emitted its first frame
        # and is parked inside the gate, THEN launch the SECOND.
        first_task = asyncio.create_task(consume("first", gated=True))
        await asyncio.wait_for(first_started.wait(), timeout=5)

        second_task = asyncio.create_task(consume("second", gated=False))
        # Give the second task ample scheduling opportunity; it must NOT
        # produce a frame yet because the run lock is held by the first
        # (still-gated) run.
        await asyncio.sleep(0.2)
        assert not any(tag.startswith("second:") for tag in observed), (
            "second stream emitted before the first completed — runs not serialized"
        )

        # Release the first run; both should now complete.
        first_may_finish.set()
        first_frames, second_frames = await asyncio.wait_for(
            asyncio.gather(first_task, second_task), timeout=5
        )

        # Every first-run frame precedes every second-run frame (strict
        # ordering imposed by the module-level run lock).
        last_first_idx = max(
            i for i, t in enumerate(observed) if t.startswith("first:")
        )
        first_second_idx = min(
            i for i, t in enumerate(observed) if t.startswith("second:")
        )
        assert last_first_idx < first_second_idx

        assert first_frames[-1] == "first:ValidationCompleted"
        assert second_frames[0] == "second:OperationStarted"

    asyncio.run(scenario())
    # No worker thread outlives the scenario.
    assert not any(t.name == "lhp-stream-worker" for t in threading.enumerate())


def test_run_lock_released_after_each_stream() -> None:
    """The module-level run lock is not held after a stream finishes."""

    def run(sink: ProgressSink) -> Iterator[LHPEvent]:
        yield OperationStarted("validate", env="dev")
        yield ValidationCompleted(response={"ok": True})

    drain(run)
    assert not stream_adapter._run_lock.locked()
