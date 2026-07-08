"""Unit tests for the run recorder: DB rows, bus events, pass-through frames.

Drives :func:`lhp.webapp.services.run_recorder.record` (and the byte-level
``record_ndjson``) directly with canned async frame generators — no facade,
no HTTP. The canned terminal frames mirror the REAL adapter serialization:
``{"type": "<EventClassName>", "response": {...to_dict(response)...}}`` for
success terminals and the pinned ``{"type": "error", ...}`` shape for the
failure terminal.

``pytest-asyncio`` is not a project dependency; each test wraps its async
scenario in ``asyncio.run`` (same convention as ``test_stream_adapter``).
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator, Sequence
from pathlib import Path
from typing import Any

import pytest

from lhp.webapp.services import run_history, sqlite_store
from lhp.webapp.services.event_bus import EventBus
from lhp.webapp.services.run_recorder import record, record_ndjson

pytestmark = pytest.mark.webapp


OP_STARTED = {
    "type": "OperationStarted",
    "operation_name": "validate_pipelines",
    "env": "dev",
}
PROGRESS = {"type": "progress", "total": 2, "done": 1, "current": "p1"}
VALIDATION_OK = {
    "type": "ValidationCompleted",
    "response": {
        "success": True,
        "pipeline_responses": {
            "p1": {
                "success": True,
                "issues": [],
                "validated_pipelines": ["p1"],
                "error_message": None,
            }
        },
        "total_errors": 0,
        "total_warnings": 0,
        "validated_pipelines": ["p1"],
        "error_message": None,
        "error_code": None,
    },
}
ERROR_FRAME = {
    "type": "error",
    "code": "LHP-ACT-001",
    "title": "Unknown action type",
    "details": "'nope' is not a registered action type",
    "suggestions": ["Check the action type"],
    "context": {},
    "doc_link": None,
}


@pytest.fixture
def project(tmp_path: Path) -> Path:
    sqlite_store.run_migrations(tmp_path)
    return tmp_path


async def _frames(items: Sequence[dict[str, Any]]) -> AsyncIterator[dict[str, Any]]:
    for item in items:
        yield item


def _drain(queue: asyncio.Queue) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    while True:
        try:
            events.append(queue.get_nowait())
        except asyncio.QueueEmpty:
            return events


def _drive(
    project: Path,
    bus: EventBus,
    items: Sequence[dict[str, Any]],
    kind: str = "validate",
) -> list[dict[str, Any]]:
    """Run ``record`` over canned frames to exhaustion; return forwarded frames."""

    async def scenario() -> list[dict[str, Any]]:
        forwarded: list[dict[str, Any]] = []
        async for frame in record(
            _frames(items),
            project_root=project,
            event_bus=bus,
            kind=kind,
            env="dev",
        ):
            forwarded.append(frame)
        return forwarded

    return asyncio.run(scenario())


def _single_run(project: Path) -> dict[str, Any]:
    runs = run_history.list_runs(project)
    assert len(runs) == 1
    return runs[0]


# --- happy path --------------------------------------------------------------


def test_completed_run_records_rows_and_bus_events(project: Path) -> None:
    bus = EventBus()
    queue = bus.subscribe()
    items = [OP_STARTED, PROGRESS, VALIDATION_OK]

    forwarded = _drive(project, bus, items)

    # Frames pass through unchanged, in order.
    assert forwarded == items

    run = _single_run(project)
    assert run["kind"] == "validate"
    assert run["env"] == "dev"
    assert run["status"] == "completed"
    assert run["finished_at"] is not None
    assert run["summary"] == {
        "success": True,
        "total_errors": 0,
        "total_warnings": 0,
        "pipeline_count": 1,
    }

    detail = run_history.get_run(project, run["run_id"], include_events=True)
    assert detail is not None
    assert detail["events"] == items
    assert detail["issues"] == []

    published = _drain(queue)
    assert [e["event"] for e in published] == ["run-updated", "run-updated"]
    assert [e["data"]["status"] for e in published] == ["running", "completed"]
    assert {e["data"]["run_id"] for e in published} == {run["run_id"]}
    assert {e["data"]["kind"] for e in published} == {"validate"}


# --- failure paths -----------------------------------------------------------


def test_upstream_exception_marks_run_failed(project: Path) -> None:
    bus = EventBus()
    queue = bus.subscribe()

    async def raising() -> AsyncIterator[dict[str, Any]]:
        yield OP_STARTED
        raise RuntimeError("boom")

    async def scenario() -> None:
        async for _ in record(
            raising(), project_root=project, event_bus=bus, kind="validate", env="dev"
        ):
            pass

    with pytest.raises(RuntimeError, match="boom"):
        asyncio.run(scenario())

    run = _single_run(project)
    assert run["status"] == "failed"
    assert run["summary"] is None

    detail = run_history.get_run(project, run["run_id"], include_events=True)
    assert detail is not None
    assert detail["events"] == [OP_STARTED]

    assert [e["data"]["status"] for e in _drain(queue)] == ["running", "failed"]


def test_error_terminal_frame_marks_failed_with_issue(project: Path) -> None:
    bus = EventBus()
    items = [OP_STARTED, ERROR_FRAME]

    forwarded = _drive(project, bus, items)
    assert forwarded == items

    run = _single_run(project)
    assert run["status"] == "failed"
    assert run["summary"] == {"success": False, "error_code": "LHP-ACT-001"}

    detail = run_history.get_run(project, run["run_id"])
    assert detail is not None
    assert detail["issues"] == [
        {
            "severity": "error",
            "code": "LHP-ACT-001",
            "message": ("Unknown action type: 'nope' is not a registered action type"),
            "file": None,
            "line": None,
        }
    ]


def test_early_close_without_terminal_marks_failed(project: Path) -> None:
    """A client disconnect (aclose before the terminal) closes the run as failed."""
    bus = EventBus()
    queue = bus.subscribe()

    async def scenario() -> None:
        recorder = record(
            _frames([OP_STARTED, PROGRESS, VALIDATION_OK]),
            project_root=project,
            event_bus=bus,
            kind="validate",
            env="dev",
        )
        first = await recorder.__anext__()
        assert first == OP_STARTED
        await recorder.aclose()

    asyncio.run(scenario())

    run = _single_run(project)
    assert run["status"] == "failed"
    assert [e["data"]["status"] for e in _drain(queue)] == ["running", "failed"]


# --- issue extraction from a completed-but-unsuccessful validation ----------


def test_validation_issues_are_extracted(project: Path) -> None:
    issue_view = {
        "code": "LHP-VAL-021",
        "category": "VAL",
        "severity": "error",
        "title": "Bad target",
        "details": "table does not exist",
        "pipeline_name": "p1",
        "flowgroup_name": "fg1",
        "file_path": "pipelines/p1/fg1.yaml",
        "suggestions": [],
        "context": {},
        "doc_link": None,
    }
    unstructured_view = {
        "code": "",
        "category": "VAL",
        "severity": "warning",
        "title": "deprecated field",
        "details": None,
        "pipeline_name": "p1",
        "flowgroup_name": None,
        "file_path": None,
        "suggestions": [],
        "context": {},
        "doc_link": None,
    }
    terminal = {
        "type": "ValidationCompleted",
        "response": {
            "success": False,
            "pipeline_responses": {
                "p1": {
                    "success": False,
                    "issues": [issue_view, unstructured_view],
                    "validated_pipelines": ["p1"],
                    "error_message": None,
                }
            },
            "total_errors": 1,
            "total_warnings": 1,
            "validated_pipelines": ["p1"],
            "error_message": None,
            "error_code": None,
        },
    }
    bus = EventBus()

    _drive(project, bus, [OP_STARTED, terminal])

    run = _single_run(project)
    assert run["status"] == "failed"

    detail = run_history.get_run(project, run["run_id"])
    assert detail is not None
    assert detail["issues"] == [
        {
            "severity": "error",
            "code": "LHP-VAL-021",
            "message": "Bad target: table does not exist",
            "file": "pipelines/p1/fg1.yaml",
            "line": None,
        },
        {
            "severity": "warning",
            "code": None,
            "message": "deprecated field",
            "file": None,
            "line": None,
        },
    ]


def test_generation_pipeline_error_is_extracted(project: Path) -> None:
    terminal = {
        "type": "GenerationCompleted",
        "response": {
            "success": False,
            "pipeline_responses": {
                "p1": {
                    "success": False,
                    "generated_filenames": [],
                    "files_written": 0,
                    "total_flowgroups": 1,
                    "output_location": None,
                    "performance_info": {},
                    "duration_s": 0.1,
                    "error_message": "generation blew up",
                    "error_code": "LHP-GEN-001",
                    "error": None,
                }
            },
            "total_files_written": 0,
            "aggregate_generated_filenames": [],
            "output_location": None,
            "error_message": None,
            "error_code": None,
        },
    }
    bus = EventBus()

    _drive(project, bus, [OP_STARTED, terminal], kind="generate")

    run = _single_run(project)
    assert run["kind"] == "generate"
    assert run["status"] == "failed"
    assert run["summary"] == {
        "success": False,
        "total_files_written": 0,
        "pipeline_count": 1,
    }

    detail = run_history.get_run(project, run["run_id"])
    assert detail is not None
    assert detail["issues"] == [
        {
            "severity": "error",
            "code": "LHP-GEN-001",
            "message": "generation blew up",
            "file": None,
            "line": None,
        }
    ]


# --- batching ----------------------------------------------------------------


def test_large_stream_flushes_in_batches_order_preserved(project: Path) -> None:
    many = [
        {"type": "progress", "total": 60, "done": i, "current": f"p{i}"}
        for i in range(60)
    ]
    items = [OP_STARTED, *many, VALIDATION_OK]
    bus = EventBus()

    forwarded = _drive(project, bus, items)
    assert forwarded == items

    run = _single_run(project)
    detail = run_history.get_run(project, run["run_id"], include_events=True)
    assert detail is not None
    assert detail["events"] == items


# --- byte-level wrapper ------------------------------------------------------


def test_record_ndjson_is_byte_identical_and_records(project: Path) -> None:
    items = [OP_STARTED, PROGRESS, VALIDATION_OK]
    lines = [
        json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
        for frame in items
    ]
    bus = EventBus()

    async def line_source() -> AsyncIterator[bytes]:
        for line in lines:
            yield line

    async def scenario() -> list[bytes]:
        out: list[bytes] = []
        async for chunk in record_ndjson(
            line_source(),
            project_root=project,
            event_bus=bus,
            kind="validate",
            env="dev",
        ):
            out.append(chunk)
        return out

    assert asyncio.run(scenario()) == lines

    run = _single_run(project)
    assert run["status"] == "completed"
    detail = run_history.get_run(project, run["run_id"], include_events=True)
    assert detail is not None
    assert detail["events"] == items
