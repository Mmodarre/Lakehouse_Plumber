"""Bridge a SYNC ``lhp.api`` event stream to an ASYNC NDJSON byte stream.

The public facade long-running operations (``generate_pipelines`` /
``validate_pipelines``) are SYNCHRONOUS generators yielding
``Iterator[LHPEvent]`` (constitution §13.2). FastAPI's ``StreamingResponse``
wants an async iterator. This module owns that bridge.

Architecture
------------
:func:`stream_events` is an ASYNC generator that yields one NDJSON line
(``bytes``, JSON object + ``"\\n"``) per frame. It:

1. Runs the caller-supplied SYNC ``run(sink) -> Iterator[LHPEvent]`` in a
   dedicated WORKER THREAD (the iterator's per-event work is blocking CPU/IO
   that must not stall the event loop).
2. Hands a :class:`_QueueProgressSink` to ``run`` so the run's progress
   callbacks (``on_total`` / ``on_advance``) push ``progress`` frames through
   the SAME queue that event frames use — preserving exact arrival order
   (§5.7: no reordering, no buffering beyond the queue).
3. Frames cross the thread→loop boundary via
   ``loop.call_soon_threadsafe(queue.put_nowait, ...)``; the async side
   ``await``\\s the queue and yields the encoded bytes.

Frame protocol (pinned — the frontend types are built against EXACTLY this):

* EVENT frame: ``{"type": "<EventClassName>", **to_dict(event)}``.
* PROGRESS frame: ``{"type": "progress", "total": int, "done": int,
  "current": str | null}``.
* INFO frame: ``{"type": "info", "code": str, "message": str}`` — emitted
  INSTEAD of an event frame when a :class:`WarningEmitted` carries the
  soft-cap code ``LHP-EVT-SOFT-CAP`` (ordinary warnings stay event frames).
* TERMINAL ERROR frame: ``{"type": "error", "code", "title", "details",
  "suggestions", "context", "doc_link"}`` — built MANUALLY from
  :class:`~lhp.errors.LHPError` attributes (``to_dict`` would raise on the
  live exception carried by :class:`ErrorEmitted`). Emitted for (a) an
  ``ErrorEmitted`` event and (b) any ``LHPError`` raised by the iterator
  itself; its fields are curated, user-facing copy and are sent as-is. An
  UNEXPECTED (non-LHP) exception is a bug: its traceback is logged server-side
  and the client receives a REDACTED frame — the stable wrapper ``code`` /
  ``title`` (``LHP-GEN-902``) but a generic ``details`` and empty ``context``,
  never the raw exception text — matching the HTTP middleware's posture so
  internal paths / SQL never leak. The stream terminates cleanly after it.

Run serialization
------------------
A module-level :data:`_run_lock` (``asyncio.Lock``) admits only one
validate/generate run at a time (single-user local server). A concurrent
caller WAITS for the in-flight run to finish; it is never rejected.

Client disconnect
------------------
When the consumer aborts (``StreamingResponse`` calls ``aclose()`` →
``GeneratorExit``), the worker is signalled to stop via a
:class:`threading.Event` checked between events. A long in-flight pipeline
step finishes first (the sync iterator is not forcibly interrupted), then the
worker exits and is joined — no thread leak.

This module imports ONLY ``lhp.api`` / ``lhp.errors`` from the lhp package
plus stdlib (no anyio): it honours the ``webapp-uses-public-api`` boundary
contract (§5.3).

:stability: internal
"""

from __future__ import annotations

import asyncio
import json
import logging
import threading
from typing import Any, AsyncIterator, Callable, Iterator, Optional

from lhp.api import (
    ErrorEmitted,
    LHPEvent,
    ProgressSink,
    WarningEmitted,
    to_dict,
)
from lhp.errors import LHPError

logger = logging.getLogger(__name__)

#: Locked soft-cap code (§13 item 4) — a ``WarningEmitted`` carrying this is
#: rendered as an ``info`` frame, not an event frame.
_SOFT_CAP_CODE = "LHP-EVT-SOFT-CAP"

#: Single-run gate (§ run-serialization): only one validate/generate stream is
#: live at a time on this single-user local server. Concurrent callers wait.
_run_lock = asyncio.Lock()

#: Sentinel pushed onto the queue by the worker when the run is fully done
#: (the iterator was exhausted or it raised and the terminal frame was already
#: enqueued). The async side stops draining when it sees this.
_DONE = object()

# A frame is either a ready-to-encode JSON-compatible dict, or the _DONE
# sentinel. Worker-side exceptions are converted to error-frame dicts by the
# worker itself, so they never cross the queue as live objects.
_Frame = Any


def _event_frame(event: LHPEvent) -> dict[str, Any]:
    """Build the EVENT frame ``{"type": <ClassName>, **to_dict(event)}``.

    ``to_dict`` of a frozen-dataclass event returns a ``dict`` of its fields;
    the class name is prepended under ``"type"`` so the frontend can
    discriminate. Never called for :class:`ErrorEmitted` (``to_dict`` raises
    on the live ``LHPError`` it carries) — that path goes through
    :func:`_error_frame`.
    """
    payload = to_dict(event)
    # to_dict on a dataclass event always yields a dict; assert for the type
    # checker and to fail loudly if a non-dataclass LHPEvent ever appears.
    if not isinstance(payload, dict):  # pragma: no cover - defensive
        raise TypeError(f"event {type(event).__name__} did not serialise to a dict")
    return {"type": type(event).__name__, **payload}


def _info_frame(warning: WarningEmitted) -> dict[str, Any]:
    """Build the INFO frame for a soft-cap :class:`WarningEmitted`."""
    return {"type": "info", "code": warning.code, "message": warning.message}


def _error_frame(error: LHPError) -> dict[str, Any]:
    """Build the TERMINAL ERROR frame from :class:`~lhp.errors.LHPError` attrs.

    Built MANUALLY (not via ``to_dict``) because ``LHPError`` is not a
    dataclass — ``to_dict`` raises ``TypeError`` on it. The shape is pinned to
    what the frontend expects. Every field here is curated, user-facing copy
    carried by the domain error, so it is safe to send to the browser as-is.
    For an *unexpected* (non-LHP) exception use :func:`_unexpected_error_frame`
    instead — that path must NOT echo the raw exception text to the client.
    """
    return {
        "type": "error",
        "code": error.code,
        "title": error.title,
        "details": error.details if error.details else None,
        "suggestions": list(error.suggestions),
        "context": dict(error.context),
        "doc_link": error.doc_link,
    }


def _unexpected_error_frame(error: LHPError) -> dict[str, Any]:
    """Build a REDACTED terminal error frame for an unexpected exception.

    Mirrors the HTTP middleware's ``generic_error_handler`` posture: a non-LHP
    exception escaping the run is a bug, and the wrapper's ``details`` echoes the
    raw ``str(exc)`` (and ``context`` the exception class name) which may carry
    internal paths / SQL / identifiers. The full detail is logged server-side
    via ``logger.exception``; the client keeps the stable wrapper ``code`` /
    ``title`` (``LHP-GEN-902`` — the pinned frontend discriminator) but gets a
    generic ``details`` and an empty ``context`` instead of the raw text. The
    shape matches :func:`_error_frame` so the frontend handles it identically.
    """
    return {
        "type": "error",
        "code": error.code,
        "title": error.title,
        "details": "An unexpected error occurred. Check the server logs for details.",
        "suggestions": ["Check the server logs for the full Python traceback"],
        "context": {},
        "doc_link": error.doc_link,
    }


def _classify_frame(event: LHPEvent) -> dict[str, Any]:
    """Map a single ``LHPEvent`` to its frame dict (event / info / error).

    * :class:`ErrorEmitted` → terminal ERROR frame from its ``lhp_error``.
    * Soft-cap :class:`WarningEmitted` → INFO frame.
    * Everything else → EVENT frame.
    """
    if isinstance(event, ErrorEmitted):
        return _error_frame(event.lhp_error)
    if isinstance(event, WarningEmitted) and event.code == _SOFT_CAP_CODE:
        return _info_frame(event)
    return _event_frame(event)


class _QueueProgressSink(ProgressSink):
    """A :class:`ProgressSink` that forwards progress as queue frames.

    Subclasses the public sink (it stays a concrete counter the run reads),
    and ALSO emits a ``progress`` frame onto the shared queue on every
    ``on_total`` / ``on_advance`` so progress interleaves with event frames in
    exact arrival order. The base counter fields are still updated first, so a
    frame always carries the post-update snapshot.

    The callbacks run inside the worker thread, so they hand the frame across
    to the loop via ``call_soon_threadsafe``. They MUST NOT raise (§ ProgressSink
    contract) — a closed loop / stopped run is swallowed (the consumer has gone
    away; there is no one left to receive the frame).
    """

    def __init__(
        self,
        loop: asyncio.AbstractEventLoop,
        queue: "asyncio.Queue[_Frame]",
    ) -> None:
        super().__init__()
        self._loop = loop
        self._queue = queue

    def _emit(self) -> None:
        frame = {
            "type": "progress",
            "total": self.total,
            "done": self.done,
            "current": self.current,
        }
        try:
            self._loop.call_soon_threadsafe(self._queue.put_nowait, frame)
        except RuntimeError:
            # Loop is closed / not running — the consumer disconnected. Drop
            # the frame silently; the sink contract forbids raising here.
            logger.debug("progress frame dropped: event loop unavailable")

    def on_total(self, n: int) -> None:
        super().on_total(n)
        self._emit()

    def on_advance(self, current: Optional[str] = None) -> None:
        super().on_advance(current)
        self._emit()


def _run_worker(
    run: Callable[[ProgressSink], Iterator[LHPEvent]],
    sink: ProgressSink,
    loop: asyncio.AbstractEventLoop,
    queue: "asyncio.Queue[_Frame]",
    stop: threading.Event,
) -> None:
    """Worker-thread body: drive the sync iterator, push frames to the queue.

    Runs ``run(sink)`` and iterates the resulting sync event stream. Each
    event is classified into a frame and handed to the loop thread via
    ``call_soon_threadsafe``. Between events it checks ``stop`` (set on client
    disconnect) and bails out early — a long in-flight step finishes first.

    Terminal behaviour:

    * Normal exhaustion → enqueue :data:`_DONE`.
    * An :class:`ErrorEmitted` event was already converted to a terminal error
      frame; the §1.4 protocol then re-raises the same ``LHPError`` from the
      iterator. That raise is caught here, NOT re-converted (the error frame
      already went out), and the stream ends with :data:`_DONE`.
    * Any OTHER ``LHPError`` / unexpected exception raised by the iterator (no
      preceding ``ErrorEmitted``) → enqueue a terminal error frame, then
      :data:`_DONE`.

    Frames produced by the progress sink are interleaved in arrival order on
    the same queue automatically (the sink pushes from this same thread).
    """

    def _push(frame: _Frame) -> None:
        try:
            loop.call_soon_threadsafe(queue.put_nowait, frame)
        except RuntimeError:
            # Loop gone (consumer disconnected mid-flight). Nothing to do —
            # the worker will notice ``stop`` on the next iteration anyway.
            logger.debug("frame dropped: event loop unavailable")

    error_frame_sent = False
    try:
        iterator = run(sink)
        for event in iterator:
            if stop.is_set():
                logger.debug("stream worker stopping: client disconnected")
                break
            frame = _classify_frame(event)
            if frame.get("type") == "error":
                error_frame_sent = True
            _push(frame)
            if error_frame_sent:
                # An ErrorEmitted terminal error frame is out; the protocol
                # will re-raise next. Stop pulling to avoid a redundant cycle.
                break
    except LHPError as exc:
        if not error_frame_sent:
            # An LHPError raised WITHOUT a preceding ErrorEmitted (e.g. a
            # preflight raise) — surface it as the terminal error frame.
            _push(_error_frame(exc))
        # else: the re-raise that follows an ErrorEmitted; frame already sent.
    except Exception as exc:
        logger.exception("stream worker: unexpected error draining event stream")
        if not error_frame_sent:
            # An unexpected (non-LHP) exception is a bug: the traceback is
            # already in the server log; the client gets a REDACTED frame so the
            # raw ``str(exc)`` (internal paths / SQL / identifiers) never leaks.
            _push(
                _unexpected_error_frame(
                    LHPError.from_unexpected_exception(exc, "stream")
                )
            )
    finally:
        _push(_DONE)


async def stream_events(
    run: Callable[[ProgressSink], Iterator[LHPEvent]],
) -> AsyncIterator[bytes]:
    """Stream a sync facade event run as async NDJSON ``bytes`` lines.

    The single public entry point for the streaming router. ``run`` is a
    zero-config callable taking a :class:`~lhp.api.ProgressSink` and returning
    the sync event iterator — in practice a ``functools.partial`` of
    ``facade.validate_pipelines`` / ``facade.generate_pipelines`` with every
    keyword already bound except ``progress``. The adapter constructs the sink,
    passes it as ``progress=``, and owns the thread/queue bridge.

    Yields one NDJSON line per frame (a JSON object encoded UTF-8 + ``b"\\n"``)
    in the EXACT order the iterator and progress sink produce them (§5.7).
    After a terminal error frame (or normal completion) the generator stops.

    Run serialization: acquires :data:`_run_lock` for the whole stream, so a
    second concurrent call's first frame appears only after the first stream
    completes (single-user local server).

    Client disconnect: if the consumer ``aclose()``\\s early, the worker is
    signalled to stop (a long in-flight pipeline step finishes first) and is
    joined before this generator returns — no thread leak.

    :stability: internal
    """
    async with _run_lock:
        loop = asyncio.get_running_loop()
        queue: "asyncio.Queue[_Frame]" = asyncio.Queue()
        stop = threading.Event()
        sink = _QueueProgressSink(loop, queue)

        worker = threading.Thread(
            target=_run_worker,
            args=(run, sink, loop, queue, stop),
            name="lhp-stream-worker",
            daemon=True,
        )
        worker.start()
        try:
            while True:
                frame = await queue.get()
                if frame is _DONE:
                    break
                yield json.dumps(frame, separators=(",", ":")).encode("utf-8") + b"\n"
        finally:
            # Reached on normal completion, on consumer ``aclose()``
            # (GeneratorExit), or on a yield-side error. Signal the worker to
            # stop and join it so the thread never outlives the stream.
            stop.set()
            await asyncio.to_thread(worker.join)
