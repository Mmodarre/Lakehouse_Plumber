"""Hard wall-clock limit for draft rendering, isolated from the web server."""

from __future__ import annotations

import json
import multiprocessing
import threading
from multiprocessing.connection import Connection
from pathlib import Path
from typing import Any

from lhp.core.processing.template_authoring import diagnostic
from lhp.core.processing.template_preview import initial_result, run_preview

PREVIEW_TIMEOUT_SECONDS = 10.0
MAX_RESULT_BYTES = 2 * 1024 * 1024
_slots = threading.BoundedSemaphore(2)


def _failure(request: dict[str, Any], message: str, code: str) -> dict[str, Any]:
    result = initial_result(request)
    result["status"] = "invalid"
    result["diagnostics"] = [
        diagnostic(request["source_path"], message, code=code, stage=request["stage"])
    ]
    return result


def _worker(connection: Connection, root: str, request: dict[str, Any]) -> None:
    try:
        from lhp.generators.registration import register_all

        register_all()
        result = run_preview(Path(root), request)
        payload = json.dumps(result, allow_nan=False).encode("utf-8")
        if len(payload) > MAX_RESULT_BYTES:
            payload = json.dumps(
                _failure(
                    request,
                    "Preview output exceeds 2 MiB. Reduce the sample values or template output.",
                    "LHP-TEMPLATE-LIMIT",
                )
            ).encode()
        connection.send_bytes(payload)
    except Exception as exc:
        connection.send_bytes(
            json.dumps(
                _failure(
                    request,
                    f"Preview could not complete: {exc}",
                    "LHP-TEMPLATE-PREVIEW",
                )
            ).encode()
        )
    finally:
        connection.close()


def bounded_preview(root: Path, request: dict[str, Any]) -> dict[str, Any]:
    """Spawn one request-local worker and terminate it on timeout, including Jinja.

    A timed-out thread cannot stop Jinja work. A child process can be killed,
    and cannot mutate the server's renderer/facade caches. At most two workers
    run concurrently; requests never form an unbounded server-side queue.

    :stability: provisional
    """
    if not _slots.acquire(blocking=False):
        return _failure(
            request,
            "Two previews are already running. Retry when one finishes.",
            "LHP-TEMPLATE-BUSY",
        )
    ctx = multiprocessing.get_context("spawn")
    receive, send = ctx.Pipe(duplex=False)
    worker = ctx.Process(target=_worker, args=(send, str(root), request), daemon=True)
    try:
        worker.start()
        send.close()
        if not receive.poll(PREVIEW_TIMEOUT_SECONDS):
            return _failure(
                request,
                "Preview exceeded 10 seconds. Reduce the sample or expression and retry.",
                "LHP-TEMPLATE-TIMEOUT",
            )
        result: dict[str, Any] = json.loads(receive.recv_bytes(MAX_RESULT_BYTES))
        return result
    except (EOFError, OSError, ValueError) as exc:
        return _failure(
            request,
            f"Preview worker stopped before returning a result: {exc}",
            "LHP-TEMPLATE-WORKER",
        )
    finally:
        receive.close()
        send.close()
        if worker.pid is not None:
            worker.join(timeout=0.1)
            if worker.is_alive():
                worker.terminate()
                worker.join(timeout=0.5)
            if worker.is_alive():
                worker.kill()
                worker.join(timeout=0.5)
            worker.close()
        _slots.release()
