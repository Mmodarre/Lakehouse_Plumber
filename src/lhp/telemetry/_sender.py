"""One HTTP attempt per batch, and the daemon thread that makes it.

The endpoint is pinned to ``https://`` or loopback ``http://`` before any
request is built, which is what makes the ``urlopen`` call below safe. One
attempt, a three-second timeout, and every failure is logged at DEBUG without
the payload and classified: ``drop`` (the Worker refused the batch for good),
``retry`` (the batch never reached it, or it answered 429 or 5xx) or
``unconfirmed`` (anything else, typically a request sent but unanswered: the
Worker may hold the batch, so the spool caps its resends). A redirect is a
``drop``: ``urllib`` re-issues a redirected POST as a bodiless GET, so its
2xx is not the Worker's verdict, and a permanent redirect must not re-send
every batch. The default opener honours ``HTTPS_PROXY`` and ``NO_PROXY`` like
every other ``urllib`` client.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
import urllib.error
import urllib.request
from contextlib import AbstractContextManager, nullcontext
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Literal, Optional, Sequence

from lhp.telemetry import _spool, _store
from lhp.telemetry._events import SCHEMA_VERSION
from lhp.telemetry._paths import endpoint_allowed

logger = logging.getLogger(__name__)

MAX_BATCH_EVENTS = 500
MAX_BATCH_BYTES = 512 * 1024
# The Worker's reply is a few dozen bytes; anything bigger is not worth reading.
_MAX_REPLY_BYTES = 64 * 1024
_MAX_LATEST_CHARS = 32
# How long a Worker kill switch silences this install.
_SERVER_DISABLE_PERIOD = timedelta(hours=24)

Opener = Callable[..., Any]
StatusClass = Literal["ok", "drop", "retry", "unconfirmed"]


@dataclass(frozen=True)
class SendResult:
    """What one attempt established: the batch's fate and the Worker's extras."""

    status_class: StatusClass
    latest: Optional[str]
    disabled: bool


def _fit(events_json: Sequence[str], budget: int) -> List[bytes]:
    """The newest events that fit the count cap and ``budget`` bytes."""
    kept: List[bytes] = []
    for event in reversed(events_json[-MAX_BATCH_EVENTS:]):
        encoded = event.encode("utf-8")
        cost = len(encoded) + (1 if kept else 0)
        if cost > budget:
            break
        budget -= cost
        kept.append(encoded)
    if len(kept) < len(events_json):
        logger.debug(f"Sending the newest {len(kept)} of {len(events_json)} events")
    kept.reverse()
    return kept


def _status_result(code: int) -> SendResult:
    retry = code == 429 or code >= 500
    return SendResult("retry" if retry else "drop", None, False)


def _accepted(payload: bytes) -> SendResult:
    """Read ``latest``/``disabled`` from a 2xx body. An empty body is a plain
    success; a non-empty non-JSON one came from a proxy or portal page, not
    the Worker, so the batch is kept rather than silently lost."""
    if not payload.strip():
        return SendResult("ok", None, False)
    try:
        document = json.loads(payload)
    except ValueError:  # not the Worker's reply: keep the batch
        logger.debug("Telemetry upload answered 2xx with a non-JSON body")
        return SendResult("retry", None, False)
    extras = document if isinstance(document, dict) else {}
    latest = extras.get("latest")
    if not isinstance(latest, str) or len(latest) > _MAX_LATEST_CHARS:
        latest = None
    return SendResult("ok", latest, extras.get("disabled") is True)


def send_batch(
    events_json: Sequence[str],
    *,
    endpoint: str,
    version: str,
    timeout_s: float = 3.0,
    opener: Optional[Opener] = None,
) -> SendResult:
    """Post one batch of envelope lines and classify the outcome; never raises.

    ``opener`` defaults to ``urllib.request.urlopen`` and is the test seam.
    """
    if not events_json:
        return SendResult("drop", None, False)
    if not endpoint_allowed(endpoint):
        logger.debug("Refusing to post telemetry outside the endpoint scheme guard")
        return SendResult("retry", None, False)
    prefix = f'{{"schema_version":{SCHEMA_VERSION},"client":"lhp/{version}","events":['
    head = prefix.encode("utf-8")
    body = head + b",".join(_fit(events_json, MAX_BATCH_BYTES - len(head) - 2)) + b"]}"
    request = urllib.request.Request(endpoint, data=body, method="POST")
    request.add_header("Content-Type", "application/json")
    request.add_header("User-Agent", f"lhp/{version}")
    open_url = urllib.request.urlopen if opener is None else opener
    try:
        # Scheme pinned to https or loopback http by the guard above.
        with open_url(  # nosec B310
            request, timeout=timeout_s, context=ssl.create_default_context()
        ) as response:
            status = int(getattr(response, "status", 200))
            geturl = getattr(response, "geturl", None)
            answered_by = request.full_url if geturl is None else geturl()
            payload = response.read(_MAX_REPLY_BYTES)
    except urllib.error.HTTPError as error:  # a verdict from the Worker, not a fault
        logger.debug(f"Telemetry upload rejected with HTTP {error.code}")
        return _status_result(error.code)
    except urllib.error.URLError:  # never fully sent: the Worker has not seen it
        logger.debug("Telemetry upload could not be sent", exc_info=True)
        return SendResult("retry", None, False)
    except Exception:  # anything else, typically sent but unanswered: may have landed
        logger.debug("Telemetry upload went unanswered", exc_info=True)
        return SendResult("unconfirmed", None, False)
    if answered_by != request.full_url:
        logger.debug("Telemetry upload was redirected; dropping the batch")
        return SendResult("drop", None, False)
    return _accepted(payload) if 200 <= status < 300 else _status_result(status)


def _settle(cfg: Path, inflight: Path, result: SendResult) -> None:
    """Apply the outcome to the inflight batch and to an EXISTING state file.

    The state file is never created here: CI runs and ``log`` mode leave none
    behind, and a ``latest`` hint is worthless without an install to show it to.
    """
    if result.status_class in ("retry", "unconfirmed"):
        unconfirmed = result.status_class == "unconfirmed"
        _spool.restore_inflight(cfg, inflight, unconfirmed=unconfirmed)
        return
    _spool.discard_inflight(inflight)
    state = _store.read_state(cfg)
    if result.status_class != "ok" or state is None:
        return
    changes: Dict[str, Any] = {"latest_checked_at": _store.utc_now_iso()}
    if result.latest:
        changes["latest_known_version"] = result.latest
    if result.disabled:
        changes["server_disabled_until"] = _store.utc_now_iso(_SERVER_DISABLE_PERIOD)
    _store.write_state(cfg, replace(state, **changes))


def start_sender(
    cfg: Path,
    *,
    endpoint: str,
    version: str,
    opener: Optional[Opener] = None,
    lock: Optional[AbstractContextManager[Any]] = None,
) -> Optional[threading.Thread]:
    """Claim the spool and send it on a daemon thread; ``None`` when it is empty.

    The claim happens on the caller's thread so a batch is never taken twice,
    and the thread is returned so the caller can bound its exit latency with
    a join. ``lock`` is held for the settle step only: the spool rewrite and
    the state-file update race the recording lock's other writers, while the
    network attempt races nothing.
    """
    inflight = _spool.take_inflight(cfg)
    if inflight is None:
        return None

    def _run() -> None:
        try:
            lines = _spool.unmarked_lines(inflight)
            sent = send_batch(lines, endpoint=endpoint, version=version, opener=opener)
            with lock or nullcontext():
                _settle(cfg, inflight, sent)
        except Exception:  # a daemon thread has no caller to report to
            logger.debug("Telemetry sender thread failed", exc_info=True)

    thread = threading.Thread(target=_run, name="lhp-telemetry-sender", daemon=True)
    thread.start()
    return thread
