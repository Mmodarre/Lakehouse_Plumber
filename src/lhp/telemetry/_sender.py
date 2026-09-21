"""One HTTP attempt per batch, and the daemon thread that makes it.

The endpoint is pinned to ``https://`` or loopback ``http://`` before any
request is built, which is what makes the ``urlopen`` call below safe. One
attempt, a three-second timeout, and every failure — network, TLS, HTTP or
a bug — is logged at DEBUG without the payload and classified as ``drop``
(the Worker refused the batch for good) or ``retry`` (it may accept it
later). The default opener honours ``HTTPS_PROXY`` and ``NO_PROXY`` like
every other ``urllib`` client.
"""

from __future__ import annotations

import json
import logging
import ssl
import threading
import urllib.error
import urllib.request
from dataclasses import dataclass, replace
from datetime import timedelta
from pathlib import Path
from typing import Any, Callable, List, Literal, Mapping, Optional, Sequence
from urllib.parse import urlsplit

from lhp.telemetry import _spool, _store
from lhp.telemetry._events import SCHEMA_VERSION

logger = logging.getLogger(__name__)

# Placeholder until the LHP-owned hostname exists, tracked by the merge-blocker
# issue "replace placeholder telemetry hostname". The ``.invalid`` TLD never
# resolves, so a build that ships with it fails closed into the spool.
DEFAULT_ENDPOINT = "https://telemetry.lakehouse-plumber.invalid/v1/events"
MAX_BATCH_EVENTS = 500
MAX_BATCH_BYTES = 512 * 1024
DEFAULT_TIMEOUT_S = 3.0
SENDER_THREAD_NAME = "lhp-telemetry-sender"
# How long a Worker kill switch silences this install.
_SERVER_DISABLE_PERIOD = timedelta(hours=24)
_LOOPBACK_HOSTS = frozenset({"127.0.0.1", "localhost", "::1"})

Opener = Callable[..., Any]
StatusClass = Literal["ok", "drop", "retry"]


@dataclass(frozen=True)
class SendResult:
    """What one attempt established: the batch's fate and the Worker's extras."""

    status_class: StatusClass
    latest: Optional[str]
    disabled: bool


def _endpoint_allowed(endpoint: str) -> bool:
    """``https://`` to any host, or ``http://`` to a loopback host only."""
    try:
        parts = urlsplit(endpoint)
    except ValueError:  # not a URL at all
        return False
    if parts.scheme == "https":
        return bool(parts.hostname)
    return parts.scheme == "http" and parts.hostname in _LOOPBACK_HOSTS


def resolve_endpoint(environ: Mapping[str, str]) -> str:
    """The endpoint to post to: a guarded ``LHP_TELEMETRY_ENDPOINT`` or the default."""
    override = environ.get("LHP_TELEMETRY_ENDPOINT")
    if not override:
        return DEFAULT_ENDPOINT
    if _endpoint_allowed(override):
        return override
    logger.debug("Ignoring LHP_TELEMETRY_ENDPOINT: only https:// or loopback http://")
    return DEFAULT_ENDPOINT


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
    """Read ``latest`` and ``disabled`` from a 2xx body."""
    try:
        document = json.loads(payload)
    except ValueError:  # the events were accepted; only the extras are lost
        document = {}
    extras = document if isinstance(document, dict) else {}
    latest = extras.get("latest")
    disabled = extras.get("disabled") is True
    return SendResult("ok", latest if isinstance(latest, str) else None, disabled)


def send_batch(
    events_json: Sequence[str],
    *,
    endpoint: str,
    version: str,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    opener: Optional[Opener] = None,
) -> SendResult:
    """Post one batch of envelope lines and classify the outcome; never raises.

    ``opener`` defaults to ``urllib.request.urlopen`` and is the test seam.
    """
    if not events_json:
        return SendResult("drop", None, False)
    if not _endpoint_allowed(endpoint):
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
            payload = response.read()
    except urllib.error.HTTPError as error:
        logger.debug(f"Telemetry upload rejected with HTTP {error.code}")
        return _status_result(error.code)
    except Exception:  # network, TLS, timeout or a bug: the batch waits
        logger.debug("Telemetry upload failed", exc_info=True)
        return SendResult("retry", None, False)
    return _accepted(payload) if 200 <= status < 300 else _status_result(status)


def _settle(cfg: Path, inflight: Path, result: SendResult) -> None:
    """Apply the outcome to the inflight batch and to an EXISTING state file.

    The state file is never created here: CI runs and ``log`` mode leave none
    behind, and a ``latest`` hint is worthless without an install to show it to.
    """
    if result.status_class == "retry":
        _spool.restore_inflight(cfg, inflight)
        return
    _spool.discard_inflight(inflight)
    state = _store.read_state(cfg)
    if result.status_class != "ok" or state is None:
        return
    until = state.server_disabled_until
    if result.disabled:
        until = _store.utc_now_iso(_SERVER_DISABLE_PERIOD)
    updated = replace(
        state,
        latest_checked_at=_store.utc_now_iso(),
        latest_known_version=result.latest or state.latest_known_version,
        server_disabled_until=until,
    )
    _store.write_state(cfg, updated)


def start_sender(
    cfg: Path, *, endpoint: str, version: str, opener: Optional[Opener] = None
) -> Optional[threading.Thread]:
    """Claim the spool and send it on a daemon thread.

    ``None`` when there is nothing to send. The claim happens on the caller's
    thread so a batch is never taken twice; the thread is returned so the
    caller can bound its exit latency with a join.
    """
    inflight = _spool.take_inflight(cfg)
    if inflight is None:
        return None

    def _run() -> None:
        try:
            lines = _spool.read_lines(inflight)
            sent = send_batch(lines, endpoint=endpoint, version=version, opener=opener)
            _settle(cfg, inflight, sent)
        except Exception:  # a daemon thread has no caller to report to
            logger.debug("Telemetry sender thread failed", exc_info=True)

    thread = threading.Thread(target=_run, name=SENDER_THREAD_NAME, daemon=True)
    thread.start()
    return thread
