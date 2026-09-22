"""Tests for :mod:`lhp.telemetry._sender`.

``send_batch`` always receives a fake opener here: the suite's network guard
would refuse a real connection anyway, and the fake lets each response class
be produced on demand. The opener records the request it was given so the
headers and body can be asserted without any socket.
"""

import http.client
import http.server
import io
import json
import logging
import socket
import threading
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import pytest

from lhp.telemetry._paths import spool_path
from lhp.telemetry._sender import (
    MAX_BATCH_BYTES,
    MAX_BATCH_EVENTS,
    SendResult,
    send_batch,
    start_sender,
)
from lhp.telemetry._spool import append_spool, read_lines, spool_count, unmarked_lines
from lhp.telemetry._store import StateFile, read_state, write_state

ENDPOINT = "https://telemetry.example.invalid/v1/events"
VERSION = "0.9.2"
EVENT_A = '{"schema_version":1,"event":"cli.command","props":{"command":"generate"}}'
EVENT_B = '{"schema_version":1,"event":"web.run","props":{"kind":"validate"}}'
SECRET_MARKER = "marker-that-must-never-be-logged"


class _Response:
    def __init__(self, status: int, body: bytes, url: Optional[str] = None) -> None:
        self.status = status
        self._body = body
        self._url = url
        self.read_sizes: List[Optional[int]] = []

    def read(self, amt: Optional[int] = None) -> bytes:
        self.read_sizes.append(amt)
        return self._body if amt is None else self._body[:amt]

    def geturl(self) -> str:
        return ENDPOINT if self._url is None else self._url

    def __enter__(self) -> "_Response":
        return self

    def __exit__(self, *exc: Any) -> None:
        return None


def _opener_returning(
    status: int = 200, body: Any = None, seen: Optional[List[Any]] = None
) -> Callable[..., _Response]:
    payload = b"" if body is None else json.dumps(body).encode()

    def opener(request: urllib.request.Request, **kwargs: Any) -> _Response:
        if seen is not None:
            seen.append((request, kwargs))
        return _Response(status, payload)

    return opener


def _opener_raising(exc: BaseException) -> Callable[..., _Response]:
    def opener(request: urllib.request.Request, **kwargs: Any) -> _Response:
        raise exc

    return opener


def _http_error(code: int) -> urllib.error.HTTPError:
    return urllib.error.HTTPError(ENDPOINT, code, "status", {}, io.BytesIO(b""))


def _marked(line: str) -> str:
    """``line`` as the spool stores it after a send that went unanswered."""
    return line[:-1] + ',"_unconfirmed":true}'


def _send(opener: Callable[..., Any], events: Optional[List[str]] = None) -> SendResult:
    return send_batch(
        events if events is not None else [EVENT_A, EVENT_B],
        endpoint=ENDPOINT,
        version=VERSION,
        opener=opener,
    )


# request shape


@pytest.mark.unit
def test_send_batch_posts_the_documented_request() -> None:
    seen: List[Any] = []
    _send(_opener_returning(200, {"accepted": 2, "rejected": 0}, seen))
    ((request, kwargs),) = seen
    assert request.full_url == ENDPOINT
    assert request.get_method() == "POST"
    assert request.get_header("Content-type") == "application/json"
    assert request.get_header("User-agent") == f"lhp/{VERSION}"
    assert kwargs["timeout"] == 3.0
    assert kwargs["context"] is not None
    body = json.loads(request.data)
    assert body == {
        "schema_version": 1,
        "client": f"lhp/{VERSION}",
        "events": [json.loads(EVENT_A), json.loads(EVENT_B)],
    }


@pytest.mark.unit
def test_send_batch_honours_the_timeout_argument() -> None:
    seen: List[Any] = []
    send_batch(
        [EVENT_A],
        endpoint=ENDPOINT,
        version=VERSION,
        timeout_s=0.5,
        opener=_opener_returning(200, {}, seen),
    )
    assert seen[0][1]["timeout"] == 0.5


@pytest.mark.unit
def test_send_batch_caps_the_event_count_keeping_the_newest() -> None:
    seen: List[Any] = []
    events = [f'{{"n":{n}}}' for n in range(MAX_BATCH_EVENTS + 7)]
    _send(_opener_returning(200, {}, seen), events)
    sent = json.loads(seen[0][0].data)["events"]
    assert len(sent) == MAX_BATCH_EVENTS
    assert sent[0] == {"n": 7}
    assert sent[-1] == {"n": MAX_BATCH_EVENTS + 6}


@pytest.mark.unit
def test_send_batch_caps_the_body_size() -> None:
    seen: List[Any] = []
    big = '{"pad":"' + "z" * 7000 + '"}'
    events = [big] * (MAX_BATCH_BYTES // len(big) + 5)
    _send(_opener_returning(200, {}, seen), events)
    assert len(seen[0][0].data) <= MAX_BATCH_BYTES


# response classes


@pytest.mark.unit
def test_200_with_latest_is_ok() -> None:
    result = _send(
        _opener_returning(200, {"accepted": 2, "rejected": 0, "latest": "0.9.3"})
    )
    assert result == SendResult("ok", "0.9.3", False)


@pytest.mark.unit
def test_200_with_null_latest_is_ok_without_a_version() -> None:
    result = _send(_opener_returning(200, {"accepted": 2, "latest": None}))
    assert result == SendResult("ok", None, False)


@pytest.mark.unit
def test_200_with_disabled_flags_the_kill_switch() -> None:
    result = _send(_opener_returning(200, {"disabled": True}))
    assert result == SendResult("ok", None, True)


@pytest.mark.unit
def test_200_with_a_non_json_body_keeps_the_batch() -> None:
    def opener(request: Any, **kwargs: Any) -> _Response:
        return _Response(200, b"<html>captive portal</html>")

    assert _send(opener) == SendResult("retry", None, False)


@pytest.mark.unit
@pytest.mark.parametrize("body", [b"", b"  \n"])
def test_2xx_with_an_empty_body_is_ok(body: bytes) -> None:
    def opener(request: Any, **kwargs: Any) -> _Response:
        return _Response(204, body)

    assert _send(opener) == SendResult("ok", None, False)


@pytest.mark.unit
def test_200_with_a_non_string_latest_is_ignored() -> None:
    result = _send(_opener_returning(200, {"latest": 93}))
    assert result == SendResult("ok", None, False)


@pytest.mark.unit
def test_a_latest_longer_than_a_version_string_is_ignored() -> None:
    assert _send(_opener_returning(200, {"latest": "9" * 33})) == SendResult(
        "ok", None, False
    )
    assert _send(_opener_returning(200, {"latest": "9" * 32})).latest == "9" * 32


@pytest.mark.unit
def test_the_reply_body_is_read_through_a_64_kib_cap() -> None:
    response = _Response(200, b'{"latest":"0.9.3"}')

    def opener(request: Any, **kwargs: Any) -> _Response:
        return response

    assert _send(opener) == SendResult("ok", "0.9.3", False)
    assert response.read_sizes == [64 * 1024]


@pytest.mark.unit
@pytest.mark.parametrize("status", [301, 302, 303])
def test_a_followed_redirect_drops_the_batch_after_one_request(status: int) -> None:
    # urllib follows these statuses by re-issuing the POST as a bodiless GET,
    # so the 2xx that comes back is the redirect target's, not the Worker's.
    seen: List[Any] = []

    def opener(request: Any, **kwargs: Any) -> _Response:
        seen.append(request)
        return _Response(200, b"", url=f"https://elsewhere.example.invalid/{status}")

    assert _send(opener) == SendResult("drop", None, False)
    assert len(seen) == 1


@pytest.mark.unit
@pytest.mark.parametrize("status", [301, 302, 303])
def test_a_real_redirect_is_detected_through_urllib(status: int) -> None:
    requests: List[str] = []

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            requests.append(f"POST {self.path}")
            self.send_response(status)
            self.send_header("Location", "/moved")
            self.send_header("Content-Length", "0")
            self.end_headers()

        def do_GET(self) -> None:
            requests.append(f"GET {self.path}")
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

        def log_message(self, format: str, *args: Any) -> None:
            return None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    try:
        result = send_batch(
            [EVENT_A],
            endpoint=f"http://127.0.0.1:{server.server_address[1]}/v1/events",
            version=VERSION,
        )
    finally:
        server.shutdown()
        server.server_close()
    assert result == SendResult("drop", None, False)
    assert requests == ["POST /v1/events", "GET /moved"]


@pytest.mark.unit
def test_a_real_request_left_unanswered_is_unconfirmed() -> None:
    received: List[bytes] = []
    got_body, release = threading.Event(), threading.Event()

    class _Handler(http.server.BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            received.append(self.rfile.read(int(self.headers["Content-Length"])))
            got_body.set()
            release.wait(5.0)

        def log_message(self, format: str, *args: Any) -> None:
            return None

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _Handler)
    serving = threading.Thread(target=server.serve_forever, daemon=True)
    serving.start()
    try:
        result = send_batch(
            [EVENT_A],
            endpoint=f"http://127.0.0.1:{server.server_address[1]}/v1/events",
            version=VERSION,
            timeout_s=0.2,
        )
        assert got_body.wait(5.0)
    finally:
        release.set()
        server.shutdown()
        server.server_close()
    assert result == SendResult("unconfirmed", None, False)
    assert json.loads(received[0])["events"] == [json.loads(EVENT_A)]


@pytest.mark.unit
def test_a_real_refused_connection_is_a_retry() -> None:
    probe = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    probe.bind(("127.0.0.1", 0))
    port = probe.getsockname()[1]
    probe.close()
    result = send_batch(
        [EVENT_A],
        endpoint=f"http://127.0.0.1:{port}/v1/events",
        version=VERSION,
        timeout_s=0.2,
    )
    assert result == SendResult("retry", None, False)


@pytest.mark.unit
@pytest.mark.parametrize("status", [202, 204])
def test_other_2xx_statuses_are_ok(status: int) -> None:
    assert _send(_opener_returning(status)).status_class == "ok"


@pytest.mark.unit
@pytest.mark.parametrize("code", [400, 413, 401, 404])
def test_client_errors_drop_the_batch(code: int) -> None:
    assert _send(_opener_raising(_http_error(code))) == SendResult("drop", None, False)


@pytest.mark.unit
@pytest.mark.parametrize("code", [429, 500, 502, 503])
def test_throttling_and_server_errors_keep_the_batch(code: int) -> None:
    assert _send(_opener_raising(_http_error(code))) == SendResult("retry", None, False)


@pytest.mark.unit
@pytest.mark.parametrize(
    "reason",
    [
        "name resolution failed",
        socket.timeout("timed out"),
        ConnectionRefusedError(),
        OSError("tls handshake"),
        "unknown url type: ftp",
    ],
)
def test_transport_failures_keep_the_batch(reason: Any) -> None:
    # urllib wraps a failure to send the request in URLError: nothing arrived.
    exc = urllib.error.URLError(reason)
    assert _send(_opener_raising(exc)) == SendResult("retry", None, False)


@pytest.mark.unit
@pytest.mark.parametrize(
    "exc",
    [
        TimeoutError("timed out"),
        ConnectionResetError(),
        http.client.RemoteDisconnected("closed without a reply"),
        http.client.BadStatusLine("garbage"),
        http.client.IncompleteRead(b"{"),
    ],
)
def test_a_sent_request_without_an_answer_is_unconfirmed(exc: BaseException) -> None:
    # urllib lets reply-side failures through raw: the request was sent.
    assert _send(_opener_raising(exc)) == SendResult("unconfirmed", None, False)


@pytest.mark.unit
def test_an_empty_batch_is_dropped_without_a_request() -> None:
    seen: List[Any] = []
    result = _send(_opener_returning(200, {}, seen), [])
    assert result.status_class == "drop"
    assert seen == []


@pytest.mark.unit
def test_an_endpoint_failing_the_scheme_guard_never_opens() -> None:
    seen: List[Any] = []
    result = send_batch(
        [EVENT_A],
        endpoint="http://evil.example/v1/events",
        version=VERSION,
        opener=_opener_returning(200, {}, seen),
    )
    assert result.status_class == "retry"
    assert seen == []


# logging discipline


@pytest.mark.unit
def test_failures_log_at_debug_only_and_never_the_payload(
    caplog: pytest.LogCaptureFixture,
) -> None:
    event = f'{{"event":"cli.command","props":{{"x":"{SECRET_MARKER}"}}}}'
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        _send(_opener_raising(_http_error(503)), [event])
        _send(_opener_raising(urllib.error.URLError("boom")), [event])
        _send(_opener_raising(_http_error(400)), [event])
    assert caplog.records
    assert all(record.levelno == logging.DEBUG for record in caplog.records)
    assert SECRET_MARKER not in caplog.text


@pytest.mark.unit
def test_success_logs_nothing_above_debug(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        _send(_opener_returning(200, {"latest": "0.9.3"}))
    assert all(record.levelno == logging.DEBUG for record in caplog.records)


@pytest.mark.unit
def test_send_result_is_frozen() -> None:
    from dataclasses import FrozenInstanceError

    result = SendResult("ok", None, False)
    with pytest.raises(FrozenInstanceError):
        result.latest = "x"  # type: ignore[misc]


# start_sender


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


def _spool(cfg: Path, *events: str) -> None:
    for event in events:
        assert append_spool(cfg, event)


def _run(cfg: Path, opener: Callable[..., Any]) -> Optional[threading.Thread]:
    thread = start_sender(cfg, endpoint=ENDPOINT, version=VERSION, opener=opener)
    if thread is not None:
        thread.join(5.0)
        assert not thread.is_alive()
    return thread


@pytest.mark.unit
def test_start_sender_is_none_when_nothing_is_spooled(cfg: Path) -> None:
    assert start_sender(cfg, endpoint=ENDPOINT, version=VERSION) is None
    assert not cfg.exists()


@pytest.mark.unit
def test_start_sender_runs_a_named_daemon_thread(cfg: Path) -> None:
    _spool(cfg, EVENT_A)
    thread = start_sender(
        cfg, endpoint=ENDPOINT, version=VERSION, opener=_opener_returning(200, {})
    )
    assert thread is not None
    assert thread.daemon is True
    assert thread.name == "lhp-telemetry-sender"
    thread.join(5.0)
    assert not thread.is_alive()


@pytest.mark.unit
def test_start_sender_settles_only_under_the_given_lock(cfg: Path) -> None:
    lock = threading.Lock()
    lock.acquire()
    _spool(cfg, EVENT_A)
    thread = start_sender(
        cfg,
        endpoint=ENDPOINT,
        version=VERSION,
        opener=_opener_returning(200, {}),
        lock=lock,
    )
    assert thread is not None
    thread.join(0.3)
    assert thread.is_alive()
    assert len(list(spool_path(cfg).parent.glob("spool.inflight-*"))) == 1
    lock.release()
    thread.join(5.0)
    assert not thread.is_alive()
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_ok_discards_the_inflight_batch(cfg: Path) -> None:
    _spool(cfg, EVENT_A, EVENT_B)
    seen: List[Any] = []
    _run(cfg, _opener_returning(200, {"accepted": 2}, seen))
    assert len(json.loads(seen[0][0].data)["events"]) == 2
    assert spool_count(cfg) == 0
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_drop_discards_the_inflight_batch(cfg: Path) -> None:
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_raising(_http_error(400)))
    assert spool_count(cfg) == 0
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_retry_restores_the_batch_to_the_spool(cfg: Path) -> None:
    _spool(cfg, EVENT_A, EVENT_B)
    _run(cfg, _opener_raising(_http_error(503)))
    assert read_lines(spool_path(cfg)) == [EVENT_A, EVENT_B]
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_a_second_unconfirmed_outcome_empties_the_spool(cfg: Path) -> None:
    _spool(cfg, EVENT_A, EVENT_B)
    _run(cfg, _opener_raising(TimeoutError("timed out")))
    assert read_lines(spool_path(cfg)) == [_marked(EVENT_A), _marked(EVENT_B)]
    _run(cfg, _opener_raising(TimeoutError("timed out")))
    assert spool_count(cfg) == 0
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == []


@pytest.mark.unit
def test_a_marked_batch_is_posted_unmarked(cfg: Path) -> None:
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_raising(ConnectionResetError()))
    seen: List[Any] = []
    _run(cfg, _opener_returning(200, {"accepted": 1}, seen))
    body = seen[0][0].data
    assert json.loads(body)["events"] == [json.loads(EVENT_A)]
    assert b"_unconfirmed" not in body
    assert spool_count(cfg) == 0


@pytest.mark.unit
def test_a_503_after_an_unconfirmed_outcome_keeps_the_mark(cfg: Path) -> None:
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_raising(ConnectionResetError()))
    _run(cfg, _opener_raising(_http_error(503)))
    assert read_lines(spool_path(cfg)) == [_marked(EVENT_A)]


@pytest.mark.unit
def test_ok_stores_latest_in_an_existing_state_file(cfg: Path) -> None:
    write_state(cfg, StateFile(install_id="x", created_at="2026-09-21T00:00:00.000Z"))
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_returning(200, {"latest": "0.9.3"}))
    state = read_state(cfg)
    assert state is not None
    assert state.latest_known_version == "0.9.3"
    assert state.latest_checked_at is not None
    assert state.server_disabled_until is None
    assert state.install_id == "x"


@pytest.mark.unit
def test_ok_never_creates_a_state_file(cfg: Path) -> None:
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_returning(200, {"latest": "0.9.3"}))
    assert read_state(cfg) is None


@pytest.mark.unit
def test_ok_without_latest_keeps_the_known_version(cfg: Path) -> None:
    write_state(cfg, StateFile(latest_known_version="0.9.3"))
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_returning(200, {"accepted": 1}))
    state = read_state(cfg)
    assert state is not None and state.latest_known_version == "0.9.3"


@pytest.mark.unit
def test_disabled_sets_server_disabled_until_about_a_day_ahead(cfg: Path) -> None:
    from datetime import datetime, timedelta, timezone

    write_state(cfg, StateFile(install_id="x"))
    _spool(cfg, EVENT_A)
    _run(cfg, _opener_returning(200, {"disabled": True}))
    state = read_state(cfg)
    assert state is not None and state.server_disabled_until is not None
    until = datetime.fromisoformat(state.server_disabled_until)
    assert until.tzinfo is not None
    remaining = until - datetime.now(timezone.utc)
    assert timedelta(hours=23) < remaining <= timedelta(hours=24)


@pytest.mark.unit
def test_an_opener_that_blows_up_is_contained_in_the_thread(
    cfg: Path, caplog: pytest.LogCaptureFixture
) -> None:
    event = f'{{"event":"cli.command","props":{{"x":"{SECRET_MARKER}"}}}}'
    _spool(cfg, event)
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        _run(cfg, _opener_raising(RuntimeError("unexpected")))
    assert read_lines(spool_path(cfg)) == [_marked(event)]
    assert unmarked_lines(spool_path(cfg)) == [event]
    assert all(record.levelno == logging.DEBUG for record in caplog.records)
    assert SECRET_MARKER not in caplog.text
