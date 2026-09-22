"""Tests for :mod:`lhp.telemetry._spool`.

Every case runs against a throwaway config directory under ``tmp_path``. The
spool is inert: a failure leaves a DEBUG record and a falsy return, never an
exception, and no call leaves a file handle open behind it.
"""

import json
import logging
import os
import stat
import time
import uuid
from collections import Counter
from pathlib import Path
from typing import List

import pytest

from lhp.telemetry._paths import spool_path
from lhp.telemetry._spool import (
    MAX_LINE_BYTES,
    MAX_SPOOL_BYTES,
    MAX_SPOOL_LINES,
    STALE_INFLIGHT_S,
    append_spool,
    discard_inflight,
    read_lines,
    restore_inflight,
    spool_count,
    take_inflight,
    unmarked_lines,
)
from lhp.telemetry._store import StateFile, read_state, write_state

POSIX_ONLY = pytest.mark.skipif(os.name == "nt", reason="POSIX file modes")


@pytest.fixture
def cfg(tmp_path: Path) -> Path:
    return tmp_path / "cfg"


@pytest.fixture
def unwritable_cfg(tmp_path: Path) -> Path:
    """A config-dir path that is a regular file, so it can never be created."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory")
    return blocker / "cfg"


def _marked(line: str) -> str:
    """``line`` as the spool stores it after a send that went unanswered."""
    return line[:-1] + ',"_unconfirmed":true}'


def _age_claim(inflight: Path, seconds: float) -> Path:
    """Rename a claimed batch as if it had been claimed ``seconds`` ago."""
    stamp = int((time.time() - seconds) * 1000)
    return inflight.rename(
        inflight.with_name(f"spool.inflight-{os.getpid()}-{stamp}.jsonl")
    )


class _Clock:
    """A hand-cranked stand-in for the ``time`` module as ``_spool`` uses it."""

    def __init__(self, now: float) -> None:
        self.now = now

    def time(self) -> float:
        return self.now

    def time_ns(self) -> int:
        return int(self.now * 1_000_000_000)


def _next_fd(tmp_path: Path) -> int:
    """The lowest free descriptor number: a leaked handle makes it rise."""
    probe = tmp_path / "fd-probe"
    probe.touch()
    fd = os.open(probe, os.O_RDONLY)
    os.close(fd)
    return fd


# spool


@pytest.mark.unit
def test_append_spool_creates_the_spool_lazily(cfg: Path) -> None:
    assert append_spool(cfg, '{"event":"a"}') is True
    assert read_lines(spool_path(cfg)) == ['{"event":"a"}']
    assert spool_count(cfg) == 1


@pytest.mark.unit
def test_append_spool_appends_in_order(cfg: Path) -> None:
    for n in range(3):
        append_spool(cfg, f'{{"n":{n}}}')
    assert read_lines(spool_path(cfg)) == ['{"n":0}', '{"n":1}', '{"n":2}']


@pytest.mark.unit
def test_append_spool_drops_an_oversized_line(
    cfg: Path, caplog: pytest.LogCaptureFixture
) -> None:
    oversized = "x" * (MAX_LINE_BYTES + 1)
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        assert append_spool(cfg, oversized) is False
    assert spool_count(cfg) == 0
    assert oversized not in caplog.text


@pytest.mark.unit
def test_append_spool_keeps_only_the_newest_lines(cfg: Path) -> None:
    for n in range(MAX_SPOOL_LINES + 5):
        append_spool(cfg, f'{{"n":{n}}}')
    lines = read_lines(spool_path(cfg))
    assert len(lines) == MAX_SPOOL_LINES
    assert lines[0] == '{"n":5}'
    assert lines[-1] == f'{{"n":{MAX_SPOOL_LINES + 4}}}'


@pytest.mark.unit
def test_append_spool_keeps_the_spool_under_the_byte_cap(cfg: Path) -> None:
    line = "y" * (MAX_LINE_BYTES - 10)
    for _ in range(MAX_SPOOL_BYTES // len(line) + 3):
        append_spool(cfg, line)
    assert spool_path(cfg).stat().st_size <= MAX_SPOOL_BYTES
    assert spool_count(cfg) == MAX_SPOOL_BYTES // (len(line) + 1)


@POSIX_ONLY
@pytest.mark.unit
def test_spool_file_is_private(cfg: Path) -> None:
    append_spool(cfg, "{}")
    assert stat.S_IMODE(spool_path(cfg).stat().st_mode) == 0o600


@pytest.mark.unit
def test_append_spool_is_inert_when_unwritable(
    unwritable_cfg: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        assert append_spool(unwritable_cfg, "{}") is False
    assert caplog.records and all(r.levelno == logging.DEBUG for r in caplog.records)


@pytest.mark.unit
def test_spool_count_and_read_lines_are_zero_for_a_missing_spool(cfg: Path) -> None:
    assert spool_count(cfg) == 0
    assert read_lines(spool_path(cfg)) == []


@pytest.mark.unit
def test_read_lines_skips_blank_lines(cfg: Path) -> None:
    spool_path(cfg).parent.mkdir(parents=True)
    spool_path(cfg).write_text('{"a":1}\n\n{"b":2}\n', "utf-8")
    assert read_lines(spool_path(cfg)) == ['{"a":1}', '{"b":2}']


# inflight hand-off


@pytest.mark.unit
def test_take_inflight_is_none_when_there_is_nothing_to_send(cfg: Path) -> None:
    assert take_inflight(cfg) is None
    assert not cfg.exists()


@pytest.mark.unit
def test_take_inflight_moves_the_spool_aside(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    inflight = take_inflight(cfg)
    assert inflight is not None
    assert inflight.parent == spool_path(cfg).parent
    assert inflight.name.startswith(f"spool.inflight-{os.getpid()}-")
    assert inflight.suffix == ".jsonl"
    assert read_lines(inflight) == ['{"n":1}']
    assert not spool_path(cfg).exists()
    assert spool_count(cfg) == 0


@pytest.mark.unit
def test_take_inflight_leaves_a_fresh_inflight_file_alone(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    first = take_inflight(cfg)
    append_spool(cfg, '{"n":2}')
    second = take_inflight(cfg)
    assert first is not None and second is not None and first != second
    assert read_lines(first) == ['{"n":1}']
    assert read_lines(second) == ['{"n":2}']


@pytest.mark.unit
def test_take_inflight_merges_a_stale_inflight_file_back_first(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    stale = take_inflight(cfg)
    assert stale is not None
    _age_claim(stale, 120)
    append_spool(cfg, '{"n":2}')
    merged = take_inflight(cfg)
    assert merged is not None
    assert list(spool_path(cfg).parent.glob("spool.inflight-*")) == [merged]
    assert read_lines(merged) == [_marked('{"n":1}'), '{"n":2}']


@pytest.mark.unit
def test_a_stale_inflight_file_alone_is_enough_to_send(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    stale = take_inflight(cfg)
    assert stale is not None
    _age_claim(stale, 120)
    merged = take_inflight(cfg)
    assert merged is not None and read_lines(merged) == [_marked('{"n":1}')]


@pytest.mark.unit
def test_a_live_claim_with_an_old_mtime_is_not_stolen(cfg: Path) -> None:
    # ``os.replace`` keeps the spool's mtime, so it dates the last append,
    # not the claim.
    append_spool(cfg, '{"n":1}')
    live = take_inflight(cfg)
    assert live is not None
    old = time.time() - 120
    os.utime(live, (old, old))
    append_spool(cfg, '{"n":2}')
    second = take_inflight(cfg)
    assert second is not None
    assert read_lines(live) == ['{"n":1}']
    assert read_lines(second) == ['{"n":2}']


@pytest.mark.unit
def test_an_inflight_file_with_an_unparseable_name_is_stale(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    claimed = take_inflight(cfg)
    assert claimed is not None
    odd = claimed.rename(claimed.with_name("spool.inflight-unparseable.jsonl"))
    merged = take_inflight(cfg)
    assert merged is not None and not odd.exists()
    assert read_lines(merged) == [_marked('{"n":1}')]


@pytest.mark.unit
def test_orphaned_sends_claim_each_event_at_most_twice(
    cfg: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    clock = _Clock(time.time())
    monkeypatch.setattr("lhp.telemetry._spool.time", clock)
    claims: List[List[str]] = []
    for _ in range(10):
        envelope = {"event_id": str(uuid.uuid4()), "event": "cli.command"}
        append_spool(cfg, json.dumps(envelope, separators=(",", ":")))
        inflight = take_inflight(cfg)
        assert inflight is not None
        lines = unmarked_lines(inflight)
        claims.append([json.loads(line)["event_id"] for line in lines])
        clock.now += STALE_INFLIGHT_S + 1
    claimed = Counter(event_id for claim in claims for event_id in claim)
    assert len(claimed) == 10
    assert max(claimed.values()) <= 2
    assert max(len(claim) for claim in claims) <= 2


@pytest.mark.unit
def test_restore_inflight_puts_the_batch_back_in_front(cfg: Path) -> None:
    append_spool(cfg, '{"n":1}')
    inflight = take_inflight(cfg)
    assert inflight is not None
    append_spool(cfg, '{"n":2}')
    restore_inflight(cfg, inflight)
    assert not inflight.exists()
    assert read_lines(spool_path(cfg)) == ['{"n":1}', '{"n":2}']


@pytest.mark.unit
def test_restore_inflight_enforces_the_line_cap(cfg: Path) -> None:
    for n in range(MAX_SPOOL_LINES):
        append_spool(cfg, f'{{"n":{n}}}')
    inflight = take_inflight(cfg)
    assert inflight is not None
    for n in range(3):
        append_spool(cfg, f'{{"new":{n}}}')
    restore_inflight(cfg, inflight)
    lines = read_lines(spool_path(cfg))
    assert len(lines) == MAX_SPOOL_LINES
    assert lines[0] == '{"n":3}'
    assert lines[-3:] == ['{"new":0}', '{"new":1}', '{"new":2}']


@pytest.mark.unit
def test_an_unconfirmed_restore_marks_drops_marked_lines_and_goes_first(
    cfg: Path,
) -> None:
    append_spool(cfg, '{"n":1}')
    append_spool(cfg, _marked('{"n":2}'))
    inflight = take_inflight(cfg)
    assert inflight is not None
    append_spool(cfg, '{"n":3}')
    restore_inflight(cfg, inflight, unconfirmed=True)
    assert not inflight.exists()
    assert read_lines(spool_path(cfg)) == [_marked('{"n":1}'), '{"n":3}']


@pytest.mark.unit
def test_a_definitive_restore_keeps_marks_byte_for_byte(cfg: Path) -> None:
    append_spool(cfg, _marked('{"n":1}'))
    append_spool(cfg, '{"n":2}')
    inflight = take_inflight(cfg)
    assert inflight is not None
    batch = inflight.read_bytes()
    append_spool(cfg, '{"n":3}')
    current = spool_path(cfg).read_bytes()
    restore_inflight(cfg, inflight)
    assert spool_path(cfg).read_bytes() == batch + current


@pytest.mark.unit
def test_a_non_ascii_envelope_strips_back_to_its_exact_bytes(cfg: Path) -> None:
    props = {"command": "g\u00e9n\u00e9rer \u2713 \u65e5\u672c"}
    envelope = json.dumps({"props": props}, ensure_ascii=False, separators=(",", ":"))
    append_spool(cfg, envelope)
    inflight = take_inflight(cfg)
    assert inflight is not None
    restore_inflight(cfg, inflight, unconfirmed=True)
    assert read_lines(spool_path(cfg)) == [_marked(envelope)]
    (stripped,) = unmarked_lines(spool_path(cfg))
    assert stripped.encode("utf-8") == envelope.encode("utf-8")


@pytest.mark.unit
def test_discard_inflight_removes_the_file_and_tolerates_absence(cfg: Path) -> None:
    append_spool(cfg, "{}")
    inflight = take_inflight(cfg)
    assert inflight is not None
    discard_inflight(inflight)
    assert not inflight.exists()
    discard_inflight(inflight)


@pytest.mark.unit
def test_restore_inflight_of_a_missing_file_is_inert(
    cfg: Path, caplog: pytest.LogCaptureFixture
) -> None:
    with caplog.at_level(logging.DEBUG, logger="lhp.telemetry"):
        restore_inflight(cfg, spool_path(cfg).parent / "spool.inflight-0-0.jsonl")
    assert spool_count(cfg) == 0


# handles


@pytest.mark.unit
def test_no_handle_survives_any_store_call(cfg: Path, tmp_path: Path) -> None:
    baseline = _next_fd(tmp_path)
    write_state(cfg, StateFile(install_id="x"))
    read_state(cfg)
    for n in range(MAX_SPOOL_LINES + 2):
        append_spool(cfg, f'{{"n":{n}}}')
    inflight = take_inflight(cfg)
    assert inflight is not None
    read_lines(inflight)
    restore_inflight(cfg, inflight)
    spool_count(cfg)
    taken = take_inflight(cfg)
    assert taken is not None
    discard_inflight(taken)
    assert _next_fd(tmp_path) == baseline
