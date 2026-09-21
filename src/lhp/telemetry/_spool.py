"""The JSONL spool: envelopes waiting to be sent, and the inflight hand-off.

One envelope per line, appended with ``O_APPEND`` in a single write so lines
from concurrent recorders never interleave. The caps (lines and bytes) are
enforced after every write by rewriting the file with only the newest lines.
A send claims the whole spool by renaming it to an inflight file; the sender
then discards or restores that file. Every handle is opened and closed inside
the call, and every failure is logged at DEBUG and swallowed.
"""

from __future__ import annotations

import logging
import os
import time
from pathlib import Path
from typing import List, Optional

from lhp.telemetry._paths import spool_path
from lhp.telemetry._store import write_private

logger = logging.getLogger(__name__)

MAX_LINE_BYTES = 8 * 1024
MAX_SPOOL_LINES = 500
MAX_SPOOL_BYTES = 512 * 1024
# An inflight file older than this was left by a sender that never finished.
STALE_INFLIGHT_S = 60.0

_INFLIGHT_GLOB = "spool.inflight-*.jsonl"
_DIR_MODE = 0o700
_FILE_MODE = 0o600


def read_lines(path: Path) -> List[str]:
    """The non-blank lines of a spool or inflight file; ``[]`` when missing."""
    try:
        text = path.read_text("utf-8")
    except FileNotFoundError:
        return []
    except (OSError, ValueError):  # an unreadable spool is treated as empty
        logger.debug("Could not read a telemetry spool file", exc_info=True)
        return []
    return [line for line in text.splitlines() if line.strip()]


def _trim(cfg: Path, path: Path) -> None:
    """Rewrite the spool keeping only the newest lines within both caps."""
    lines = path.read_bytes().splitlines(keepends=True)
    kept = lines[-MAX_SPOOL_LINES:]
    total = sum(len(line) for line in kept)
    while kept and total > MAX_SPOOL_BYTES:
        total -= len(kept.pop(0))
    if len(kept) != len(lines):
        write_private(cfg, path, b"".join(kept))


def append_spool(cfg: Path, line: str) -> bool:
    """Append one envelope line in a single write; ``False`` if oversized or failed."""
    data = f"{line}\n".encode("utf-8")
    if len(data) > MAX_LINE_BYTES + 1:
        logger.debug(f"Dropping a telemetry event larger than {MAX_LINE_BYTES} bytes")
        return False
    path = spool_path(cfg)
    try:
        cfg.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        path.parent.mkdir(parents=True, exist_ok=True, mode=_DIR_MODE)
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_APPEND, _FILE_MODE)
        try:
            os.write(fd, data)
        finally:
            os.close(fd)
        _trim(cfg, path)
    except OSError:  # the spool is best-effort; a lost event is acceptable
        logger.debug("Could not append to the telemetry spool", exc_info=True)
        return False
    return True


def spool_count(cfg: Path) -> int:
    """How many envelopes wait in the spool (inflight batches excluded)."""
    return len(read_lines(spool_path(cfg)))


def _inflight_name(spool: Path, stamp: int) -> Path:
    return spool.with_name(f"spool.inflight-{os.getpid()}-{stamp}.jsonl")


def take_inflight(cfg: Path) -> Optional[Path]:
    """Claim the spool for one send by renaming it; ``None`` when it is empty.

    The rename is atomic, so a concurrent append lands either in the claimed
    batch or in a fresh spool, never in both. Inflight files older than
    ``STALE_INFLIGHT_S`` were left by a sender that never finished and are
    merged back first so their events get another chance.
    """
    spool = spool_path(cfg)
    try:
        cutoff = time.time() - STALE_INFLIGHT_S
        for stale in spool.parent.glob(_INFLIGHT_GLOB):
            if stale.stat().st_mtime < cutoff:
                restore_inflight(cfg, stale)
        if not read_lines(spool):
            return None
        stamp = time.time_ns() // 1_000_000
        inflight = _inflight_name(spool, stamp)
        # Two claims in one millisecond must not clobber a batch still in flight.
        while inflight.exists():
            stamp += 1
            inflight = _inflight_name(spool, stamp)
        os.replace(spool, inflight)
    except OSError:  # nothing claimed; the spool waits for the next attempt
        logger.debug("Could not claim the telemetry spool", exc_info=True)
        return None
    return inflight


def restore_inflight(cfg: Path, inflight: Path) -> None:
    """Return a claimed batch to the spool and remove the file.

    The batch is older than anything spooled since, so it goes in FRONT: the
    caps then drop what is genuinely oldest.
    """
    spool = spool_path(cfg)
    try:
        batch = inflight.read_bytes() if inflight.is_file() else b""
        current = spool.read_bytes() if spool.is_file() else b""
        if batch:
            write_private(cfg, spool, batch + current)
            _trim(cfg, spool)
        inflight.unlink(missing_ok=True)
    except OSError:  # the batch stays in its inflight file for a later merge
        logger.debug("Could not restore a telemetry batch to the spool", exc_info=True)


def discard_inflight(inflight: Path) -> None:
    """Remove a claimed batch that was accepted, or rejected for good."""
    try:
        inflight.unlink(missing_ok=True)
    except OSError:  # a leftover file is merged back later, never fatal
        logger.debug("Could not remove a telemetry inflight file", exc_info=True)


def clear_spool(cfg: Path) -> None:
    """Remove the spool and every inflight file."""
    spool = spool_path(cfg)
    try:
        for path in [spool, *spool.parent.glob(_INFLIGHT_GLOB)]:
            path.unlink(missing_ok=True)
    except OSError:  # a spool that cannot be cleared is still bounded by its caps
        logger.debug("Could not clear the telemetry spool", exc_info=True)
