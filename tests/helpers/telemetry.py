"""Shared helpers for tests that observe what telemetry records.

In ``log`` mode the telemetry client prints one compact JSON envelope per
recorded event to stderr. The envelope helpers find that line, check that its
props are exactly the allowlisted keys, and gather the names a fixture project
contains so a test can prove none of them leaked into the line. The spool
helpers build the on-disk state a send leaves behind when it goes unanswered;
the unconfirmed mark's on-disk spelling is pinned here and nowhere else in tests.
"""

from __future__ import annotations

import json
import re
from dataclasses import fields
from pathlib import Path
from typing import Any, Dict, Iterable, List, Set

from lhp.telemetry import PROJECT_SHAPE_KEYS, CliCommandProps
from lhp.telemetry._spool import append_spool, restore_inflight, take_inflight

ENVELOPE_PREFIX = '{"schema_version"'
_NAME_LINE = re.compile(
    r"^(?:pipeline|flowgroup|name):\s*[\"']?([^\s\"']+)", re.MULTILINE
)


def last_cli_command_line(stderr: str) -> str:
    """The last envelope line on ``stderr``; fails when none was printed."""
    lines = [line for line in stderr.splitlines() if line.startswith(ENVELOPE_PREFIX)]
    assert lines, f"no telemetry envelope on stderr:\n{stderr}"
    return lines[-1]


def assert_allowlisted_cli_command(envelope: Dict[str, Any]) -> None:
    """The envelope is a ``cli.command`` whose props carry only allowlisted keys."""
    assert envelope["event"] == "cli.command"
    props = envelope["props"]
    assert set(props) == {f.name for f in fields(CliCommandProps)}
    if props["project"] is not None:
        assert set(props["project"]) == set(PROJECT_SHAPE_KEYS)


def project_names(root: Path) -> Set[str]:
    """Every project, pipeline and flowgroup name declared under ``root``."""
    names: Set[str] = set()
    for path in [root / "lhp.yaml", *sorted((root / "pipelines").rglob("*.yaml"))]:
        names.update(_NAME_LINE.findall(path.read_text("utf-8")))
    return names


def string_values(payload: Any) -> List[str]:
    """Every string VALUE in ``payload``, recursively; keys are not values."""
    if isinstance(payload, str):
        return [payload]
    if isinstance(payload, dict):
        return [s for value in payload.values() for s in string_values(value)]
    if isinstance(payload, (list, tuple)):
        return [s for value in payload for s in string_values(value)]
    return []


def assert_no_names_in_values(envelope: Dict[str, Any], names: Iterable[str]) -> None:
    """No project, pipeline or flowgroup name appears in any value.

    The keys are a fixed allowlist (checked separately) and may legitimately
    contain a name as a substring — ``load_custom_datasource`` against a
    flowgroup called ``custom_datasource`` — so only the values are scanned.
    """
    values = string_values(envelope)
    for name in names:
        leaked = [value for value in values if name in value]
        assert not leaked, f"{name!r} leaked into the envelope: {leaked}"


def parse_last_cli_command(stderr: str) -> Dict[str, Any]:
    """Parse the last envelope line and check its shape."""
    envelope = json.loads(last_cli_command_line(stderr))
    assert_allowlisted_cli_command(envelope)
    return envelope


def marked(line: str) -> str:
    """``line`` as the spool stores it after a send that went unanswered."""
    return line[:-1] + ',"_unconfirmed":true}'


def claim_unanswered(cfg: Path, *lines: str) -> None:
    """Leave ``lines`` claimed by a send, as the resend of an unanswered batch."""
    for line in lines:
        assert append_spool(cfg, line)
    unanswered = take_inflight(cfg)
    assert unanswered is not None
    restore_inflight(cfg, unanswered, unconfirmed=True)
    assert take_inflight(cfg) is not None
