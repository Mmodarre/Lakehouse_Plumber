"""Frozen value objects shared by the event-stream renderers.

Plain presentation data only: no Rich types, no live exceptions, and —
per the sole-bridge invariant (constitution §9.5) — no ``lhp.errors``
imports. Error codes travel as duck-typed ``str`` fields. The renderers
(``LiveRenderer`` / ``LogRenderer``) produce a :class:`RunOutcome`; the
summary presenter consumes it.
"""

from dataclasses import dataclass
from typing import Optional, Tuple


@dataclass(frozen=True)
class RunHeader:
    """Identifies a single CLI run for the renderers and summary."""

    command: str
    env: str
    pipeline_count: int


@dataclass(frozen=True)
class RenderOptions:
    """Per-run rendering toggles selected from CLI flags."""

    show_details: bool = False
    strict: bool = False


@dataclass(frozen=True)
class WarningLine:
    """A non-fatal warning recorded from a :class:`WarningEmitted` event."""

    code: str
    message: str
    file: Optional[str]


@dataclass(frozen=True)
class FailureLine:
    """A per-pipeline failure for the summary's failure table.

    ``flowgroup`` and ``file`` enrich the plan contract for validate
    attribution (sec 6.6: pipeline / flowgroup / file / CODE / msg).
    """

    pipeline: str
    code: str
    message: str
    flowgroup: Optional[str] = None
    file: Optional[str] = None


@dataclass(frozen=True)
class RunOutcome:
    """Terminal result of draining one event stream.

    ``errored`` is ``True`` iff an ``ErrorEmitted`` was seen in-stream;
    the LHPError-raises path does not return a :class:`RunOutcome`
    (render re-raises instead).
    """

    response: object
    warnings: Tuple[WarningLine, ...]
    failures: Tuple[FailureLine, ...]
    errored: bool
