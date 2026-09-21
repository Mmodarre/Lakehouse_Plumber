"""Rendering for the telemetry surface: the update hint line.

Presenters format only primitives and frozen ``lhp.api`` views; this one
turns two version strings into the single line the CLI prints when a newer
release is known. Deciding whether the hint is due is the telemetry client's
job, printing it is the hook's — neither shapes the text.

Sole-bridge invariant (constitution §5.2 / §9.5): this module MUST NOT import
``lhp.errors``.
"""

from __future__ import annotations


def render_update_hint(latest: str, current: str) -> str:
    """The one-line hint naming ``latest`` over the installed ``current``.

    Plain text with no markup: the caller styles the whole line. The
    ``LHP_UPDATE_CHECK=off`` tail is part of the line so a user who never reads
    the docs still learns how to silence it.
    """
    return (
        f"lhp {latest} is available (installed {current}): "
        f"pip install -U lakehouse-plumber  [LHP_UPDATE_CHECK=off to silence]"
    )
