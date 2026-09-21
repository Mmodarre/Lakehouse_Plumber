"""Rendering for the telemetry surface: ``lhp telemetry`` output and the update hint.

Presenters format only primitives (§9.11) and MUST NOT import ``lhp.errors``
(sole-bridge invariant §5.2 / §9.5). The output is plain and unstyled whether
or not stdout is a terminal: it is pasted into bug reports and piped into
``jq``, so it must not vary with the terminal that rendered it.
"""

from __future__ import annotations

import json
from typing import Any, Dict, Mapping, Optional, Sequence

import click

DOCS_URL = "https://lakehouse-plumber.readthedocs.io/en/latest/reference/telemetry.html"

_OFF_NOTICE = "Telemetry is off; nothing would be sent."

# A consent reason names the layer that decided: one named after an environment
# variable reads as ``env <VARIABLE>``, and only these two need their own word.
_DECIDED_BY: Mapping[str, str] = {
    "user_state": "user state file",
    "server": "server (remote pause)",
}


def _decided_by(reason: str) -> str:
    """The wording for a consent ``reason``; ``env:X`` reads as ``env X``."""
    return _DECIDED_BY.get(reason, reason.replace("env:", "env ", 1))


def _compact(event: Mapping[str, Any]) -> str:
    return json.dumps(event, separators=(",", ":"))


def render_update_hint(latest: str, current: str) -> str:
    """The one-line hint naming ``latest`` over the installed ``current``.

    Plain text with no markup — the caller styles the whole line — and the
    ``LHP_UPDATE_CHECK=off`` tail is part of it so the reader can silence it.
    """
    return (
        f"lhp {latest} is available (installed {current}): "
        f"pip install -U lakehouse-plumber  [LHP_UPDATE_CHECK=off to silence]"
    )


def render_status(
    *,
    enabled: bool,
    reason: str,
    mode: str,
    config_dir: str,
    install_id: str,
    endpoint: str,
    spooled: int,
) -> None:
    """Print the status lines in their fixed order, one ``key: value`` each."""
    for key, value in (
        ("Telemetry", "on" if enabled else "off"),
        ("Decided by", _decided_by(reason)),
        ("Mode", mode),
        ("Config dir", config_dir),
        ("Install id", install_id),
        ("Endpoint", endpoint),
        ("Spooled events", str(spooled)),
        ("Docs", DOCS_URL),
    ):
        click.echo(f"{key}: {value}")


def render_events(
    preview: Optional[Dict[str, Any]], spooled: Sequence[Dict[str, Any]]
) -> None:
    """Print the preview envelope and ``spooled``, one compact JSON line each.

    The events go to stdout so the listing pipes into a JSON reader unchanged;
    the count closing it is commentary, so it goes to stderr.
    """
    click.echo(_OFF_NOTICE if preview is None else _compact(preview))
    for event in spooled:
        click.echo(_compact(event))
    shown = len(spooled)
    click.echo(
        f"{shown} spooled event(s) shown." if shown else "No spooled events.", err=True
    )


def render_on(state_path: str, still_off_because: Optional[str]) -> None:
    """Confirm telemetry is on; name the layer that still forces it off."""
    click.echo(f"✓ Telemetry on. State: {state_path}")
    if still_off_because is not None:
        layer = _decided_by(still_off_because)
        click.echo(f"⚠ Telemetry remains off: {layer}", err=True)


def render_off(state_path: str) -> None:
    """Confirm telemetry is off for this user on this machine."""
    click.echo(f"✓ Telemetry off for this user on this machine. State: {state_path}")
