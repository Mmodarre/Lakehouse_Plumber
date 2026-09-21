"""Consent resolution: the ordered switches that decide whether an event may
be recorded, and whether it is sent or merely printed.

Strict any-off-wins: the layers are checked in a fixed order and the first
one that says "off" decides, carrying its own reason so ``lhp telemetry
status`` can name it. The state file sits after every environment switch
and is read lazily, so an environment that has already opted out never
touches the disk. Pure: nothing here performs I/O of its own.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import TYPE_CHECKING, Callable, Literal, Mapping, Optional

from lhp.utils.env_flags import env_truthy, env_value_in

if TYPE_CHECKING:
    from lhp.telemetry._store import StateFile

logger = logging.getLogger(__name__)

Mode = Literal["send", "log", "off"]
StateReader = Callable[[], Optional["StateFile"]]

_OFF_VALUES = ("off", "0", "false")
_LOG_VALUES = ("log",)
_TRUTHY_OFF_SWITCHES = ("DO_NOT_TRACK", "LHP_DISABLE_ANALYTICS")


@dataclass(frozen=True)
class TelemetryState:
    """The resolved consent: whether events are recorded, how, and why.

    ``mode == "log"`` reports ``enabled=True`` because events are still built
    and printed; only the network leg is replaced. ``reason`` names the layer
    that decided: ``default``, ``env:<VARIABLE>``, ``pytest``, ``user_state``
    or ``server``.
    """

    enabled: bool
    mode: Mode
    reason: str


def _off(reason: str) -> TelemetryState:
    return TelemetryState(enabled=False, mode="off", reason=reason)


def _environment_off_reason(environ: Mapping[str, str]) -> Optional[str]:
    """The first environment layer that opts out, or ``None``."""
    if env_value_in(environ, "LHP_TELEMETRY", _OFF_VALUES):
        return "env:LHP_TELEMETRY"
    for variable in _TRUTHY_OFF_SWITCHES:
        if env_truthy(environ, variable):
            return f"env:{variable}"
    # pytest exports this for every test phase: a suite that imports LHP must
    # never emit, whatever the developer's own preference is.
    if environ.get("PYTEST_CURRENT_TEST"):
        return "pytest"
    return None


def _server_disabled(until: Optional[str], now_iso: str) -> bool:
    """Whether the Worker's kill switch is still in force."""
    if not until:
        return False
    try:
        return datetime.fromisoformat(now_iso) < datetime.fromisoformat(until)
    except (ValueError, TypeError):  # an unreadable stamp cannot silence the client
        logger.debug(
            "Ignoring an unreadable server_disabled_until stamp", exc_info=True
        )
        return False


def resolve_consent(
    environ: Mapping[str, str], *, state: StateReader, now_iso: str
) -> TelemetryState:
    """Collapse the environment and the stored preference into one state.

    Order: ``LHP_TELEMETRY`` off values, ``DO_NOT_TRACK``,
    ``LHP_DISABLE_ANALYTICS``, ``PYTEST_CURRENT_TEST``, then the state file's
    ``enabled`` flag and ``server_disabled_until``, then ``LHP_TELEMETRY=log``.
    ``state`` is called at most once, and only after every environment layer
    has passed.
    """
    reason = _environment_off_reason(environ)
    if reason is not None:
        return _off(reason)
    stored = state()
    if stored is not None:
        if not stored.enabled:
            return _off("user_state")
        if _server_disabled(stored.server_disabled_until, now_iso):
            return _off("server")
    if env_value_in(environ, "LHP_TELEMETRY", _LOG_VALUES):
        return TelemetryState(enabled=True, mode="log", reason="env:LHP_TELEMETRY")
    return TelemetryState(enabled=True, mode="send", reason="default")
