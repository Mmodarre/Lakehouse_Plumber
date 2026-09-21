"""Decides whether an "a newer LHP is available" hint is due.

Pure decision only: the caller supplies the state it read, the current time
as an ISO string and the environment, and gets back the version to mention or
``None``. Rendering the line belongs to the CLI presenter, and persisting the
"shown at" stamp belongs to the store.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from typing import Any, Mapping, Optional

from packaging.version import InvalidVersion, Version

from lhp.utils.env_flags import env_value_in

logger = logging.getLogger(__name__)

# One hint per day at most, so a developer running many commands sees it once.
_HINT_INTERVAL = timedelta(hours=24)
_UPDATE_CHECK_OFF = ("off", "0", "false")


def is_newer(candidate: str, current: str) -> bool:
    """Report whether ``candidate`` is a later release than ``current``.

    PEP 440 ordering, so ``0.10.0`` beats ``0.9.9`` and a release beats its
    own release candidate. A version either side cannot parse is never an
    upgrade.
    """
    try:
        return Version(candidate) > Version(current)
    except (InvalidVersion, TypeError):  # an unreadable version proves nothing
        return False


def update_check_disabled(environ: Mapping[str, str]) -> bool:
    """Whether ``LHP_UPDATE_CHECK`` opts this environment out of the hint."""
    return env_value_in(environ, "LHP_UPDATE_CHECK", _UPDATE_CHECK_OFF)


def _hint_is_due_again(shown_at: Any, now_iso: str) -> bool:
    """Report whether enough time has passed since the last hint.

    A missing or unreadable stamp counts as "never shown": the hint is worth
    one extra appearance more than it is worth suppressing forever.
    """
    if not isinstance(shown_at, str):
        return True
    try:
        elapsed = datetime.fromisoformat(now_iso) - datetime.fromisoformat(shown_at)
    except (ValueError, TypeError):  # an unreadable stamp cannot suppress
        logger.debug("Could not compare update-hint timestamps", exc_info=True)
        return True
    return elapsed >= _HINT_INTERVAL


def pending_update_hint(
    state: Mapping[str, Any],
    *,
    current_version: str,
    now_iso: str,
    environ: Mapping[str, str],
    interactive: bool,
    in_ci: bool,
) -> Optional[str]:
    """Return the newer version worth mentioning, or ``None``.

    The hint is due only on an interactive, non-CI run that has not opted out
    through ``LHP_UPDATE_CHECK``, when the last known release is newer than
    the installed one and no hint has been shown in the past 24 hours. The
    check never performs a request of its own: ``latest_known_version`` is
    whatever a previous upload response left in the state file.
    """
    if not interactive or in_ci:
        return None
    if update_check_disabled(environ):
        return None

    latest = state.get("latest_known_version")
    if not isinstance(latest, str) or not is_newer(latest, current_version):
        return None
    if not _hint_is_due_again(state.get("update_hint_shown_at"), now_iso):
        return None
    return latest
