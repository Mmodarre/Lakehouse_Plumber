"""Per-turn approval/interrupt plumbing for the Claude SDK assistant provider.

The turn engine (:mod:`~lhp.webapp.services.claude_sdk_chat`) runs the SDK
turn inside the chat stream's request, but approvals and interrupts arrive on
SEPARATE router requests (``POST /assistant/approval`` / ``/interrupt``).
:class:`ClaudeTurnRegistry` is the rendezvous between them, cached on
``app.state.claude_turns`` via :func:`get_claude_turns` (mirroring the lazy
``get_omnigent_client`` pattern).

Per turn the engine registers a :class:`TurnHandle`; the SDK's
``can_use_tool`` callback parks on an approval :class:`asyncio.Future` minted
by :meth:`ClaudeTurnRegistry.create_approval`, and the ``/approval`` endpoint
resolves it with the user's action (``accept`` / ``decline`` / ``cancel``).

Interrupt ordering matters: while a ``can_use_tool`` callback is awaiting an
approval, the SDK control loop cannot accept an interrupt request. So
:meth:`ClaudeTurnRegistry.request_interrupt` FIRST marks the turn interrupted
and resolves every pending approval Future with :data:`INTERRUPT_DENIED`
(unblocking the callback, which denies the tool), and only THEN awaits the
SDK ``interrupt()`` coroutine registered on the handle.

:stability: internal
"""

from __future__ import annotations

import asyncio
import logging
import uuid
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Optional

logger = logging.getLogger(__name__)

#: Resolution value for approval Futures unblocked by an interrupt request —
#: distinct from the user actions (``accept`` / ``decline`` / ``cancel``) so
#: the callback can deny without re-triggering the interrupt.
INTERRUPT_DENIED = "interrupt_denied"


@dataclass
class TurnHandle:
    """Live-turn state shared between the turn engine and the routers."""

    #: Awaits the SDK client's ``interrupt()``; registered by the engine once
    #: the client is connected, ``None`` before that.
    interrupt: Optional[Callable[[], Awaitable[None]]] = None
    #: Set (once, never cleared) when an interrupt was requested; the
    #: translator renders any subsequent terminal as ``interrupted``.
    interrupt_requested: bool = False
    #: Pending approval Futures keyed by ``elicitation_id`` (``elic_...``).
    approvals: dict[str, "asyncio.Future[str]"] = field(default_factory=dict)


class ClaudeTurnRegistry:
    """Registry of in-flight Claude SDK turns, keyed by LHP session id.

    The turn lock in :mod:`~lhp.webapp.services.claude_sdk_chat` admits one
    turn at a time, so at most one entry is live; the dict keying keeps the
    router lookups honest regardless.
    """

    def __init__(self) -> None:
        self._turns: dict[str, TurnHandle] = {}

    def begin_turn(self, session_id: str) -> TurnHandle:
        """Register (and return) the handle for a starting turn."""
        handle = TurnHandle()
        self._turns[session_id] = handle
        return handle

    def get(self, session_id: str) -> Optional[TurnHandle]:
        """Return the live handle for ``session_id``, or ``None``."""
        return self._turns.get(session_id)

    def create_approval(self, session_id: str) -> tuple[str, "asyncio.Future[str]"]:
        """Mint a pending approval for the live turn.

        Returns ``(elicitation_id, future)``; the future resolves to the
        user's action string or :data:`INTERRUPT_DENIED`.

        :raises KeyError: when no turn is live for ``session_id``.
        """
        handle = self._turns[session_id]
        elicitation_id = f"elic_{uuid.uuid4().hex}"
        future: asyncio.Future[str] = asyncio.get_running_loop().create_future()
        handle.approvals[elicitation_id] = future
        return elicitation_id, future

    def resolve_approval(
        self, session_id: str, elicitation_id: str, action: str
    ) -> bool:
        """Resolve a pending approval with the user's action.

        Returns ``False`` (the router 404s) when the session has no live turn
        or the elicitation is unknown / already resolved.
        """
        handle = self._turns.get(session_id)
        if handle is None:
            return False
        future = handle.approvals.pop(elicitation_id, None)
        if future is None or future.done():
            return False
        future.set_result(action)
        return True

    async def request_interrupt(self, session_id: str) -> bool:
        """Interrupt the live turn; return ``False`` when none is live.

        Marks the turn, unblocks any parked ``can_use_tool`` callback with
        :data:`INTERRUPT_DENIED`, then awaits the SDK interrupt (registered by
        the engine; a turn interrupted before the client connects is covered
        by the flag alone — the engine checks it before pumping).
        """
        handle = self._turns.get(session_id)
        if handle is None:
            return False
        handle.interrupt_requested = True
        for elicitation_id, future in list(handle.approvals.items()):
            if not future.done():
                future.set_result(INTERRUPT_DENIED)
            handle.approvals.pop(elicitation_id, None)
        if handle.interrupt is not None:
            await handle.interrupt()
        return True

    def end_turn(self, session_id: str) -> None:
        """Drop the turn's handle, cancelling any stray approval Futures."""
        handle = self._turns.pop(session_id, None)
        if handle is None:
            return
        for future in handle.approvals.values():
            if not future.done():
                future.cancel()
        handle.approvals.clear()


def get_claude_turns(app: Any) -> ClaudeTurnRegistry:
    """Return the app-scoped registry, creating it on first use.

    Mirrors the lazy ``get_omnigent_client`` construction seam: tests inject
    by writing ``app.state.claude_turns`` directly.
    """
    registry = getattr(app.state, "claude_turns", None)
    if registry is None:
        registry = ClaudeTurnRegistry()
        app.state.claude_turns = registry
    return registry
