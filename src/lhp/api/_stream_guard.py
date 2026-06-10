"""Event-buffer soft-cap guard for the public facade streams.

Underscore-prefixed module: not part of :mod:`lhp.api`'s public surface.
External callers MUST NOT import from here.

Holds the single shared generator wrapper that enforces the §13.4
locked event-buffer soft cap (1000 events; warn at 999) on the three
public facade streams (``generate_pipelines``, ``validate_pipelines``,
``plan_generation``). Each of those public methods forwards its inner
§5.7 stream through :func:`_cap_event_stream` so the cap is enforced
uniformly from one place.

:stability: internal
"""

from __future__ import annotations

from typing import Iterator

from lhp.api.events import LHPEvent, WarningEmitted

# §13.4 locked: long-running facade operations buffer up to 1000 events;
# exceeding the soft cap (the 999th event) emits the warning exactly once.
_SOFT_CAP: int = 999


def _cap_event_stream(events: Iterator[LHPEvent]) -> Iterator[LHPEvent]:
    """Pass an LHPEvent stream through, enforcing the §13.4 soft cap.

    Counts the events yielded by ``events`` and, when the count reaches
    the soft cap (``_SOFT_CAP`` = 999), emits a single
    ``WarningEmitted("event buffer near limit",
    code="LHP-EVT-SOFT-CAP")`` exactly once, then keeps forwarding the
    remaining events unchanged. Streams shorter than the cap emit no
    warning.

    The warning is injected IMMEDIATELY BEFORE the 999th inner event is
    re-yielded (not after), so the §5.7 ordering invariants hold on
    every stream length:

    * The soft-cap warning is never the last event — the inner event
      that tripped the cap (the 999th) always follows it. In particular,
      on a stream whose 999th event is its terminal
      ``OperationCompleted``, the warning lands BEFORE that terminal, so
      "exactly one terminal event is the last event" (§5.7) still holds.
    * It is a standalone, non-terminal ``WarningEmitted`` data event, so
      it touches neither the ``PhaseStarted``/``PhaseCompleted`` nor the
      ``PipelineStarted``/``PipelineCompleted``-or-``PipelineFailed``
      pairing — no dangling Starts are introduced.

    Only the wrapped stream's own events are counted; the injected
    warning is not counted toward the cap (so it can never re-trigger
    itself). The wrapper is fully lazy: it neither materialises the
    stream nor changes its terminal/raise behaviour — a raise from
    ``events`` (the §1.4 ``ErrorEmitted`` → ``raise`` rendezvous)
    propagates unchanged.

    :stability: internal
    """
    emitted = False
    count = 0
    for event in events:
        count += 1
        if count == _SOFT_CAP and not emitted:
            emitted = True
            yield WarningEmitted("event buffer near limit", code="LHP-EVT-SOFT-CAP")
        yield event
