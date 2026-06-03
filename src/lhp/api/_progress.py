"""Public-API progress observer for long-running facade operations.

Exposes :class:`ProgressSink`, a concrete mutable counter that the
generate / validate event streams advance as flowgroups complete. The
four public-API audiences (§1.1 — CLI, WebUI, VSCode, library callers)
can all read live progress off the same object: pass an instance into
:meth:`~lhp.api.GenerationFacade.generate_pipelines` /
:meth:`~lhp.api.ValidationFacade.validate_pipelines`, then read its
fields while the stream is being consumed.

Why a concrete class and NOT a ``typing.Protocol`` or ``abc.ABC``: there
is exactly ONE consumer today (the in-stream progress hooks the
coordinator added in the matching core change). §3.6 forbids an
abstraction without two concrete implementors, and §13.8 / §9.25 / §4.12
ban ``typing.Protocol`` outside sanctioned plugin boundaries (none here).
A bare counter holder is the right shape; the abstraction is deferred
until a second real implementor exists.

:stability: provisional
"""

from __future__ import annotations


class ProgressSink:
    """Mutable counter the generate / validate streams advance.

    Holds two INDEPENDENT scalar counters and nothing else:

    - ``total`` — the total flowgroup count for the run, set once via
      :meth:`on_total`.
    - ``done`` — flowgroups completed so far, bumped via
      :meth:`on_advance`.

    Construct one, hand it to a facade long-running operation via its
    ``progress=`` parameter, and read ``total`` / ``done`` to observe
    live progress (e.g. to drive a progress bar) while iterating the
    event stream.

    The two scalars carry NO cross-field invariant: a reader may observe
    them at any interleaving (a future presentation-thread refresh reads
    them while the coordinator writes them, one scalar per tick). Keeping
    them independent is what makes that lock-free read safe — do NOT add
    a derived field that must agree with both.

    :meth:`on_total` and :meth:`on_advance` are deliberately trivial (a
    bare assignment / increment) and MUST NOT raise: they run inside the
    coordinator loop, where an escaping exception would abort the run.

    :stability: provisional
    """

    def __init__(self) -> None:
        self.total: int = 0
        self.done: int = 0

    def on_total(self, n: int) -> None:
        """Record the total flowgroup count for the run.

        Called once, before any :meth:`on_advance`. A plain assignment —
        it never raises (see the class docstring).

        :stability: provisional
        """
        self.total = n

    def on_advance(self) -> None:
        """Mark one more flowgroup complete.

        Called once per finished flowgroup. A plain increment — it never
        raises (see the class docstring).

        :stability: provisional
        """
        self.done += 1
