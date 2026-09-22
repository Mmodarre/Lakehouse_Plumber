"""Per-invocation telemetry seam: one ``cli.command`` event per command run.

Free functions only and no module state, on the ``_app_context.py``
standard: everything a run accumulates lives in ``ctx.obj["telemetry"]``,
the plain dict every command context shares. ``begin`` opens the run,
``note_alias`` renames it for a deprecated alias that forwards to a command,
``note_run`` lets a command add what only it knows, ``finish`` records and
flushes, and ``classify_exit`` maps how a command ended to the recorded exit
facts. The Click context is the only state carrier, so a call made outside
one has nothing to record and does nothing.

``lhp.telemetry`` and ``lhp.api._telemetry_shape`` are imported inside the
functions that need them: ``error_boundary`` imports this module for every
command, and the startup floor (``lhp --version``) must not pay for them.

Nothing here raises into a command: every hook turns its failures into a
DEBUG record, and the exit code is decided before ``finish`` runs. A code
that is not an LHP error code never reaches the wire: as ``error_code`` it is
sent as null, and as a counter key it is counted under ``other``.
"""

from __future__ import annotations

import logging
import os
from collections import Counter
from dataclasses import asdict
from pathlib import Path
from time import perf_counter
from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Tuple

import click
from click.core import ParameterSource

from lhp.cli import console as _console_module
from lhp.cli._version import get_version
from lhp.cli.exit_codes import ExitCode
from lhp.cli.presenters.telemetry_presenter import render_update_hint
from lhp.errors import LHPError, codes
from lhp.utils.env_flags import env_truthy

if TYPE_CHECKING:
    from lhp.api.facade import LakehousePlumberApplicationFacade
    from lhp.cli.presenters.event_stream._model import RunOutcome

logger = logging.getLogger(__name__)

RunState = Dict[str, Any]
ExitFacts = Tuple[int, Optional[str], Optional[str]]

_KEYBOARD_INTERRUPT_EXIT = 130
_OTHER_CODE = "other"
# The bound on exit latency: how long a command waits for an in-flight send.
_FLUSH_JOIN_S = 1.0
_PROJECT_MARKER = "lhp.yaml"


def _run_state(ctx: Optional[click.Context]) -> Optional[RunState]:
    """The run's telemetry dict, created on first use; ``None`` outside a context.

    ``ctx.obj`` is created when a command runs without the group; an ``obj``
    that is not a dict belongs to someone else and is left alone. The dict is
    mutated in place, never rebound, so a marker set before ``ctx.forward``
    — the ``deps`` alias — survives.
    """
    if ctx is None:
        return None
    if ctx.obj is None:
        ctx.ensure_object(dict)
    if not isinstance(ctx.obj, dict):
        return None
    run: RunState = ctx.obj.setdefault("telemetry", {})
    return run


def _project_root(ctx: click.Context) -> Optional[Path]:
    """The root the group discovered, else the cwd when it holds ``lhp.yaml``."""
    root = ctx.obj.get("project_root") if isinstance(ctx.obj, dict) else None
    if isinstance(root, Path):
        return root
    cwd = Path.cwd()
    return cwd if (cwd / _PROJECT_MARKER).is_file() else None


def _flags(ctx: click.Context) -> Tuple[str, ...]:
    """Parameter NAMES given on the command line, over the whole context chain.

    ``ctx.forward`` runs its target in a child context that parsed nothing, so
    the ``deps`` alias's options are found on the parent. Values are never
    read: that a flag was given is telemetry, what it was set to is not.
    """
    names: set[str] = set()
    node: Optional[click.Context] = ctx
    while node is not None:
        names.update(
            name
            for name in node.params
            if node.get_parameter_source(name) is ParameterSource.COMMANDLINE
        )
        node = node.parent
    return tuple(sorted(names))


def _code_counts(lines: Iterable[Any]) -> Dict[str, int]:
    """Count ``lines`` by code; the keys become wire keys, so a blank, missing
    or free-form code is counted under ``other`` instead."""
    from lhp.telemetry import is_lhp_code

    return dict(
        Counter(line.code if is_lhp_code(line.code) else _OTHER_CODE for line in lines)
    )


def _show_update_hint() -> None:
    """Print the one dim hint line when a newer release is due to be mentioned."""
    from lhp import telemetry

    latest = telemetry.due_update_hint()
    if latest is None:
        return
    line = render_update_hint(latest, get_version())
    _console_module.err_console.print(line, style="dim", markup=False, highlight=False)
    telemetry.mark_update_hint_shown()


def begin(operation: str) -> None:
    """Open the run: remember the boundary's operation and start the clock."""
    try:
        run = _run_state(click.get_current_context(silent=True))
        if run is not None:
            run["operation"] = operation
            run["started"] = perf_counter()
            run["recorded"] = False
    except Exception:  # telemetry must never affect the command
        logger.debug("Could not open the run for telemetry", exc_info=True)


def note_alias(alias: str) -> None:
    """Record the run under ``alias``, the deprecated name the user typed.

    Call it before ``ctx.forward``: the forwarded command's context shares the
    same ``obj``, so its ``finish`` finds the alias. Never raises.
    """
    try:
        run = _run_state(click.get_current_context(silent=True))
        if run is not None:
            run["alias"] = alias
    except Exception:  # telemetry must never affect the command
        logger.debug("Could not note the alias for telemetry", exc_info=True)


def note_run(
    facade: "LakehousePlumberApplicationFacade",
    outcome: Optional["RunOutcome"],
    *,
    bundle_enabled: Optional[bool],
    no_cache: bool,
) -> None:
    """Add what only the command knows: the project shape and the run's counters.

    Called once the facade exists and the run has ended, before the command
    exits — including a run its stream aborted, whose outcome ``render``
    passes through ``on_abort``. Nothing is read when consent is off, so an
    opted-out run pays for no facade reads at all. An aborted outcome with
    neither a terminal response nor a failure may come from a failed
    discovery, which leaves ``compute_stats`` unmemoised: its shape would
    re-run discovery in one read the shape's budget cannot cut short, so it
    is recorded as ``None``. Never raises.
    """
    try:
        ctx = click.get_current_context(silent=True)
        run = _run_state(ctx)
        if ctx is None or run is None:
            return
        from lhp import telemetry

        if not telemetry.effective_state().enabled:
            return
        from lhp.api._telemetry_shape import build_project_shape

        root = _project_root(ctx)
        discovered = (
            outcome is None or outcome.response is not None or bool(outcome.failures)
        )
        run["project"] = (
            build_project_shape(facade, root)
            if root is not None and discovered
            else None
        )
        run["warning_codes"] = _code_counts(outcome.warnings) if outcome else {}
        run["failure_codes"] = _code_counts(outcome.failures) if outcome else {}
        written = (
            getattr(outcome.response, "total_files_written", None) if outcome else None
        )
        run["files_written"] = written if isinstance(written, int) else None
        run["bundle_enabled"] = bundle_enabled
        run["cache_used"] = not (no_cache or env_truthy(os.environ, "LHP_NO_CACHE"))
    except Exception:  # telemetry must never affect the command
        logger.debug("Could not note the run for telemetry", exc_info=True)


def finish(
    *, exit_code: int, error_code: Optional[str], exception_class: Optional[str]
) -> None:
    """Record the run's ``cli.command`` event, show a due update hint, flush.

    Idempotent per run — the boundary reaches it on exactly one path, but a
    second call records nothing — and never raises: the exit code is already
    decided and nothing here may change it. An opted-out run returns after one
    consent check, reading no identity. The flush joins the sender for at
    most ``_FLUSH_JOIN_S``, which is the whole latency telemetry may add.
    """
    try:
        ctx = click.get_current_context(silent=True)
        run = _run_state(ctx)
        if ctx is None or run is None or run.get("recorded") or "operation" not in run:
            return
        run["recorded"] = True
        from lhp import telemetry

        if not telemetry.effective_state().enabled:
            return
        root = _project_root(ctx)
        shape = run.get("project")
        props = telemetry.CliCommandProps(
            command=run.get("alias") or run["operation"].replace(" ", "."),
            flags=_flags(ctx),
            env_class=(
                telemetry.env_class(root, ctx.params["env"])
                if "env" in ctx.params
                else None
            ),
            duration_ms=int((perf_counter() - run["started"]) * 1000),
            exit_code=int(exit_code),
            error_code=error_code if telemetry.is_lhp_code(error_code) else None,
            exception_class=exception_class,
            warning_codes=run.get("warning_codes", {}),
            failure_codes=run.get("failure_codes", {}),
            files_written=run.get("files_written"),
            bundle_enabled=run.get("bundle_enabled"),
            cache_used=run.get("cache_used"),
            project=None if shape is None else telemetry.ProjectShape(**shape),
        )
        telemetry.record("cli.command", project_root=root, props=asdict(props))
        if exit_code == ExitCode.SUCCESS:
            _show_update_hint()
        telemetry.flush(join_s=_FLUSH_JOIN_S)
    except Exception:  # telemetry must never affect the command
        logger.debug("Could not record the run for telemetry", exc_info=True)


def classify_exit(exc: Optional[BaseException]) -> ExitFacts:
    """Map how a command ended to ``(exit_code, error_code, exception_class)``.

    Pure. The codes mirror the boundary's own mapping: a ``SystemExit`` carries
    its code (``None`` is clean, a message is an error), a usage error is 2,
    an ``LHPError`` is 1 with its code, an interrupt is 130, and anything else
    is the internal-error exit under the unexpected-error code.
    """
    if exc is None:
        return int(ExitCode.SUCCESS), None, None
    name = type(exc).__name__
    if isinstance(exc, SystemExit):
        code = exc.code
        if code is None:
            return int(ExitCode.SUCCESS), None, None
        return (int(code) if isinstance(code, int) else int(ExitCode.ERROR)), None, None
    if isinstance(exc, KeyboardInterrupt):
        return _KEYBOARD_INTERRUPT_EXIT, None, name
    if isinstance(exc, click.UsageError):
        return int(ExitCode.USAGE_ERROR), None, name
    if isinstance(exc, (click.ClickException, click.exceptions.Exit)):
        return int(exc.exit_code), None, name
    if isinstance(exc, LHPError):
        return int(ExitCode.ERROR), exc.code, name
    return int(ExitCode.INTERNAL_ERROR), codes.GEN_902.code, name
