"""``lhp telemetry`` command group — inspect or change the local preference.

Thin CLI shell (constitution §9.11): every fact comes from ``lhp.telemetry``'s
public surface and every line of output belongs to the presenter. This is the
one command that may surface a telemetry failure — a user who asked to change
the preference has to hear when the write did not take — so a refused write
becomes ``LHP-IO-028`` here. ``off`` records nothing for its own invocation:
the boundary resolves consent after the body ran, and by then it says no.
"""

from __future__ import annotations

import os
from dataclasses import asdict
from pathlib import Path

import click
from rich_click import RichGroup

from lhp import telemetry as client
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.presenters import telemetry_presenter
from lhp.errors import ErrorFactory, codes


def _set_enabled(enabled: bool) -> Path:
    """Store the preference, reporting a refused write as ``LHP-IO-028``."""
    try:
        return client.set_user_enabled(enabled)
    except OSError as e:
        path = client.state_path(client.config_dir(os.environ))
        raise ErrorFactory.io_error(
            codes.IO_028,
            title="Telemetry preference could not be saved",
            details=f"Writing {path} failed: {e.strerror or e}.",
            suggestions=["Set LHP_CONFIG_DIR to a writable directory and re-run"],
        ) from e


@click.group(name="telemetry", cls=RichGroup)
def telemetry() -> None:
    """Inspect or change what LHP reports about its own usage."""


@telemetry.command(name="status")
@cli_error_boundary("telemetry status")
def status() -> None:
    """Show whether telemetry is on, what decided that, and where it is stored."""
    state = client.effective_state(os.environ)
    in_ci = client.ci_vendor(os.environ) != "none"
    telemetry_presenter.render_status(
        enabled=state.enabled,
        reason=state.reason,
        mode=state.mode,
        config_dir=str(client.config_dir(os.environ)),
        install_id="none (CI)" if in_ci else (client.install_id(os.environ) or "none"),
        endpoint=client.resolve_endpoint(os.environ),
        spooled=client.spool_count(os.environ),
    )


@telemetry.command(name="show")
@click.option("--last", default=10, show_default=True, help="Spooled events to list.")
@cli_error_boundary("telemetry show")
def show(last: int) -> None:
    """Print the event this run would send, then the newest spooled events.

    The preview's duration and exit code are zero: the run is still in flight,
    and it shows what would leave the machine, not this run's measurements.
    """
    obj = click.get_current_context().obj
    root = obj.get("project_root") if isinstance(obj, dict) else None
    props = client.CliCommandProps(
        command="telemetry.show", flags=(), env_class=None, duration_ms=0, exit_code=0
    )
    envelope = client.new_event(
        "cli.command", project_root=root, props=asdict(props), environ=os.environ
    )
    spooled = client.spooled_events(last, os.environ)
    telemetry_presenter.render_events(
        None if envelope is None else client.to_json_dict(envelope),
        [event for event in reversed(spooled) if isinstance(event, dict)],
    )


@telemetry.command(name="on")
@cli_error_boundary("telemetry on")
def on() -> None:
    """Turn telemetry on for this user on this machine."""
    path = _set_enabled(True)
    state = client.effective_state(os.environ)
    telemetry_presenter.render_on(str(path), None if state.enabled else state.reason)


@telemetry.command(name="off")
@cli_error_boundary("telemetry off")
def off() -> None:
    """Turn telemetry off for this user on this machine."""
    telemetry_presenter.render_off(str(_set_enabled(False)))
