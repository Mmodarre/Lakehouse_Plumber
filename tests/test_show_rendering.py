"""Snapshot test for ``lhp show`` flowgroup YAML+Syntax+Panel rendering.

A single syrupy snapshot pins the contract: the resolved flowgroup is
dumped as YAML and wrapped in a Rich ``Syntax`` block inside a
``Panel`` whose title carries ``pipeline.flowgroup   env: ENV``.
"""

from io import StringIO

from rich.console import Console as RichConsole

import lhp.cli.console as _lhp_console_module
from lhp.cli.commands.show_command import ShowCommand
from lhp.models import Action, ActionType, FlowGroup


def _render(processed_fg: FlowGroup, env: str) -> str:
    """Render the show command's flowgroup display, capturing to StringIO."""
    cmd = ShowCommand.__new__(ShowCommand)  # skip __init__ — method uses no state
    buf = StringIO()
    fake = RichConsole(file=buf, force_terminal=False, no_color=True, width=80)
    old = _lhp_console_module.console
    try:
        _lhp_console_module.console = fake
        cmd._display_flowgroup_configuration(processed_fg, env)
    finally:
        _lhp_console_module.console = old
    return buf.getvalue()


def test_show_flowgroup_renders_yaml_panel(snapshot):
    """Resolved flowgroup renders as syntax-highlighted YAML inside a Panel."""
    fg = FlowGroup(
        pipeline="bronze_pipeline",
        flowgroup="customer_bronze",
        presets=["bronze_defaults"],
        actions=[
            Action(
                name="load_customer",
                type=ActionType.LOAD,
                source={"type": "cloudfiles", "path": "/Volumes/raw/customer"},
                target="v_customer_raw",
            ),
            Action(
                name="write_customer",
                type=ActionType.WRITE,
                source="v_customer_raw",
                target="customer_bronze",
            ),
        ],
    )
    assert _render(fg, env="dev") == snapshot
