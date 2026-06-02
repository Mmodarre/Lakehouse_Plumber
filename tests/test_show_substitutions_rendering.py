"""Rendering tests for ``lhp substitutions`` (``ShowCommand.show_substitutions``).

Closes a green-lie gap: no test covered the substitution panels. The command
was rewired to build its data via
``application_facade.inspection.build_substitution_view(env)`` and to split
``view.raw_mappings.items()`` into reserved / maps / simple buckets — rendered
as a Rich ``Tree`` on a TTY and dotted-flattened on a non-TTY sink. These tests
drive that rewired path end to end against a real on-disk project.

Why a NEW file (not an extension of ``tests/test_show_rendering.py``): that
module's ``_render`` harness invokes ``_display_flowgroup_configuration``
directly — a method that takes no Click context and only needs
``lhp.cli.console.console`` swapped. ``show_substitutions`` is materially
different to invoke: it calls ``setup_from_context()`` (needs an active Click
context), ``ensure_project_root()`` (needs ``_project_root``), and its TTY
branch renders the Simple/Reserved tables via ``lhp.cli.render.render_listing_table``,
which resolves its sink from ``lhp.cli.render._default_console`` — a name bound
at import time, so swapping ``lhp.cli.console.console`` alone does NOT redirect
those tables. The adapted harness below is modeled on ``_render`` but pushes a
Click context, sets ``_project_root``, patches BOTH console handles, and takes
a ``force_terminal`` flag so the TTY (Tree) and non-TTY (flatten) branches can
each be exercised.
"""

from __future__ import annotations

import re
import textwrap
from io import StringIO
from pathlib import Path

import click
import pytest
from rich.console import Console as RichConsole

import lhp.cli.console as _console_module
import lhp.cli.render as _render_module
from lhp.cli.commands.show_command import ShowCommand

ENV = "test"

# Token shapes the assertions depend on. The nested-map token lands in the
# "maps" bucket; the scalar lands in "simple"; the manager auto-injects the
# reserved ``workspace_env`` / ``logical_env`` tokens. Values are kept free of
# ``${token}`` syntax so expansion behaviour does not perturb the rendered text.
NESTED_KEY = "catalog_map"
NESTED_VALUE = {"prod": "acme_prod", "dev": "acme_dev"}
SCALAR_KEY = "catalog"
SCALAR_VALUE = "acme_edw_test"

_ANSI = re.compile(r"\x1b\[[0-9;]*m")


def _strip_ansi(text: str) -> str:
    """Drop SGR escape sequences so TTY assertions match visible glyphs.

    ``force_terminal=True`` keeps Rich's bold/dim style codes (and box-drawing)
    even under ``no_color=True``; stripping them leaves the literal tree/table
    text the code emits.
    """
    return _ANSI.sub("", text)


def _write_minimal_project(root: Path) -> None:
    """Write the smallest project ``for_project`` will accept for this path.

    ``show_substitutions`` reads only ``substitutions/<env>.yaml`` through the
    facade, so no ``pipelines/`` / ``templates/`` / ``presets/`` content is
    needed. The nested-map value uses block style so YAML parses it as a real
    ``dict`` (which is what routes it into the "maps" bucket).
    """
    (root / "lhp.yaml").write_text(
        textwrap.dedent("""\
            name: substitution_render_test_project
            version: "1.0"
            """)
    )
    subs_dir = root / "substitutions"
    subs_dir.mkdir(parents=True, exist_ok=True)
    (subs_dir / f"{ENV}.yaml").write_text(
        textwrap.dedent(f"""\
            {ENV}:
              {SCALAR_KEY}: {SCALAR_VALUE}
              {NESTED_KEY}:
                prod: {NESTED_VALUE["prod"]}
                dev: {NESTED_VALUE["dev"]}
            """)
    )


def _render_substitutions(project_root: Path, *, force_terminal: bool) -> str:
    """Invoke ``show_substitutions`` and capture its rendered output.

    Adapted from ``tests/test_show_rendering.py::_render``. The command is
    instantiated without ``__init__`` (the method uses only ``_project_root``
    plus the verbose/log/perf flags ``setup_from_context`` sets), and is driven
    inside an active Click ``Context`` so ``setup_from_context()`` resolves.
    Both ``lhp.cli.console.console`` (header + Maps Tree) and
    ``lhp.cli.render._default_console`` (Simple/Reserved tables) are pointed at
    one in-memory ``Console`` so the full panel set lands in a single buffer.
    """
    cmd = ShowCommand.__new__(ShowCommand)
    cmd.verbose = False
    cmd.log_file = None
    cmd.perf = False
    cmd._project_root = project_root

    buf = StringIO()
    fake = RichConsole(file=buf, force_terminal=force_terminal, no_color=True, width=80)
    old_console = _console_module.console
    old_default = _render_module._default_console
    try:
        _console_module.console = fake
        _render_module._default_console = fake
        with click.Context(click.Command("substitutions"), obj={}):
            cmd.show_substitutions(ENV)
    finally:
        _console_module.console = old_console
        _render_module._default_console = old_default
    return buf.getvalue()


@pytest.mark.unit
def test_show_substitutions_tty_renders_nested_map_as_tree(tmp_path: Path) -> None:
    """TTY: the nested-map token renders as a Rich ``Tree`` (parent + leaves).

    Drives ``build_substitution_view`` -> ``raw_mappings`` -> maps bucket ->
    ``_render_maps_tty``. The tree branch must be taken, NOT the dotted-flatten
    branch, so the dotted keys are asserted ABSENT.
    """
    _write_minimal_project(tmp_path)

    out = _strip_ansi(_render_substitutions(tmp_path, force_terminal=True))

    # Maps section header + the map name as the Tree's parent node.
    assert "Maps" in out
    assert NESTED_KEY in out  # "catalog_map" parent node
    # Nested children rendered as Tree leaves with box-drawing connectors and
    # the legacy ``key: "value"`` quoting (see ShowCommand._add_tree_nodes).
    assert '├── prod: "acme_prod"' in out
    assert '└── dev: "acme_dev"' in out
    # The non-TTY dotted-flatten format must NOT appear in TTY mode.
    assert "catalog_map.prod" not in out
    assert "Maps: catalog_map" not in out

    # Simple bucket (Rich table) — scalar token + value present.
    assert "Simple Tokens" in out
    assert "${catalog}" in out
    assert "acme_edw_test" in out

    # Reserved bucket (Rich table) — manager-injected tokens present.
    assert "Reserved Tokens" in out
    assert "${workspace_env}" in out
    assert "${logical_env}" in out


@pytest.mark.unit
def test_show_substitutions_non_tty_flattens_nested_map(tmp_path: Path) -> None:
    """Non-TTY: the nested-map token renders dotted-flattened, not as a tree.

    Drives the same buckets through the plain ``Category: name = value`` branch
    (``_flatten_map``). The tree connector glyphs must be ABSENT.
    """
    _write_minimal_project(tmp_path)

    out = _render_substitutions(tmp_path, force_terminal=False)

    # Dotted-flattened map leaves (ShowCommand._flatten_map + the ``Maps:`` prefix).
    assert "Maps: catalog_map.prod = acme_prod" in out
    assert "Maps: catalog_map.dev = acme_dev" in out
    # Tree rendering must NOT appear in non-TTY mode.
    assert "├──" not in out
    assert "└──" not in out
    assert 'prod: "acme_prod"' not in out

    # Simple + reserved buckets as plain ``Category: ${name} = value`` lines.
    assert "Simple Tokens: ${catalog} = acme_edw_test" in out
    assert "Reserved Tokens: ${workspace_env} = test" in out
    assert "Reserved Tokens: ${logical_env} = test" in out

    # Header line.
    assert "Available Substitutions for Environment: test" in out
