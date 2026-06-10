"""Presenter for the ``lhp substitutions`` command.

Renders a :class:`lhp.api.views.SubstitutionView` to a Rich console: a
scalar-token table, nested maps as Rich ``Tree`` instances, the observed
``${secret:scope/key}`` references, and the configured default secret scope.

Sole-bridge invariant (constitution §5.2 / §9.5): a presenter renders but
MUST NOT import ``lhp.errors``. Domain failures are raised by the command and
rendered by ``cli.error_boundary``; nothing in this module touches the error
taxonomy. No business logic lives here (§9.11) — the view arrives fully
resolved and this module only formats it.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Mapping

from rich.text import Text
from rich.tree import Tree

from ._layout import ColumnSpec, render_listing_table

if TYPE_CHECKING:
    from rich.console import Console

    from lhp.api.responses import JSONValue
    from lhp.api.views import SubstitutionView


def render(view: "SubstitutionView", *, console: "Console") -> None:
    """Render ``view`` to ``console`` (stdout).

    Output order: a header naming the environment, then the scalar tokens
    table, then any nested maps as trees, then the ``${secret:scope/key}``
    references, then the default secret scope. Sections with no content are
    skipped so a project with only flat tokens produces just the table.
    """
    console.print(
        Text.assemble(
            ("Substitutions for environment: ", "bold dim"),
            (view.env, ""),
        )
    )

    scalars, maps = _partition(view.raw_mappings)
    _render_tokens(scalars, console=console)
    _render_maps(maps, console=console)
    _render_secret_references(view, console=console)
    _render_default_scope(view, console=console)


def _partition(
    raw_mappings: "Mapping[str, JSONValue]",
) -> tuple[dict[str, "JSONValue"], dict[str, dict]]:
    """Split ``raw_mappings`` into scalar tokens and nested maps.

    A value that is a ``dict`` is a nested map rendered as a tree; everything
    else (strings, the already-flattened nested values) is a scalar token
    rendered in the table. ``list`` values are treated as scalars and shown
    via their ``str`` form, matching the flat ``tokens`` projection.
    """
    scalars: dict[str, "JSONValue"] = {}
    maps: dict[str, dict] = {}
    for key, value in raw_mappings.items():
        if isinstance(value, dict):
            maps[key] = value
        else:
            scalars[key] = value
    return scalars, maps


def _render_tokens(scalars: Mapping[str, "JSONValue"], *, console: "Console") -> None:
    """Render scalar tokens as a ``${name} = value`` table, sorted by name."""
    if not scalars:
        return
    rows = [(f"${{{key}}}", str(value)) for key, value in sorted(scalars.items())]
    render_listing_table(
        "Tokens",
        [
            ColumnSpec("Token", style="bold"),
            ColumnSpec("Value"),
        ],
        rows,
        sink=console,
    )


def _render_maps(maps: Mapping[str, dict], *, console: "Console") -> None:
    """Render each nested map as a Rich ``Tree``, sorted by map name."""
    if not maps:
        return
    console.print(Text("Maps", style="bold dim"))
    for map_name, map_value in sorted(maps.items()):
        tree = Tree(Text(map_name, style="bold"))
        _add_tree_nodes(tree, map_value)
        console.print(tree)


def _add_tree_nodes(parent: Tree, data: Mapping[str, "JSONValue"]) -> None:
    """Attach ``data`` to ``parent``: dicts recurse, scalars become leaves.

    Leaves render as ``key: "value"`` so the wrapping quotes signal the leaf
    is a value rather than a further branch.
    """
    for key, value in data.items():
        if isinstance(value, dict):
            subtree = parent.add(Text(str(key), style="bold"))
            _add_tree_nodes(subtree, value)
        else:
            parent.add(Text(f'{key}: "{value}"'))


def _render_secret_references(view: "SubstitutionView", *, console: "Console") -> None:
    """Render observed secret references in ``${secret:scope/key}`` form.

    The view already sorts and deduplicates the references; this only formats
    them. Nothing is printed when no secret references were observed.
    """
    if not view.secret_references:
        return
    console.print(Text("Secret references", style="bold dim"))
    for ref in view.secret_references:
        console.print(f"${{secret:{ref.scope}/{ref.key}}}")


def _render_default_scope(view: "SubstitutionView", *, console: "Console") -> None:
    """Render the configured default secret scope, if any."""
    if view.default_secret_scope is None:
        return
    console.print(
        Text.assemble(
            ("Default secret scope: ", "bold dim"),
            (view.default_secret_scope, ""),
        )
    )
