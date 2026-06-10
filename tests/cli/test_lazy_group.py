"""Direct unit tests for the lazy command-resolution mechanics (``cli/_lazy_group.py``).

The startup-floor suite (``test_startup_imports.py``) covers ``LazyGroup`` only
*end-to-end*, via clean-subprocess import-absence probes on the ``lhp --version``
flow. Those assert the observable outcome (no command module leaks into the
version path) but exercise the resolution machinery indirectly. This file unit-
tests the machinery itself (constitution §8.1): ``list_commands`` completeness,
``get_command`` resolve-and-register, single-target import, and the unknown-
command contract.

The "resolving one command does not eagerly import the others" property is the
load-bearing claim, and it is asserted by **import count**, not by ``sys.modules``
membership. An in-process membership check would be meaningless here: this test
module runs under pytest alongside the other CLI tests, which have already
imported several command modules (``generate_command`` among them), so the
target modules are typically already in ``sys.modules`` before this test starts.
Patching ``importlib.import_module`` on a freshly constructed ``LazyGroup``
instead gives a deterministic, isolated signal — resolving command X must call
``import_module`` exactly once, with X's module target, and never with another
command's target.
"""

from __future__ import annotations

import click
import pytest

from lhp.cli._lazy_group import COMMAND_IMPORTS, LazyGroup

pytestmark = pytest.mark.unit


@pytest.fixture
def ctx() -> click.Context:
    """A minimal Click context bound to a fresh lazy group.

    ``list_commands`` / ``get_command`` take a ``ctx`` but do not read group
    state off it for the assertions here, so a bare context over a throwaway
    group is sufficient.
    """
    return click.Context(click.Group(name="lhp"))


def _make_group() -> LazyGroup:
    """A fresh ``LazyGroup`` wired exactly as ``cli/main.py`` wires the real one.

    Each test gets its own instance so resolution side effects (entries moving
    from the lazy map into ``self.commands``) never leak between tests.
    """
    return LazyGroup(name="lhp", lazy_commands=COMMAND_IMPORTS)


def test_list_commands_returns_full_command_set(ctx: click.Context) -> None:
    """``list_commands`` returns every ``COMMAND_IMPORTS`` key, including hidden ``deps``.

    Sorted, matching ``click.Group`` ordering. The hidden backward-compat alias
    ``deps`` must be present (rich_click filters ``hidden`` at *render* time, not
    in ``list_commands``); dropping it here would break the alias resolution
    path that ``deps`` -> ``dag_command:deps`` depends on.
    """
    listed = _make_group().list_commands(ctx)

    assert listed == sorted(COMMAND_IMPORTS), listed
    assert "deps" in listed, "hidden `deps` alias must not be dropped from listing"


def test_get_command_resolves_and_registers(ctx: click.Context) -> None:
    """``get_command`` returns the real Click command and registers it.

    After first resolution the command must be a ``click.Command`` and must have
    moved into ``self.commands`` (Click's eager registry) and out of the lazy
    map, so a second lookup hits the registry rather than re-importing.
    """
    group = _make_group()
    assert "generate" not in group.commands

    command = group.get_command(ctx, "generate")

    assert isinstance(command, click.Command), command
    assert command.name == "generate"
    # Resolved -> moved into the eager registry, removed from the lazy map.
    assert "generate" in group.commands
    assert "generate" not in group._lazy_commands
    # Idempotent: the registry copy is returned on subsequent lookups.
    assert group.get_command(ctx, "generate") is group.commands["generate"]


def test_resolving_one_command_imports_only_that_module(
    ctx: click.Context, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Resolving one command imports that command's module and no other's.

    Asserted by import count (see module docstring for why ``sys.modules`` is
    unreliable in-process). ``importlib.import_module`` — as referenced from the
    ``_lazy_group`` module — is patched to record its targets and delegate to the
    real importer. Resolving ``generate`` must call it exactly once, with
    ``generate``'s module, and never with another command's module (here
    ``validate``'s).
    """
    from lhp.cli import _lazy_group

    generate_module = COMMAND_IMPORTS["generate"].partition(":")[0]
    validate_module = COMMAND_IMPORTS["validate"].partition(":")[0]

    # Capture the *original function* before patching, not the ``importlib``
    # module object: ``_lazy_group.importlib`` is the shared module singleton, so
    # delegating through it after patching would re-enter the wrapper (infinite
    # recursion). Bind the unpatched callable so the delegation hits the real one.
    real_import_module = _lazy_group.importlib.import_module
    calls: list[str] = []

    def _recording_import(name: str, *args: object, **kwargs: object) -> object:
        calls.append(name)
        return real_import_module(name, *args, **kwargs)

    monkeypatch.setattr(_lazy_group.importlib, "import_module", _recording_import)

    _make_group().get_command(ctx, "generate")

    assert calls == [generate_module], (
        "resolving `generate` must import exactly its own module once; "
        f"observed import_module calls: {calls}"
    )
    assert validate_module not in calls, (
        f"resolving `generate` eagerly imported `validate`'s module "
        f"({validate_module!r}) — lazy resolution must not pull sibling commands"
    )


def test_get_command_unknown_returns_none(ctx: click.Context) -> None:
    """An unknown command name returns ``None`` (Click's contract), no raise.

    ``None`` is how a ``click.Group`` signals "no such command" so the caller can
    emit the usage error; a lazy entry that is absent from both the registry and
    the lazy map must follow the same contract rather than raising (e.g. a
    ``KeyError`` from the lazy-map pop).
    """
    assert _make_group().get_command(ctx, "no-such-command") is None
