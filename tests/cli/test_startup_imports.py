"""Startup-floor import regression tests (Task U4).

Locks in three startup-latency fixes so a future eager import cannot silently
regress them:

* **U1** — ``import lhp`` no longer eagerly pulls generator registration /
  ``networkx``.
* **U2** — command modules load lazily via ``LazyGroup``; the ``lhp --version``
  flow does not import any command module (e.g.
  ``lhp.cli.commands.generate_command``).
* **U3** — ``import lhp.api`` no longer transitively pulls ``networkx``.
* ``import lhp.api`` no longer eagerly pulls the codegen stack
  (``lhp.core.codegen``) or ``jinja2`` via it; the generation stream bodies
  import them function-locally, so the public-API surface stays light.

The hard assertions are *import-absence* checks: each is run in a clean
subprocess (a fresh interpreter is required so the test runner's own already-
imported modules — pytest pulls in plenty — cannot pollute the observed
``sys.modules``). The subprocess executes the relevant entry path, then prints
its ``sys.modules`` membership as JSON on the last stdout line; the parent
parses that and asserts.

Entry point: ``pyproject.toml`` ``[project.scripts]`` maps ``lhp`` to
``lhp.cli.main:cli``. The version-flow probe therefore drives the *real* entry
module via ``runpy.run_module("lhp.cli.main", run_name="__main__")`` with
``sys.argv == ["lhp", "--version"]`` — i.e. exactly the ``if __name__ ==
"__main__": cli()`` path the installed console script reaches — and catches the
``SystemExit`` the eager ``--version`` callback raises.

Wall-times are *recorded* as the documented startup floor but are deliberately
NOT hard assertions (subprocess cold-start is CI-flaky). A single generous soft
guard (``< _WALLTIME_CEILING_S`` seconds) catches only a gross regression — an
accidentally re-added heavy eager import that balloons cold start by an order of
magnitude — without failing on normal CI jitter.

Measured locally (best-of-3, macOS, editable install), recorded for reference:

* ``lhp --version`` ~0.08 s  (no command module, no networkx)
* ``lhp --help``    ~0.28 s  (heavier *by design*: rich_click's help renderer
  resolves every command, so ``--help`` legitimately imports all command
  modules — see ``cli/_lazy_group.py`` module docstring. ``--help`` is therefore
  NOT subject to the command-module import-absence assertion; ``--version`` is.)
"""

from __future__ import annotations

import json
import subprocess
import sys
import time

import pytest

pytestmark = pytest.mark.unit

# Generous soft ceiling for a clean-subprocess cold start of the version/help
# flows. Real values are ~0.1-0.3 s; this trips only on an order-of-magnitude
# regression (e.g. a re-added eager ``networkx``/domain import), never on CI
# jitter. Soft guard only — the hard contract is the import-absence checks.
_WALLTIME_CEILING_S = 8.0

# A command module is any ``lhp.cli.commands.<name>_command`` — the lazily
# loaded modules registered in ``cli/_lazy_group.py``'s ``COMMAND_IMPORTS``.
# The ``--version`` flow must touch none of them.
_GENERATE_COMMAND_MODULE = "lhp.cli.commands.generate_command"


def _run_probe(body: str) -> dict[str, bool]:
    """Run ``body`` in a clean subprocess; return the JSON dict it prints last.

    ``body`` is Python source whose final stdout line is a JSON object mapping
    probe-name -> bool (typically ``sys.modules`` membership). A fresh
    interpreter is mandatory: this test module runs under pytest, which has
    already imported much of LHP, so an in-process ``sys.modules`` check would
    be meaningless. ``sys.executable`` guarantees the same interpreter/venv as
    the test run, so the editable install is on the path.
    """
    result = subprocess.run(
        [sys.executable, "-c", body],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, (
        f"probe subprocess exited {result.returncode}\n"
        f"stdout:\n{result.stdout}\n"
        f"stderr:\n{result.stderr}"
    )
    # The probe may legitimately emit other stdout (e.g. the printed version);
    # the contract is that the LAST non-empty line is the JSON payload.
    last_line = result.stdout.strip().splitlines()[-1]
    return json.loads(last_line)


# Probe source executed in the clean subprocess for the ``lhp --version`` flow.
# Drives the actual entry module (``lhp.cli.main`` run as ``__main__``) with
# ``--version`` argv so the eager ``_print_version`` callback fires and calls
# ``ctx.exit(0)`` -> ``SystemExit``, which we swallow. Then we report whether
# the heavy imports leaked into the version path.
_VERSION_FLOW_PROBE = """
import json
import runpy
import sys

sys.argv = ["lhp", "--version"]
try:
    runpy.run_module("lhp.cli.main", run_name="__main__")
except SystemExit:
    pass

mods = sys.modules
print(json.dumps({
    "networkx": "networkx" in mods,
    "generate_command": "lhp.cli.commands.generate_command" in mods,
    "any_command_module": any(
        m.startswith("lhp.cli.commands.") and m.endswith("_command")
        for m in mods
    ),
}))
"""

_IMPORT_LHP_PROBE = """
import json
import lhp  # noqa: F401
import sys

print(json.dumps({"networkx": "networkx" in sys.modules}))
"""

_IMPORT_LHP_API_PROBE = """
import json
import lhp.api  # noqa: F401
import sys

mods = sys.modules
print(json.dumps({
    "networkx": "networkx" in mods,
    "codegen": "lhp.core.codegen" in mods,
    "jinja2": "jinja2" in mods,
}))
"""


def test_version_flow_does_not_import_networkx() -> None:
    """U2/U3: the ``lhp --version`` flow must not pull ``networkx``.

    networkx is the dependency-analysis graph engine; it has no business being
    imported just to print a version string. Its presence here would mean the
    version path is dragging in the dependency-analysis stack.
    """
    probed = _run_probe(_VERSION_FLOW_PROBE)
    assert probed["networkx"] is False, (
        "`networkx` was imported by the `lhp --version` flow — the version "
        "path is transitively pulling the dependency-analysis stack. "
        "Make that import lazy (inside the function that needs it)."
    )


def test_version_flow_imports_no_command_module() -> None:
    """U2: ``lhp --version`` must import no ``*_command`` module.

    Commands load lazily via ``LazyGroup`` (``cli/_lazy_group.py``). ``--version``
    is an eager callback that exits before any subcommand resolves, so no command
    module — and in particular not ``lhp.cli.commands.generate_command``, the
    heaviest one — should be in ``sys.modules``. A regression here means a
    command module is being imported at group-definition time again.
    """
    probed = _run_probe(_VERSION_FLOW_PROBE)
    assert probed["generate_command"] is False, (
        f"{_GENERATE_COMMAND_MODULE!r} was imported by the `lhp --version` "
        "flow — a command module is being eagerly imported again. Keep "
        "registration lazy via `COMMAND_IMPORTS` / `LazyGroup`."
    )
    assert probed["any_command_module"] is False, (
        "A `lhp.cli.commands.*_command` module was imported by the "
        "`lhp --version` flow — command modules must load lazily, not at "
        "group-definition time."
    )


def test_import_lhp_does_not_import_networkx() -> None:
    """U1: a bare ``import lhp`` must not pull ``networkx``.

    ``import lhp`` should be cheap — it must not eagerly trigger generator
    registration or the dependency-analysis stack. ``networkx`` in
    ``sys.modules`` after ``import lhp`` is the canonical signal that an eager
    import crept back in.
    """
    probed = _run_probe(_IMPORT_LHP_PROBE)
    assert probed["networkx"] is False, (
        "`import lhp` pulled `networkx` into sys.modules — an eager import "
        "regressed. Keep the dependency-analysis stack out of `lhp/__init__.py`'s "
        "transitive import graph."
    )


def test_import_lhp_api_does_not_import_networkx() -> None:
    """U3: ``import lhp.api`` must not transitively pull ``networkx``.

    The public facade surface should be importable without dragging in the
    dependency-analysis graph engine; networkx is imported lazily by the
    operation that actually builds a graph.
    """
    probed = _run_probe(_IMPORT_LHP_API_PROBE)
    assert probed["networkx"] is False, (
        "`import lhp.api` transitively pulled `networkx` — the public API "
        "surface is dragging in the dependency-analysis stack. Defer that "
        "import to the operation that needs it."
    )


def test_import_lhp_api_does_not_import_codegen_or_jinja2() -> None:
    """``import lhp.api`` must not pull the codegen stack or ``jinja2``.

    The public facade re-exports the generation surface, but the heavy stream
    bodies (``lhp.core.codegen`` and the ``jinja2`` it pulls for template
    rendering) are needed only when a generate/plan stream is actually
    consumed — they must be imported function-locally, not at module load.
    Either name in ``sys.modules`` after a bare ``import lhp.api`` is the
    canonical signal that an eager codegen import regressed.
    """
    probed = _run_probe(_IMPORT_LHP_API_PROBE)
    assert probed["codegen"] is False, (
        "`import lhp.api` transitively pulled `lhp.core.codegen` — the public "
        "API surface is dragging in the codegen stack eagerly. Defer that "
        "import into the generation stream body that needs it."
    )
    assert probed["jinja2"] is False, (
        "`import lhp.api` transitively pulled `jinja2` (via the codegen "
        "stack) — the public API surface is importing the template engine "
        "eagerly. Defer the codegen import into the stream body that needs it."
    )


def _measure_wall_time_s(args: list[str]) -> float:
    """Best-of-3 wall-clock seconds for a successful run of ``args``.

    Best-of-3 to damp scheduler/cold-cache noise. Asserts a clean exit so a
    crashing startup path can't masquerade as a fast one. ``args`` runs the real
    entry module via ``python -m lhp.cli.main`` (the repo's CLI-subprocess
    convention) so it works whenever the package is importable, regardless of
    whether the ``lhp`` console script is on PATH.
    """
    best: float | None = None
    for _ in range(3):
        start = time.perf_counter()
        result = subprocess.run(args, capture_output=True, text=True, timeout=120)
        elapsed = time.perf_counter() - start
        assert result.returncode == 0, (
            f"{args!r} exited {result.returncode}\nstderr:\n{result.stderr}"
        )
        best = elapsed if best is None else min(best, elapsed)
    assert best is not None
    return best


@pytest.mark.slow
def test_startup_walltime_soft_floor() -> None:
    """Record ``--version`` / ``--help`` wall-times; soft-guard a gross regression.

    NOT a precise latency assertion (subprocess cold-start is CI-flaky). The
    recorded numbers document the startup floor; the only assertion is a
    generous ceiling that trips on an order-of-magnitude regression (e.g. a
    re-added heavy eager import), never on normal jitter. Marked ``slow``
    because it spawns six subprocesses.
    """
    version_args = [sys.executable, "-m", "lhp.cli.main", "--version"]
    help_args = [sys.executable, "-m", "lhp.cli.main", "--help"]

    version_s = _measure_wall_time_s(version_args)
    help_s = _measure_wall_time_s(help_args)

    # Surfaced in `pytest -rA` / `-s` output as the documented startup floor.
    print(
        f"\n[startup floor] lhp --version: {version_s * 1000:.0f} ms | "
        f"lhp --help: {help_s * 1000:.0f} ms (best-of-3, clean subprocess)"
    )

    assert version_s < _WALLTIME_CEILING_S, (
        f"`lhp --version` cold start {version_s:.2f}s exceeded the "
        f"{_WALLTIME_CEILING_S:.0f}s soft ceiling — likely a heavy eager import "
        "regressed the startup path."
    )
    assert help_s < _WALLTIME_CEILING_S, (
        f"`lhp --help` cold start {help_s:.2f}s exceeded the "
        f"{_WALLTIME_CEILING_S:.0f}s soft ceiling — likely a heavy eager import "
        "regressed the startup path. (`--help` is heavier than `--version` by "
        "design: it resolves every command for the help panels.)"
    )
