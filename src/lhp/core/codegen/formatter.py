"""Code formatting utilities for LakehousePlumber.

The generation hot-path formats generated code in a single terminal pass
that shells out to ``ruff format`` (see :func:`format_generated_tree`). The
in-worker step only asserts the generated source parses
(:func:`assert_generated_python_valid`); the actual formatting happens once,
on the coordinator, over the whole output tree.
"""

import ast
import logging
import os
import shutil
import subprocess
import sys
import sysconfig
from pathlib import Path

from ...errors import ErrorFactory, codes

logger = logging.getLogger(__name__)

# Deterministic ``ruff format`` flags for the generated-output terminal pass.
#
# The pass runs inside the USER's project directory, where a bare ``ruff
# format`` would discover and honor the user's own ``pyproject.toml`` /
# ``ruff.toml`` (line length, target version, quote style, ...). That would
# make generated-code formatting depend on the user's environment and break
# the reproducibility generated output requires. ``--isolated`` makes ruff
# ignore ALL discovered config files; the two ``--config`` overrides then pin
# the formatting LHP controls:
#   * ``target-version='py311'`` — matches LHP's minimum supported Python.
#   * ``line-length=88`` — the long-standing 88-column generated-code width.
_RUFF_FORMAT_BASE_ARGS = [
    "format",
    "--isolated",
    "--config",
    "target-version='py311'",
    "--config",
    "line-length=88",
]


def _ruff_exe() -> str:
    """Locate the ruff executable bundled with the active environment.

    Returns the path to the ``ruff`` shipped in the *current* environment's
    scripts directory — NOT a bare ``PATH`` lookup, which could resolve an
    unrelated ruff installed elsewhere on the machine. The terminal
    formatting pass shells out to this executable, so it must be the
    one that satisfies LHP's own ``ruff`` runtime dependency.

    Resolution order:
      1. ``sysconfig.get_path("scripts")`` (the active venv's ``bin``/
         ``Scripts`` dir) joined with ``ruff`` (``ruff.exe`` on Windows).
      2. ``shutil.which("ruff")`` as a defensive fallback only if the
         sysconfig candidate does not exist.

    Returns:
        Absolute path to the ruff executable, as a ``str`` suitable for
        passing directly as a subprocess argument.

    Raises:
        LHPConfigError: ``LHP-CFG-034`` if no ruff executable can be found,
            naming ruff as a required runtime dependency.
    """
    exe_name = "ruff.exe" if os.name == "nt" else "ruff"
    candidate = Path(sysconfig.get_path("scripts")) / exe_name
    if candidate.exists():
        return str(candidate)

    # Defensive fallback: the sysconfig scripts dir did not contain ruff
    # (unusual layouts, console-script shims relocated, etc.).
    found = shutil.which("ruff")
    if found:
        return found

    raise ErrorFactory.config_error(
        codes.CFG_034,
        title="ruff executable not found",
        details=(
            "LHP could not locate the ``ruff`` executable required for the "
            "generated-code formatting pass. ruff is a runtime dependency of "
            f"LHP but was not found in the active environment's scripts "
            f"directory ({sysconfig.get_path('scripts')!r}) or on PATH."
        ),
        suggestions=[
            "Install ruff into the active environment: `pip install ruff`",
            "Reinstall LHP with its dependencies: `pip install lakehouse-plumber`",
            "If using an isolated/custom environment, ensure ruff is on PATH "
            "or installed alongside LHP",
        ],
        context={
            "Scripts directory checked": sysconfig.get_path("scripts"),
            "Python executable": sys.executable,
        },
    )


def assert_generated_python_valid(code: str, flowgroup: str) -> None:
    """Assert that LHP-generated Python parses, with per-flowgroup attribution.

    This is the cheap (microsecond ``ast.parse``) syntax guard that runs
    inside the generation worker; the actual formatting is relocated to a
    single terminal pass (:func:`format_generated_tree`). It reproduces the
    exact ``LHP-CFG-031`` error a user sees when the generated source cannot
    be parsed: same category, same code number, same title/details/
    suggestions text. The only addition is the ``flowgroup`` name in
    ``context`` so the per-flowgroup attribution survives outside the
    worker's failure-DTO wrapping.

    Args:
        code: Generated Python source to validate.
        flowgroup: Name of the flowgroup that produced ``code`` (surfaced
            in the error so failures stay attributable per flowgroup).

    Raises:
        LHPConfigError: ``LHP-CFG-031`` if ``code`` is not valid Python.
    """
    try:
        ast.parse(code)
    except SyntaxError as e:
        raise ErrorFactory.config_error(
            codes.CFG_031,
            title="Generated source failed to parse",
            details=(
                f"LHP produced Python source code that could not be parsed: {e}. "
                "This is almost certainly a bug in an LHP generator or template — "
                "the generator emitted syntactically invalid Python."
            ),
            suggestions=[
                "File a bug report against LHP with the failing flowgroup YAML",
                "Inspect the generated code (turn on DEBUG logging) "
                "to see the source that failed to parse",
                "If you're authoring a custom template or a snapshot-CDC source_function, "
                "verify the embedded Python parses with `python -m py_compile`",
            ],
            context={
                "Flowgroup": flowgroup,
                "Syntax error": str(e),
                "First 500 chars of generated code": code[:500],
            },
        ) from e


def format_generated_tree(output_dir: Path) -> None:
    """Format every generated ``*.py`` file under ``output_dir`` with ruff.

    The single terminal formatting pass that replaces per-flowgroup in-worker
    formatting. It shells out to the bundled ``ruff`` (:func:`_ruff_exe`) and
    formats the WHOLE output tree: generated flowgroup files, copied user
    modules (``custom_python_functions/*.py``, test-reporting provider
    modules), the per-pipeline ``_test_reporting_hook.py``, and auxiliary
    inline modules (e.g. the monitoring ``jobs_stats_loader.py``). ``ruff
    format`` only rewrites Python files, so bundle YAML under ``resources/``
    is left untouched.

    The invocation pins ``--isolated`` plus explicit ``target-version`` /
    ``line-length`` overrides (see :data:`_RUFF_FORMAT_BASE_ARGS`) so the
    result is LHP-controlled and reproducible regardless of any
    ``pyproject.toml`` / ``ruff.toml`` in the user's project directory.

    Args:
        output_dir: The env-level generated output directory
            (``generated/<env>``) to format in place. The directory is
            created by the commit step before this pass runs.

    Raises:
        LHPConfigError: ``LHP-CFG-033`` if ruff exits non-zero (e.g. it could
            not read or parse a generated file). The error carries ruff's
            stderr so the failure surfaces clearly rather than silently
            shipping unformatted code.
    """
    cmd = [_ruff_exe(), *_RUFF_FORMAT_BASE_ARGS, str(output_dir)]
    result = subprocess.run(cmd, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        raise ErrorFactory.config_error(
            codes.CFG_033,
            title="ruff failed to format generated code",
            details=(
                "The terminal `ruff format` pass over the generated output "
                f"directory exited with code {result.returncode}. The generated "
                "code was written to disk but could not be formatted, so the "
                "output may be left unformatted."
            ),
            suggestions=[
                "Inspect the ruff error output below for the offending file",
                "If a generated file is syntactically invalid, this is an LHP "
                "generator/template bug — file a bug report with the flowgroup YAML",
                "Re-run with `--no-format` to skip formatting and inspect the "
                "raw generated code",
            ],
            context={
                "ruff command": " ".join(cmd),
                "Output directory": str(output_dir),
                "ruff exit code": result.returncode,
                "ruff stderr": (result.stderr or "").strip(),
                "ruff stdout": (result.stdout or "").strip(),
            },
        )
