"""Sole-bridge invariant: no presenter may import ``lhp.errors``.

This is the only *mechanical* enforcement of constitution §9.5 (the
``lhp.errors`` sole-bridge rule). ``cli/error_panel.py`` is the single
place allowed to combine ``rich`` with ``lhp.errors`` and render an LHP
error payload to a Panel; every module under ``cli/presenters/`` must
format only frozen ``lhp.api`` view DTOs and stay ignorant of the
errors package.

``import-linter`` cannot catch this on its own: presenters are
deliberately allowed to import ``rich`` (they render to it), and the
boundary contracts operate at the package level, so a stray
``from lhp.errors.codes import ...`` inside a presenter would slip
through. This test closes that gap by AST-scanning every ``.py`` module
under ``src/lhp/cli/presenters/`` (recursively) and failing if any of
them imports ``lhp.errors`` in any form — ``import lhp.errors`` /
``import lhp.errors.codes`` (``ast.Import``) or ``from lhp.errors ...
import ...`` / ``from lhp.errors.codes import ...`` (``ast.ImportFrom``).

``error_panel.py`` lives in ``cli/`` and NOT in ``cli/presenters/``, so
it is outside the scanned tree and is correctly never flagged — it
remains the sole ``rich`` + ``lhp.errors`` bridge.
"""

from __future__ import annotations

import ast
from pathlib import Path
from typing import List

import lhp.cli.presenters as _presenters_pkg

# Root of the scanned tree: the on-disk directory backing the
# ``lhp.cli.presenters`` package. Resolving via the imported package
# (rather than a path relative to this test file) keeps the scan correct
# regardless of cwd or how the suite is invoked.
PRESENTERS_DIR = Path(_presenters_pkg.__file__).resolve().parent

# The forbidden package. Matching on this prefix covers both the package
# itself (``lhp.errors``) and any submodule such as ``lhp.errors.codes``.
FORBIDDEN_ROOT = "lhp.errors"


def _presenter_modules() -> List[Path]:
    """Every ``.py`` file under the presenters package, recursively."""
    return sorted(PRESENTERS_DIR.rglob("*.py"))


def _module_id(path: Path) -> str:
    """Repo-relative-ish id for readable parametrize labels."""
    return str(path.relative_to(PRESENTERS_DIR))


def _imports_forbidden_root(module_name: str | None) -> bool:
    """True if ``module_name`` is ``lhp.errors`` or a submodule of it."""
    if not module_name:
        return False
    return module_name == FORBIDDEN_ROOT or module_name.startswith(FORBIDDEN_ROOT + ".")


def _offending_imports(source: str) -> List[str]:
    """Return the source lines (as text) that import ``lhp.errors``.

    Inspects ``ast.Import`` (``import lhp.errors[...]``) and
    ``ast.ImportFrom`` (``from lhp.errors[...] import ...``). Relative
    imports (``node.level > 0``) carry no absolute module path here and
    are ignored — presenters reach the errors package, if at all, via its
    absolute name, which is what §9.5 forbids.
    """
    tree = ast.parse(source)
    offenders: List[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                if _imports_forbidden_root(alias.name):
                    offenders.append(f"import {alias.name}")
        elif isinstance(node, ast.ImportFrom):
            if node.level == 0 and _imports_forbidden_root(node.module):
                names = ", ".join(alias.name for alias in node.names)
                offenders.append(f"from {node.module} import {names}")
    return offenders


def test_presenters_tree_is_non_empty():
    """Guard the guard: an empty scan would make this suite vacuously green."""
    modules = _presenter_modules()
    assert modules, f"no presenter modules found under {PRESENTERS_DIR}"


def test_no_presenter_imports_lhp_errors():
    """No module under ``cli/presenters/`` may import ``lhp.errors`` (§9.5)."""
    violations: dict[str, List[str]] = {}
    for module in _presenter_modules():
        offenders = _offending_imports(module.read_text(encoding="utf-8"))
        if offenders:
            violations[_module_id(module)] = offenders

    assert not violations, (
        "Sole-bridge invariant violated (constitution §9.5): the following "
        "presenter modules import 'lhp.errors'. Only cli/error_panel.py "
        "(outside cli/presenters/) may bridge rich + lhp.errors; presenters "
        "must format frozen lhp.api DTOs instead.\n"
        + "\n".join(
            f"  {mod}:\n" + "\n".join(f"    - {line}" for line in lines)
            for mod, lines in sorted(violations.items())
        )
    )
