"""Static soundness of the packaged ``--sample`` data-prep notebook (issue #232).

``src/lhp/templates/init_sample/notebooks/data_prep.py`` is Databricks notebook
source that ships verbatim in the wheel and is copied byte-for-byte into every
``lhp init --sample`` scaffold. Because it references the notebook-injected
``spark`` / ``dbutils`` globals, its whole tree is excluded from ruff, mypy and
vulture (``pyproject.toml``: ``[tool.ruff] extend-exclude``,
``[tool.mypy] exclude``, ``[tool.vulture] exclude``) — so no linter ever sees
it, and a plain undefined-name typo reaches users as a job failure on the very
first task of the quickstart.

These tests are the gate that was missing. They use :mod:`symtable` rather than
importing the module, so they need neither Spark nor Databricks: every name read
at *module* scope must also be bound at module scope, once the runtime-injected
globals are allowed for.

Issue #232: ``for schema in (bronze_schema, silver_schema, gold_schema, _meta)``
read a bare ``_meta`` that was never assigned (the sibling statements use the
quoted literal). Databricks runs every cell in one shared global namespace, so
the cell raised ``NameError: name '_meta' is not defined`` and the ``data_prep``
task of ``lhp_sample_quickstart`` failed before creating any schema.
"""

import builtins
import symtable
from pathlib import Path

import pytest

REPO = Path(__file__).resolve().parents[2]

#: The packaged template and the ``docs/`` fixture copy of it. They must stay
#: byte-identical, so both are checked.
NOTEBOOK_COPIES = [
    REPO / "src/lhp/templates/init_sample/notebooks/data_prep.py",
    REPO / "docs/_fixtures/sample_project/notebooks/data_prep.py",
]

#: Globals the Databricks notebook runtime injects into the user namespace.
#: Anything outside this set (and builtins) must be bound in the file itself.
DATABRICKS_INJECTED_GLOBALS = frozenset(
    {
        "spark",
        "dbutils",
        "sc",
        "sqlContext",
        "display",
        "displayHTML",
        "getArgument",
        "dlt",
    }
)


def _unbound_module_scope_names(path: Path) -> list[str]:
    """Names read at module scope of ``path`` that are never bound there."""
    table = symtable.symtable(path.read_text(encoding="utf-8"), str(path), "exec")
    builtin_names = frozenset(dir(builtins))
    return sorted(
        sym.get_name()
        for sym in table.get_symbols()
        if sym.is_referenced()
        and not sym.is_assigned()
        and not sym.is_imported()
        and sym.get_name() not in builtin_names
        and sym.get_name() not in DATABRICKS_INJECTED_GLOBALS
    )


@pytest.mark.unit
@pytest.mark.parametrize("notebook", NOTEBOOK_COPIES, ids=lambda p: p.parts[-4])
def test_data_prep_notebook_binds_every_module_scope_name(notebook: Path) -> None:
    """Regression for issue #232 — no undefined names at notebook module scope."""
    unbound = _unbound_module_scope_names(notebook)
    assert unbound == [], (
        f"{notebook.relative_to(REPO)} reads name(s) {unbound} at module scope "
        "that are never bound there. Databricks executes notebook cells in one "
        "shared global namespace, so this raises NameError at runtime and fails "
        "the data_prep task of the sample quickstart job (issue #232). A schema "
        "or table name needs quoting; a real value needs an assignment (or a "
        "dbutils widget)."
    )


@pytest.mark.unit
def test_docs_fixture_notebook_matches_packaged_template() -> None:
    """The ``docs/`` fixture copy must mirror the packaged template byte-for-byte."""
    packaged, fixture = NOTEBOOK_COPIES
    assert fixture.read_bytes() == packaged.read_bytes(), (
        f"{fixture.relative_to(REPO)} has drifted from "
        f"{packaged.relative_to(REPO)}. The fixture is the copy that "
        "docs/get-started/ describes; keep both in sync in the same commit."
    )
