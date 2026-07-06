"""Public surface of the developer-sandbox rewrite sub-package.

Implements the ``--sandbox`` rewrite engine (TARGET_ARCHITECTURE §5ter):
renaming tables produced by in-scope pipelines to per-developer names.
Layering: this sub-package imports DOWNWARD only (``core/dependencies`` via
its package ``__init__``, ``models``, ``errors``, ``utils``, stdlib) — never
from ``core/coordination``, ``api``, ``cli``, ``generators``, or ``bundle``.
"""

from lhp.core.sandbox._flowgroup_rewriter import rewrite_flowgroup_tables
from lhp.core.sandbox._python_rewriter import (
    UnrewritableTableRead,
    UnverifiableSqlRead,
    rewrite_python_table_literals,
)
from lhp.core.sandbox._renames import (
    SandboxRewritePlan,
    SandboxTableRenames,
    TableRenameStrategy,
    build_sandbox_table_renames,
    match_renamed_table,
    rename_parts,
)
from lhp.core.sandbox.scope_resolver import resolve_sandbox_run

__all__ = [
    "SandboxRewritePlan",
    "SandboxTableRenames",
    "TableRenameStrategy",
    "UnrewritableTableRead",
    "UnverifiableSqlRead",
    "build_sandbox_table_renames",
    "match_renamed_table",
    "rename_parts",
    "resolve_sandbox_run",
    "rewrite_flowgroup_tables",
    "rewrite_python_table_literals",
]
