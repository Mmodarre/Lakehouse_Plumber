"""Per-file folding of sandbox Python-rewriter warnings into transport records.

:func:`~lhp.core.sandbox.rewrite_python_table_literals` returns a mixed
per-site warning stream for ONE file; these helpers fold it into the
:class:`~lhp.models.processing.SandboxWarningRecord`s the worker rides back to
the main thread. Split out of :mod:`._flowgroup_pool` so that module stays under
the §3.3 size cap.

Two warning codes, one record of each at most per file (the ``(code, file)``
dedup grain the merge chain uses):

- ``LHP-VAL-066`` — :class:`~lhp.core.sandbox.UnrewritableTableRead`: an in-scope
  read left untouched because its argument statically resolved but is not a
  plain literal.
- ``LHP-VAL-067`` — :class:`~lhp.core.sandbox.UnverifiableSqlRead`: an opaque
  ``spark.sql`` body whose table refs are only known at runtime.

Sandbox / error-code imports stay function-local, matching the worker's
lazy-import discipline so non-sandbox runs never eagerly load the sandbox
sub-package (and its Jinja2 dependency).
"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, List, Optional

from ...models.processing import SandboxWarningRecord

if TYPE_CHECKING:
    from lhp.core.sandbox import UnrewritableTableRead, UnverifiableSqlRead


def fold_file_warnings(
    reads: "tuple[UnrewritableTableRead | UnverifiableSqlRead, ...]",
    file: Path,
    flowgroup_name: str,
) -> List[SandboxWarningRecord]:
    """Split ONE file's rewriter warnings into per-code records.

    :class:`UnrewritableTableRead` folds to ``LHP-VAL-066``;
    :class:`UnverifiableSqlRead` folds to the ``LHP-VAL-067`` advisory. At most
    one record of each code per file.
    """
    from lhp.core.sandbox import UnrewritableTableRead, UnverifiableSqlRead

    out: List[SandboxWarningRecord] = []
    unrewritable = tuple(r for r in reads if isinstance(r, UnrewritableTableRead))
    unverifiable = tuple(r for r in reads if isinstance(r, UnverifiableSqlRead))
    val_066 = _fold_unrewritable_reads(unrewritable, file, flowgroup_name)
    if val_066 is not None:
        out.append(val_066)
    val_067 = _fold_unverifiable_reads(unverifiable, file, flowgroup_name)
    if val_067 is not None:
        out.append(val_067)
    return out


def _fold_unrewritable_reads(
    reads: "tuple[UnrewritableTableRead, ...]",
    file: Path,
    flowgroup_name: str,
) -> Optional[SandboxWarningRecord]:
    """Fold ONE file's unrewritable in-scope reads into ONE ``LHP-VAL-066``.

    Returns ``None`` when the file has no such reads. Sites are deduped and
    sorted by ``(lineno, table, kind)`` so the message is deterministic.
    """
    if not reads:
        return None
    from lhp.errors.codes import VAL_066

    sites = sorted({(read.lineno, read.table, read.kind) for read in reads})
    detail = "; ".join(
        f"line {lineno}: {table} ({kind})" for lineno, table, kind in sites
    )
    message = (
        f"Sandbox rewrite could not rename {len(sites)} in-scope table "
        f"read(s) in {file.name}: {detail}. These sites still read the "
        f"original (non-sandbox) table(s)."
    )
    return SandboxWarningRecord(
        code=VAL_066.code,
        message=message,
        file=file,
        flowgroup=flowgroup_name,
    )


def _fold_unverifiable_reads(
    reads: "tuple[UnverifiableSqlRead, ...]",
    file: Path,
    flowgroup_name: str,
) -> Optional[SandboxWarningRecord]:
    """Fold ONE file's opaque ``spark.sql`` sites into ONE ``LHP-VAL-067``.

    Returns ``None`` when the file has no such sites. Line numbers are deduped
    and sorted so the advisory message is deterministic.
    """
    if not reads:
        return None
    from lhp.errors.codes import VAL_067

    lines = sorted({read.lineno for read in reads})
    detail = ", ".join(str(lineno) for lineno in lines)
    message = (
        f"Sandbox mode could not verify {len(lines)} dynamic SQL statement(s) "
        f"in {file.name} (line(s) {detail}): table references resolved at "
        f"runtime cannot be verified or rewritten to sandbox table names."
    )
    return SandboxWarningRecord(
        code=VAL_067.code,
        message=message,
        file=file,
        flowgroup=flowgroup_name,
    )
