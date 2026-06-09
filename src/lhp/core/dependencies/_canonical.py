r"""Pure string/parts helpers for canonicalizing table references.

Compares table references by value rather than surface syntax. Contains ONLY
pure string/parts helpers — no matching, lookup, or graph logic, and no imports
from any ``lhp`` domain module.

LOCKED reconciliation rule:

    A 2-part ``schema.table`` source matches a 3-part
    ``catalog.schema.table`` producer only when schema and table align AND the
    catalog producer is unique; an ambiguous (multiple-catalog) match yields NO
    match (treated as external). Views remain pipeline-scoped; tables are
    global.

Documented authoring constraint: a backtick-quoted identifier containing a
literal dot (e.g. ``\`my.weird\`.tab``) is split on the dot and mis-parsed —
backtick-quoted names must not contain dots.
"""

from __future__ import annotations

__all__ = ["canonicalize_table_ref", "split_canonical_parts"]


def canonicalize_table_ref(ref: str) -> str:
    """Normalize a table reference for byte-level matching.

    Lowercases the reference, strips surrounding and embedded backticks, trims
    whitespace around each dotted part, and rejoins the parts with ``.``.

    Example::

        "  `MyCat`.`MySchema`.`MyTable` " -> "mycat.myschema.mytable"

    The transform is idempotent: ``canonicalize_table_ref(canonicalize_table_ref(x))``
    equals ``canonicalize_table_ref(x)``.

    Args:
        ref: A raw table reference, possibly quoted with backticks, mixed-case,
            and padded with whitespace.

    Returns:
        The canonical lowercase ``a.b.c`` form (empty parts preserved as empty).
    """
    parts = (part.replace("`", "").strip() for part in ref.split("."))
    return ".".join(parts).lower()


def split_canonical_parts(ref: str) -> list[str]:
    """Canonicalize ``ref`` then split it into its dotted parts.

    This is the seam the 2-part vs 3-part reconciliation is built on (see the
    LOCKED rule in the module docstring). For a 2-part ``schema.table`` ref this
    returns ``[schema, table]``; for a 3-part ``catalog.schema.table`` ref it
    returns ``[catalog, schema, table]``.

    Args:
        ref: A raw table reference (same accepted forms as
            :func:`canonicalize_table_ref`).

    Returns:
        The canonical parts in order. A ref with no ``.`` yields a single-element
        list.
    """
    return canonicalize_table_ref(ref).split(".")
