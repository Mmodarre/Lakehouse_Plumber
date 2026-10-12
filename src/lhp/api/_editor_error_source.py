"""Name the ``lhp.yaml`` field an editor error is about, when it is not the flowgroup.

An operational-metadata column is defined in ``lhp.yaml``, but its expression
is resolved while each flowgroup that selects it resolves, so its errors are
raised from the flowgroup. ``LHP-CFG-010`` (unresolved token) and
``LHP-CFG-070`` (secret reference, directly or through a token value) raised
for such an expression carry the ``Column`` and ``Expression`` context keys.
Other ``LHP-CFG-010`` errors (a flowgroup's own unresolved tokens, a CDC fan-in
mismatch, a deprecated field) carry neither, so they keep the flowgroup source.
"""

from __future__ import annotations

from typing import Optional, Tuple

from lhp.errors import LHPError, codes

_EXPRESSION_ERROR_CODES = frozenset({codes.CFG_010.code, codes.CFG_070.code})


def lhp_yaml_field(error: LHPError) -> Optional[Tuple[str, ...]]:
    """Return the ``lhp.yaml`` path of the expression ``error`` is about, else ``None``.

    :stability: provisional
    """
    if error.code not in _EXPRESSION_ERROR_CODES:
        return None
    column = error.context.get("Column")
    if not isinstance(column, str) or "Expression" not in error.context:
        return None
    return ("operational_metadata", "columns", column, "expression")
