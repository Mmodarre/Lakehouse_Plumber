"""Error category taxonomy for LHP.

Eight categories matching ``LHP-<CAT>-<NNN>`` error code prefix.
"""

from enum import Enum
from typing import Optional, Tuple


class ErrorCategory(Enum):
    """Error categories with prefixes."""

    CLOUDFILES = "CF"
    VALIDATION = "VAL"
    IO = "IO"
    CONFIG = "CFG"
    DEPENDENCY = "DEP"
    ACTION = "ACT"
    GENERAL = "GEN"
    DEPRECATION = "DEPR"


def parse_error_code(code: Optional[str]) -> Optional[Tuple[ErrorCategory, str]]:
    """Split ``"LHP-VAL-009"`` into ``(ErrorCategory.VALIDATION, "009")``.

    Returns ``None`` for a missing code, a code not shaped ``LHP-<CAT>-<NNN>``,
    or an unknown category; each caller chooses its own fallback.
    """
    parts = (code or "").split("-")
    if len(parts) != 3 or parts[0] != "LHP":
        return None
    try:
        return ErrorCategory(parts[1]), parts[2]
    except ValueError:
        return None
