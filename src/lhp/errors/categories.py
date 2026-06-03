"""Error category taxonomy for LHP.

Eight categories matching ``LHP-<CAT>-<NNN>`` error code prefix.
"""

from enum import Enum


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
