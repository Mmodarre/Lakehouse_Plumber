"""Error category taxonomy for LHP.

Eight categories matching ``LHP-<CAT>-<NNN>`` error code prefix.
"""

from enum import Enum


class ErrorCategory(Enum):
    """Error categories with prefixes."""

    CLOUDFILES = "CF"  # CloudFiles specific errors
    VALIDATION = "VAL"  # Validation errors
    IO = "IO"  # File/IO errors
    CONFIG = "CFG"  # Configuration errors
    DEPENDENCY = "DEP"  # Dependency errors
    ACTION = "ACT"  # Action type errors
    GENERAL = "GEN"  # General errors
    DEPRECATION = "DEPR"  # Soft-deprecation warnings
