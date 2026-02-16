"""POSIX-compliant exit codes for LakehousePlumber CLI."""

from enum import IntEnum


class ExitCode(IntEnum):
    """Exit codes following POSIX sysexits.h conventions."""

    SUCCESS = 0
    GENERAL_ERROR = 1
    USAGE_ERROR = 64  # EX_USAGE — command-line usage error
    DATA_ERROR = 65  # EX_DATAERR — validation, dependency errors
    NO_INPUT = 66  # EX_NOINPUT — file not found
    SOFTWARE_ERROR = 70  # EX_SOFTWARE — internal software error
    IO_ERROR = 74  # EX_IOERR — permissions, disk errors
    CONFIG_ERROR = 78  # EX_CONFIG — configuration errors

    @classmethod
    def from_error_category(cls, category) -> "ExitCode":
        """Map ErrorCategory to ExitCode."""
        from .error_formatter import ErrorCategory

        mapping = {
            ErrorCategory.VALIDATION: cls.DATA_ERROR,
            ErrorCategory.DEPENDENCY: cls.DATA_ERROR,
            ErrorCategory.IO: cls.NO_INPUT,
            ErrorCategory.CONFIG: cls.CONFIG_ERROR,
            ErrorCategory.CLOUDFILES: cls.CONFIG_ERROR,
            ErrorCategory.ACTION: cls.DATA_ERROR,
            ErrorCategory.GENERAL: cls.GENERAL_ERROR,
        }
        return mapping.get(category, cls.GENERAL_ERROR)

    @classmethod
    def from_lhp_error(cls, error) -> "ExitCode":
        """Extract exit code from an LHPError instance using its category."""
        from .error_formatter import LHPError

        if isinstance(error, LHPError):
            return cls.from_error_category(error.category)
        return cls.GENERAL_ERROR
