"""Portable 4-value exit code scheme for the LHP CLI.

Convention:
  0  SUCCESS       — command completed normally
  1  ERROR         — domain-level error (bad config, failed validation, etc.)
  2  USAGE_ERROR   — caller passed wrong arguments or options
  3  INTERNAL_ERROR — unexpected bug; file an issue
"""

from enum import IntEnum


class ExitCode(IntEnum):
    """Exit codes for the LHP CLI."""

    SUCCESS = 0
    ERROR = 1
    USAGE_ERROR = 2
    INTERNAL_ERROR = 3
