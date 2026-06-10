"""LHP CLI package.

Domain code (``lhp.core``, ``lhp.api``, ``lhp.generators``) must not import
from this package.
"""

from lhp.cli.exit_codes import ExitCode

__all__ = ["ExitCode"]
