"""LHP CLI package.

Houses Click-based command implementations, error-boundary rendering,
and the POSIX-compliant exit-code mapping that the CLI exposes to
shell callers and CI systems. Domain code (``lhp.core``, ``lhp.api``,
``lhp.generators``) must not import from this package.
"""

from lhp.cli.exit_codes import ExitCode

__all__ = ["ExitCode"]
