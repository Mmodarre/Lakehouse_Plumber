"""CLI error boundary decorator for consistent error handling."""

import logging
import sys
from functools import wraps
from typing import Callable

import click

from ..utils.error_formatter import LHPError
from ..utils.exit_codes import ExitCode

logger = logging.getLogger(__name__)


def cli_error_boundary(operation: str) -> Callable:
    """Decorator that provides consistent error handling for CLI commands.

    Catches exceptions and displays user-friendly error messages with
    appropriate POSIX exit codes.

    Args:
        operation: Human-readable name of the operation
            (e.g. "Code generation")
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except SystemExit:
                raise  # Let SystemExit pass through
            except LHPError as e:
                click.echo(str(e), err=True)
                exit_code = ExitCode.from_lhp_error(e)
                logger.debug(f"{operation} failed with {e.code}: {e.title}")
                sys.exit(exit_code)
            except Exception as e:
                # Import here to avoid circular imports
                from ..bundle.exceptions import BundleResourceError, TemplateError

                if isinstance(e, (BundleResourceError, TemplateError)):
                    from ..bundle.error_factories import (
                        convert_bundle_error,
                    )

                    lhp_error = convert_bundle_error(e, operation)
                    click.echo(str(lhp_error), err=True)
                    exit_code = ExitCode.from_lhp_error(lhp_error)
                    logger.debug(f"{operation} failed with bundle error: {e}")
                    sys.exit(exit_code)

                # Safety net: detect formatted LHPError strings in wrapped exceptions.
                # This handles cases where an LHPError was stringified before being
                # re-raised as a different exception type. Try to extract the error
                # category prefix for a more accurate exit code.
                error_message = str(e)
                if "Error [LHP-" in error_message and "======" in error_message:
                    click.echo(error_message, err=True)
                    # Try to extract category for better exit code mapping
                    exit_code = ExitCode.GENERAL_ERROR
                    if "LHP-VAL-" in error_message:
                        exit_code = ExitCode.DATA_ERROR
                    elif "LHP-IO-" in error_message:
                        exit_code = ExitCode.IO_ERROR
                    elif "LHP-CFG-" in error_message:
                        exit_code = ExitCode.CONFIG_ERROR
                    elif "LHP-DEP-" in error_message:
                        exit_code = ExitCode.DATA_ERROR
                    logger.debug(f"{operation} failed with wrapped LHPError")
                    sys.exit(exit_code)

                # Generic unexpected error — show concise message, log full trace
                click.echo(
                    f"\n❌ {operation} failed: {e}\n"
                    f"💡 Use --verbose flag for detailed error information",
                    err=True,
                )
                logger.exception(f"{operation} failed with unexpected error")
                sys.exit(ExitCode.GENERAL_ERROR)

        return wrapper

    return decorator
