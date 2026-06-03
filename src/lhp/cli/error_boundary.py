"""CLI error boundary decorator for consistent error handling."""

import logging
import sys
from functools import wraps
from typing import Callable

import click

from ..errors import LHPError
from . import console as _console_module
from .error_panel import render_error_panel
from .exit_codes import ExitCode

logger = logging.getLogger(__name__)


def cli_error_boundary(operation: str) -> Callable:
    """Decorator that catches exceptions and displays user-friendly error messages with POSIX exit codes.

    Exit code mapping:
      0  SUCCESS        — no exception
      1  ERROR          — LHPError (domain-level failure)
      2  USAGE_ERROR    — click.UsageError propagates natively (Click owns this path)
      3  INTERNAL_ERROR — unexpected exception (bug)
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            try:
                return func(*args, **kwargs)
            except SystemExit:
                raise
            except click.UsageError:
                raise
            except LHPError as e:
                _console_module.err_console.print(render_error_panel(e))
                logger.debug(f"{operation} failed with {e.code}: {e.title}")
                sys.exit(ExitCode.ERROR)
            except Exception as e:
                lhp_error = LHPError.from_unexpected_exception(e, operation)
                _console_module.err_console.print(render_error_panel(lhp_error))
                logger.exception(f"{operation} failed with unexpected error")
                sys.exit(ExitCode.INTERNAL_ERROR)

        return wrapper

    return decorator
