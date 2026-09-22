"""CLI error boundary decorator for consistent error handling."""

import logging
import sys
from functools import wraps
from typing import Callable

import click

from ..errors import LHPError
from . import _telemetry_hook
from . import console as _console_module
from .error_panel import render_error_panel
from .exit_codes import ExitCode

logger = logging.getLogger(__name__)


def _finish_for(exc: BaseException) -> None:
    """Record the run for an exception the boundary re-raises unchanged."""
    exit_code, error_code, exception_class = _telemetry_hook.classify_exit(exc)
    _telemetry_hook.finish(
        exit_code=exit_code, error_code=error_code, exception_class=exception_class
    )


def cli_error_boundary(operation: str) -> Callable:
    """Decorator that catches exceptions and displays user-friendly error messages with POSIX exit codes.

    Exit code mapping:
      0  SUCCESS        — no exception
      1  ERROR          — LHPError (domain-level failure)
      2  USAGE_ERROR    — click.UsageError propagates natively (Click owns this path)
      3  INTERNAL_ERROR — unexpected exception (bug)

    Every path also records the run's one telemetry event through
    ``_telemetry_hook`` — before ``sys.exit`` or the re-raise, so the bounded
    flush completes inside the boundary — and none of them changes the exit
    code above. ``KeyboardInterrupt`` is recorded as 130 and re-raised.
    """

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args: object, **kwargs: object) -> object:
            _telemetry_hook.begin(operation)
            try:
                result = func(*args, **kwargs)
            except SystemExit as e:
                _finish_for(e)
                raise
            except click.UsageError as e:
                _finish_for(e)
                raise
            except LHPError as e:
                _console_module.err_console.print(render_error_panel(e))
                logger.debug(f"{operation} failed with {e.code}: {e.title}")
                _telemetry_hook.finish(
                    exit_code=ExitCode.ERROR,
                    error_code=e.code,
                    exception_class=type(e).__name__,
                )
                sys.exit(ExitCode.ERROR)
            except Exception as e:
                lhp_error = LHPError.from_unexpected_exception(e, operation)
                _console_module.err_console.print(render_error_panel(lhp_error))
                logger.exception(f"{operation} failed with unexpected error")
                _telemetry_hook.finish(
                    exit_code=ExitCode.INTERNAL_ERROR,
                    error_code=lhp_error.code,
                    exception_class=type(e).__name__,
                )
                sys.exit(ExitCode.INTERNAL_ERROR)
            except BaseException as e:
                _finish_for(e)
                raise
            _telemetry_hook.finish(
                exit_code=ExitCode.SUCCESS, error_code=None, exception_class=None
            )
            return result

        return wrapper

    return decorator
