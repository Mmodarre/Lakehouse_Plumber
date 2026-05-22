"""CLI error boundary decorator for consistent error handling."""

import logging
import sys
from functools import wraps
from typing import Callable

from ..utils.error_formatter import LHPError
from ..utils.exit_codes import ExitCode
from . import console as _console_module

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
                _console_module.err_console.print(e)
                exit_code = ExitCode.from_lhp_error(e)
                logger.debug(f"{operation} failed with {e.code}: {e.title}")
                sys.exit(exit_code)
            except Exception as e:
                from ..bundle.exceptions import BundleResourceError, TemplateError

                if isinstance(e, (BundleResourceError, TemplateError)):
                    from ..bundle.error_factories import (
                        convert_bundle_error,
                    )

                    lhp_error = convert_bundle_error(e, operation)
                    _console_module.err_console.print(lhp_error)
                    exit_code = ExitCode.from_lhp_error(lhp_error)
                    logger.debug(f"{operation} failed with bundle error: {e}")
                    sys.exit(exit_code)

                lhp_error = LHPError.from_unexpected_exception(e, operation)
                _console_module.err_console.print(lhp_error)
                logger.exception(f"{operation} failed with unexpected error")
                sys.exit(ExitCode.GENERAL_ERROR)

        return wrapper

    return decorator
