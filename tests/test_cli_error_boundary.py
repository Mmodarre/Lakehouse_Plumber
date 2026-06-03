"""Tests for CLI error boundary decorator."""

import logging

import pytest

from lhp.bundle.exceptions import BundleResourceError, TemplateError
from lhp.cli.error_boundary import cli_error_boundary
from lhp.cli.exit_codes import ExitCode
from lhp.errors import ErrorCategory, LHPError


class TestCliErrorBoundary:
    """Tests for the cli_error_boundary decorator."""

    def test_successful_execution_returns_value(self):
        """Should return the function's return value when no exception is raised."""

        @cli_error_boundary("test operation")
        def success_func():
            return "success"

        assert success_func() == "success"

    def test_system_exit_passes_through(self):
        """Should let SystemExit propagate without catching it."""

        @cli_error_boundary("test operation")
        def exit_func():
            raise SystemExit(0)

        with pytest.raises(SystemExit) as exc_info:
            exit_func()
        assert exc_info.value.code == 0

    def test_lhp_error_causes_sys_exit_with_error_code(self):
        """Should catch LHPError and sys.exit with ExitCode.ERROR (1)."""
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config error",
            details="Bad config",
        )

        @cli_error_boundary("test operation")
        def config_error_func():
            raise lhp_error

        with pytest.raises(SystemExit) as exc_info:
            config_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_lhp_validation_error_maps_to_error(self):
        """Any LHPError category collapses to ExitCode.ERROR (1)."""
        lhp_error = LHPError(
            category=ErrorCategory.VALIDATION,
            code_number="001",
            title="Validation error",
            details="Invalid data",
        )

        @cli_error_boundary("validation")
        def validation_error_func():
            raise lhp_error

        with pytest.raises(SystemExit) as exc_info:
            validation_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_lhp_io_error_maps_to_error(self):
        """Any LHPError category collapses to ExitCode.ERROR (1)."""
        lhp_error = LHPError(
            category=ErrorCategory.IO,
            code_number="001",
            title="IO error",
            details="File not found",
        )

        @cli_error_boundary("file read")
        def io_error_func():
            raise lhp_error

        with pytest.raises(SystemExit) as exc_info:
            io_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_bundle_resource_error_exits_with_error(self):
        """BundleResourceError is an LHPError subclass -> caught and exits ExitCode.ERROR."""
        bundle_error = BundleResourceError("Bundle sync failed")

        @cli_error_boundary("bundle sync")
        def bundle_error_func():
            raise bundle_error

        with pytest.raises(SystemExit) as exc_info:
            bundle_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_template_error_exits_with_error(self):
        """TemplateError is an LHPError subclass -> caught and exits ExitCode.ERROR."""
        template_error = TemplateError("Template fetch failed")

        @cli_error_boundary("template init")
        def template_error_func():
            raise template_error

        with pytest.raises(SystemExit) as exc_info:
            template_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_generic_exception_exits_with_internal_error(self):
        """Should catch generic exceptions and exit with ExitCode.INTERNAL_ERROR (3)."""

        @cli_error_boundary("process data")
        def generic_error_func():
            raise ValueError("Something went wrong")

        with pytest.raises(SystemExit) as exc_info:
            generic_error_func()
        assert exc_info.value.code == ExitCode.INTERNAL_ERROR

    def test_error_message_echoed_to_stderr_for_lhp_error(self, capsys):
        """Should print the LHPError Rich Panel to stderr."""
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config error",
            details="Bad config",
        )

        @cli_error_boundary("test op")
        def error_func():
            raise lhp_error

        with pytest.raises(SystemExit):
            error_func()

        captured = capsys.readouterr()
        assert "LHP-CFG-001" in captured.err
        assert captured.out == ""

    def test_generic_error_message_echoed_to_stderr(self, capsys):
        """Should print the generic-error message to stderr."""

        @cli_error_boundary("my operation")
        def error_func():
            raise RuntimeError("Unexpected failure")

        with pytest.raises(SystemExit):
            error_func()

        captured = capsys.readouterr()
        assert "my operation failed" in captured.err
        assert "Unexpected failure" in captured.err
        assert captured.out == ""

    def test_decorator_preserves_function_metadata(self):
        """Should preserve the decorated function's name and docstring."""

        @cli_error_boundary("test op")
        def my_command():
            """My command docstring."""

        assert my_command.__name__ == "my_command"
        assert my_command.__doc__ == "My command docstring."

    def test_decorator_passes_args_and_kwargs(self):
        """Should forward args and kwargs to the decorated function."""

        @cli_error_boundary("test op")
        def func_with_args(a, b, key=None):
            return (a, b, key)

        result = func_with_args(1, 2, key="value")
        assert result == (1, 2, "value")

    def test_bundle_resource_error_subclass_dispatched_correctly(self):
        """BundleConfigurationError (an LHPError subclass) is caught and exits ExitCode.ERROR."""
        from lhp.bundle.exceptions import BundleConfigurationError

        config_error = BundleConfigurationError("Missing bundle name")

        @cli_error_boundary("validate bundle")
        def config_error_func():
            raise config_error

        with pytest.raises(SystemExit) as exc_info:
            config_error_func()
        assert exc_info.value.code == ExitCode.ERROR

    def test_generic_exception_renders_lhp_gen_902_panel(self, capsys):
        """Generic fallback should render a Rich panel with LHP-GEN-902."""

        @cli_error_boundary("my operation")
        def boom_func():
            raise RuntimeError("boom")

        with pytest.raises(SystemExit) as exc_info:
            boom_func()
        assert exc_info.value.code == ExitCode.INTERNAL_ERROR

        captured = capsys.readouterr()
        assert "LHP-GEN-902" in captured.err
        assert "unexpected error" in captured.err.lower()
        assert "Traceback (most recent call last)" not in captured.err
        assert "Use --verbose flag for detailed" not in captured.err
        assert captured.out == ""

    def test_generic_exception_traceback_is_logged_not_stderr(self, capsys, caplog):
        """Generic fallback must log the traceback but never print it to stderr."""

        @cli_error_boundary("my operation")
        def boom_func():
            raise RuntimeError("boom")

        with caplog.at_level(logging.ERROR, logger="lhp.cli.error_boundary"):
            with pytest.raises(SystemExit):
                boom_func()

        captured = capsys.readouterr()
        assert "Traceback (most recent call last)" not in captured.err

        traceback_records = [
            r
            for r in caplog.records
            if r.name == "lhp.cli.error_boundary" and r.exc_info is not None
        ]
        assert traceback_records, (
            "Expected logger.exception to attach exc_info on the error boundary log "
            "so the traceback lands in the log file."
        )
        formatted = "\n".join(r.getMessage() for r in traceback_records)
        formatted_with_tb = "\n".join(
            logging.Formatter().format(r) for r in traceback_records
        )
        assert "my operation failed with unexpected error" in formatted
        assert "Traceback (most recent call last)" in formatted_with_tb
        assert "RuntimeError: boom" in formatted_with_tb
