"""Tests for CLI error boundary decorator."""

import pytest
from unittest.mock import patch, MagicMock

from lhp.cli.error_boundary import cli_error_boundary
from lhp.utils.error_formatter import ErrorCategory, LHPError
from lhp.utils.exit_codes import ExitCode
from lhp.bundle.exceptions import BundleResourceError, TemplateError


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

    def test_lhp_error_causes_sys_exit_with_mapped_code(self):
        """Should catch LHPError and sys.exit with the mapped exit code."""
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
        assert exc_info.value.code == ExitCode.CONFIG_ERROR

    def test_lhp_validation_error_maps_to_data_error(self):
        """Should map VALIDATION category to DATA_ERROR exit code."""
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
        assert exc_info.value.code == ExitCode.DATA_ERROR

    def test_lhp_io_error_maps_to_no_input(self):
        """Should map IO category to NO_INPUT exit code."""
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
        assert exc_info.value.code == ExitCode.NO_INPUT

    def test_bundle_resource_error_is_converted_and_exits(self):
        """Should catch BundleResourceError, convert it, and sys.exit with CONFIG_ERROR."""
        bundle_error = BundleResourceError("Bundle sync failed")

        @cli_error_boundary("bundle sync")
        def bundle_error_func():
            raise bundle_error

        with pytest.raises(SystemExit) as exc_info:
            bundle_error_func()
        # BundleResourceError converts to CONFIG category -> CONFIG_ERROR
        assert exc_info.value.code == ExitCode.CONFIG_ERROR

    def test_template_error_is_converted_and_exits(self):
        """Should catch TemplateError, convert it, and sys.exit."""
        template_error = TemplateError("Template fetch failed")

        @cli_error_boundary("template init")
        def template_error_func():
            raise template_error

        with pytest.raises(SystemExit) as exc_info:
            template_error_func()
        # TemplateError converts to CONFIG category -> CONFIG_ERROR
        assert exc_info.value.code == ExitCode.CONFIG_ERROR

    def test_wrapped_lhp_val_error_maps_to_data_error(self):
        """Should detect 'Error [LHP-VAL-' in exception string and exit with DATA_ERROR."""
        # Simulate a wrapped LHPError string (contains both markers)
        error_msg = (
            "\nError [LHP-VAL-001]: Validation failed\n"
            "======================================================================\n"
            "Some details"
        )

        @cli_error_boundary("validate")
        def wrapped_val_error_func():
            raise Exception(error_msg)

        with pytest.raises(SystemExit) as exc_info:
            wrapped_val_error_func()
        assert exc_info.value.code == ExitCode.DATA_ERROR

    def test_wrapped_lhp_io_error_maps_to_io_error(self):
        """Should detect 'LHP-IO-' in exception string and exit with IO_ERROR."""
        error_msg = (
            "\nError [LHP-IO-001]: File not found\n"
            "======================================================================\n"
            "Details here"
        )

        @cli_error_boundary("read file")
        def wrapped_io_error_func():
            raise Exception(error_msg)

        with pytest.raises(SystemExit) as exc_info:
            wrapped_io_error_func()
        assert exc_info.value.code == ExitCode.IO_ERROR

    def test_wrapped_lhp_cfg_error_maps_to_config_error(self):
        """Should detect 'LHP-CFG-' in exception string and exit with CONFIG_ERROR."""
        error_msg = (
            "\nError [LHP-CFG-009]: YAML parsing error\n"
            "======================================================================\n"
            "Bad YAML"
        )

        @cli_error_boundary("parse config")
        def wrapped_cfg_error_func():
            raise Exception(error_msg)

        with pytest.raises(SystemExit) as exc_info:
            wrapped_cfg_error_func()
        assert exc_info.value.code == ExitCode.CONFIG_ERROR

    def test_wrapped_lhp_dep_error_maps_to_data_error(self):
        """Should detect 'LHP-DEP-' in exception string and exit with DATA_ERROR."""
        error_msg = (
            "\nError [LHP-DEP-001]: Circular dependency\n"
            "======================================================================\n"
            "A -> B -> A"
        )

        @cli_error_boundary("dependency check")
        def wrapped_dep_error_func():
            raise Exception(error_msg)

        with pytest.raises(SystemExit) as exc_info:
            wrapped_dep_error_func()
        assert exc_info.value.code == ExitCode.DATA_ERROR

    def test_wrapped_lhp_unknown_prefix_maps_to_general_error(self):
        """Should fall back to GENERAL_ERROR for unknown LHP- prefix in wrapped error."""
        error_msg = (
            "\nError [LHP-GEN-001]: General error\n"
            "======================================================================\n"
            "Something happened"
        )

        @cli_error_boundary("general op")
        def wrapped_gen_error_func():
            raise Exception(error_msg)

        with pytest.raises(SystemExit) as exc_info:
            wrapped_gen_error_func()
        assert exc_info.value.code == ExitCode.GENERAL_ERROR

    def test_generic_exception_exits_with_general_error(self):
        """Should catch generic exceptions and exit with GENERAL_ERROR."""

        @cli_error_boundary("process data")
        def generic_error_func():
            raise ValueError("Something went wrong")

        with pytest.raises(SystemExit) as exc_info:
            generic_error_func()
        assert exc_info.value.code == ExitCode.GENERAL_ERROR

    def test_error_message_echoed_to_stderr_for_lhp_error(self):
        """Should echo the LHPError message to stderr."""
        lhp_error = LHPError(
            category=ErrorCategory.CONFIG,
            code_number="001",
            title="Config error",
            details="Bad config",
        )

        @cli_error_boundary("test op")
        def error_func():
            raise lhp_error

        with patch("lhp.cli.error_boundary.click.echo") as mock_echo:
            with pytest.raises(SystemExit):
                error_func()

            mock_echo.assert_called_once()
            call_args = mock_echo.call_args
            assert call_args[1]["err"] is True
            assert "LHP-CFG-001" in call_args[0][0]

    def test_generic_error_message_echoed_to_stderr(self):
        """Should echo generic error message to stderr."""

        @cli_error_boundary("my operation")
        def error_func():
            raise RuntimeError("Unexpected failure")

        with patch("lhp.cli.error_boundary.click.echo") as mock_echo:
            with pytest.raises(SystemExit):
                error_func()

            mock_echo.assert_called_once()
            call_args = mock_echo.call_args
            assert call_args[1]["err"] is True
            assert "my operation failed" in call_args[0][0]
            assert "Unexpected failure" in call_args[0][0]

    def test_decorator_preserves_function_metadata(self):
        """Should preserve the decorated function's name and docstring."""

        @cli_error_boundary("test op")
        def my_command():
            """My command docstring."""
            pass

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
        """Should dispatch BundleConfigurationError (subclass of BundleResourceError) via convert."""
        from lhp.bundle.exceptions import BundleConfigurationError

        config_error = BundleConfigurationError("Missing bundle name")

        @cli_error_boundary("validate bundle")
        def config_error_func():
            raise config_error

        with pytest.raises(SystemExit) as exc_info:
            config_error_func()
        assert exc_info.value.code == ExitCode.CONFIG_ERROR
