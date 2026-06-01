"""Unit tests for configure_logging() and cleanup_logging()."""

import logging
from unittest.mock import patch

import pytest

from lhp.cli.logging_config import cleanup_logging, configure_logging


@pytest.mark.unit
class TestConfigureLogging:
    """Cover configure_logging branches including file handler setup."""

    def test_configure_logging_with_project_root(self, tmp_path):
        """Creates a file handler when log_to_file is on and project_root is given."""
        log_file = configure_logging(
            verbose=False, project_root=tmp_path, log_to_file=True
        )
        assert log_file is not None
        assert "lhp.log" in log_file
        assert (tmp_path / ".lhp" / "logs" / "lhp.log").exists()
        cleanup_logging()

    def test_configure_logging_verbose_with_project_root(self, tmp_path):
        """Verbose is decoupled from file logging: -v drives the root level only.

        Even with a project_root present, ``verbose=True`` (and log_to_file
        defaulting False) must NOT create a file handler or write a log file;
        it only raises the root logger to DEBUG for console output.
        """
        log_file = configure_logging(verbose=True, project_root=tmp_path)
        assert log_file is None

        root = logging.getLogger()
        assert root.level == logging.DEBUG
        assert not any(
            isinstance(handler, logging.FileHandler) for handler in root.handlers
        )
        assert not (tmp_path / ".lhp" / "logs" / "lhp.log").exists()
        assert not (tmp_path / ".lhp" / "logs").exists()
        cleanup_logging()

    def test_configure_logging_no_project_root(self):
        """Returns None when no project_root."""
        result = configure_logging(verbose=False, project_root=None)
        assert result is None
        cleanup_logging()

    def test_default_no_file_handler_and_no_logs_dir(self, tmp_path):
        """Default (log_to_file off) writes no file and never creates the logs dir.

        With ``log_to_file`` left at its default of False, a present
        project_root must not be enough to opt into file logging: no path is
        returned, no FileHandler is attached, and ``.lhp/logs`` is never
        created on disk (the mkdir lives inside the file-logging branch).
        """
        log_file = configure_logging(verbose=False, project_root=tmp_path)
        assert log_file is None

        root = logging.getLogger()
        assert not any(
            isinstance(handler, logging.FileHandler) for handler in root.handlers
        )
        assert not (tmp_path / ".lhp" / "logs").exists()
        cleanup_logging()

    def test_log_to_file_without_verbose_quiet_console_debug_file(self, tmp_path):
        """Matrix row: log_to_file=True, verbose=False -> quiet console + DEBUG file.

        File logging is decoupled from verbosity. Opting into the log file must
        raise the file handler (and root) to DEBUG for full capture while
        leaving the console handler at WARNING so terminal output stays quiet.
        """
        configure_logging(verbose=False, project_root=tmp_path, log_to_file=True)

        handlers = logging.getLogger().handlers
        # FileHandler subclasses StreamHandler, so the console check must
        # explicitly exclude FileHandlers to isolate the stderr console handler.
        console = [
            h
            for h in handlers
            if isinstance(h, logging.StreamHandler)
            and not isinstance(h, logging.FileHandler)
        ]
        files = [h for h in handlers if isinstance(h, logging.FileHandler)]

        assert len(console) == 1
        assert console[0].level == logging.WARNING
        assert len(files) == 1
        assert files[0].level == logging.DEBUG
        assert logging.getLogger().level == logging.DEBUG
        cleanup_logging()

    def test_log_to_file_appends_across_runs(self, tmp_path):
        """The log file is opened in append mode, surviving a second run.

        A second configure_logging() against the same project_root must NOT
        truncate the file, so run one's record survives alongside run two's.
        (Were the handler opened with mode="w", the second run would clobber
        SENTINEL_RUN_ONE.)
        """
        configure_logging(verbose=False, project_root=tmp_path, log_to_file=True)
        logging.getLogger().debug("SENTINEL_RUN_ONE")
        cleanup_logging()  # flushes + closes the file handler from run one

        configure_logging(verbose=False, project_root=tmp_path, log_to_file=True)
        logging.getLogger().debug("SENTINEL_RUN_TWO")
        cleanup_logging()  # flush + close before reading

        log_text = (tmp_path / ".lhp" / "logs" / "lhp.log").read_text()
        # Run one's record surviving the second run proves append, not truncate.
        assert "SENTINEL_RUN_ONE" in log_text
        assert "SENTINEL_RUN_TWO" in log_text


@pytest.mark.unit
class TestCleanupLogging:
    """Cover cleanup_logging removing handlers."""

    def test_cleanup_removes_existing_handlers(self):
        """Removes and closes all root logger handlers."""
        root = logging.getLogger()
        handler = logging.StreamHandler()
        root.addHandler(handler)
        count_before = len(root.handlers)
        assert count_before > 0

        cleanup_logging()
        assert len(root.handlers) == 0

    def test_cleanup_logging_edge_case(self):
        """cleanup_logging() clears handlers from the logger getLogger() returns.

        Patches the getLogger name in logging_config's own namespace (where
        cleanup_logging's body resolves it) so the patch is genuinely exercised
        rather than silently missed.
        """
        # A logger with one real handler attached, so the clear is observable.
        test_logger = logging.getLogger("test_empty_logger")
        test_logger.handlers.clear()
        test_logger.addHandler(logging.StreamHandler())
        assert len(test_logger.handlers) == 1

        with patch("lhp.cli.logging_config.logging.getLogger") as mock_get_logger:
            mock_get_logger.return_value = test_logger

            try:
                cleanup_logging()
            except Exception as e:
                pytest.fail(f"cleanup_logging() raised an exception: {e}")

            # The patched getLogger must have been the one cleanup_logging used.
            mock_get_logger.assert_called_once_with()

        assert len(test_logger.handlers) == 0
