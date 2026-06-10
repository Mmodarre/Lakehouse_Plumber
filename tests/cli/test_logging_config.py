"""Unit tests for configure_logging() and cleanup_logging()."""

import logging
from unittest.mock import patch

import pytest

from lhp.cli.logging_config import cleanup_logging, configure_logging


@pytest.mark.unit
class TestConfigureLogging:
    def test_configure_logging_with_project_root(self, tmp_path):
        log_file = configure_logging(
            verbose=False, project_root=tmp_path, log_to_file=True
        )
        assert log_file is not None
        assert "lhp.log" in log_file
        assert (tmp_path / ".lhp" / "logs" / "lhp.log").exists()
        cleanup_logging()

    def test_configure_logging_verbose_with_project_root(self, tmp_path):
        """Verbose is decoupled from file logging: -v raises root to DEBUG but must NOT create a file handler."""
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
        result = configure_logging(verbose=False, project_root=None)
        assert result is None
        cleanup_logging()

    def test_default_no_file_handler_and_no_logs_dir(self, tmp_path):
        """A present project_root must not be enough to opt into file logging — the mkdir lives inside the file-logging branch."""
        log_file = configure_logging(verbose=False, project_root=tmp_path)
        assert log_file is None

        root = logging.getLogger()
        assert not any(
            isinstance(handler, logging.FileHandler) for handler in root.handlers
        )
        assert not (tmp_path / ".lhp" / "logs").exists()
        cleanup_logging()

    def test_log_to_file_without_verbose_quiet_console_debug_file(self, tmp_path):
        """File logging is decoupled from verbosity: log file gets DEBUG, console handler stays at WARNING."""
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
        """Log file opened in append mode — a second run must NOT truncate (were mode="w", SENTINEL_RUN_ONE would be clobbered)."""
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
    def test_cleanup_removes_existing_handlers(self):
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
